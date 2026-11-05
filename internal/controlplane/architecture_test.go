package controlplane

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// Import rules for the control plane. One `go list` call gives every package and
// the packages it imports directly; the rules below read that list.

const (
	module = "github.com/manifesterio/mds-controlplane/"
	dst    = "github.com/manifesterio/dst"
	cp     = "internal/controlplane/"
)

type pkg struct {
	dir     string   // module-relative, for example internal/controlplane/metadb/engine
	files   []string // non-test .go files
	imports []string // direct imports of the non-test files
	tests   []string // direct imports of the test files
}

// load lists every package of the module with go list. It uses the import path, not ./...,
// because the test runs in a sub-folder and ./... would miss the rest of the module.
func load(t *testing.T) map[string]pkg {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "go", "list", "-f",
		`{{.ImportPath}}|{{.Dir}}|{{join .GoFiles ","}}|{{join .Imports ","}}|{{join .TestImports ","}} {{join .XTestImports ","}}`, module+"...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	pkgs := map[string]pkg{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "|")
		p := pkg{dir: strings.TrimPrefix(f[0], module)}
		for name := range strings.SplitSeq(f[2], ",") {
			if name != "" {
				p.files = append(p.files, filepath.Join(f[1], name))
			}
		}
		p.imports = strings.Split(f[3], ",")
		p.tests = strings.Fields(strings.ReplaceAll(f[4], ",", " "))
		pkgs[p.dir] = p
	}
	return pkgs
}

// local turns a package path into a module-relative one ("" if it is not in the module).
func local(path string) string {
	if rel, ok := strings.CutPrefix(path, module); ok {
		return rel
	}
	return ""
}

// onlyImports fails for every import of dir that is outside allowed. Third-party packages
// are refused too, except dst, when strict is set.
func onlyImports(t *testing.T, pkgs map[string]pkg, dir string, allowed []string, strict bool) {
	t.Helper()
	for _, imp := range pkgs[dir].imports {
		rel := local(imp)
		switch {
		case rel != "" && !slices.Contains(allowed, rel):
			t.Errorf("%s imports %s: it may import only %v", dir, rel, allowed)
		case rel == "" && strict && strings.Contains(strings.Split(imp, "/")[0], ".") && !strings.HasPrefix(imp, dst):
			t.Errorf("%s imports %s: a third-party package; put that code in an adapter", dir, imp)
		}
	}
}

func TestImportRules(t *testing.T) {
	pkgs := load(t)
	rt := "internal/runtime"
	// The domain packages depend inward only, and take no third-party package.
	layers := map[string][]string{
		cp + "contract":       {rt},
		cp + "metadb/engine":  {cp + "contract", rt},
		cp + "membership":     {cp + "contract", rt},
		cp + "module":         {cp + "metadb/engine", rt},
		cp + "topics":         {cp + "contract", cp + "metadb/engine", cp + "module", rt},
		cp + "groups/storage": {cp + "contract", cp + "metadb/engine", cp + "module", rt},
		cp + "groups":         {cp + "contract", rt, "internal/system/realclock"},
		cp + "metadb":         {cp + "contract", cp + "groups/storage", cp + "metadb/engine", cp + "metadb/engine/memory", cp + "module", cp + "topics", rt},
		cp + "replica":        {cp + "contract", cp + "metadb/engine", cp + "metadb", cp + "module", rt},
		cp + "executor":       {rt},
	}
	for dir, allowed := range layers {
		onlyImports(t, pkgs, dir, allowed, true)
	}
	// live is the production runner: it knows no control-plane type.
	onlyImports(t, pkgs, "internal/system/live", nil, true)
	// The log adapters import the port and little else of the module.
	for _, a := range []string{"dynamolog", "sqllog"} {
		onlyImports(t, pkgs, cp+"executor/"+a, []string{cp + "executor", rt, "internal/system/realclock"}, false)
	}
	// The log entries and the shared messages do not pull in grpc: only the service package does.
	onlyImports(t, pkgs, cp+"api/controlplane/log/v1", []string{cp + "api/controlplane/shared/v1"}, false)
	for _, dir := range []string{cp + "api/controlplane/log/v1", cp + "api/controlplane/shared/v1"} {
		for _, imp := range pkgs[dir].imports {
			if strings.HasPrefix(imp, "google.golang.org/grpc") {
				t.Errorf("%s imports %s: only the service package may use grpc", dir, imp)
			}
		}
	}
	// runtime never imports contract, and neither imports the wire packages (tests included).
	for _, dir := range []string{rt, cp + "contract"} {
		for _, imp := range append(slices.Clone(pkgs[dir].imports), pkgs[dir].tests...) {
			if rel := local(imp); strings.HasPrefix(rel, cp+"api/") || rel == cp+"wire" || (dir == rt && rel == cp+"contract") {
				t.Errorf("%s imports %s: not allowed", dir, rel)
			}
		}
	}
}

// All time comes from runtime.Clock: these packages never read the wall clock.
var wallClock = regexp.MustCompile(`\btime\.(Now|Since|Until|AfterFunc|After|NewTimer|NewTicker|Tick|Sleep)\(`)

func TestNoWallClockInTheDomain(t *testing.T) {
	pkgs := load(t)
	for _, dir := range []string{"metadb/engine", "membership", "module", "topics", "groups", "groups/storage", "metadb", "replica", "server"} {
		for _, file := range pkgs[cp+dir].files {
			src, err := os.ReadFile(file) //nolint:gosec // G304: the path comes from go list
			if err != nil {
				t.Fatal(err)
			}
			if m := wallClock.Find(src); m != nil {
				t.Errorf("%s calls %s: take a runtime.Clock instead", file, m)
			}
		}
	}
}
