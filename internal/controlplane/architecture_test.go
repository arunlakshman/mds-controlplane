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
		cp + "contract":      {rt},
		cp + "metadb/engine": {cp + "contract", rt},
		cp + "domain/broker": {cp + "contract", rt},
		cp + "domain/topic":  {cp + "contract", cp + "metadb/engine", rt},
		cp + "domain/group":  {cp + "contract", cp + "metadb/engine", rt},
		// The group protocol machine. Its call types are written in the package
		// until T6.6 generates them there, so it does not import groups.
		cp + "domain/rebalance": {cp + "contract", rt},
		cp + "groups":           {cp + "contract", rt, "internal/system/realclock"},
		cp + "replica":          {cp + "contract", cp + "metadb/engine", cp + "metadb", rt},
		cp + "executor":         {rt},
	}
	for dir, allowed := range layers {
		onlyImports(t, pkgs, dir, allowed, true)
	}
	// metadb owns the log entry format, which is protobuf (the log and shared
	// messages). Its only third-party packages are two of the protobuf runtime,
	// audited in docs/dep-audits/google.golang.org/protobuf@<version>.md.
	metadb := cp + "metadb"
	protobuf := []string{"google.golang.org/protobuf/proto", "google.golang.org/protobuf/reflect/protoreflect"}
	onlyImports(t, pkgs, metadb, []string{cp + "api/controlplane/log/v1", cp + "api/controlplane/shared/v1", cp + "contract",
		cp + "domain/group", cp + "domain/topic", cp + "metadb/engine", cp + "metadb/engine/memory", rt}, false)
	for _, imp := range pkgs[metadb].imports {
		if local(imp) == "" && strings.Contains(strings.Split(imp, "/")[0], ".") && !slices.Contains(protobuf, imp) {
			t.Errorf("%s imports %s: the only third-party packages it may use are %v", metadb, imp, protobuf)
		}
	}
	// The log does not know the group protocol machine.
	for _, imp := range pkgs[metadb].imports {
		if local(imp) == cp+"domain/rebalance" {
			t.Errorf("%s imports %s: not allowed", metadb, imp)
		}
	}
	// The node hosts the rebalance machine, not the old coordinator (tests
	// included). It still reaches groups through the generated converters of
	// wire, until T6.6 generates the call types into domain/rebalance.
	server := cp + "server"
	for _, imp := range append(slices.Clone(pkgs[server].imports), pkgs[server].tests...) {
		if local(imp) == cp+"groups" {
			t.Errorf("%s imports %s: the node hosts domain/rebalance", server, local(imp))
		}
	}
	// live is the production runner: it knows no control-plane type.
	onlyImports(t, pkgs, "internal/system/live", nil, true)
	// The log adapters import the port and little else of the module.
	for _, a := range []string{"dynamolog", "sqllog"} {
		onlyImports(t, pkgs, cp+"executor/"+a, []string{cp + "executor", rt, "internal/system/realclock"}, false)
	}
	// The log entries and the shared messages do not pull in grpc: only the service package does.
	// Every api package imports the options of protoc-gen-mdswire, which import no package of the module.
	onlyImports(t, pkgs, cp+"api/controlplane/log/v1", []string{cp + "api/controlplane/shared/v1", cp + "api/controlplane/options/v1"}, false)
	onlyImports(t, pkgs, cp+"api/controlplane/options/v1", nil, false)
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
	for _, dir := range []string{"metadb/engine", "domain/broker", "domain/group", "domain/rebalance", "domain/topic", "groups", "metadb", "replica", "server"} {
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
