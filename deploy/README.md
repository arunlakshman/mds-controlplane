# Deploy the control plane to a local kind cluster

Status: these steps have not been run end to end yet; `deploy/docker/mdscontrolplane.Dockerfile`
has not been built. `dst` is a public module pinned in `go.mod`, so the image build needs no
secret for it. Treat the commands as the plan, not as a tested recipe.

Needs docker, kind and helm. The Dockerfile makes the generated files itself (they are
not in git).

    docker build -f deploy/docker/mdscontrolplane.Dockerfile -t mdscontrolplane:dev .
    kind create cluster --name mds
    kind load docker-image mdscontrolplane:dev --name mds
    helm upgrade --install cp deploy/helm/mdscontrolplane --kube-context kind-mds --wait
    kubectl --context kind-mds get pods

The pod is Ready when the gRPC health check passes (the chart uses a gRPC probe).
Metrics: `kubectl port-forward svc/cp-mdscontrolplane 9101` then open `localhost:9101/metrics`.

The default store is `mem`, so keep `replicaCount: 1`. Change `controlPlane.store`
and `controlPlane.extraArgs` in `values.yaml` for dynamolog or postgres.

Clean up: `kind delete cluster --name mds`.
