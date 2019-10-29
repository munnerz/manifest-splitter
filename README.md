# manifest-splitter

This is a super simple tool that can be used to split up Kubernetes manifests
into directories named after the namespace they exist within.

This is great for users using [Anthos Config Management](https://cloud.google.com/anthos-config-management),
which requires all manifests to be sorted into namespace-named directories.

## Support level

This is a pet-project, created to serve my own needs. Whilst others may find it
useful, this is _not_ a product and I will not be able to provide timely
responses.

## Usage

To run the manifest splitter, a Kubernetes apiserver must be available in order
to determine whether a given resource is namespace or cluster scoped.

To run the manifest-splitter and split up a bunch of manifests into a single
config directory, run the following from within this repo:

```
$ go run . --kubeconfig $HOME/.kube/config --output=/path/to/output/dir /path/to/manifests/to/split/*
```

The tool **will not** recurse through the input directories to find manifests.
To recursively match all YAML files within a directory, use a glob like so:

```
/path/to/manifests/to/split/**/*.yaml
```
