# Strimzi 1.3.0 install files (vendored)

These are copied unmodified from `packaging/install/cluster-operator/` at tag `1.3.0` of
https://github.com/strimzi/strimzi-kafka-operator (commit 0d138c48b936d07cfb4715a27fda2948cdee0fe5).
They are licensed under Apache-2.0.

They are used for two things:
- `deploy/kind/up.sh` installs them, with the namespace rewritten at apply time;
- the e2e suite installs the `04*-Crd-*.yaml` CRDs into envtest, so that every rendered object is validated against Strimzi's own schemas.
