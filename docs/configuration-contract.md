# Producer Configuration Contract

Codefly defines the configuration contract. Infrastructure producers, including
infra-base, emit it. The CLI does not translate a producer's resource
inventory, database engine conventions, proxy mode, or credential naming scheme.

`environments.CoordinateContract` schema `codefly/coordinate/v1` wraps the existing
`environments.Environment` model under `environment`. JSON keys inside that object
are exactly the workspace YAML keys. The CLI owns this deployment document; it projects runtime context into Core. Core does not parse the coordinate contract.
`coordinate` is an optional opaque provenance label; the document is named for
its subject, and "cell" is not a word the model has.

There is one spelling. A document naming itself `codefly/cell/v1` or
`codefly/cell/v2` is refused: under a strict parser the alternative to an error
is a silent guess, and a permanent fallback is how a retired word outlives every
decision to retire it.

Examples accepted by the CLI are in `../pkg/environments/testdata/coordinates/managed-identity.json`
and `../pkg/environments/testdata/coordinates/password-auth.json`. They are contract fixtures,
not claims that a particular infrastructure producer already emits this format.

Producers must supply:

- The selected environment name and namespace. Import refuses a different target
  rather than reusing credentials or delivery paths resolved for another target.
- Each managed service under its explicit service key, with its endpoint and port.
  Multiple services are supported without assigning the first one to `store`.
  The key is `<module>/<service>`, or a bare `<service>` where only one module of
  the workspace declares that name. Composed modules routinely ship a service of
  the same name — a `redis`, say — and a bare key covers every one of them, so all
  of them would render with this entry's address and secrets while only one was
  meant; a bare key matching several is refused at workspace load, naming the
  candidates. A qualified key matching no service is refused too: it is a typo in
  the module or the service name, and the service it should have replaced would
  otherwise deploy as its module declares it with nothing reporting the entry went
  unused. A bare key matching nothing stays inert, so a contract may declare a
  fleet's managed services for a workspace composing only some of them.
- Explicit secret references, including the target Secret name, remote key,
  optional property and secret store. No declaration means no reference is added;
  it does not assert that the service is passwordless.
- Any workload identity principal and opaque annotation/label attachments.
  Identities and secret references can coexist. The CLI does not infer an auth mode.
  There are two homes, for two questions. A managed service's `identity` is
  reachable only through a managed service the workload consumes; what a workload
  authenticates as regardless of that goes under `service-identity`, keyed the way
  `service-config` and `service-secrets` are — an environment-wide `default` plus
  the services that differ. A per-service entry replaces the default outright: an
  override is total, and must restate the annotations and labels it still needs.
  A principal whose platform attachment is missing authenticates as nothing, so an
  override that carries only a principal is a declaration that cannot work where
  the identity webhook keys off a label. Unknown keys in either block are refused
  rather than dropped: a mistyped `services` would otherwise leave a valid
  `default` standing and silently collapse every override onto it.
- Any application secret mappings through the existing `service-secrets` model.
- Any resolved non-secret values through `service-config`, keyed by consuming
  service and then by the exact key the service reads. The producer resolves the
  value; The CLI carries it verbatim and derives none of it. A declared service with
  nothing to inject, or a value that resolved to empty, is refused at load.

Values and secret references are two blocks, not one dictionary of either-or
entries: a resolved value belongs in `service-config`, a reference in
`service-secrets`. One service declaring the same key in both is refused rather
than resolved — both render an entry of that name into one container, where one
silently overwrites the other. The refusal compares explicit `remote-keys`; a
`defaults` template covers whichever of a service's own keys are declared
secret, which the environment block alone cannot see.
- Resolved delivery repository, branch and path when declaring a GitOps target.
  The CLI neither appends the namespace nor chooses a branch.

Local configuration can declare `configuration-profile` (or the explicit chain
`configuration-profiles`, e.g. `[staging, local]`: each configuration location
reads the first profile it holds, never merging profiles — Core's
`docs/workspace-composition.md`) and `secrets` without a cluster or registry. Import is configuration admission, not deployment approval:
CLI still validates the selected operation, service graph and deployment target.
Local secret resolution and deployed secret projection remain separate consumers
of the existing configuration model; secret values do not belong in descriptors.

Unknown fields and required capabilities are rejected. A producer declaring an
identity on a managed service uses
`requires_capabilities: ["managed-service-identity"]`; one declaring a consuming
service's own identity uses `["service-identity"]`.
Proxy containers, image choices and loopback routing are not part of this contract.

## Configuration and secret injection

Injecting a workload's configuration and secrets needs four declarations and no
others: the target (`name`, `namespace`, `cluster.context`), resolved values
under `service-config`, secret references under `service-secrets`, and a workload
identity under `service-identity`.
`../pkg/environments/testdata/coordinates/config-injection.json` is that whole shape.

The fourth is why `service-identity` exists rather than being read off a managed
service. A descriptor for this flow declares no `managed-services`, so an
identity carried there would be unreachable: the consumer renders no
ServiceAccount, the pod keeps the namespace default, and the ExternalSecret that
did render cannot authenticate to the store. Nothing fails at import or at
render, and the first signal is in-cluster. `Environment.WorkloadIdentity(service)`
resolves the per-service entry or the default; composing that with a managed
service's own identity belongs to the consumer, which knows which services
consume what — an `Environment` does not.

A producer emitting for this path populates nothing else. `managed-services`,
`registry`, `ingress`, `resource-quota` and `dns` serve other flows and are
correctly absent here; a producer's own inventory — database engines, store
isolation tiers, cloud and location, operator cluster access — has no home in
this contract by design, and a non-secret value does not become one by being
routed through a secret store to find somewhere to live.

## Migration

Retired `codefly/cell/v1` is rejected, not converted. Its database-to-`store` translation,
`password_auth` inference, manufactured `secret-store`/`<namespace>/store` handoff
and implicit delivery paths are removed. The producer must emit the resolved
Codefly environment directly, and the CLI import fixtures must move with it.
No compatibility shim reads infra-base's private format. Producers and CLI must
qualify against the CLI-owned coordinate contract before release.

Workload identity projection validates a staged manifest tree and atomically
exchanges it with the destination on Linux and macOS. Cancellation before the
exchange leaves the destination unchanged; after publication the complete tree
is committed. Filesystems without atomic directory exchange fail without a
per-file publication fallback. Existing file and directory permissions survive.

## Ownership

The strict parser and deployment declarations live in `pkg/environments`. Core
owns generic runtime configuration and secrets, not this document. Kubernetes
identity attachments, ingress, quotas and cloud deployment policy belong here.
The spelling `codefly/coordinate/v1` and existing producer fields are unchanged.

`service-config.values` entries are injected under their exact environment names
into containers declaring that service through Core's `CODEFLY__SERVICE` runtime
variable, directly or through a referenced, namespace-matched ConfigMap. Container
names are agent-owned and are not service selectors. Only the selected overlay's
resource graph supplies these declarations. Explicit `service-secrets`
keys render both a container `secretKeyRef` and an ExternalSecret reference.
Neither secrets nor configuration are implicitly copied to sidecars. Rendering
refuses a declaration that cannot bind to a service container, and a literal
configuration value cannot replace a rendered secret reference.

A per-service secret mapping may specify `refresh-interval` and a `template`
with `engine-version: v2`, `merge-policy: Merge` and `data` expressions for its
explicit remote keys. These are External Secrets declarations, evaluated only by
ESO. The CLI never resolves or evaluates secret expressions. Other template
engines, replacement policies and undeclared output keys fail admission.

A producer may also declare one of its secret values as a template over its own
secret configuration values (core's `ConfigurationValue.template`). The CLI
translates it into the consumer's ExternalSecret `target.template` over the
producer's primitives, read from the producer's remote keys through whichever
surface the producer itself resolves through, and never reads the assembled
value from the store. The translation reproduces core's
`EvaluateConfigurationValueTemplate` byte for byte. A template of literals only
is refused: under a credential-named key it would be a value in the tree. So is
a template referencing a key the producer's own deployment does not read as a
secret, and a producer whose keys resolve through a different store than the
consumer. The template is emitted with `mergePolicy: Replace` and an entry for
every key the consumer references, so the primitives it reads are fetched but
never emitted into the consumer's Secret.
Rendered manifests admit template delimiters, and credential-named keys, only at
an ExternalSecret's `spec.target.template.data.<key>`, and only for a value that
carries a template action.

Validation rebuilds the selected overlay and checks the complete CLI-owned
ExternalSecret delivery specification, including its store, target, keys and
properties. Overlay patches may not redirect or replace that specification.
The target Secret and every consuming workload must share a namespace. Workload
identity is checked at pod scope against the ServiceAccount's namespace and name,
independently of container configuration bindings.

## Qualification limits

The actual infra-base fixture and its immutable source provenance are under
`pkg/environments/testdata/coordinates`. The Lodestar output selects Accounts'
writer identity, public identity keys its source reads, exact remote secret
references, the existing registration-digest expression, cluster and GitOps
target. The integration test imports this emitted JSON and verifies effective
Kustomize output without managed services. Database convention values remain
informational for consumers that have not adopted those keys. Azure runtime
database bindings are not inferred from the GCP declaration.

Core transports structured runtime data through its scoped, versioned JSON
carrier, and SDK-Go exposes raw and typed document accessors. This is separate
from `service-config.values`, whose explicitly declared environment values
remain strings. Render qualification does not establish live cloud identity,
secret-store access or database connectivity.

### Per-key secret stores

A `service-secrets.services.<service>.remote-keys.<key>` mapping may declare
`secret-store: {name: ..., kind: SecretStore | ClusterSecretStore}` beside `key`
and optional `property`. The renderer preserves this as the External Secrets
entry's `sourceRef.storeRef`; other keys continue using the service or environment
store. Import and workspace serialization preserve the override even without a
property. Defaults templates can carry the same store override.

This uses the [External Secrets per-entry source reference](https://external-secrets.io/latest/api/externalsecret/).
No secret value enters the coordinate document or rendered manifests.

### In-cluster service egress

`service-egress.services.<service>` carries Kubernetes `NetworkPolicyEgressRule`
objects. The CLI uses the upstream Kubernetes API types and strict serialization.
Each peer must combine an exact namespace-name selector with a nonempty pod
selector; ports must be explicit numerical ports. Public IP blocks, unbounded
destinations and unbounded ports are refused by this interface.

The renderer binds these rules to the consuming service's actual pod labels and
namespace in the selected overlay. It checks the resulting Kustomize output so a
patch cannot silently remove or broaden the generated grant. The infrastructure
producer must derive the destination selectors and listener port from the same
record it uses to render the destination. This declaration does not provision the
destination or prove that a CNI has enforced the resulting policy.
