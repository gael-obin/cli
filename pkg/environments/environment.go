package environments

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
	"gopkg.in/yaml.v3"
)

/*
An environment is where your modules are deployed.

It exists at the  level.
*/

type EnvironmentExistsError struct {
	name string
}

func (err *EnvironmentExistsError) Error() string {
	return fmt.Sprintf("environment %s already exists", err.name)
}

// ClusterKindK3d is the cluster kind of a local k3d cluster.
const ClusterKindK3d = "k3d"

// EnvironmentCluster declares which Kubernetes cluster an environment
// targets. Lets `codefly deploy --env <name>` route kubectl to the
// right kubeconfig instead of string-matching env names in CLI source.
//
//	Kind: cluster category — "k3d", "kind", "minikube", "eks", "gke", "aks",
//	       or "external". Drives behavior decisions (image-import for k3d
//	       is a no-op on EKS, ECR auth only matters on EKS, etc.).
//	Kubeconfig: path to the kubeconfig file. Tilde expansion is supported.
//	            If empty, defaults to $KUBECONFIG or ~/.kube/config.
//	Context: optional kubectl context within the kubeconfig.
//	Architectures: the CPU architectures of the cluster's nodes, as Go/OCI
//	       names ("amd64", "arm64"). An image built for the environment is
//	       built for exactly these, see ImagePlatforms.
type EnvironmentCluster struct {
	Kind          string   `yaml:"kind,omitempty"`
	Kubeconfig    string   `yaml:"kubeconfig,omitempty"`
	Context       string   `yaml:"context,omitempty"`
	Architectures []string `yaml:"architectures,omitempty"`
}

// EnvironmentRegistry declares the container image registry an environment
// pushes to. Was previously a CLI `--org` flag with a hardcoded ECR URL.
//
//	URL: registry base — "localhost:5001", "ghcr.io/myorg",
//	     "621829027644.dkr.ecr.us-east-1.amazonaws.com/myrepo".
//	Auth: how to authenticate before push — "" (anonymous / docker-creds),
//	      "ecr" (run `aws ecr get-login-password`), "gcr" / "gar" (gcloud
//	      access token), "ghcr" (GITHUB_TOKEN env). The CLI handles auth
//	      side-effects based on this value.
type EnvironmentRegistry struct {
	URL  string `yaml:"url,omitempty"`
	Auth string `yaml:"auth,omitempty"`
}

// EnvironmentGitops identifies the reviewed repository snapshot reconciled for
// one environment.
type EnvironmentGitops struct {
	RepoURL      string `yaml:"repo-url"`
	FetchRepoURL string `yaml:"fetch-repo-url,omitempty"`
	Path         string `yaml:"path"`
	Branch       string `yaml:"branch"`
	Revision     string `yaml:"revision"`
	Checkout     string `yaml:"checkout,omitempty"`
	Inventory    string `yaml:"inventory"`
}

// EnvironmentIngressRoute binds one public service endpoint to exact hosts.
type EnvironmentIngressRoute struct {
	Name     string   `yaml:"name"`
	Service  string   `yaml:"service"`
	Endpoint string   `yaml:"endpoint"`
	Hosts    []string `yaml:"hosts"`
}

// EnvironmentSecretStoreReference selects the External Secrets store that
// resolves a managed-service handoff.
type EnvironmentSecretStoreReference struct {
	Name string `yaml:"name"`
	Kind string `yaml:"kind"`
}

// EnvironmentManagedSecretReference maps a remote managed secret into a
// namespaced Kubernetes Secret. Property, when set, names the field inside a
// structured remote document (a Key Vault JSON secret, a Vault KV path) that
// holds the value, for stores that hold documents rather than bare scalars.
type EnvironmentManagedSecretReference struct {
	Name        string                          `yaml:"name"`
	RemoteKey   string                          `yaml:"remote-key"`
	Property    string                          `yaml:"property,omitempty"`
	SecretStore EnvironmentSecretStoreReference `yaml:"secret-store"`
}

// EnvironmentSecretRemoteRef names where one secret key lives in the external
// store: the remote key (an Azure Key Vault secret name, a Vault path, …) and,
// for stores that hold structured documents, the property inside it.
type EnvironmentSecretRemoteRef struct {
	Key         string                           `yaml:"key"`
	Property    string                           `yaml:"property,omitempty"`
	SecretStore *EnvironmentSecretStoreReference `yaml:"secret-store,omitempty"`
}

// UnmarshalYAML accepts either a scalar remote key ("lodestar-accounts", which
// means {key: "lodestar-accounts"}) or an explicit {key, property, secret-store} mapping, so a
// store that holds bare scalars stays terse while a store of JSON documents can
// name the property.
func (ref *EnvironmentSecretRemoteRef) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&ref.Key)
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i < len(node.Content); i += 2 {
			if key := node.Content[i].Value; key != "key" && key != "property" && key != "secret-store" {
				return fmt.Errorf("unknown secret reference field %q", key)
			}
		}
	}
	type plain EnvironmentSecretRemoteRef
	return node.Decode((*plain)(ref))
}

// MarshalYAML emits the terse scalar form (the remote key alone) when neither property nor a store override
// is set, so a round-trip that re-serializes the workspace — `environment import`
// rewrites it in place — preserves a scalar remote-key declaration instead of
// expanding it to a {key: …} mapping. With a property or store override it emits the full mapping.
func (ref EnvironmentSecretRemoteRef) MarshalYAML() (any, error) {
	if ref.Property == "" && ref.SecretStore == nil {
		return ref.Key, nil
	}
	type plain EnvironmentSecretRemoteRef
	return plain(ref), nil
}

// EnvironmentManagedService describes an environment-owned replacement for a
// service that is otherwise part of the module graph.
type EnvironmentManagedService struct {
	Kind         string `yaml:"kind"`
	ExternalName string `yaml:"external-name"`
	// Port is the explicitly selected endpoint port; the CLI never infers it from Kind.
	Port             int                                 `yaml:"port,omitempty"`
	EgressCIDRs      []string                            `yaml:"egress-cidrs,omitempty"`
	SecretReferences []EnvironmentManagedSecretReference `yaml:"secret-references,omitempty"`
	// Identity is independent of any explicitly declared secret references.
	Identity *EnvironmentWorkloadIdentity `yaml:"identity,omitempty"`
}

// ManagedService returns the environment's replacement for a service, or false
// when the service deploys as its module declares it. A key naming the service's
// module wins over a bare one: an environment that manages one module's "redis"
// and leaves another's alone says so by qualifying the entry, and a bare key
// left standing beside it would otherwise capture both.
//
// A bare key that matches this service may also match a same-named service in
// another module. That is a property of the declaration and the workspace graph
// together, neither of which a single lookup can see, so it is refused by
// ValidateManagedServices instead of guessed at here. Every entry point that
// reaches this resolver runs that check first — the gitops passes through
// ValidateWorkspace, a run through NewFlow — so adding one means running it
// there too.
func (env *Environment) ManagedService(module, service string) (EnvironmentManagedService, bool) {
	if env == nil {
		return EnvironmentManagedService{}, false
	}
	if managed, declared := env.ManagedServices[resources.ServiceUnique(module, service)]; declared {
		return managed, true
	}
	managed, declared := env.ManagedServices[service]
	return managed, declared
}

// validateManagedServiceKey accepts either shape a managed-services key takes: a
// module-qualified "<module>/<service>" or a bare "<service>".
func validateManagedServiceKey(key string) error {
	module, service, qualified := strings.Cut(key, "/")
	if !qualified {
		return validateResourcePathComponent("managed service", key)
	}
	if err := validateResourcePathComponent("managed service module", module); err != nil {
		return err
	}
	return validateResourcePathComponent("managed service", service)
}

// EnvironmentWorkloadIdentity is the runtime principal a workload authenticates
// as, and the platform's own means of attaching it. Annotations land on the
// workload's ServiceAccount and Labels on its pod template, verbatim: an environment
// declares whatever its identity webhook keys off and codefly stamps it without
// interpreting the keys.
type EnvironmentWorkloadIdentity struct {
	Kind        string            `yaml:"kind,omitempty"`
	Principal   string            `yaml:"principal"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
}

// validate reports whether a declared identity names the principal a
// ServiceAccount authenticates as and keys every attachment it stamps. label
// names the offending block so an identity that would reach the cluster as a
// keyless annotation, or as a ServiceAccount bound to no principal, fails at
// load instead. A nil receiver is a valid "not declared" state.
func (identity *EnvironmentWorkloadIdentity) validate(label string) error {
	if identity == nil {
		return nil
	}
	if strings.TrimSpace(identity.Principal) == "" {
		return fmt.Errorf("%s declares an identity without a principal", label)
	}
	for key := range identity.Annotations {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s: identity annotation name cannot be empty", label)
		}
	}
	for key := range identity.Labels {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s: identity label name cannot be empty", label)
		}
	}
	return nil
}

// rejectUnknownKeys reports any key of a mapping node that is not one of
// permitted. Workspace YAML drops unknown keys, which is right for a block whose
// absence is inert but wrong for one whose partial presence changes behaviour.
func rejectUnknownKeys(node *yaml.Node, label string, permitted ...string) error {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index < len(node.Content); index += 2 {
		key := node.Content[index].Value
		if !slices.Contains(permitted, key) {
			return fmt.Errorf("unknown %s field %q", label, key)
		}
	}
	return nil
}

// UnmarshalYAML refuses an unknown key instead of dropping it, for the same
// reason the enclosing block does: a mistyped `annotations` or `labels` is
// dropped by workspace YAML, leaving a principal whose platform attachment never
// lands, so the identity webhook never fires and the workload authenticates as
// nothing.
func (identity *EnvironmentWorkloadIdentity) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownKeys(node, "workload identity", "kind", "principal", "annotations", "labels"); err != nil {
		return err
	}
	type plain EnvironmentWorkloadIdentity
	return node.Decode((*plain)(identity))
}

// clone returns a copy that shares nothing with the declaration, so a consumer
// stamping onto a resolved identity cannot write back into the environment it
// came from. The maps matter as much as the struct: a shallow copy still shares
// Annotations and Labels, and the environment-wide default is shared by every
// service that does not override it, so one such write changes what every later
// service resolves to.
func (identity *EnvironmentWorkloadIdentity) clone() *EnvironmentWorkloadIdentity {
	if identity == nil {
		return nil
	}
	copied := *identity
	copied.Annotations = maps.Clone(identity.Annotations)
	copied.Labels = maps.Clone(identity.Labels)
	return &copied
}

// EnvironmentServiceIdentity declares what an environment's regular services
// authenticate as. It is the identity home for a workload that consumes no
// environment-owned managed service: EnvironmentManagedService.Identity reaches a
// workload only through a managed service it consumes, so a service whose
// declarations are service-config and service-secrets alone would otherwise have
// nowhere to say what it authenticates as and would land on the namespace
// default, where token minting has no identity — its projected ExternalSecret
// then cannot reach the store.
//
// Default is the identity every service takes, which is the shape of an
// environment federating one principal to a namespace. Services names the ones
// that differ, and an entry there replaces the default outright: an override is
// total, so it states its own attachments rather than inheriting a subset of
// someone else's. The cost is that an override must restate the annotations and
// labels it still needs — a principal whose platform attachment is missing
// authenticates as nothing — which is why a declared override carries them.
type EnvironmentServiceIdentity struct {
	Default  *EnvironmentWorkloadIdentity           `yaml:"default,omitempty"`
	Services map[string]EnvironmentWorkloadIdentity `yaml:"services,omitempty"`
}

// UnmarshalYAML refuses an unknown key instead of dropping it. Workspace YAML is
// lenient everywhere else, and the emptiness check cannot cover this block: a
// mistyped `services` leaves a valid `default` standing, so every service
// silently resolves to the default principal. That is worse than the missing
// identity this block exists to prevent — the workload authenticates as the
// wrong principal, so the secret store denies it in-cluster rather than anything
// reporting an absent identity.
func (i *EnvironmentServiceIdentity) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownKeys(node, "service-identity", "default", "services"); err != nil {
		return err
	}
	type plain EnvironmentServiceIdentity
	return node.Decode((*plain)(i))
}

// Validate checks the structural invariants of a declared service-identity
// block. A non-nil block naming neither a default nor a service attaches nothing
// while claiming to declare an identity, which is the silent failure this block
// exists to remove. A nil receiver is a valid "not declared" state.
func (i *EnvironmentServiceIdentity) Validate() error {
	if i == nil {
		return nil
	}
	if i.Default == nil && len(i.Services) == 0 {
		return fmt.Errorf("service-identity declares neither a default nor any service")
	}
	if err := i.Default.validate("service-identity default"); err != nil {
		return err
	}
	for name, identity := range i.Services {
		if err := validateResourcePathComponent("service-identity service", name); err != nil {
			return err
		}
		if err := identity.validate(fmt.Sprintf("service-identity service %q", name)); err != nil {
			return err
		}
	}
	return nil
}

// WorkloadIdentity returns what a service authenticates as in this environment,
// or nil when nothing is declared for it. A per-service entry wins over the
// environment-wide default. The result is independent of the declaration, so
// both paths behave the same way under mutation by a caller.
//
// It resolves service-identity alone. A managed service's own identity is keyed
// by that managed service, and which services consume it is not something an
// Environment can see, so composing the two belongs to the consumer that knows
// the service graph.
func (env *Environment) WorkloadIdentity(service string) *EnvironmentWorkloadIdentity {
	if env == nil || env.ServiceIdentity == nil {
		return nil
	}
	if identity, declared := env.ServiceIdentity.Services[service]; declared {
		return identity.clone()
	}
	return env.ServiceIdentity.Default.clone()
}

// EnvironmentServiceSecrets declares the External Secrets store that resolves a
// regular (app) service's secret-service-configurations for an environment. It is
// the app-service counterpart to EnvironmentManagedService.SecretReferences: the
// promotion bundle projects each consuming service's secret-<service> from this
// store, so the Secret its promotable manifests reference via non-optional
// secretKeyRefs materializes in-cluster without any secret value entering git,
// state, or manifests.
type EnvironmentServiceSecrets struct {
	SecretStore EnvironmentSecretStoreReference `yaml:"secret-store"`
	// Defaults is the environment-wide template a key resolves through when
	// neither the service's RemoteKeys nor its own Defaults name it. One
	// declaration covers every service instead of the same template repeated
	// once per service; see SecretTemplatePlaceholders for what it may
	// substitute. Absent, an unmatched key falls back to "<service>/<key>".
	Defaults *EnvironmentSecretRemoteRef                `yaml:"defaults,omitempty"`
	Services map[string]EnvironmentServiceSecretMapping `yaml:"services,omitempty"`
	// Generate declares the secret configuration keys this environment's store
	// holds as values minted at random rather than supplied from outside — the
	// only thing `codefly deploy secrets` generates besides the federation
	// credentials it derives itself. A key no generator names, no federation
	// derivation covers and no stored secret already holds is reported as one the
	// operator must supply; it is never guessed.
	Generate []EnvironmentSecretGenerator `yaml:"generate,omitempty"`
	// MayBeEmpty names secret configuration keys whose correct value in this
	// environment is the empty string. A stored property is otherwise refused
	// when it is present and empty, because an empty credential is almost
	// always a half-finished seed rather than a decision — but "almost always"
	// is not always, and some keys have no value to hold: a Sentry DSN where
	// there is no Sentry project, a WebAuthn relying-party origin on a cell
	// with no browser-facing origin at all.
	//
	// Declared per key, never inferred, and never a pattern: an environment
	// says which of its own keys mean nothing, and every other empty value
	// still stops the plan. Matched on the secret KEY the render reads, like
	// Generate, not on the property name a store happens to file it under.
	MayBeEmpty []string `yaml:"may-be-empty,omitempty"`
}

// Secret generator scopes: which configuration a generator's keys belong to.
const (
	SecretGeneratorScopeWorkspace = "workspace"
	SecretGeneratorScopeService   = "service"
)

// Secret generator formats: how a generated value is encoded.
const (
	SecretGeneratorFormatHex        = "hex"
	SecretGeneratorFormatBase64     = "base64"
	SecretGeneratorFormatIdentifier = "identifier"
)

// EnvironmentSecretGenerator names keys of one configuration group that the
// environment's secret store holds as random values. Scope "workspace" selects a
// workspace configuration group; scope "service" a service configuration of that
// name, of every service or of the Services (module/service uniques) listed.
//
// Format is the value's encoding: hex (default) or base64 of Bytes random bytes
// (default 32), or identifier — a lowercase letter followed by hex, for a value
// that must also be a SQL or DNS identifier (a database owner name), of Bytes
// random bytes (default 12).
type EnvironmentSecretGenerator struct {
	Scope         string   `yaml:"scope"`
	Configuration string   `yaml:"configuration"`
	Services      []string `yaml:"services,omitempty"`
	Keys          []string `yaml:"keys"`
	Format        string   `yaml:"format,omitempty"`
	Bytes         int      `yaml:"bytes,omitempty"`
}

// UnmarshalYAML refuses an unknown key: a mistyped `keys` would otherwise leave a
// generator that generates nothing, and the keys it meant are then reported as
// ones to supply by hand.
func (generator *EnvironmentSecretGenerator) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownKeys(node, "service-secrets generate", "scope", "configuration", "services", "keys", "format", "bytes"); err != nil {
		return err
	}
	type plain EnvironmentSecretGenerator
	return node.Decode((*plain)(generator))
}

func (generator *EnvironmentSecretGenerator) validate(index int) error {
	label := fmt.Sprintf("service-secrets generate[%d]", index)
	switch generator.Scope {
	case SecretGeneratorScopeWorkspace:
		if len(generator.Services) > 0 {
			return fmt.Errorf("%s: a workspace-scoped generator names no services", label)
		}
	case SecretGeneratorScopeService:
		for _, unique := range generator.Services {
			module, service, ok := strings.Cut(unique, "/")
			if !ok || validateResourcePathComponent("module", module) != nil || validateResourcePathComponent("service", service) != nil {
				return fmt.Errorf("%s: service %q must be <module>/<service>", label, unique)
			}
		}
	default:
		return fmt.Errorf("%s: scope must be %q or %q, got %q", label, SecretGeneratorScopeWorkspace, SecretGeneratorScopeService, generator.Scope)
	}
	if strings.TrimSpace(generator.Configuration) == "" {
		return fmt.Errorf("%s: configuration cannot be empty", label)
	}
	if len(generator.Keys) == 0 {
		return fmt.Errorf("%s: keys cannot be empty", label)
	}
	for _, key := range generator.Keys {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s: key names cannot be empty", label)
		}
	}
	switch generator.Format {
	case "", SecretGeneratorFormatHex, SecretGeneratorFormatBase64, SecretGeneratorFormatIdentifier:
	default:
		return fmt.Errorf("%s: format must be hex, base64 or identifier, got %q", label, generator.Format)
	}
	if generator.Bytes < 0 {
		return fmt.Errorf("%s: bytes must be positive", label)
	}
	return nil
}

// StoredKeys is every stored key the generator covers — the names core gives
// the configuration values in a service's environment, which are the keys a
// rendered ExternalSecret reads. A service-scoped generator with no services
// covers the configuration of each of services.
func (generator *EnvironmentSecretGenerator) StoredKeys(services []string) []string {
	prefixes := []string{resources.WorkspaceSecretConfigurationPrefix}
	if generator.Scope == SecretGeneratorScopeService {
		selected := generator.Services
		if len(selected) == 0 {
			selected = services
		}
		prefixes = make([]string, 0, len(selected))
		for _, unique := range selected {
			prefixes = append(prefixes, resources.ServiceSecretConfigurationEnvironmentKeyPrefixFromUnique(unique))
		}
	}
	keys := make([]string, 0, len(prefixes)*len(generator.Keys))
	for _, prefix := range prefixes {
		for _, key := range generator.Keys {
			keys = append(keys, prefix+"__"+resources.NameToKey(generator.Configuration)+"__"+resources.NameToKey(key))
		}
	}
	return keys
}

// EnvironmentServiceSecretMapping overrides how one service resolves its
// secret-service-configurations. SecretStore, when set, overrides the
// environment-wide EnvironmentServiceSecrets.SecretStore for this service so that
// services in a single environment can resolve from different External Secrets
// stores — mirroring EnvironmentManagedSecretReference, which also carries a
// per-reference store.
type EnvironmentServiceSecretMapping struct {
	SecretStore *EnvironmentSecretStoreReference `yaml:"secret-store,omitempty"`
	// RemoteKeys maps a secret key the rendered manifests reference (the
	// CODEFLY__… env name) to its remote location. The short string form "key" is
	// still accepted and means {key: "key"}.
	RemoteKeys map[string]EnvironmentSecretRemoteRef `yaml:"remote-keys,omitempty"`
	// Defaults applies to every key of this service not listed in RemoteKeys: Key
	// and Property may carry any of SecretTemplatePlaceholders. Absent, the
	// environment-wide EnvironmentServiceSecrets.Defaults applies, and absent that
	// too, codefly's default "<service>/<key>".
	Defaults *EnvironmentSecretRemoteRef `yaml:"defaults,omitempty"`
	// Template is evaluated by ESO, never by the CLI. It preserves producer
	// transformations without exposing the resolved secret to the renderer.
	Template        *EnvironmentSecretTemplate `yaml:"template,omitempty"`
	RefreshInterval string                     `yaml:"refresh-interval,omitempty"`
}

type EnvironmentSecretTemplate struct {
	EngineVersion string            `yaml:"engine-version"`
	MergePolicy   string            `yaml:"merge-policy"`
	Data          map[string]string `yaml:"data"`
}

func (t *EnvironmentSecretTemplate) UnmarshalYAML(node *yaml.Node) error {
	if err := rejectUnknownKeys(node, "secret template", "engine-version", "merge-policy", "data"); err != nil {
		return err
	}
	type plain EnvironmentSecretTemplate
	return node.Decode((*plain)(t))
}

// EnvironmentServiceConfig declares resolved, non-secret configuration values
// for an environment's regular services. It is the non-secret twin of
// EnvironmentServiceSecrets and keys entries the same way — by the consuming
// service, then by the exact key the service reads — except that the value
// travels in the declaration instead of a reference into a secret store. The
// producer resolves it; codefly injects it and derives none of it, so nothing
// here names a producer's own inventory.
type EnvironmentServiceConfig struct {
	Services map[string]EnvironmentServiceConfigMapping `yaml:"services,omitempty"`
}

// EnvironmentServiceConfigMapping holds one service's resolved values keyed by
// the configuration key that consumes them.
type EnvironmentServiceConfigMapping struct {
	Values map[string]string `yaml:"values,omitempty"`
}

// EnvironmentResourceQuota sizes the ResourceQuota rendered into an
// environment's namespace. Requests and Limits map to the ResourceQuota's hard
// requests.cpu/requests.memory and limits.cpu/limits.memory; Pods caps the pod
// count. DefaultContainer, when set, also renders a LimitRange giving every
// container in the namespace default requests/limits — a ResourceQuota that
// caps a compute resource otherwise rejects any pod that leaves that resource
// unset.
type EnvironmentResourceQuota struct {
	Requests         *EnvironmentResourceList       `yaml:"requests,omitempty"`
	Limits           *EnvironmentResourceList       `yaml:"limits,omitempty"`
	Pods             string                         `yaml:"pods,omitempty"`
	DefaultContainer *EnvironmentContainerResources `yaml:"default-container,omitempty"`
}

// EnvironmentResourceList is a cpu/memory pair expressed as Kubernetes quantity
// strings ("500m", "512Mi"). Kubernetes validates the quantity syntax at
// admission; only presence is checked here.
type EnvironmentResourceList struct {
	CPU    string `yaml:"cpu,omitempty"`
	Memory string `yaml:"memory,omitempty"`
}

// EnvironmentContainerResources is the per-container default requests/limits a
// LimitRange applies to pods that omit their own.
type EnvironmentContainerResources struct {
	Requests *EnvironmentResourceList `yaml:"requests,omitempty"`
	Limits   *EnvironmentResourceList `yaml:"limits,omitempty"`
}

func (l *EnvironmentResourceList) empty() bool {
	return l == nil || (strings.TrimSpace(l.CPU) == "" && strings.TrimSpace(l.Memory) == "")
}

// Validate reports whether a declared resource-quota carries at least one cap.
// A non-nil block that sets nothing would render an empty ResourceMap that
// caps nothing yet still claims namespace ownership, so it fails at load. A nil
// receiver is a valid "not declared" state.
func (q *EnvironmentResourceQuota) Validate() error {
	if q == nil {
		return nil
	}
	if q.Requests.empty() && q.Limits.empty() && strings.TrimSpace(q.Pods) == "" {
		if q.DefaultContainer == nil ||
			(q.DefaultContainer.Requests.empty() && q.DefaultContainer.Limits.empty()) {
			return fmt.Errorf("resource-quota must set at least one of requests, limits, pods, or default-container")
		}
	}
	return nil
}

// validate reports whether the store reference resolves to a usable name/kind.
// label names the offending block in the error so a misdeclared store fails
// loudly at load instead of projecting an ExternalSecret against store "".
func (ref EnvironmentSecretStoreReference) validate(label string) error {
	if strings.TrimSpace(ref.Name) == "" {
		return fmt.Errorf("%s: name cannot be empty", label)
	}
	if strings.TrimSpace(ref.Kind) == "" {
		return fmt.Errorf("%s: kind cannot be empty", label)
	}
	if ref.Kind != "SecretStore" && ref.Kind != "ClusterSecretStore" {
		return fmt.Errorf("%s: kind must be SecretStore or ClusterSecretStore", label)
	}
	return nil
}

// Validate checks the structural invariants of a declared service-secrets block.
// A non-nil ServiceSecrets that carries an empty store name/kind or a remote-key
// path that resolves to "" would otherwise load without error and reach the
// cluster as a broken (or store-less) ExternalSecret; this makes that fail at
// workspace load instead. A nil receiver is a valid "not declared" state.
func (s *EnvironmentServiceSecrets) Validate() error {
	if s == nil {
		return nil
	}
	if err := s.SecretStore.validate("service-secrets secret-store"); err != nil {
		return err
	}
	if err := s.Defaults.validate("service-secrets"); err != nil {
		return err
	}
	for index := range s.Generate {
		if err := s.Generate[index].validate(index); err != nil {
			return err
		}
	}
	for name, mapping := range s.Services {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("service-secrets: service name cannot be empty")
		}
		if mapping.RefreshInterval != "" {
			interval, err := time.ParseDuration(mapping.RefreshInterval)
			if err != nil || interval <= 0 {
				return fmt.Errorf("service-secrets service %q: refresh-interval must be a positive duration", name)
			}
		}
		if mapping.Template != nil {
			if mapping.Template.EngineVersion != "v2" || mapping.Template.MergePolicy != "Merge" || len(mapping.Template.Data) == 0 {
				return fmt.Errorf("service-secrets service %q: template requires engine-version v2, merge-policy Merge and data", name)
			}
			for key, value := range mapping.Template.Data {
				if _, declared := mapping.RemoteKeys[key]; !declared || strings.TrimSpace(value) == "" {
					return fmt.Errorf("service-secrets service %q: template key %q requires an explicit remote-key and nonempty expression", name, key)
				}
			}
		}
		if mapping.SecretStore != nil {
			if err := mapping.SecretStore.validate(fmt.Sprintf("service-secrets service %q secret-store", name)); err != nil {
				return err
			}
		}
		for key, remote := range mapping.RemoteKeys {
			if remote.SecretStore != nil {
				if err := remote.SecretStore.validate(fmt.Sprintf("service-secrets service %q remote-key %q secret-store", name, key)); err != nil {
					return err
				}
			}
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("service-secrets service %q: remote-key name cannot be empty", name)
			}
			if strings.TrimSpace(remote.Key) == "" {
				return fmt.Errorf("service-secrets service %q: remote-key %q resolves to an empty path", name, key)
			}
		}
		if err := mapping.Defaults.validate(fmt.Sprintf("service-secrets service %q", name)); err != nil {
			return err
		}
	}
	return nil
}

// validate checks a defaults template: a non-empty key and only placeholders
// the projection substitutes. A nil receiver is a valid "not declared" state.
func (ref *EnvironmentSecretRemoteRef) validate(label string) error {
	if ref == nil {
		return nil
	}
	if ref.SecretStore != nil {
		if err := ref.SecretStore.validate(label + " secret-store"); err != nil {
			return err
		}
	}
	if strings.TrimSpace(ref.Key) == "" {
		return fmt.Errorf("%s: defaults key cannot be empty", label)
	}
	if err := validateSecretTemplate(ref.Key); err != nil {
		return fmt.Errorf("%s defaults key: %w", label, err)
	}
	if err := validateSecretTemplate(ref.Property); err != nil {
		return fmt.Errorf("%s defaults property: %w", label, err)
	}
	return nil
}

// SecretScope names the coordinates a service-secrets defaults template can
// substitute. Module and Workspace are what let two modules that each ship a
// service of the same name resolve to different remote secrets: a template of
// "{service}/{key}" alone would send both modules' "store" to the same entry.
type SecretScope struct {
	Workspace string
	Module    string
	Service   string
}

// RemoteRef locates one secret key of a service in the remote store. An explicit
// per-service RemoteKeys entry wins; else the service's own Defaults template;
// else the environment-wide Defaults template; else the "<service>/<key>" store
// path. Property rides along with a template so a store of structured documents
// can name the field inside the remote entry. A nil receiver resolves the
// fallback path, the same as an environment that declares nothing.
func (s *EnvironmentServiceSecrets) RemoteRef(scope SecretScope, key string) EnvironmentSecretRemoteRef {
	if s != nil {
		mapping := s.Services[scope.Service]
		if remote, ok := mapping.RemoteKeys[key]; ok {
			return remote
		}
		for _, defaults := range []*EnvironmentSecretRemoteRef{mapping.Defaults, s.Defaults} {
			if defaults == nil {
				continue
			}
			substitute := strings.NewReplacer(
				"{workspace}", scope.Workspace,
				"{module}", scope.Module,
				"{service}", scope.Service,
				"{key}", key,
			)
			return EnvironmentSecretRemoteRef{
				Key:         substitute.Replace(defaults.Key),
				Property:    substitute.Replace(defaults.Property),
				SecretStore: defaults.SecretStore,
			}
		}
	}
	return EnvironmentSecretRemoteRef{Key: scope.Service + "/" + key}
}

// Validate checks the structural invariants of a declared service-config block.
// Workspace YAML tolerates unknown keys, so a mistyped `values:` (or `services:`)
// leaves a block that declares a service and injects nothing into it — the
// workload then starts missing exactly the value it was handed over for. An
// empty value is the same failure one level down: a producer that failed to
// resolve a host injects "" and the service dials nothing. Both fail at load
// instead. A nil receiver is a valid "not declared" state.
func (c *EnvironmentServiceConfig) Validate() error {
	if c == nil {
		return nil
	}
	if len(c.Services) == 0 {
		return fmt.Errorf("service-config declares no services")
	}
	for name, mapping := range c.Services {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("service-config: service name cannot be empty")
		}
		if len(mapping.Values) == 0 {
			return fmt.Errorf("service-config service %q declares no values", name)
		}
		for key, value := range mapping.Values {
			if strings.TrimSpace(key) == "" {
				return fmt.Errorf("service-config service %q: value name cannot be empty", name)
			}
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("service-config service %q: value %q is empty", name, key)
			}
		}
	}
	return nil
}

// validateServiceKeyCollisions refuses a key one service declares both as a
// resolved value and as a secret reference. Each renders an entry of that name
// into the same container, so one silently overwrites the other and the workload
// starts with a plausible wrong value rather than failing. Choosing a winner
// here would only move that silence into codefly. It compares against explicit
// remote-keys: a service-secrets `defaults` template covers whichever of the
// service's own keys are declared secret, which this receiver cannot see.
func (env *Environment) validateServiceKeyCollisions() error {
	if env.ServiceConfig == nil || env.ServiceSecrets == nil {
		return nil
	}
	for name, config := range env.ServiceConfig.Services {
		secrets, declared := env.ServiceSecrets.Services[name]
		if !declared {
			continue
		}
		for key := range config.Values {
			if _, collides := secrets.RemoteKeys[key]; collides {
				return fmt.Errorf("service %q declares %q as both a service-config value and a service-secrets remote key", name, key)
			}
		}
	}
	return nil
}

// serviceScopedNames lists, per yaml block, the service names an environment
// keys declarations by. A name matching no loaded service is a silent no-op at
// projection time, so ValidateEnvironments cross-checks every such block.
func (env *Environment) serviceScopedNames() map[string][]string {
	names := make(map[string][]string, 3)
	if env.ServiceSecrets != nil {
		for name := range env.ServiceSecrets.Services {
			names["service-secrets"] = append(names["service-secrets"], name)
		}
	}
	if env.ServiceConfig != nil {
		for name := range env.ServiceConfig.Services {
			names["service-config"] = append(names["service-config"], name)
		}
	}
	if env.ServiceEgress != nil {
		for name := range env.ServiceEgress.Services {
			names["service-egress"] = append(names["service-egress"], name)
		}
	}
	if env.ServiceIdentity != nil {
		for name := range env.ServiceIdentity.Services {
			names["service-identity"] = append(names["service-identity"], name)
		}
	}
	return names
}

// secretTemplatePlaceholder matches any "{…}" token in a defaults template.
var secretTemplatePlaceholder = regexp.MustCompile(`\{[^{}]*\}`)

// SecretTemplatePlaceholders are the tokens a service-secrets defaults template
// may carry, each replaced by RemoteRef from the SecretScope of the key being
// resolved: the workspace name, the module name, the service name, and the
// secret key itself.
var SecretTemplatePlaceholders = []string{"{workspace}", "{module}", "{service}", "{key}"}

// validateSecretTemplate rejects any placeholder in a defaults template other
// than the ones the projection substitutes (SecretTemplatePlaceholders). An unrecognized
// token — a misspelled placeholder name, say — is left un-substituted, so it
// would render literally into the ExternalSecret's remoteRef and pass every
// render check (codefly's single-brace syntax is invisible to the manifest
// placeholder guard), only failing in-cluster when the store lookup misses.
// Catching it at load turns that silent runtime break into a loud
// workspace-load error.
func validateSecretTemplate(template string) error {
	for _, token := range secretTemplatePlaceholder.FindAllString(template, -1) {
		if !slices.Contains(SecretTemplatePlaceholders, token) {
			return fmt.Errorf("unknown placeholder %q (only %s are supported)", token, strings.Join(SecretTemplatePlaceholders, ", "))
		}
	}
	return nil
}

// dns1123Label is the RFC 1123 label grammar Kubernetes enforces on a namespace
// name: lowercase alphanumerics and '-', beginning and ending alphanumeric.
var dns1123Label = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidateNamespaceName rejects a name Kubernetes would refuse as a namespace: it
// must be a lowercase RFC 1123 label of at most 63 characters. Kind names what
// the name is for in the error ("solution", "module namespace").
func ValidateNamespaceName(kind, name string) error {
	if len(name) > 63 || !dns1123Label.MatchString(name) {
		return fmt.Errorf("%s %q is not a valid Kubernetes namespace: it must be a lowercase RFC 1123 label (a-z, 0-9, '-') of at most 63 characters", kind, name)
	}
	return nil
}

// ModuleNamespace is the Kubernetes namespace a module's workloads bind to in
// this environment, for the workspace that composes it.
//
// A workspace composing a single module — a flat workspace, or a modules layout
// with one reference — renders it into the declared Namespace unchanged. A
// workspace composing several modules gives each its own "<namespace>-<module>":
// modules are independent service graphs that routinely ship a service of the
// same name (a "store", say), and one namespace cannot hold two Services, two
// StatefulSets or two "cm-store" ConfigMaps of that name, nor answer
// "store.<namespace>.svc.cluster.local" for both. The suffix is derived, not
// declared, so every projection that names the namespace — manifests, render
// record, Argo destinations, quota, ExternalSecrets, cross-module addresses —
// agrees by construction.
//
// An environment that declares no Namespace returns "" so each caller keeps its
// own derivation (the remote network manager already synthesizes one per module).
func (env *Environment) ModuleNamespace(workspace *resources.Workspace, module string) string {
	if env == nil || env.Namespace == "" {
		return ""
	}
	if !ComposesSeveralModules(workspace) {
		return env.Namespace
	}
	return env.Namespace + "-" + module
}

// ComposesSeveralModules reports whether the workspace composes more than one
// module, the condition under which ModuleNamespace suffixes the environment's
// namespace. A flat workspace embeds its services directly and composes one.
func ComposesSeveralModules(workspace *resources.Workspace) bool {
	return workspace != nil && workspace.Layout != resources.LayoutKindFlat && len(workspace.Modules) > 1
}

// Environment is a configuration for an environment
type Environment struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description,omitempty"`
	NamingScope string `yaml:"naming-scope,omitempty"`
	Fixture     string `yaml:"fixture,omitempty"`

	// ConfigurationProfile selects the checked-in configuration directory
	// independently from the environment identity sent to service agents.
	// This is useful for a production execution profile running against local
	// backing services: agents still receive environment.name=production while
	// Codefly deliberately loads configurations/local. It is explicit and
	// opt-in; the default remains the environment's own name.
	ConfigurationProfile string `yaml:"configuration-profile,omitempty"`
	// ConfigurationProfiles is Core's explicit profile chain, the alternative
	// to ConfigurationProfile: each configuration location reads the first
	// profile it holds (resources.Environment.ConfigurationProfiles).
	ConfigurationProfiles []string `yaml:"configuration-profiles,omitempty"`

	// Deploy-target overrides (CLI-side; not serialized to proto).
	// Empty values fall back to legacy defaults (local k3d, ~/.kube/config,
	// the default namespace, the --org flag's hardcoded registry) so
	// existing workspace YAMLs keep working unchanged.
	Cluster   *EnvironmentCluster  `yaml:"cluster,omitempty"`
	Registry  *EnvironmentRegistry `yaml:"registry,omitempty"`
	Namespace string               `yaml:"namespace,omitempty"`
	Gitops    *EnvironmentGitops   `yaml:"gitops,omitempty"`

	Ingress []EnvironmentIngressRoute `yaml:"ingress,omitempty"`

	// ManagedServices keys a replacement by the identity of the service it
	// replaces: "<module>/<service>", or a bare "<service>" when exactly one
	// module in the workspace declares that name. Composed modules routinely ship
	// a service of the same name — a "redis", say — and a bare key covers every
	// one of them, so both would render with this entry's address and secrets;
	// ValidateManagedServices refuses an ambiguous bare key rather than replacing
	// a service nobody declared managed. Read it through ManagedService, never by
	// indexing the map with a bare name.
	ManagedServices map[string]EnvironmentManagedService `yaml:"managed-services,omitempty"`

	// DNS carries the environment's DNS contract. Its AppHostSuffix lets the network layer
	// derive an external endpoint's public host from declared config instead of
	// a local dns.codefly.yaml, keeping a promotable render value-free. CLI-side;
	// not serialized to proto.
	DNS *EnvironmentDNS `yaml:"dns,omitempty"`

	// ServiceSecrets declares where this environment's regular services resolve
	// their secret-service-configurations. Absent, no service secret projection is
	// rendered and secret-<service> stays an operator precondition. CLI-side; not
	// serialized to proto.
	ServiceSecrets *EnvironmentServiceSecrets `yaml:"service-secrets,omitempty"`

	// ServiceConfig declares resolved non-secret values this environment's
	// regular services consume. Absent, services take their configuration from
	// the workspace's own configuration flow alone. CLI-side; not serialized to
	// proto.
	ServiceConfig *EnvironmentServiceConfig `yaml:"service-config,omitempty"`

	// ServiceIdentity declares what this environment's regular services
	// authenticate as, independently of whether they also consume a managed
	// service. Absent, no identity is projected for them and their pods keep the
	// namespace default service account. CLI-side; not serialized to proto.
	ServiceIdentity *EnvironmentServiceIdentity `yaml:"service-identity,omitempty"`

	// ServiceEgress grants declared in-cluster access to regular service pods.
	ServiceEgress *EnvironmentServiceEgress `yaml:"service-egress,omitempty"`

	// ResourceQuota, when set, renders a ResourceQuota (and an optional
	// LimitRange of container defaults) into this environment's namespace so one
	// workspace sharing an environment cannot starve another. Absent, no quota is
	// rendered and the namespace stays uncapped. CLI-side; not serialized to
	// proto.
	ResourceQuota *EnvironmentResourceQuota `yaml:"resource-quota,omitempty"`

	// Secrets lists the secret backends for this environment. Reference-only
	// manifests fail when their backend is absent. Legacy plaintext *.secret.*
	// files remain local-only. CLI-side; not serialized to proto.
	Secrets []*resources.EnvironmentSecretProvider `yaml:"secrets,omitempty"`
}

// EnvironmentDNS is the environment's DNS contract.
type EnvironmentDNS struct {
	// AppHostSuffix is the public host suffix an app's external endpoints hang
	// off of in this environment (e.g. "staging.eastus2.azure.example.com"). Empty means
	// no declared suffix, so external hosts fall back to a local dns.codefly.yaml.
	AppHostSuffix string `yaml:"app-host-suffix,omitempty"`
}

// AppHost returns the public hostname a service's external endpoints are
// reachable at in this environment, or "" when no app host suffix is declared.
// The label is "<service>-<module>" (the service-module subdomain convention,
// see shared.ToDNSCase) so services sharing a name across modules do not collide
// under one host suffix; the suffix already scopes the environment.
func (env *Environment) AppHost(service *resources.ServiceIdentity) string {
	if env == nil || env.DNS == nil || env.DNS.AppHostSuffix == "" || service == nil {
		return ""
	}
	label := strings.ToLower(fmt.Sprintf("%s-%s", service.Name, service.Module))
	return fmt.Sprintf("%s.%s", label, env.DNS.AppHostSuffix)
}

// Runtime projects only the configuration context agents and services consume.
func (env *Environment) Runtime() *resources.Environment {
	if env == nil {
		return nil
	}
	return &resources.Environment{
		Name: env.Name, Description: env.Description, NamingScope: env.NamingScope,
		Fixture: env.Fixture, ConfigurationProfile: env.ConfigurationProfile,
		ConfigurationProfiles: env.ConfigurationProfiles, Secrets: env.Secrets,
	}
}

func (env *Environment) Proto() (*basev0.Environment, error) { return env.Runtime().Proto() }
func (env *Environment) ConfigurationProfileName() (string, error) {
	return env.Runtime().ConfigurationProfileName()
}
func (env *Environment) Local() bool { return env.Runtime().Local() }

// LocalEnvironment is a local environment that is always available
func LocalEnvironment() *Environment {
	return &Environment{
		Name: "local",
		Cluster: &EnvironmentCluster{
			Kind: ClusterKindK3d,
		},
	}
}

// IsK3d reports whether the environment targets a k3d cluster. Used to
// decide whether to import freshly-built images into the cluster
// (k3d-only — EKS/GKE pull from a registry instead).
func (env *Environment) IsK3d() bool {
	if env.Cluster != nil && env.Cluster.Kind != "" {
		return env.Cluster.Kind == ClusterKindK3d
	}
	// Legacy fallback: any env not explicitly cluster-typed is treated
	// as local-k3d. Preserves the old "default to k3d image import"
	// behavior in cli/pkg/deployments/manager.go.
	return env.Local()
}
