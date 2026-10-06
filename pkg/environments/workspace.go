package environments

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/codefly-dev/core/resources"
	"github.com/codefly-dev/core/wool"
	"gopkg.in/yaml.v3"
)

// FromRuntime admits the deployment declarations preserved by the Core loader.
// Decode a fresh value so concurrent invocations cannot share mutable maps.
func FromRuntime(runtime *resources.Environment) (*Environment, error) {
	if runtime == nil {
		return nil, fmt.Errorf("empty environment entry")
	}
	data, err := yaml.Marshal(runtime)
	if err != nil {
		return nil, err
	}
	var env Environment
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&env); err != nil {
		return nil, fmt.Errorf("environment %q: %w", runtime.Name, err)
	}
	if err := env.Validate(); err != nil {
		return nil, fmt.Errorf("environment %q: %w", runtime.Name, err)
	}
	return &env, nil
}

// Resource preserves the CLI document when a workspace is passed to Core.
// Use Runtime when invoking agents or loading runtime configuration.
func (env *Environment) Resource() (*resources.Environment, error) {
	data, err := yaml.Marshal(env)
	if err != nil {
		return nil, err
	}
	var resource resources.Environment
	if err := yaml.Unmarshal(data, &resource); err != nil {
		return nil, err
	}
	return &resource, nil
}

func FromWorkspace(workspace *resources.Workspace) ([]*Environment, error) {
	environments := make([]*Environment, 0, len(workspace.Environments))
	for _, resource := range workspace.Environments {
		env, err := FromRuntime(resource)
		if err != nil {
			return nil, err
		}
		environments = append(environments, env)
	}
	return environments, nil
}

func Select(workspace *resources.Workspace, name string) (*Environment, error) {
	if name == "local" {
		declared := false
		for _, env := range workspace.Environments {
			if env != nil && env.Name == name {
				declared = true
				break
			}
		}
		if !declared {
			return LocalEnvironment(), nil
		}
	}
	resource := workspace.FindEnvironment(name)
	if resource == nil {
		return nil, fmt.Errorf("workspace %q does not declare environment %q in %s", workspace.Name, name, resources.WorkspaceConfigurationName)
	}
	return FromRuntime(resource)
}

// WorkspaceGitops reads the CLI's workspace-wide delivery defaults.
func WorkspaceGitops(workspace *resources.Workspace) (*EnvironmentGitops, error) {
	node, exists := workspace.Extensions["gitops"]
	if !exists {
		return nil, nil
	}
	data, err := yaml.Marshal(&node)
	if err != nil {
		return nil, err
	}
	var gitops EnvironmentGitops
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&gitops); err != nil {
		return nil, fmt.Errorf("workspace gitops: %w", err)
	}
	return &gitops, nil
}

func (env *Environment) Validate() error {
	if _, err := env.Proto(); err != nil {
		return err
	}
	if err := env.ServiceSecrets.Validate(); err != nil {
		return err
	}
	if err := env.ServiceEgress.Validate(); err != nil {
		return err
	}
	if err := env.ServiceConfig.Validate(); err != nil {
		return err
	}
	if err := env.ServiceIdentity.Validate(); err != nil {
		return err
	}
	if err := env.validateServiceKeyCollisions(); err != nil {
		return err
	}
	for name, managed := range env.ManagedServices {
		if err := validateManagedServiceKey(name); err != nil {
			return err
		}
		if err := managed.Identity.validate(fmt.Sprintf("managed service %q", name)); err != nil {
			return err
		}
	}
	return env.ResourceQuota.Validate()
}

func validateResourcePathComponent(kind, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%s name cannot be empty", kind)
	}
	if name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("%s name %q must be a single path component", kind, name)
	}
	return nil
}

// ValidateWorkspace cross-checks environment declarations that name services
// against the workspace's actual service graph. Service-secret overrides,
// service-config values and service-identity entries are keyed by service name;
// a key that matches no loaded service is otherwise a silent no-op at projection
// time — the service keeps the default "<service>/<key>" remote paths, receives
// none of the declared values, and falls back to the environment-wide identity
// or none at all, so a typo'd name resolves the wrong secret, drops a value the
// workload needs, or authenticates as the wrong principal, with nothing catching
// it earlier.
// Loading the graph is why this is a pass separate from postLoad, mirroring
// ValidateServiceDependencies.
func ValidateWorkspace(ctx context.Context, workspace *resources.Workspace) error {
	environments, err := FromWorkspace(workspace)
	if err != nil {
		return err
	}
	w := wool.Get(ctx).In("Workspace::ValidateEnvironments", wool.NameField(workspace.Name))
	if nsErr := validateModuleNamespaces(workspace, environments); nsErr != nil {
		return w.Wrap(nsErr)
	}
	needsGraph := false

	for _, env := range environments {
		if env != nil && (len(env.serviceScopedNames()) > 0 || len(env.ManagedServices) > 0) {
			needsGraph = true
			break
		}
	}
	if !needsGraph {
		return nil
	}
	byName, err := servicesByName(ctx, workspace)
	if err != nil {
		return w.Wrap(err)
	}
	for _, env := range environments {
		if env == nil {
			continue
		}
		for block, names := range env.serviceScopedNames() {
			for _, name := range names {
				switch len(byName[name]) {
				case 0:
					return w.Wrap(fmt.Errorf("environment %q %s references unknown service %q", env.Name, block, name))
				case 1:
				default:
					return fmt.Errorf("environment %q %s references ambiguous service %q", env.Name, block, name)
				}
			}
		}
		if err := validateManagedServiceKeys(env, byName); err != nil {
			return w.Wrap(err)
		}
	}
	return nil
}

// ValidateManagedServices holds a workspace's managed-service declarations to
// its service graph. It is the managed-services half of ValidateWorkspace,
// reachable on its own because the resolver it guards — Environment.ManagedService
// — is read on paths that never run the workspace-wide pass: a run exposing
// endpoints to a remote environment asks whether a service is replaced, and an
// ambiguous bare key would answer yes for a service that is in fact rendered
// in-cluster, leaving its endpoint with no address.
//
// It loads the graph only when an environment declares a managed service, so a
// workspace without any pays one in-memory projection and no disk read.
func ValidateManagedServices(ctx context.Context, workspace *resources.Workspace) error {
	declared, err := FromWorkspace(workspace)
	if err != nil {
		return err
	}
	w := wool.Get(ctx).In("Workspace::ValidateManagedServices", wool.NameField(workspace.Name))
	needsGraph := false
	for _, env := range declared {
		if env != nil && len(env.ManagedServices) > 0 {
			needsGraph = true
			break
		}
	}
	if !needsGraph {
		return nil
	}
	byName, err := servicesByName(ctx, workspace)
	if err != nil {
		return w.Wrap(err)
	}
	for _, env := range declared {
		if env == nil {
			continue
		}
		if err := validateManagedServiceKeys(env, byName); err != nil {
			return w.Wrap(err)
		}
	}
	return nil
}

// servicesByName indexes the workspace's services by bare name, each entry the
// distinct module-qualified uniques carrying that name, sorted. Reading the module
// references rather than loading each service is enough: this answers which
// modules declare a name, not what any service contains.
//
// Distinct is what makes the count mean "how many services could this name
// refer to". A workspace may pin the same module twice — nothing rejects that —
// and both pins resolve to one service, so counting the pins would report a name
// as ambiguous between a candidate and itself, which no qualification can fix.
func servicesByName(ctx context.Context, workspace *resources.Workspace) (map[string][]string, error) {
	services, err := workspace.LoadServiceWithModules(ctx)
	if err != nil {
		return nil, err
	}
	byName := make(map[string][]string, len(services))
	for _, svc := range services {
		byName[svc.Name] = append(byName[svc.Name], resources.ServiceUnique(svc.Module, svc.Name))
	}
	for name, uniques := range byName {
		slices.Sort(uniques)
		byName[name] = slices.Compact(uniques)
	}
	return byName, nil
}

// validateManagedServiceKeys holds every managed-services key to a single
// service of the workspace graph. A bare key matching several is the bug this
// keying exists to remove: it replaces each same-named service with one entry, so
// every one of them renders with that entry's address and secrets, and only the
// declaration can say which was meant. A module-qualified key matching nothing is
// a typo in a module or service name — inert at projection time, where the
// service it should have replaced deploys as its module declares it instead, with
// nothing reporting the entry went unused.
//
// A bare key matching nothing is left alone: an imported coordinate contract
// declares a fleet's managed services, and a workspace that composes none of them
// is entitled to carry the entry unused.
func validateManagedServiceKeys(env *Environment, byName map[string][]string) error {
	for key := range env.ManagedServices {
		if err := validateManagedServiceKey(key); err != nil {
			return fmt.Errorf("environment %q: %w", env.Name, err)
		}
		if _, service, qualified := strings.Cut(key, "/"); qualified {
			if !slices.Contains(byName[service], key) {
				return fmt.Errorf("environment %q managed service %q references unknown service", env.Name, key)
			}
			continue
		}
		if candidates := byName[key]; len(candidates) > 1 {
			return fmt.Errorf("environment %q managed service %q is ambiguous: %s each declare a service named %q, so qualify the entry with the module whose service is managed",
				env.Name, key, strings.Join(candidates, " and "), key)
		}
	}
	return nil
}

// validateModuleNamespaces checks that the namespace each module derives in
// every environment (Environment.ModuleNamespace) is one Kubernetes accepts. The
// derivation suffixes the declared namespace with the module name only when the
// workspace composes several modules, so this is where a declared namespace that
// was a valid label on its own can stop being one — over 63 characters, say. A
// namespace that is not a label sails through render and publish unvalidated and
// fails only when Argo CD applies it, far from the workspace that caused it.
func validateModuleNamespaces(workspace *resources.Workspace, environments []*Environment) error {
	if !ComposesSeveralModules(workspace) {
		return nil
	}
	for _, env := range environments {
		if env == nil || env.Namespace == "" {
			continue
		}
		for _, module := range workspace.Modules {
			if module == nil {
				continue
			}
			if err := ValidateNamespaceName("module namespace", env.ModuleNamespace(workspace, module.Name)); err != nil {
				return fmt.Errorf("environment %q: %w (the workspace composes %d modules, so module %q renders into \"<namespace>-<module>\")", env.Name, err, len(workspace.Modules), module.Name)
			}
		}
	}
	return nil
}
