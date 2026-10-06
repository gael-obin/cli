package gitops

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/codefly-dev/cli/pkg/environments"
	"gopkg.in/yaml.v3"
)

// externalSecretAPIVersion is the External Secrets Operator API the promotable
// bundle already understands: render.go whitelists external-secrets.io secretKey
// references and pkg/tenants patches an ExternalSecret's store per tenant.
const (
	externalSecretAPIVersion = "external-secrets.io/v1"
	kindExternalSecret       = "ExternalSecret"
	// secretRefreshInterval keeps the External Secrets Operator re-reading the
	// remote store on a fixed cadence so a rotated vault value propagates into the
	// in-cluster Secret. Omitting it lets the field default to "0" on some ESO
	// installs, which syncs once and never refreshes — silently defeating rotation.
	secretRefreshInterval = "1h"

	kindSecretStore        = "SecretStore"
	kindClusterSecretStore = "ClusterSecretStore"
)

// externalSecretStoreKinds are the only kinds an ExternalSecret's secretStoreRef
// accepts. An environment declaration naming a backend type ("azure-keyvault")
// instead of one of these renders a manifest that passes codefly's render checks
// but is rejected by ESO admission after promotion, so it is caught here.
var externalSecretStoreKinds = map[string]struct{}{
	kindSecretStore:        {},
	kindClusterSecretStore: {},
}

type externalSecret struct {
	APIVersion string             `yaml:"apiVersion"`
	Kind       string             `yaml:"kind"`
	Metadata   externalSecretMeta `yaml:"metadata"`
	Spec       externalSecretSpec `yaml:"spec"`
}

type externalSecretMeta struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type externalSecretSpec struct {
	RefreshInterval string                 `yaml:"refreshInterval"`
	SecretStoreRef  externalSecretStoreRef `yaml:"secretStoreRef"`
	Target          externalSecretTarget   `yaml:"target"`
	Data            []externalSecretData   `yaml:"data"`
}

type externalSecretStoreRef struct {
	Name string `yaml:"name"`
	Kind string `yaml:"kind"`
}

type externalSecretTarget struct {
	Name     string                  `yaml:"name"`
	Template *externalSecretTemplate `yaml:"template,omitempty"`
}

type externalSecretTemplate struct {
	EngineVersion string            `yaml:"engineVersion"`
	MergePolicy   string            `yaml:"mergePolicy"`
	Data          map[string]string `yaml:"data"`
}

type externalSecretData struct {
	SecretKey string                   `yaml:"secretKey"`
	RemoteRef externalSecretRemote     `yaml:"remoteRef"`
	SourceRef *externalSecretSourceRef `yaml:"sourceRef,omitempty"`
}

type externalSecretSourceRef struct {
	StoreRef externalSecretStoreRef `yaml:"storeRef"`
}

type externalSecretRemote struct {
	Key      string `yaml:"key"`
	Property string `yaml:"property,omitempty"`
}

// managedSecretProjection renders the ExternalSecret that materializes a managed
// service's secret-<service> from a remote secret store. It is the missing joint
// of the KV→secretKeyRef chain: an environment declares where a managed service's
// secrets live (EnvironmentManagedSecretReference), the operator writes the real
// values into that store once, and this projection copies the declared remote
// keys into the in-cluster Secret the promotable manifests already reference — no
// secret value ever entering git, state, or manifests.
//
// Every reference of one managed service must resolve through the same store: an
// ExternalSecret owns a single target Secret, so mixing stores would silently
// drop all but one store's keys.
func managedSecretProjection(service, namespace string, refs []environments.EnvironmentManagedSecretReference) (*externalSecret, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	store := refs[0].SecretStore
	data := make([]externalSecretData, 0, len(refs))
	for _, ref := range refs {
		if ref.Name == "" || ref.RemoteKey == "" {
			return nil, fmt.Errorf("service %q secret reference requires both name and remote-key", service)
		}
		if ref.SecretStore != store {
			return nil, fmt.Errorf("service %q secret references resolve through more than one store", service)
		}
		data = append(data, externalSecretData{
			SecretKey: ref.Name,
			RemoteRef: externalSecretRemote{Key: ref.RemoteKey, Property: ref.Property},
		})
	}
	return externalSecretProjection(service, namespace, store, data)
}

// serviceSecretProjection renders the ExternalSecret that materializes a regular
// (app) service's secret-<service> from the environment's declared service secret
// store — the app-service half of the same KV to secretKeyRef chain. Unlike a
// managed service, whose remote keys are enumerated in the environment, an app
// service's keys are exactly the ones its promotable manifests already reference
// via non-optional secretKeyRefs, discovered from the rendered tree. Each key
// resolves through EnvironmentServiceSecrets.RemoteRef: an explicit remote-key,
// the service's or the environment's defaults template, else the store path
// "<service>/<key>". The ExternalSecret binds to the scope's namespace — the
// module's — where the Secret it targets is referenced.
func serviceSecretProjection(scope unitScope, service string, secrets *environments.EnvironmentServiceSecrets, keys []string) (*externalSecret, error) {
	if secrets == nil || len(keys) == 0 {
		return nil, nil
	}
	namespace := scope.Namespace
	mapping := secrets.Services[service]
	// A service may resolve from a different store than the environment default,
	// mirroring EnvironmentManagedSecretReference's per-reference store; without
	// this the per-service secret-store declaration would load, validate, and then
	// be silently projected against the environment-wide store.
	store := secrets.SecretStore
	if mapping.SecretStore != nil {
		store = *mapping.SecretStore
	}
	remotes := make(map[string]environments.EnvironmentSecretRemoteRef, len(keys))
	read := func(key string, remote environments.EnvironmentSecretRemoteRef) error {
		if prior, seen := remotes[key]; seen && !reflect.DeepEqual(prior, remote) {
			return fmt.Errorf("service %q reads secret key %s from two remote locations", service, key)
		}
		remotes[key] = remote
		return nil
	}
	assembled := map[string]string{}
	// Primitives resolve first, and at their producer. A key this consumer also
	// references directly is still read where its producer's own ExternalSecret
	// reads it — the one place it is seeded — rather than being claimed twice
	// at two scopes and failing the render on a conflict it cannot act on.
	for _, key := range keys {
		delivered, templated := scope.Templates[key]
		if !templated {
			continue
		}
		// A producer-declared assembly: read the producer's primitives from the
		// producer's own remote keys, never the assembled value from the store.
		expression, primitives, err := externalSecretTemplateExpression(delivered)
		if err != nil {
			return nil, fmt.Errorf("service %q secret key %s: %w", service, key, err)
		}
		for _, primitive := range primitives {
			remote, primitiveStore, err := producerPrimitiveRemote(scope, delivered, primitive, secrets)
			if err != nil {
				return nil, fmt.Errorf("service %q secret key %s: %w", service, key, err)
			}
			// An ExternalSecret reads through one store. A producer whose keys
			// live in another one cannot be assembled here at all; reading them
			// from this service's store would address an entry nobody wrote.
			if primitiveStore != store {
				return nil, fmt.Errorf(
					"service %q secret key %s is assembled from %s, which producer %s reads from store %s/%s, not this service's %s/%s",
					service, key, primitive, delivered.producer,
					primitiveStore.Kind, primitiveStore.Name, store.Kind, store.Name)
			}
			if err := read(primitive, remote); err != nil {
				return nil, err
			}
		}
		assembled[key] = expression
	}
	for _, key := range keys {
		if _, templated := scope.Templates[key]; templated {
			continue
		}
		if _, claimed := remotes[key]; claimed {
			continue
		}
		if err := read(key, secrets.RemoteRef(scope.secretScope(service), key)); err != nil {
			return nil, err
		}
	}
	projected := make([]string, 0, len(remotes))
	for key := range remotes {
		projected = append(projected, key)
	}
	sort.Strings(projected)
	data := make([]externalSecretData, 0, len(projected))
	for _, key := range projected {
		remote := remotes[key]
		entry := externalSecretData{
			SecretKey: key,
			RemoteRef: externalSecretRemote{Key: remote.Key, Property: remote.Property},
		}
		if remote.SecretStore != nil {
			entry.SourceRef = &externalSecretSourceRef{StoreRef: externalSecretStoreRef{
				Name: remote.SecretStore.Name, Kind: remote.SecretStore.Kind,
			}}
		}
		data = append(data, entry)
	}
	projection, err := externalSecretProjection(service, namespace, store, data)
	if err != nil {
		return nil, err
	}
	if mapping.RefreshInterval != "" {
		projection.Spec.RefreshInterval = mapping.RefreshInterval
	}
	if mapping.Template != nil {
		projection.Spec.Target.Template = &externalSecretTemplate{
			EngineVersion: mapping.Template.EngineVersion,
			MergePolicy:   mapping.Template.MergePolicy,
			Data:          maps.Clone(mapping.Template.Data),
		}
	}
	if len(assembled) > 0 {
		// Under mergePolicy Merge an ExternalSecret emits every key it fetches,
		// so the producer's primitives would land in this service's Secret
		// beside the value they assemble — handing everything that can read
		// secret-<service> a credential this service never referenced. Replace
		// makes the template the whole Secret: the primitives are still
		// fetched, so the expressions can read them, but only the keys the
		// service's manifests actually reference are emitted. That means every
		// one of those keys needs an entry, including the ones that previously
		// passed straight through from data.
		data := map[string]string{}
		if template := projection.Spec.Target.Template; template != nil && template.Data != nil {
			data = template.Data
		}
		for _, key := range keys {
			expression, isAssembled := assembled[key]
			_, declared := data[key]
			if isAssembled {
				if declared {
					return nil, fmt.Errorf("service %q secret key %s is assembled by its producer and also templated by the environment", service, key)
				}
				data[key] = expression
				continue
			}
			if declared {
				continue
			}
			if !templateVariable.MatchString(key) {
				return nil, fmt.Errorf(
					"service %q secret key %s cannot be named in an External Secrets template, which this service needs because it also assembles %d producer-templated key(s)",
					service, key, len(assembled))
			}
			data[key] = "{{ ." + key + " }}"
		}
		projection.Spec.Target.Template = &externalSecretTemplate{
			EngineVersion: "v2",
			MergePolicy:   "Replace",
			Data:          data,
		}
	}
	return projection, nil
}

// externalSecretProjection assembles the ExternalSecret shared by the managed- and
// app-service projections: same target (secret-<service>), same rotation cadence,
// same store-kind guard. An environment declaration naming a backend type
// ("azure-keyvault") instead of SecretStore/ClusterSecretStore renders a manifest
// that passes codefly's checks but is rejected by ESO admission, so it fails here.
func externalSecretProjection(service, namespace string, store environments.EnvironmentSecretStoreReference, data []externalSecretData) (*externalSecret, error) {
	if namespace == "" {
		return nil, fmt.Errorf("service %q declares secret references but its environment has no namespace", service)
	}
	if store.Name == "" || store.Kind == "" {
		return nil, fmt.Errorf("service %q secret store requires both name and kind", service)
	}
	if _, ok := externalSecretStoreKinds[store.Kind]; !ok {
		return nil, fmt.Errorf("service %q secret store kind %q must be SecretStore or ClusterSecretStore", service, store.Kind)
	}
	target := "secret-" + service
	return &externalSecret{
		APIVersion: externalSecretAPIVersion,
		Kind:       kindExternalSecret,
		Metadata:   externalSecretMeta{Name: target, Namespace: namespace},
		Spec: externalSecretSpec{
			RefreshInterval: secretRefreshInterval,
			SecretStoreRef:  externalSecretStoreRef{Name: store.Name, Kind: store.Kind},
			Target:          externalSecretTarget{Name: target},
			Data:            data,
		},
	}, nil
}

// projectServiceSecrets writes a regular service's ExternalSecret projection into
// its environment overlay and adds it to that overlay's kustomization, so the
// store binding stays environment-scoped like the overlay it lives in. The keys it
// projects are exactly the ones the service's rendered promotable manifests already
// reference from secret-<service>, so the projection covers precisely what the
// Secret must hold. It is a no-op — reporting false — when the environment declares
// no service secret store or the service references no such Secret.
func projectServiceSecrets(serviceRoot string, scope unitScope, service, environment string, secrets *environments.EnvironmentServiceSecrets) (bool, error) {
	if secrets == nil {
		return false, nil
	}
	keys, err := serviceSecretKeys(serviceRoot, service)
	if err != nil {
		return false, err
	}
	projection, err := serviceSecretProjection(scope, service, secrets, keys)
	if err != nil {
		return false, err
	}
	if projection == nil {
		return false, nil
	}
	overlay := filepath.Join(serviceRoot, "overlays", environment)
	if info, statErr := os.Stat(overlay); statErr != nil || !info.IsDir() {
		return false, fmt.Errorf("service %q references secret-%s but has no %q environment overlay to project it into", service, service, environment)
	}
	projected, err := yaml.Marshal(projection)
	if err != nil {
		return false, err
	}
	// The projection is an ExternalSecret — a reference to remote keys, never a
	// secret value — so it stays a world-readable manifest like its siblings.
	if err := os.WriteFile(filepath.Join(overlay, "external-secret.yaml"), projected, 0o644); err != nil { //nolint:gosec
		return false, err
	}
	return true, addKustomizationResource(overlay, "external-secret.yaml")
}

// serviceSecretKeys collects, sorted and de-duplicated, the keys a service's
// rendered manifests reference from its secret-<service> Secret via secretKeyRef.
func serviceSecretKeys(root, service string) ([]string, error) {
	secretName := "secret-" + service
	seen := map[string]struct{}{}
	err := walkRegularFiles(root, func(path, relative string, _ os.FileInfo) error {
		extension := strings.ToLower(filepath.Ext(relative))
		if extension != yamlExtension && extension != ymlExtension && extension != jsonExtension {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		manifests, _, err := decodeYAML(relative, data)
		if err != nil {
			return err
		}
		for _, item := range manifests {
			collectSecretKeyRefs(item.value, secretName, seen)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func collectSecretKeyRefs(value any, secretName string, seen map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		if ref, ok := typed["secretKeyRef"].(map[string]any); ok {
			if name, _ := ref["name"].(string); name == secretName {
				if key, _ := ref["key"].(string); key != "" {
					seen[key] = struct{}{}
				}
			}
		}
		for _, child := range typed {
			collectSecretKeyRefs(child, secretName, seen)
		}
	case []any:
		for _, child := range typed {
			collectSecretKeyRefs(child, secretName, seen)
		}
	}
}

// addKustomizationResource appends a resource to an existing overlay
// kustomization, preserving its other fields.
func addKustomizationResource(directory, resource string) error {
	path, err := kustomizationPath(directory)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	document := map[string]any{}
	if err = yaml.Unmarshal(data, &document); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	existing, _ := document["resources"].([]any)
	for _, entry := range existing {
		if entry == resource {
			return nil
		}
	}
	document["resources"] = append(existing, resource)
	updated, err := yaml.Marshal(document)
	if err != nil {
		return err
	}
	// A kustomization is a plain manifest index, world-readable like its siblings.
	return os.WriteFile(path, updated, 0o644) //nolint:gosec
}

func kustomizationPath(directory string) (string, error) {
	for _, name := range []string{"kustomization.yaml", "kustomization.yml", kindKustomization} {
		path := filepath.Join(directory, name)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("no kustomization in %s", directory)
}
