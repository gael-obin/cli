package gitops

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codefly-dev/cli/pkg/environments"
	"github.com/codefly-dev/core/resources"
	"gopkg.in/yaml.v3"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

func workosSecretReferences() []environments.EnvironmentManagedSecretReference {
	store := environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod", Kind: "ClusterSecretStore"}
	return []environments.EnvironmentManagedSecretReference{
		{Name: "client-id", RemoteKey: "workos/client-id", SecretStore: store},
		{Name: "api-key", RemoteKey: "workos/api-key", SecretStore: store},
	}
}

func TestManagedSecretProjectionRendersExternalSecret(t *testing.T) {
	projection, err := managedSecretProjection("workos", "payments", workosSecretReferences())
	if err != nil {
		t.Fatal(err)
	}
	if projection.APIVersion != externalSecretAPIVersion || projection.Kind != kindExternalSecret {
		t.Fatalf("projection identity = %v/%v", projection.APIVersion, projection.Kind)
	}
	if projection.Metadata.Name != "secret-workos" || projection.Metadata.Namespace != "payments" {
		t.Fatalf("projection metadata = %+v", projection.Metadata)
	}
	if projection.Spec.SecretStoreRef.Name != "azure-keyvault-prod" || projection.Spec.SecretStoreRef.Kind != "ClusterSecretStore" {
		t.Fatalf("projection store = %+v", projection.Spec.SecretStoreRef)
	}
	if projection.Spec.Target.Name != "secret-workos" {
		t.Fatalf("projection target = %+v", projection.Spec.Target)
	}
	if projection.Spec.RefreshInterval != secretRefreshInterval {
		t.Fatalf("projection refreshInterval = %q, want %q", projection.Spec.RefreshInterval, secretRefreshInterval)
	}
	if len(projection.Spec.Data) != 2 {
		t.Fatalf("projection data length = %d", len(projection.Spec.Data))
	}
	if projection.Spec.Data[0].SecretKey != "client-id" || projection.Spec.Data[0].RemoteRef.Key != "workos/client-id" {
		t.Fatalf("projection first entry = %+v", projection.Spec.Data[0])
	}
}

// A managed reference that names a property must render remoteRef.property
// alongside remoteRef.key, so a managed service backed by a store of structured
// documents resolves the field inside the remote entry.
func TestManagedSecretProjectionCarriesProperty(t *testing.T) {
	store := environments.EnvironmentSecretStoreReference{Name: "cell-secrets", Kind: "ClusterSecretStore"}
	refs := []environments.EnvironmentManagedSecretReference{
		{Name: "store-connection", RemoteKey: "lodestar-accounts", Property: "store_read_write_connection", SecretStore: store},
	}
	projection, err := managedSecretProjection("store", "lodestar", refs)
	if err != nil {
		t.Fatal(err)
	}
	if got := projection.Spec.Data[0]; got.RemoteRef.Key != "lodestar-accounts" || got.RemoteRef.Property != "store_read_write_connection" {
		t.Fatalf("managed entry = %+v", got)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "property: store_read_write_connection") {
		t.Fatalf("rendered YAML missing property:\n%s", encoded)
	}
}

func TestManagedSecretProjectionPassesPromotableValidation(t *testing.T) {
	projection, err := managedSecretProjection("workos", "payments", workosSecretReferences())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	manifests, _, err := decodeYAML("external-secret.yaml", encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 {
		t.Fatalf("projection decoded into %d manifests", len(manifests))
	}
	if err := validateManifest(manifests[0], nil, true); err != nil {
		t.Fatalf("rendered projection is not promotable: %v", err)
	}
}

func TestManagedSecretProjectionEmptyReferencesRenderNothing(t *testing.T) {
	projection, err := managedSecretProjection("workos", "payments", nil)
	if err != nil {
		t.Fatal(err)
	}
	if projection != nil {
		t.Fatalf("empty references rendered a projection: %v", projection)
	}
}

func TestManagedSecretProjectionRejectsInvalidDeclarations(t *testing.T) {
	store := environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod", Kind: "ClusterSecretStore"}
	tests := []struct {
		name      string
		namespace string
		refs      []environments.EnvironmentManagedSecretReference
	}{
		{
			name:      "missing namespace",
			namespace: "",
			refs:      []environments.EnvironmentManagedSecretReference{{Name: "api-key", RemoteKey: "workos/api-key", SecretStore: store}},
		},
		{
			name:      "missing remote key",
			namespace: "payments",
			refs:      []environments.EnvironmentManagedSecretReference{{Name: "api-key", SecretStore: store}},
		},
		{
			name:      "incomplete store",
			namespace: "payments",
			refs:      []environments.EnvironmentManagedSecretReference{{Name: "api-key", RemoteKey: "workos/api-key", SecretStore: environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod"}}},
		},
		{
			name:      "backend type mistaken for store kind",
			namespace: "payments",
			refs:      []environments.EnvironmentManagedSecretReference{{Name: "api-key", RemoteKey: "workos/api-key", SecretStore: environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod", Kind: "azure-keyvault"}}},
		},
		{
			name:      "diverging stores",
			namespace: "payments",
			refs: []environments.EnvironmentManagedSecretReference{
				{Name: "api-key", RemoteKey: "workos/api-key", SecretStore: store},
				{Name: "client-id", RemoteKey: "workos/client-id", SecretStore: environments.EnvironmentSecretStoreReference{Name: "other", Kind: "SecretStore"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := managedSecretProjection("workos", test.namespace, test.refs); err == nil {
				t.Fatalf("expected error for %s", test.name)
			}
		})
	}
}

func TestRetainManagedBundleProjectsSecretsWithoutBootstrapJobs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "workos")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	retained, err := retainManagedBundle(root, "workos", "production", "payments", workosSecretReferences())
	if err != nil {
		t.Fatal(err)
	}
	if !retained {
		t.Fatal("managed service with secret references was not retained")
	}
	baseKustomization, err := os.ReadFile(filepath.Join(root, "base", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(baseKustomization), "external-secret.yaml") {
		t.Fatalf("base kustomization does not reference the projection: %s", baseKustomization)
	}

	overlay := filepath.Join(root, "overlays", "production")
	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	rendered, err := kustomizer.Run(filesys.MakeFsOnDisk(), overlay)
	if err != nil {
		t.Fatalf("build managed overlay: %v", err)
	}
	encoded, err := rendered.AsYaml()
	if err != nil {
		t.Fatal(err)
	}
	manifests, _, err := decodeYAML("overlay.yaml", encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || manifests[0].kind != "ExternalSecret" {
		t.Fatalf("managed overlay rendered %d manifests, want one ExternalSecret", len(manifests))
	}
	if err := validateManifest(manifests[0], nil, true); err != nil {
		t.Fatalf("managed overlay projection is not promotable: %v", err)
	}
}

func TestRetainManagedBundleRemovesTreeWithoutJobsOrSecrets(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "stray.yaml"), []byte("apiVersion: v1\nkind: ConfigMap\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	retained, err := retainManagedBundle(root, "cache", "production", "payments", nil)
	if err != nil {
		t.Fatal(err)
	}
	if retained {
		t.Fatal("managed service without jobs or secrets was retained")
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("managed tree survived: %v", err)
	}
}

func azureServiceSecrets() *environments.EnvironmentServiceSecrets {
	return &environments.EnvironmentServiceSecrets{
		SecretStore: environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod", Kind: "ClusterSecretStore"},
		Services: map[string]environments.EnvironmentServiceSecretMapping{
			"accounts": {RemoteKeys: map[string]environments.EnvironmentSecretRemoteRef{"workos-client-secret": {Key: "workos/prod/client-secret"}}},
		},
	}
}

func TestServiceSecretProjectionDefaultsAndOverridesRemoteKeys(t *testing.T) {
	projection, err := serviceSecretProjection(
		unitScope{Namespace: "payments"}, "accounts", azureServiceSecrets(),
		[]string{"workos-api-key", "workos-client-secret"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Metadata.Name != "secret-accounts" || projection.Metadata.Namespace != "payments" {
		t.Fatalf("projection metadata = %+v", projection.Metadata)
	}
	if projection.Spec.SecretStoreRef.Name != "azure-keyvault-prod" || projection.Spec.SecretStoreRef.Kind != "ClusterSecretStore" {
		t.Fatalf("projection store = %+v", projection.Spec.SecretStoreRef)
	}
	if len(projection.Spec.Data) != 2 {
		t.Fatalf("projection data length = %d", len(projection.Spec.Data))
	}
	// Keys arrive sorted; the un-overridden key falls back to "<service>/<key>".
	if projection.Spec.Data[0].SecretKey != "workos-api-key" || projection.Spec.Data[0].RemoteRef.Key != "accounts/workos-api-key" {
		t.Fatalf("defaulted entry = %+v", projection.Spec.Data[0])
	}
	if projection.Spec.Data[1].SecretKey != "workos-client-secret" || projection.Spec.Data[1].RemoteRef.Key != "workos/prod/client-secret" {
		t.Fatalf("overridden entry = %+v", projection.Spec.Data[1])
	}
}

func TestServiceSecretProjectionRendersNothingWithoutStoreOrKeys(t *testing.T) {
	if projection, err := serviceSecretProjection(unitScope{Namespace: "payments"}, "accounts", nil, []string{"api-key"}); err != nil || projection != nil {
		t.Fatalf("nil store rendered %v (err %v)", projection, err)
	}
	if projection, err := serviceSecretProjection(unitScope{Namespace: "payments"}, "accounts", azureServiceSecrets(), nil); err != nil || projection != nil {
		t.Fatalf("no keys rendered %v (err %v)", projection, err)
	}
}

func TestServiceSecretProjectionHonorsPerServiceStore(t *testing.T) {
	secrets := &environments.EnvironmentServiceSecrets{
		SecretStore: environments.EnvironmentSecretStoreReference{Name: "env-default", Kind: "ClusterSecretStore"},
		Services: map[string]environments.EnvironmentServiceSecretMapping{
			"accounts": {SecretStore: &environments.EnvironmentSecretStoreReference{Name: "accounts-vault", Kind: "SecretStore"}},
		},
	}
	projection, err := serviceSecretProjection(unitScope{Namespace: "payments"}, "accounts", secrets, []string{"api-key"})
	if err != nil {
		t.Fatal(err)
	}
	if projection.Spec.SecretStoreRef.Name != "accounts-vault" || projection.Spec.SecretStoreRef.Kind != "SecretStore" {
		t.Fatalf("per-service store override not honored: %+v", projection.Spec.SecretStoreRef)
	}

	// A service without an override still resolves through the environment store.
	other, err := serviceSecretProjection(unitScope{Namespace: "payments"}, "billing", secrets, []string{"api-key"})
	if err != nil {
		t.Fatal(err)
	}
	if other.Spec.SecretStoreRef.Name != "env-default" {
		t.Fatalf("service without override should use env store: %+v", other.Spec.SecretStoreRef)
	}
}

func TestServiceSecretProjectionRejectsInvalidStore(t *testing.T) {
	backendKind := &environments.EnvironmentServiceSecrets{
		SecretStore: environments.EnvironmentSecretStoreReference{Name: "azure-keyvault-prod", Kind: "azure-keyvault"},
	}
	if _, err := serviceSecretProjection(unitScope{Namespace: "payments"}, "accounts", backendKind, []string{"api-key"}); err == nil {
		t.Fatal("expected error for backend-type store kind")
	}
	valid := azureServiceSecrets()
	if _, err := serviceSecretProjection(unitScope{Namespace: ""}, "accounts", valid, []string{"api-key"}); err == nil {
		t.Fatal("expected error for missing namespace")
	}
}

func cellServiceSecrets(mapping environments.EnvironmentServiceSecretMapping) *environments.EnvironmentServiceSecrets {
	return &environments.EnvironmentServiceSecrets{
		SecretStore: environments.EnvironmentSecretStoreReference{Name: "cell-secrets", Kind: "ClusterSecretStore"},
		Services:    map[string]environments.EnvironmentServiceSecretMapping{"accounts": mapping},
	}
}

// A mapping that names a property renders remoteRef.key AND remoteRef.property,
// which is how a store of structured documents (a Key Vault JSON secret) is
// addressed — the shape infra-base maintains by hand today.
func TestServiceSecretProjectionUsesProperty(t *testing.T) {
	const key = "CODEFLY__WORKSPACE_SECRET_CONFIGURATION__IDENTITY__IDENTITY_CLIENT_SECRET"
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{
		RemoteKeys: map[string]environments.EnvironmentSecretRemoteRef{
			key: {Key: "lodestar-identity", Property: "client_secret"},
		},
	})
	projection, err := serviceSecretProjection(unitScope{Namespace: "lodestar"}, "accounts", secrets, []string{key})
	if err != nil {
		t.Fatal(err)
	}
	if got := projection.Spec.Data[0]; got.SecretKey != key || got.RemoteRef.Key != "lodestar-identity" || got.RemoteRef.Property != "client_secret" {
		t.Fatalf("entry = %+v", got)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "key: lodestar-identity") || !strings.Contains(string(encoded), "property: client_secret") {
		t.Fatalf("rendered YAML missing key/property:\n%s", encoded)
	}
}

// A remote key with no property (the scalar declaration form) renders only key,
// leaving property absent so a store of bare scalars is addressed unchanged.
func TestServiceSecretProjectionScalarFormStillWorks(t *testing.T) {
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{
		RemoteKeys: map[string]environments.EnvironmentSecretRemoteRef{"K": {Key: "some-key"}},
	})
	projection, err := serviceSecretProjection(unitScope{Namespace: "lodestar"}, "accounts", secrets, []string{"K"})
	if err != nil {
		t.Fatal(err)
	}
	if got := projection.Spec.Data[0]; got.RemoteRef.Key != "some-key" || got.RemoteRef.Property != "" {
		t.Fatalf("entry = %+v", got)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "property:") {
		t.Fatalf("scalar form must emit no property:\n%s", encoded)
	}
}

// A defaults template applies to every key not listed in RemoteKeys, with
// "{service}" and "{key}" substituted in both key and property.
func TestServiceSecretProjectionDefaultsTemplate(t *testing.T) {
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{
		Defaults: &environments.EnvironmentSecretRemoteRef{Key: "lodestar-{service}", Property: "{key}"},
	})
	projection, err := serviceSecretProjection(unitScope{Namespace: "lodestar"}, "accounts", secrets, []string{"K"})
	if err != nil {
		t.Fatal(err)
	}
	if got := projection.Spec.Data[0]; got.RemoteRef.Key != "lodestar-accounts" || got.RemoteRef.Property != "K" {
		t.Fatalf("defaulted entry = %+v", got)
	}
}

// A rendered ExternalSecret whose remoteRef.property names a credential-shaped
// field (client_secret, gateway_token) must pass promotable validation: property
// is a store address, not a secret value, so the credential-key heuristic that
// guards manifest values must not trip on it.
func TestRenderAcceptsExternalSecretWithProperty(t *testing.T) {
	const (
		clientSecret = "CODEFLY__WORKSPACE_SECRET_CONFIGURATION__IDENTITY__IDENTITY_CLIENT_SECRET"
		gatewayToken = "CODEFLY__WORKSPACE_SECRET_CONFIGURATION__INTERNAL_AUTH__CODEFLY_GATEWAY_TOKEN"
	)
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{
		RemoteKeys: map[string]environments.EnvironmentSecretRemoteRef{
			clientSecret: {Key: "lodestar-identity", Property: "client_secret"},
			gatewayToken: {Key: "lodestar-internal-auth", Property: "gateway_token"},
		},
	})
	projection, err := serviceSecretProjection(unitScope{Namespace: "lodestar"}, "accounts", secrets, []string{clientSecret, gatewayToken})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	manifests, _, err := decodeYAML("external-secret.yaml", encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(manifests[0], nil, true); err != nil {
		t.Fatalf("ExternalSecret with credential-shaped property rejected: %v", err)
	}
}

// goldenExternalSecretData is the decoded spec.data of a testdata ExternalSecret.
type goldenExternalSecretData struct {
	Data []externalSecretData `yaml:"data"`
}

func loadGoldenExternalSecretData(t *testing.T, path string) []externalSecretData {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var golden goldenExternalSecretData
	if err := yaml.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	if len(golden.Data) == 0 {
		t.Fatalf("golden %s has no data entries", path)
	}
	return golden.Data
}

func remoteRefsBySecretKey(data []externalSecretData) map[string]externalSecretRemote {
	byKey := make(map[string]externalSecretRemote, len(data))
	for _, entry := range data {
		byKey[entry.SecretKey] = entry.RemoteRef
	}
	return byKey
}

// writeServiceTreeReferencingKeys lays down a base+overlay service tree whose
// Deployment references each of keys from secret-<service> via a non-optional
// secretKeyRef — the same shape serviceSecretKeys discovers in production.
func writeServiceTreeReferencingKeys(t *testing.T, root, environment, namespace, service string, keys []string) {
	t.Helper()
	base := filepath.Join(root, "base")
	overlay := filepath.Join(root, "overlays", environment)
	for _, dir := range []string{base, overlay} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var envVars strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&envVars, "            - name: %s\n"+
			"              valueFrom:\n"+
			"                secretKeyRef:\n"+
			"                  name: secret-%s\n"+
			"                  key: %s\n", key, service, key)
	}
	deployment := "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: " + service +
		"\n  namespace: " + namespace + "\nspec:\n  template:\n    spec:\n      containers:\n" +
		"        - name: " + service + "\n          image: registry.example.com/" + service +
		"@sha256:" + strings.Repeat("a", 64) + "\n          env:\n" + envVars.String()
	if err := os.WriteFile(filepath.Join(base, "deployment.yaml"), []byte(deployment), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "kustomization.yaml"), []byte("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(overlay, "kustomization.yaml"), []byte("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../../base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// codefly reproduces infra-base's hand-authored secret-accounts ExternalSecret
// data block end to end: a service tree references the 20 keys, the production
// projectServiceSecrets discovers them from that tree and renders the
// ExternalSecret from a lodestar-side declaration, and the written manifest's
// spec.data set-equals the infra-base golden. The keys are discovered from the
// tree, not read from the golden, so the projection — not a map echo — is under
// test.
func TestServiceSecretProjectionReproducesInfraBaseAccounts(t *testing.T) {
	golden := loadGoldenExternalSecretData(t, filepath.Join("testdata", "lodestar-accounts-externalsecret.yaml"))

	// The only hand-authored input: the lodestar-side declaration mapping each
	// rendered secret key to its remote {key, property}.
	remoteKeys := make(map[string]environments.EnvironmentSecretRemoteRef, len(golden))
	keys := make([]string, 0, len(golden))
	for _, entry := range golden {
		remoteKeys[entry.SecretKey] = environments.EnvironmentSecretRemoteRef{Key: entry.RemoteRef.Key, Property: entry.RemoteRef.Property}
		keys = append(keys, entry.SecretKey)
	}
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{RemoteKeys: remoteKeys})

	root := filepath.Join(t.TempDir(), "accounts")
	writeServiceTreeReferencingKeys(t, root, "prod", "lodestar", "accounts", keys)

	projected, err := projectServiceSecrets(root, unitScope{Namespace: "lodestar"}, "accounts", "prod", secrets)
	if err != nil {
		t.Fatal(err)
	}
	if !projected {
		t.Fatal("service referencing secret-accounts was not projected")
	}

	written, err := os.ReadFile(filepath.Join(root, "overlays", "prod", "external-secret.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var rendered externalSecret
	if err := yaml.Unmarshal(written, &rendered); err != nil {
		t.Fatal(err)
	}

	got := remoteRefsBySecretKey(rendered.Spec.Data)
	want := remoteRefsBySecretKey(golden)
	if len(got) != len(want) {
		t.Fatalf("projected %d keys, golden has %d", len(got), len(want))
	}
	for key, wantRef := range want {
		if got[key] != wantRef {
			t.Fatalf("secretKey %q: projected %+v, golden %+v", key, got[key], wantRef)
		}
	}
}

func writeServiceTree(t *testing.T, root, environment string) {
	t.Helper()
	base := filepath.Join(root, "base")
	overlay := filepath.Join(root, "overlays", environment)
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(overlay, 0o755); err != nil {
		t.Fatal(err)
	}
	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: accounts
  namespace: payments
spec:
  template:
    spec:
      containers:
        - name: accounts
          image: registry.example.com/accounts@sha256:` + strings.Repeat("a", 64) + `
          env:
            - name: CODEFLY__SERVICE
              value: accounts
            - name: WORKOS_CLIENT_SECRET
              valueFrom:
                secretKeyRef:
                  name: secret-accounts
                  key: workos-client-secret
            - name: WORKOS_API_KEY
              valueFrom:
                secretKeyRef:
                  name: secret-accounts
                  key: workos-api-key
`
	if err := os.WriteFile(filepath.Join(base, "deployment.yaml"), []byte(deployment), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "kustomization.yaml"), []byte("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - deployment.yaml\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(overlay, "kustomization.yaml"), []byte("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../../base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestServiceSecretKeysDiscoversReferencedKeys(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accounts")
	writeServiceTree(t, root, "production")
	keys, err := serviceSecretKeys(root, "accounts")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != "workos-api-key" || keys[1] != "workos-client-secret" {
		t.Fatalf("discovered keys = %v", keys)
	}
}

func TestProjectServiceSecretsInjectsPromotableOverlay(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accounts")
	writeServiceTree(t, root, "production")
	projected, err := projectServiceSecrets(root, unitScope{Namespace: "payments"}, "accounts", "production", azureServiceSecrets())
	if err != nil {
		t.Fatal(err)
	}
	if !projected {
		t.Fatal("service referencing secret-accounts was not projected")
	}
	overlay := filepath.Join(root, "overlays", "production")
	overlayKustomization, err := os.ReadFile(filepath.Join(overlay, "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(overlayKustomization), "external-secret.yaml") {
		t.Fatalf("overlay kustomization does not reference the projection: %s", overlayKustomization)
	}

	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	rendered, err := kustomizer.Run(filesys.MakeFsOnDisk(), overlay)
	if err != nil {
		t.Fatalf("build service overlay: %v", err)
	}
	encoded, err := rendered.AsYaml()
	if err != nil {
		t.Fatal(err)
	}
	manifests, _, err := decodeYAML("overlay.yaml", encoded)
	if err != nil {
		t.Fatal(err)
	}
	var external *manifest
	for index := range manifests {
		if manifests[index].kind == "ExternalSecret" {
			external = &manifests[index]
		}
	}
	if external == nil {
		t.Fatalf("service overlay rendered no ExternalSecret: %d manifests", len(manifests))
	}
	if err := validateManifest(*external, nil, true); err != nil {
		t.Fatalf("projected ExternalSecret is not promotable: %v", err)
	}
}

func TestProjectServiceSecretsNoOpWithoutDeclaration(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accounts")
	writeServiceTree(t, root, "production")
	projected, err := projectServiceSecrets(root, unitScope{Namespace: "payments"}, "accounts", "production", nil)
	if err != nil {
		t.Fatal(err)
	}
	if projected {
		t.Fatal("projection ran without an environment declaration")
	}
	if _, err := os.Stat(filepath.Join(root, "overlays", "production", "external-secret.yaml")); !os.IsNotExist(err) {
		t.Fatalf("external-secret.yaml was written without a declaration: %v", err)
	}
}

// The renderers must invoke Workspace.ValidateEnvironments before doing any work,
// so a service-secrets override naming an unknown service fails fast instead of
// silently projecting the default <service>/<key> paths for a typo.
func TestRenderServiceRejectsUnknownServiceSecretOverride(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		resources.WorkspaceConfigurationName: `name: platform
layout: modules
modules:
  - name: web
environments:
  - name: prod
    namespace: platform
    service-secrets:
      secret-store:
        name: azure-keyvault-prod
        kind: ClusterSecretStore
      services:
        ghost:
          remote-keys:
            client-secret: ghost/client-secret
`,
		filepath.Join("modules", "web", resources.ModuleConfigurationName): `kind: module
name: web
services:
    - name: web
`,
		filepath.Join("modules", "web", "services", "web", resources.ServiceConfigurationName): `kind: service
name: web
version: 0.0.0
agent:
  kind: runtime::service
  name: go-grpc
  version: 0.0.1
  publisher: codefly.ai
`,
	}
	for rel, content := range files {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	workspace, err := resources.LoadWorkspaceFromDir(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	env := selectedEnvironment(t, workspace, "prod")
	if env == nil {
		t.Fatal("prod environment did not load")
	}
	_, err = RenderService(ctx, workspace, &resources.Module{Name: "web"}, &resources.Service{Name: "web"}, env, "", false, nil)
	if err == nil {
		t.Fatal("expected RenderService to reject the unknown service-secrets override")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("error = %v, want it to name the unknown service", err)
	}
}

func TestProjectServiceSecretsFailsClearlyWithoutOverlay(t *testing.T) {
	root := filepath.Join(t.TempDir(), "accounts")
	writeServiceTree(t, root, "production")
	if err := os.RemoveAll(filepath.Join(root, "overlays")); err != nil {
		t.Fatal(err)
	}
	_, err := projectServiceSecrets(root, unitScope{Namespace: "payments"}, "accounts", "production", azureServiceSecrets())
	if err == nil {
		t.Fatal("expected an error when the environment overlay is missing")
	}
	if !strings.Contains(err.Error(), "overlay") {
		t.Fatalf("error = %v, want it to name the missing overlay", err)
	}
}

func TestProjectRenderedServiceSecretsCoversEveryServiceTree(t *testing.T) {
	stage := t.TempDir()
	accounts := filepath.Join(stage, "modules", "identity", "services", "accounts")
	writeServiceTree(t, accounts, "production")
	// A dependency that references no secret-<service> must not gain a projection.
	web := filepath.Join(stage, "modules", "identity", "services", "web")
	if err := os.MkdirAll(filepath.Join(web, "overlays", "production"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(web, "overlays", "production", "kustomization.yaml"), []byte("apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	env := &environments.Environment{Name: "production", Namespace: "payments", ServiceSecrets: azureServiceSecrets()}
	graph := map[string]*resources.Service{
		resources.ServiceUnique("identity", "accounts"): {Name: "accounts"},
		resources.ServiceUnique("identity", "web"):      {Name: "web"},
	}
	if err := projectRenderedServiceConfiguration(t.Context(), stage, singleModuleWorkspace(), env, graph, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(accounts, "overlays", "production", "external-secret.yaml")); err != nil {
		t.Fatalf("accounts projection missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(web, "overlays", "production", "external-secret.yaml")); !os.IsNotExist(err) {
		t.Fatalf("web gained a projection despite referencing no secret: %v", err)
	}
}

func TestProjectRenderedServiceSecretsNoOpWithoutDeclaration(t *testing.T) {
	stage := t.TempDir()
	writeServiceTree(t, filepath.Join(stage, "modules", "identity", "services", "accounts"), "production")
	env := &environments.Environment{Name: "production", Namespace: "payments"}
	graph := map[string]*resources.Service{resources.ServiceUnique("identity", "accounts"): {Name: "accounts"}}
	if err := projectRenderedServiceConfiguration(t.Context(), stage, singleModuleWorkspace(), env, graph, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(stage, "modules", "identity", "services", "accounts", "overlays", "production", "external-secret.yaml")); !os.IsNotExist(err) {
		t.Fatalf("projection ran without a declaration: %v", err)
	}
}

func promotableManagedOutput() *InventoryKubernetesOutput {
	return &InventoryKubernetesOutput{
		Kind:            "KUSTOMIZE",
		Profile:         "KUBERNETES_OUTPUT_PROFILE_RESTRICTED_PORTABLE_V1",
		ContractVersion: "codefly.dev/kubernetes-manifest/v1",
		Validation: &InventoryKubernetesValidation{
			StaticValidation: "STATUS_PASSED", ServerSideValidation: "STATUS_PASSED",
			Promotable: true, Violations: []string{},
		},
	}
}

// A managed service that declares only secret references now flips to a bootstrap
// unit (Path + Output set), so the inventory contract must accept that shape —
// with the deployment evidence that renderModuleTree records from the flow — and
// still reject it when that evidence is missing.
func TestValidateInventoryUnitsGovernSecretsBearingManagedBootstrapUnit(t *testing.T) {
	unit := InventoryUnit{
		Kind: UnitKindService, Module: "payments", Name: "store",
		Managed: true, Bootstrap: true,
		Path:   filepath.ToSlash(filepath.Join("services", "store")),
		Output: promotableManagedOutput(),
	}
	inventory := &Inventory{
		SchemaVersion: SchemaVersion, Module: "payments",
		OwnedPath: "environments/deployments/modules/payments",
		Units:     []InventoryUnit{unit},
	}
	if err := validateInventoryUnits(inventory); err != nil {
		t.Fatalf("secrets-bearing managed bootstrap unit rejected: %v", err)
	}

	inventory.Units[0].Output = nil
	if err := validateInventoryUnits(inventory); err == nil {
		t.Fatal("managed bootstrap unit without deployment evidence was accepted")
	}
}

// ESO's per-entry sourceRef preserves shared identity keys alongside keys in
// the service's default store, without splitting ownership of the target Secret.
func TestServiceSecretProjectionPerKeyStore(t *testing.T) {
	override := &environments.EnvironmentSecretStoreReference{Name: "identity-store", Kind: "ClusterSecretStore"}
	secrets := cellServiceSecrets(environments.EnvironmentServiceSecretMapping{
		RemoteKeys: map[string]environments.EnvironmentSecretRemoteRef{
			"IDENTITY": {Key: "shared-product", Property: "client_secret", SecretStore: override},
			"LOCAL":    {Key: "service-local"},
		},
	})
	projection, err := serviceSecretProjection(unitScope{Namespace: "application"}, "accounts", secrets, []string{"IDENTITY", "LOCAL"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	data := document["spec"].(map[string]any)["data"].([]any)
	identity := data[0].(map[string]any)
	store := identity["sourceRef"].(map[string]any)["storeRef"].(map[string]any)
	if store["name"] != override.Name || store["kind"] != override.Kind {
		t.Fatalf("store override lost: %s", encoded)
	}
	if _, exists := data[1].(map[string]any)["sourceRef"]; exists {
		t.Fatalf("default key unexpectedly overridden: %s", encoded)
	}
	if projection.Spec.SecretStoreRef.Name != "cell-secrets" {
		t.Fatalf("default store changed: %s", encoded)
	}
}
