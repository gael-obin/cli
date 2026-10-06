package environments

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestPerKeySecretStoreSurvivesRoundTrip(t *testing.T) {
	for _, property := range []string{"", "client_secret"} {
		original := EnvironmentSecretRemoteRef{Key: "shared-identity", Property: property, SecretStore: &EnvironmentSecretStoreReference{Name: "identity-store", Kind: "ClusterSecretStore"}}
		data, err := yaml.Marshal(original)
		require.NoError(t, err)
		var decoded EnvironmentSecretRemoteRef
		require.NoError(t, yaml.Unmarshal(data, &decoded))
		require.Equal(t, original, decoded)
	}
}

func TestPerKeySecretStoreValidation(t *testing.T) {
	for _, store := range []EnvironmentSecretStoreReference{{}, {Name: "identity", Kind: "Vault"}} {
		secrets := &EnvironmentServiceSecrets{
			SecretStore: EnvironmentSecretStoreReference{Name: "default", Kind: "ClusterSecretStore"},
			Services:    map[string]EnvironmentServiceSecretMapping{"accounts": {RemoteKeys: map[string]EnvironmentSecretRemoteRef{"TOKEN": {Key: "identity", SecretStore: &store}}}},
		}
		require.Error(t, secrets.Validate())
	}
}
