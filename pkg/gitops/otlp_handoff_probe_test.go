//go:build coordinate_integration

package gitops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/codefly-dev/cli/pkg/environments"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
)

func TestWorkloadOTLPContractProjection(t *testing.T) {
	data, err := os.ReadFile(os.Getenv("OTLP_HANDOFF_CONTRACT"))
	require.NoError(t, err)
	contract, err := environments.ParseCoordinateContract(data)
	require.NoError(t, err)
	env, err := contract.ToEnvironment(contract.Environment.Name, contract.Environment.Namespace)
	require.NoError(t, err)
	for _, service := range []string{"accounts", "auth-gateway"} {
		t.Run(service, func(t *testing.T) {
			root := t.TempDir()
			writeConsumerTree(t, root, env.Name, env.Namespace, service, "declared.example")
			require.NoError(t, projectServiceConfiguration(t.Context(), root, &resources.Service{Name: service}, env, scopeOf(env), serviceInjection{}))
			rendered := buildOverlay(t, root, env.Name)
			values := containerEnvironment(t, rendered)
			if output := os.Getenv("OTLP_HANDOFF_OUTPUT_DIR"); output != "" {
				var documents []map[string]any
				for _, document := range rendered {
					documents = append(documents, document.value)
				}
				encoded, err := json.MarshalIndent(documents, "", "  ")
				require.NoError(t, err)
				require.NoError(t, os.MkdirAll(output, 0o750))
				require.NoError(t, os.WriteFile(filepath.Join(output, service+".json"), encoded, 0o600))
			}
			for key, value := range env.ServiceConfig.Services[service].Values {
				require.Equal(t, value, values[key]["value"], key)
			}
			keys := make([]string, 0, len(env.ServiceSecrets.Services[service].RemoteKeys))
			for key := range env.ServiceSecrets.Services[service].RemoteKeys {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			projection, err := serviceSecretProjection(scopeOf(env), service, env.ServiceSecrets, keys)
			require.NoError(t, err)
			if len(keys) > 0 {
				require.NotNil(t, projection)
				for _, entry := range projection.Spec.Data {
					declared := env.ServiceSecrets.Services[service].RemoteKeys[entry.SecretKey]
					require.Equal(t, declared.Key, entry.RemoteRef.Key)
					require.Equal(t, declared.Property, entry.RemoteRef.Property)
					if declared.SecretStore != nil {
						require.NotNil(t, entry.SourceRef)
						require.Equal(t, declared.SecretStore.Name, entry.SourceRef.StoreRef.Name)
						require.Equal(t, declared.SecretStore.Kind, entry.SourceRef.StoreRef.Kind)
					}
				}
			} else {
				require.Nil(t, projection)
			}
			prefix := "CODEFLY__WORKSPACE_CONFIGURATION__OBSERVABILITY__"
			require.Equal(t, "enabled", values[prefix+"OBSERVABILITY_STATE"]["value"])
			require.Equal(t, "grpc", values[prefix+"OTEL_EXPORTER_OTLP_PROTOCOL"]["value"])
			require.Equal(t, "node-agent", values[prefix+"OBSERVABILITY_COLLECTOR_TIER"]["value"])
		})
	}
}
