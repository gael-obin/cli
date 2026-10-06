package go_grpc

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/codefly-dev/cli/pkg/engine"
	"github.com/codefly-dev/cli/pkg/environments"
	"github.com/codefly-dev/cli/pkg/orchestration"
	cli "github.com/codefly-dev/core/generated/go/codefly/cli/v0"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConfigurationRPCIncludesTheActiveEnvironmentScope(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS(filepath.Join("..", "..", "orchestration", "testdata", "module-layout"))))
	servicePath := filepath.Join(root, "modules", "management", "services", "organization", "service.codefly.yaml")
	manifest, err := os.ReadFile(servicePath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(servicePath, append(manifest, []byte("\nworkspace-configuration-dependencies: [observability]\n")...), 0600))
	configurationDir := filepath.Join(root, "configurations", "staging")
	require.NoError(t, os.MkdirAll(configurationDir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(configurationDir, "observability.env"), []byte("OBSERVABILITY_STATE=stdout\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(configurationDir, "observability.secret.env"), []byte("QUALIFICATION_TOKEN=test-only\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(configurationDir, "unrelated.secret.env"), []byte("FORBIDDEN_TOKEN=test-only\n"), 0600))
	workspace, err := resources.LoadWorkspaceFromDir(ctx, root)
	require.NoError(t, err)
	flow, err := orchestration.NewFlow(ctx, workspace, nil, nil, &environments.Environment{Name: "staging"}, orchestration.RunMode)
	require.NoError(t, err)
	flow.WithRuntimeContext(resources.RuntimeContextNative)
	flow.WithFixture("dev-admin")
	require.NoError(t, flow.ConfigurationManager.Load(ctx, (&environments.Environment{Name: "staging"}).Runtime()))
	flows := engine.NewFlowManager()
	require.NoError(t, flows.Register("configuration", flow))
	server, err := NewServer(&Configuration{EndpointGrpc: "127.0.0.1:0"}, workspace, flows)
	require.NoError(t, err)
	listener, err := server.Listen()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	response, err := cli.NewCLIClient(connection).GetConfiguration(ctx, &cli.GetConfigurationRequest{Module: "management", Service: "organization"})
	require.NoError(t, err)
	require.Len(t, response.ProcessVariables, 4)
	values := map[string]string{}
	for _, variable := range response.ProcessVariables {
		values[variable.Key] = variable.Value
		if variable.Key == resources.WorkspaceSecretConfigurationPrefix+"__OBSERVABILITY__QUALIFICATION_TOKEN" {
			require.True(t, variable.Secret)
		}
	}
	require.Equal(t, "staging", values[resources.EnvironmentPrefix])
	require.Equal(t, "dev-admin", values[resources.FixturePrefix])
	require.Equal(t, "stdout", values[resources.WorkspaceConfigurationPrefix+"__OBSERVABILITY__OBSERVABILITY_STATE"])
	require.Equal(t, "test-only", values[resources.WorkspaceSecretConfigurationPrefix+"__OBSERVABILITY__QUALIFICATION_TOKEN"])
	require.NotContains(t, values, resources.WorkspaceSecretConfigurationPrefix+"__UNRELATED__FORBIDDEN_TOKEN")
}
