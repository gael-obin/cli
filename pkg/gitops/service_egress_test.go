package gitops

import (
	"github.com/codefly-dev/cli/pkg/environments"
	"github.com/codefly-dev/core/resources"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
)

func collectorEgressEnvironment(t *testing.T) *environments.Environment {
	t.Helper()
	var env environments.Environment
	require.NoError(t, yaml.Unmarshal([]byte(`name: staging
namespace: application
service-egress:
  services:
    api:
      - to:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: collector
            podSelector:
              matchLabels:
                app.kubernetes.io/name: collector
        ports:
          - protocol: TCP
            port: 14317
`), &env))
	return &env
}

func TestServiceEgressProjectsOnlyDeclaredClusterPeer(t *testing.T) {
	env := collectorEgressEnvironment(t)
	root := t.TempDir()
	writeConsumerTree(t, root, env.Name, env.Namespace, "api", "declared.example")
	for range 2 {
		require.NoError(t, projectServiceConfiguration(t.Context(), root, &resources.Service{Name: "api"}, env, scopeOf(env), serviceInjection{}))
	}
	policy := manifestOfKind(t, buildOverlay(t, root, env.Name), "NetworkPolicy")
	spec := mapField(policy.value, "spec")
	require.Equal(t, []any{"Egress"}, spec["policyTypes"])
	require.NotEmpty(t, mapField(spec, "podSelector"))
	rules := sliceField(spec, "egress")
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	peers := sliceField(rule, "to")
	require.Len(t, peers, 1)
	peer := peers[0].(map[string]any)
	require.NotContains(t, peer, "ipBlock")
	require.Equal(t, "collector", mapField(mapField(peer, "namespaceSelector"), "matchLabels")["kubernetes.io/metadata.name"])
	require.Equal(t, "collector", mapField(mapField(peer, "podSelector"), "matchLabels")["app.kubernetes.io/name"])
	require.EqualValues(t, 14317, sliceField(rule, "ports")[0].(map[string]any)["port"])
}

func TestServiceEgressRejectsOverlayWidening(t *testing.T) {
	env := collectorEgressEnvironment(t)
	root := t.TempDir()
	writeConsumerTree(t, root, env.Name, env.Namespace, "api", "declared.example")
	require.NoError(t, projectServiceEgress(root, "api", env, scopeOf(env)))
	addProjectionPatch(t, root, env.Name, "NetworkPolicy", "- op: replace\n  path: /spec/egress/0/to\n  value: []")
	require.ErrorContains(t, projectServiceEgress(root, "api", env, scopeOf(env)), "changed by overlay")
}
