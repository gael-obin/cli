package environments

import (
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
)

const egressFixture = `name: staging
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
`

func TestServiceEgressRuntimeRoundTrip(t *testing.T) {
	var env Environment
	require.NoError(t, yaml.Unmarshal([]byte(egressFixture), &env))
	require.NoError(t, env.Validate())
	resource, err := env.Resource()
	require.NoError(t, err)
	decoded, err := FromRuntime(resource)
	require.NoError(t, err)
	require.Equal(t, env.ServiceEgress, decoded.ServiceEgress)
}

func TestServiceEgressRefusesBroadOrInvalidRules(t *testing.T) {
	for _, edit := range [][2]string{
		{"port: 14317", "port: 0"},
		{"port: 14317", "port: 65536"},
		{"protocol: TCP", "protocol: HTTP"},
		{"app.kubernetes.io/name: collector", ""},
		{"kubernetes.io/metadata.name: collector", "arbitrary: collector"},
		{"podSelector:", "ipBlock:"},
		{"podSelector:", "podSelektor:"},
		{"            podSelector:", "          - podSelector:"},
	} {
		t.Run(edit[1], func(t *testing.T) {
			var env Environment
			err := yaml.Unmarshal([]byte(strings.Replace(egressFixture, edit[0], edit[1], 1)), &env)
			if err == nil {
				err = env.Validate()
			}
			require.Error(t, err)
		})
	}
}
