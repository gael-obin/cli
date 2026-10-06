package environments

import (
	"fmt"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	kubeyaml "sigs.k8s.io/yaml"
)

// EnvironmentServiceEgress declares extra in-cluster reachability by consuming
// service. Rules use Kubernetes' own model; this boundary admits only explicit
// namespace-and-pod destinations and numerical ports, never public egress.
type EnvironmentServiceEgress struct {
	Services map[string][]networkingv1.NetworkPolicyEgressRule `json:"services"`
}

// The native API uses JSON field names. Its own serializer preserves those names
// when embedded in the workspace's yaml.v3 document and refuses unknown fields.
func (e *EnvironmentServiceEgress) UnmarshalYAML(node *yaml.Node) error {
	data, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	return kubeyaml.UnmarshalStrict(data, e)
}

func (e EnvironmentServiceEgress) MarshalYAML() (any, error) {
	data, err := kubeyaml.Marshal(e)
	if err != nil {
		return nil, err
	}
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	return node.Content[0], nil
}

func (e *EnvironmentServiceEgress) Validate() error {
	if e == nil {
		return nil
	}
	if len(e.Services) == 0 {
		return fmt.Errorf("service-egress declares no services")
	}
	for service, rules := range e.Services {
		if err := validateResourcePathComponent("service-egress service", service); err != nil {
			return err
		}
		if len(rules) == 0 {
			return fmt.Errorf("service-egress %q declares no rules", service)
		}
		for _, rule := range rules {
			if len(rule.To) == 0 || len(rule.Ports) == 0 {
				return fmt.Errorf("service-egress %q requires destinations and ports", service)
			}
			for _, peer := range rule.To {
				if peer.IPBlock != nil || peer.NamespaceSelector == nil || peer.PodSelector == nil {
					return fmt.Errorf("service-egress %q requires namespaceSelector and podSelector in each peer, without ipBlock", service)
				}
				namespace := peer.NamespaceSelector.MatchLabels[corev1.LabelMetadataName]
				if err := ValidateNamespaceName("service-egress destination", namespace); err != nil {
					return err
				}
				for _, selector := range []*metav1.LabelSelector{peer.NamespaceSelector, peer.PodSelector} {
					parsed, err := metav1.LabelSelectorAsSelector(selector)
					if err != nil {
						return fmt.Errorf("service-egress %q: %w", service, err)
					}
					if parsed.Empty() {
						return fmt.Errorf("service-egress %q cannot select every pod or namespace", service)
					}
				}
			}
			for _, port := range rule.Ports {
				if port.Port == nil || port.Port.Type != intstr.Int || port.Port.IntVal < 1 || port.Port.IntVal > 65535 || port.EndPort != nil {
					return fmt.Errorf("service-egress %q requires individual numerical ports in 1..65535", service)
				}
				if port.Protocol != nil && *port.Protocol != corev1.ProtocolTCP && *port.Protocol != corev1.ProtocolUDP && *port.Protocol != corev1.ProtocolSCTP {
					return fmt.Errorf("service-egress %q has an invalid transport protocol", service)
				}
			}
		}
	}
	return nil
}
