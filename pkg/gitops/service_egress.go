package gitops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/codefly-dev/cli/pkg/environments"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	kubeyaml "sigs.k8s.io/yaml"
)

const serviceEgressLabel = "codefly.dev/service-egress"

// projectServiceEgress binds infrastructure-declared destinations to the actual
// consuming pods in the selected overlay. Namespace, labels and ports are never
// inferred from a service name or copied from an infrastructure inventory.
func projectServiceEgress(root, service string, env *environments.Environment, scope unitScope) error {
	if env.ServiceEgress == nil {
		return nil
	}
	rules, declared := env.ServiceEgress.Services[service]
	if !declared {
		return nil
	}
	if err := env.ServiceEgress.Validate(); err != nil {
		return err
	}
	if err := environments.ValidateNamespaceName("service-egress workload", scope.Namespace); err != nil {
		return err
	}
	documents, err := effectiveConfiguration(root, env.Name)
	if err != nil {
		return err
	}
	configMaps, err := indexConfigurationMaps(documents)
	if err != nil {
		return err
	}
	var policies []networkingv1.NetworkPolicy
	for _, document := range documents {
		spec, ok := podSpec(document)
		if !ok {
			continue
		}
		consumes := false
		for _, candidate := range sliceField(spec, "containers") {
			container, ok := candidate.(map[string]any)
			if !ok {
				return fmt.Errorf("service-egress %q has an invalid container", service)
			}
			bound, err := configMaps.service(container, metadataString(document.value, "namespace"))
			if err != nil {
				return err
			}
			consumes = consumes || bound == service
		}
		if !consumes {
			continue
		}
		if namespace := metadataString(document.value, "namespace"); namespace != scope.Namespace {
			return fmt.Errorf("service-egress %q workload targets namespace %q, expected %q", service, namespace, scope.Namespace)
		}
		metadata := mapField(podTemplate(document), "metadata")
		podLabels := map[string]string{}
		for key, value := range mapField(metadata, "labels") {
			label, ok := value.(string)
			if !ok {
				return fmt.Errorf("service-egress %q workload label %q is not a string", service, key)
			}
			podLabels[key] = label
		}
		if len(podLabels) == 0 {
			return fmt.Errorf("service-egress %q cannot bind a workload without pod labels", service)
		}
		selector := metav1.LabelSelector{MatchLabels: podLabels}
		parsed, err := metav1.LabelSelectorAsSelector(&selector)
		if err != nil || !parsed.Matches(labels.Set(podLabels)) {
			return fmt.Errorf("service-egress %q has invalid pod labels", service)
		}
		policies = append(policies, networkingv1.NetworkPolicy{
			TypeMeta:   metav1.TypeMeta{APIVersion: networkingv1.SchemeGroupVersion.String(), Kind: "NetworkPolicy"},
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("service-egress-%s-%d", service, len(policies)), Namespace: scope.Namespace, Labels: map[string]string{serviceEgressLabel: service}},
			Spec:       networkingv1.NetworkPolicySpec{PodSelector: selector, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}, Egress: rules},
		})
	}
	if len(policies) == 0 {
		return fmt.Errorf("service-egress %q binds no effective workload", service)
	}
	var output bytes.Buffer
	for _, policy := range policies {
		data, err := kubeyaml.Marshal(policy)
		if err != nil {
			return err
		}
		output.WriteString("---\n")
		output.Write(data)
	}
	overlay := filepath.Join(root, "overlays", env.Name)
	if err := os.WriteFile(filepath.Join(overlay, "service-egress.yaml"), output.Bytes(), 0600); err != nil {
		return err
	}
	if err := addKustomizationResource(overlay, "service-egress.yaml"); err != nil {
		return err
	}
	// Check final bytes too: an overlay patch must not drop or widen the grant.
	rendered, err := effectiveConfiguration(root, env.Name)
	if err != nil {
		return err
	}
	remaining := append([]networkingv1.NetworkPolicy(nil), policies...)
	for _, doc := range rendered {
		if doc.kind != "NetworkPolicy" {
			continue
		}
		data, err := json.Marshal(doc.value)
		if err != nil {
			return err
		}
		var policy networkingv1.NetworkPolicy
		if err := json.Unmarshal(data, &policy); err != nil {
			return err
		}
		if policy.Labels[serviceEgressLabel] != service {
			continue
		}
		found := -1
		for index, expected := range remaining {
			if policy.Namespace == expected.Namespace && reflect.DeepEqual(policy.Spec, expected.Spec) {
				found = index
				break
			}
		}
		if found < 0 {
			return fmt.Errorf("service-egress %q changed by overlay", service)
		}
		remaining = append(remaining[:found], remaining[found+1:]...)
	}
	if len(remaining) > 0 {
		return fmt.Errorf("service-egress %q missing from effective overlay", service)
	}
	return nil
}
