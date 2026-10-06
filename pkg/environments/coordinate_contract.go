package environments

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"gopkg.in/yaml.v3"
)

// CoordinateContractSchema identifies the producer-independent environment
// contract. The document is named for its subject: a deployment is named by its
// coordinate, using the producer's explicit deployment declarations.
const CoordinateContractSchema = "codefly/coordinate/v1"

// CoordinateContract carries Codefly's existing Environment model, not a
// producer's infrastructure inventory. Producers resolve endpoints, names and
// references; importing this document never invents credentials or deployment
// paths.
type CoordinateContract struct {
	Schema               string      `yaml:"schema"`
	Coordinate           string      `yaml:"coordinate,omitempty"`
	RequiresCapabilities []string    `yaml:"requires_capabilities,omitempty"`
	Environment          Environment `yaml:"environment"`
}

// CapabilityManagedServiceIdentity carries an endpoint's declared runtime identity.
const CapabilityManagedServiceIdentity = "managed-service-identity"

// CapabilityServiceIdentity carries a consuming service's own runtime identity,
// declared under service-identity rather than through a managed service.
const CapabilityServiceIdentity = "service-identity"

// ParseCoordinateContract reads JSON using the same field names as workspace YAML.
func ParseCoordinateContract(data []byte) (*CoordinateContract, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("coordinate contract must contain exactly one JSON value")
	}
	var c CoordinateContract
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return nil, fmt.Errorf("decoding coordinate contract: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *CoordinateContract) validate() error {
	if c.Schema != CoordinateContractSchema {
		return fmt.Errorf("unsupported coordinate-contract schema %q (want %q); producers must emit explicit Codefly environment declarations", c.Schema, CoordinateContractSchema)
	}
	for _, capability := range c.RequiresCapabilities {
		switch capability {
		case CapabilityManagedServiceIdentity, CapabilityServiceIdentity:
		default:
			return fmt.Errorf("coordinate contract requires unsupported capability %q", capability)
		}
	}
	env := &c.Environment
	if err := validateResourcePathComponent("environment name", env.Name); err != nil {
		return err
	}
	if env.Namespace != "" {
		if err := validateResourcePathComponent("namespace", env.Namespace); err != nil {
			return err
		}
	}
	if _, err := env.ConfigurationProfileName(); err != nil {
		return err
	}
	if env.Cluster != nil && strings.TrimSpace(env.Cluster.Context) == "" {
		return fmt.Errorf("declared cluster requires a context")
	}
	if env.Registry != nil && strings.TrimSpace(env.Registry.URL) == "" {
		return fmt.Errorf("declared registry requires a URL")
	}
	if env.Gitops != nil && (strings.TrimSpace(env.Gitops.RepoURL) == "" || strings.TrimSpace(env.Gitops.Path) == "" || strings.TrimSpace(env.Gitops.Branch) == "") {
		return fmt.Errorf("declared delivery target requires repository, path and branch")
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
	if err := env.ResourceQuota.Validate(); err != nil {
		return err
	}
	return validateManagedContractServices(env.ManagedServices)
}

func validateManagedContractServices(services map[string]EnvironmentManagedService) error {
	for name, service := range services {
		if err := validateManagedServiceKey(name); err != nil {
			return err
		}
		if strings.TrimSpace(service.ExternalName) == "" || service.Port < 1 || service.Port > 65535 {
			return fmt.Errorf("managed service %q requires an endpoint and port in 1..65535", name)
		}
		for _, cidr := range service.EgressCIDRs {
			if _, _, err := net.ParseCIDR(cidr); err != nil {
				return fmt.Errorf("managed service %q has invalid egress CIDR %q: %w", name, cidr, err)
			}
		}
		if err := service.Identity.validate(fmt.Sprintf("managed service %q", name)); err != nil {
			return err
		}
		seen := make(map[string]bool)
		for _, ref := range service.SecretReferences {
			if strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.RemoteKey) == "" {
				return fmt.Errorf("managed service %q requires explicit secret names and remote keys", name)
			}
			if seen[ref.Name] {
				return fmt.Errorf("managed service %q repeats secret %q", name, ref.Name)
			}
			seen[ref.Name] = true
			if err := ref.SecretStore.validate("managed service " + name); err != nil {
				return err
			}
		}
	}
	return nil
}

// ToEnvironment returns an independent configuration. The requested target must
// match the declaration: changing a namespace must not silently retarget secrets
// or delivery paths resolved for another environment.
func (c *CoordinateContract) ToEnvironment(envName, namespace string) (*Environment, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if c.Environment.Name != envName || c.Environment.Namespace != namespace {
		return nil, fmt.Errorf("environment target %q/%q does not match declared target %q/%q", envName, namespace, c.Environment.Name, c.Environment.Namespace)
	}
	data, err := yaml.Marshal(c.Environment)
	if err != nil {
		return nil, err
	}
	var env Environment
	if err := yaml.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	return &env, nil
}
