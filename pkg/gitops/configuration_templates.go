package gitops

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/codefly-dev/cli/pkg/environments"
	basev0 "github.com/codefly-dev/core/generated/go/codefly/base/v0"
	"github.com/codefly-dev/core/resources"
	"google.golang.org/protobuf/proto"
)

// A producer can declare a secret value it exposes as a template over its own
// secret configuration values (core's ConfigurationValue.template): a Postgres
// agent's read-write connection is its role, address and database around a
// reference to its read-write password. A restricted render carries no value,
// so without the template the environment's store would have to hold the
// assembled string, typed by hand. With it, the consumer's ExternalSecret reads
// the producer's primitives from the producer's own remote keys and External
// Secrets assembles the value in the cluster: the store holds only primitives,
// and nothing here learns how the producer builds its values.

// deliveredTemplate is one producer's declared assembly of one secret key.
type deliveredTemplate struct {
	// producer is the module/service unique of the service that declared it.
	producer string
	template *basev0.ConfigurationValueTemplate
	// fetchable are the secret keys the producer's own ExternalSecret reads
	// from the store under the producer's own scope. A reference outside this
	// set names a key the store holds nowhere: the producer's plain values
	// (a host, a database name) live in its ConfigMap, never in the store, and
	// a misspelled key lives nowhere at all. Either renders a remoteRef that
	// resolves to nothing, so the consumer's ExternalSecret never syncs and
	// its pods never start — with a green render. This is the only place that
	// sees both the template and the producer's fetchable key set.
	fetchable map[string]struct{}
}

// renderTemplates maps a secret key exactly as a consumer reads it
// (CODEFLY__SERVICE_SECRET_CONFIGURATION__<MODULE>__<SERVICE>__<CONFIGURATION>__<KEY>)
// to the template its producer declared for it.
type renderTemplates map[string]deliveredTemplate

// collectRenderTemplates adds every template the deployed configurations
// declare. A key is named by its producer, so two declarations of one key can
// only come from the same producer, and they must agree.
func collectRenderTemplates(
	into renderTemplates,
	configurations map[string]*basev0.Configuration,
	secretKeys map[string][]string,
) error {
	uniques := make([]string, 0, len(configurations))
	for unique := range configurations {
		uniques = append(uniques, unique)
	}
	sort.Strings(uniques)
	for _, unique := range uniques {
		configuration := configurations[unique]
		for _, info := range configuration.GetInfos() {
			for _, value := range info.GetConfigurationValues() {
				if value.GetTemplate() == nil {
					continue
				}
				if err := resources.ValidateTemplatedConfigurationValue(value); err != nil {
					return fmt.Errorf("service %s: %w", unique, err)
				}
				if err := refusesNestedAssembly(configuration, value); err != nil {
					return fmt.Errorf("service %s: %w", unique, err)
				}
				if !referencesASecret(value.GetTemplate()) {
					// A template of literals only is a plain value under a
					// credential-named key; it would put that value in the tree.
					return fmt.Errorf("service %s: template of %s/%s references none of its secrets", unique, info.GetName(), value.GetKey())
				}
				origin := configuration.GetOrigin()
				if origin != unique {
					return fmt.Errorf("service %s exposes a configuration template under origin %q; a template assembles its producer's own values", unique, origin)
				}
				key := resources.ServiceSecretConfigurationKeyFromUnique(origin, info.GetName(), value.GetKey())
				fetchable := make(map[string]struct{}, len(secretKeys[unique]))
				for _, declared := range secretKeys[unique] {
					fetchable[declared] = struct{}{}
				}
				delivered := deliveredTemplate{producer: origin, template: value.GetTemplate(), fetchable: fetchable}
				if prior, declared := into[key]; declared && !proto.Equal(prior.template, delivered.template) {
					return fmt.Errorf("secret key %s is declared by two different templates", key)
				}
				into[key] = delivered
			}
		}
	}
	return nil
}

// templateVariable is what External Secrets can name as `.KEY` in a template:
// a Go template field name. Every core-derived secret key is one.
var templateVariable = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// externalSecretTemplateExpression translates a producer's template into an
// External Secrets v2 template expression (Go text/template with Sprig) over
// the secret keys of the producer's primitives, and returns those keys. It
// reproduces resources.EvaluateConfigurationValueTemplate byte for byte:
//
//   - a literal is copied as template text, or quoted as a template string
//     constant when it contains an action delimiter;
//   - a reference reads its primitive with `.KEY`, which fails on a missing key
//     under External Secrets' missingkey=error rather than assembling "";
//   - URL_USERINFO is urlquery (url.QueryEscape) with its "+" — the only byte it
//     emits for a space, since a literal plus is already %2B — written "%20".
func externalSecretTemplateExpression(delivered deliveredTemplate) (string, []string, error) {
	if err := resources.ValidateConfigurationValueTemplate(delivered.template); err != nil {
		return "", nil, err
	}
	var expression strings.Builder
	var primitives []string
	for _, segment := range delivered.template.GetSegments() {
		reference := segment.GetReference()
		if reference == nil {
			literal := segment.GetLiteral()
			if strings.Contains(literal, "{{") {
				expression.WriteString("{{ " + strconv.Quote(literal) + " }}")
			} else {
				expression.WriteString(literal)
			}
			continue
		}
		primitive := resources.ServiceSecretConfigurationKeyFromUnique(delivered.producer, reference.GetConfiguration(), reference.GetKey())
		if !templateVariable.MatchString(primitive) {
			return "", nil, fmt.Errorf("secret key %q cannot be named in an External Secrets template", primitive)
		}
		primitives = append(primitives, primitive)
		switch reference.GetEscape() {
		case basev0.ConfigurationValueEscape_CONFIGURATION_VALUE_ESCAPE_NONE:
			expression.WriteString("{{ ." + primitive + " }}")
		case basev0.ConfigurationValueEscape_CONFIGURATION_VALUE_ESCAPE_URL_USERINFO:
			expression.WriteString("{{ ." + primitive + ` | urlquery | replace "+" "%20" }}`)
		default:
			return "", nil, fmt.Errorf("secret key %s: escape %s has no External Secrets translation", primitive, reference.GetEscape())
		}
	}
	return expression.String(), primitives, nil
}

// producerPrimitiveRemote locates one of a producer's own secret keys in the
// remote store, through the same surface the producer's own ExternalSecret
// reads it from — which is the whole point of reading primitives at the
// producer: they are already seeded there, because the producer fetches them
// there itself.
//
// Which surface that is depends on what the producer is. A managed service's
// keys are enumerated in the environment (EnvironmentManagedSecretReference),
// and managedSecretProjection renders its ExternalSecret from them; nothing
// about a managed service resolves through EnvironmentServiceSecrets, so
// addressing one through that surface would name an entry the platform never
// wrote. A regular service's keys resolve through EnvironmentServiceSecrets
// under the producer's own scope, the same call its own projection makes.
//
// The store is returned beside the reference because an ExternalSecret owns a
// single store: a producer whose keys live in another one cannot be read from
// the consumer's ExternalSecret at all, and the caller refuses rather than
// reading the consumer's store and finding nothing.
func producerPrimitiveRemote(
	scope unitScope,
	delivered deliveredTemplate,
	primitive string,
	secrets *environments.EnvironmentServiceSecrets,
) (environments.EnvironmentSecretRemoteRef, environments.EnvironmentSecretStoreReference, error) {
	var noRemote environments.EnvironmentSecretRemoteRef
	var noStore environments.EnvironmentSecretStoreReference
	module, service, ok := strings.Cut(delivered.producer, "/")
	if !ok || module == "" || service == "" {
		return noRemote, noStore, fmt.Errorf("producer %q is not a module/service unique", delivered.producer)
	}
	if managed, isManaged := scope.Managed[service]; isManaged {
		for _, reference := range managed.SecretReferences {
			if reference.Name == primitive {
				return environments.EnvironmentSecretRemoteRef{Key: reference.RemoteKey, Property: reference.Property},
					reference.SecretStore, nil
			}
		}
		return noRemote, noStore, fmt.Errorf(
			"managed service %q declares no secret reference for %s, which producer %s assembles from",
			service, primitive, delivered.producer)
	}
	if _, reads := delivered.fetchable[primitive]; !reads {
		return noRemote, noStore, fmt.Errorf(
			"producer %s assembles from %s, which its own deployment does not read as a secret",
			delivered.producer, primitive)
	}
	var store environments.EnvironmentSecretStoreReference
	if secrets != nil {
		store = secrets.SecretStore
		// The producer's own projection takes its per-service override the same
		// way (serviceSecretProjection), so this is the store its ExternalSecret
		// actually reads from.
		if override := secrets.Services[service].SecretStore; override != nil {
			store = *override
		}
	}
	producerScope := environments.SecretScope{Workspace: scope.Workspace, Module: module, Service: service}
	remote := secrets.RemoteRef(producerScope, primitive)
	if remote.SecretStore != nil {
		store = *remote.SecretStore
	}
	return remote, store, nil
}

// refusesNestedAssembly rejects a template that references another value the
// same producer also assembles. Such a reference passes every other check —
// the referenced key is a real secret the producer's own ExternalSecret reads
// — but the store holds the referenced value nowhere, because it too is
// assembled in the cluster. The consumer would read it as a whole and its
// ExternalSecret would never sync. Core refuses the same shape at evaluation:
// EvaluateConfigurationValueTemplate looks the reference up among the
// producer's values, and an assembled one carries no value to find.
func refusesNestedAssembly(configuration *basev0.Configuration, value *basev0.ConfigurationValue) error {
	for _, segment := range value.GetTemplate().GetSegments() {
		reference := segment.GetReference()
		if reference == nil {
			continue
		}
		for _, info := range configuration.GetInfos() {
			if info.GetName() != reference.GetConfiguration() {
				continue
			}
			for _, candidate := range info.GetConfigurationValues() {
				if candidate.GetKey() == reference.GetKey() && candidate.GetTemplate() != nil {
					return fmt.Errorf(
						"template of %s/%s references %s/%s, which is itself assembled and so is held in no store",
						info.GetName(), value.GetKey(), reference.GetConfiguration(), reference.GetKey())
				}
			}
		}
	}
	return nil
}

func referencesASecret(template *basev0.ConfigurationValueTemplate) bool {
	for _, segment := range template.GetSegments() {
		if segment.GetReference() != nil {
			return true
		}
	}
	return false
}
