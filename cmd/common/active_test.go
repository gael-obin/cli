package common

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codefly-dev/core/resources"
)

func TestLoadWorkspaceReturnsErrorsInsteadOfExiting(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		t.Chdir(t.TempDir())
		if _, err := LoadWorkspace(context.Background()); err == nil {
			t.Fatal("missing workspace returned nil error")
		}
	})

	t.Run("present", func(t *testing.T) {
		dir := t.TempDir()
		workspace := &resources.Workspace{Name: "test-workspace", Layout: resources.LayoutKindModules}
		if err := workspace.SaveToDirUnsafe(context.Background(), dir); err != nil {
			t.Fatal(err)
		}
		t.Chdir(dir)
		got, err := LoadWorkspace(context.Background())
		if err != nil {
			t.Fatalf("LoadWorkspace: %v", err)
		}
		if got.Name != workspace.Name {
			t.Fatalf("workspace name = %q, want %q", got.Name, workspace.Name)
		}
	})
}

func TestLoadRequiredEReturnsMissingWorkspaceError(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, _, _, err := LoadRequiredE(context.Background(), []string{"missing"}); err == nil {
		t.Fatal("missing workspace returned nil error")
	}
}

func TestLoadRequiredNonInteractiveENeverPromptsForAmbiguousService(t *testing.T) {
	root := filepath.Join("..", "..", "pkg", "orchestration", "testdata", "module-layout")
	t.Chdir(root)
	_, _, _, err := LoadRequiredNonInteractiveE(context.Background(), nil)
	if err == nil {
		t.Fatal("ambiguous headless service selection succeeded")
	}
	if !strings.Contains(err.Error(), "pass the service name explicitly") || !strings.Contains(err.Error(), "frontend") || !strings.Contains(err.Error(), "gateway") {
		t.Fatalf("headless ambiguity error = %q", err)
	}
}

func TestLoadRequiredNonInteractiveEUsesSingleModuleServiceEntry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	writeActiveFixture(t, filepath.Join(root, resources.WorkspaceConfigurationName), `name: starter-dev
layout: modules
modules:
  - name: starter
`)
	moduleDir := filepath.Join(root, "modules", "starter")
	writeActiveFixture(t, filepath.Join(moduleDir, resources.ModuleConfigurationName), `kind: module
name: starter
service-entry: frontend
services:
  - name: accounts
  - name: frontend
`)
	serviceManifest := func(name, endpoint string) string {
		return "name: " + name + `
version: 0.0.0
agent:
  kind: codefly:service
  name: go
  version: 0.0.0
  publisher: codefly.dev
endpoints:
  - name: ` + endpoint + "\n"
	}
	writeActiveFixture(t, filepath.Join(moduleDir, "services", "accounts", resources.ServiceConfigurationName), serviceManifest("accounts", "grpc"))
	writeActiveFixture(t, filepath.Join(moduleDir, "services", "frontend", resources.ServiceConfigurationName), serviceManifest("frontend", "http"))

	t.Chdir(root)
	workspace, module, service, err := LoadRequiredNonInteractiveE(ctx, nil)
	if err != nil {
		t.Fatalf("load service entry: %v", err)
	}
	if workspace.Name != "starter-dev" || module.Name != "starter" || service.Name != "frontend" {
		t.Fatalf("resolved %q/%q/%q, want starter-dev/starter/frontend", workspace.Name, module.Name, service.Name)
	}
}

func writeActiveFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Selecting the same composed service from its directory must not bypass the
// workspace override that applies when selecting it by name or as a dependency.
func TestActiveServiceUsesWorkspaceAgentOverride(t *testing.T) {
	root := t.TempDir()
	writeActiveFixture(t, filepath.Join(root, resources.WorkspaceConfigurationName), `name: example
layout: modules
modules:
  - name: backend
agent-overrides:
  codefly.dev/go: 0.2.0
`)
	moduleDir := filepath.Join(root, "modules", "backend")
	writeActiveFixture(t, filepath.Join(moduleDir, resources.ModuleConfigurationName), `kind: module
name: backend
service-entry: api
services:
  - name: api
`)
	serviceDir := filepath.Join(moduleDir, "services", "api")
	writeActiveFixture(t, filepath.Join(serviceDir, resources.ServiceConfigurationName), `name: api
version: 0.0.0
agent:
  kind: codefly:service
  name: go
  version: 0.1.0
  publisher: codefly.dev
endpoints:
  - name: http
`)
	for _, dir := range []string{root, moduleDir, serviceDir} {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			t.Chdir(dir)
			_, _, service, err := LoadRequiredNonInteractiveE(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if service.Agent.Version != "0.2.0" {
				t.Fatalf("effective agent = %s, want workspace override 0.2.0", service.Agent.Version)
			}
		})
	}
}

func TestLoadActiveContextDoesNotReturnStaleWorkspace(t *testing.T) {
	ctx := context.Background()
	firstDir := t.TempDir()
	first := &resources.Workspace{Name: "first", Layout: resources.LayoutKindModules}
	if err := first.SaveToDirUnsafe(ctx, firstDir); err != nil {
		t.Fatal(err)
	}
	secondDir := t.TempDir()
	second := &resources.Workspace{Name: "second", Layout: resources.LayoutKindModules}
	if err := second.SaveToDirUnsafe(ctx, secondDir); err != nil {
		t.Fatal(err)
	}

	t.Chdir(firstDir)
	gotFirst, err := LoadActiveContext(ctx)
	if err != nil {
		t.Fatalf("load first active context: %v", err)
	}
	t.Chdir(secondDir)
	gotSecond, err := LoadActiveContext(ctx)
	if err != nil {
		t.Fatalf("load second active context: %v", err)
	}

	if gotFirst.Workspace.Name != "first" {
		t.Fatalf("first workspace = %q, want first", gotFirst.Workspace.Name)
	}
	if gotSecond.Workspace.Name != "second" {
		t.Fatalf("second workspace = %q, want second", gotSecond.Workspace.Name)
	}
}

func TestLoadModuleUsesCurrentEmptyModuleWithoutResolvingAService(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	workspace := &resources.Workspace{
		Name:   "test-workspace",
		Layout: resources.LayoutKindModules,
		Modules: []*resources.ModuleReference{
			{Name: "coordination"},
		},
	}
	if err := workspace.SaveToDirUnsafe(ctx, root); err != nil {
		t.Fatal(err)
	}

	moduleDir := filepath.Join(root, "modules", "coordination")
	module := &resources.Module{
		Kind:              resources.ModuleKind,
		Name:              "coordination",
		ServiceReferences: []*resources.ServiceReference{},
	}
	module.WithDir(moduleDir)
	if err := module.Save(ctx); err != nil {
		t.Fatal(err)
	}
	t.Chdir(moduleDir)

	got, err := LoadModule(ctx)
	if err != nil {
		t.Fatalf("LoadModule from empty module: %v", err)
	}
	if got.Name != "coordination" {
		t.Fatalf("module = %q, want coordination", got.Name)
	}

	gotWorkspace, gotRequired, err := LoadRequiredModuleE(ctx, nil)
	if err != nil {
		t.Fatalf("LoadRequiredModuleE from empty module: %v", err)
	}
	if gotWorkspace.Name != "test-workspace" {
		t.Fatalf("workspace = %q, want test-workspace", gotWorkspace.Name)
	}
	if gotRequired.Name != "coordination" {
		t.Fatalf("required module = %q, want coordination", gotRequired.Name)
	}
}

func TestLoadRequiredModuleEReturnsMissingWorkspaceError(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, _, err := LoadRequiredModuleE(context.Background(), []string{"missing"}); err == nil {
		t.Fatal("missing workspace returned nil error")
	}
}

// ambiguousWorkspace lays out one module with two services, so nothing but a
// person (or an explicit name) can settle which service is meant.
func ambiguousWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	workspace := &resources.Workspace{
		Name:    "ambiguous",
		Layout:  resources.LayoutKindModules,
		Modules: []*resources.ModuleReference{{Name: "mod"}},
	}
	if err := workspace.SaveToDirUnsafe(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	moduleDir := filepath.Join(root, "modules", "mod")
	write := func(path, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(moduleDir, "module.codefly.yaml"),
		"kind: module\nname: mod\nservices:\n  - name: gateway\n  - name: api\n")
	for _, service := range []string{"gateway", "api"} {
		write(filepath.Join(moduleDir, "services", service, "service.codefly.yaml"),
			"kind: service\nname: "+service+"\nversion: 0.0.0\nagent:\n  kind: runtime::service\n  name: go-grpc\n  version: 0.0.1\n  publisher: codefly.ai\n")
	}
	t.Chdir(root)
	return root
}

// TestLoadActiveContextIsHeadlessWithoutATTY is the regression. `go test` runs
// with no controlling terminal, exactly like CI, a pipe, an MCP server or an
// agent — so this test is only meaningful because it cannot show a selector.
//
// Before the fix, LoadActiveContext asked for a selector unconditionally and the
// process died with `open /dev/tty: device not configured`: no mention of the
// ambiguity it was resolving, and no hint that naming the service would have
// fixed it. `codefly generate contracts` in a two-service workspace failed that
// way, and so did every other command reaching this from a script.
//
// The assertion is on the *content* of the error, not merely that one occurred:
// an unhelpful failure is what this replaces, so a test that accepted any error
// would pass against the bug.
func TestLoadActiveContextIsHeadlessWithoutATTY(t *testing.T) {
	ambiguousWorkspace(t)

	_, err := LoadActiveContext(context.Background())
	if err == nil {
		t.Fatal("two services resolved to one without anybody choosing")
	}
	message := err.Error()
	if strings.Contains(message, "/dev/tty") || strings.Contains(message, "device not configured") {
		t.Fatalf("a headless caller was sent to a terminal: %v", err)
	}
	for _, want := range []string{"multiple services found", "gateway", "api", "pass the service name explicitly"} {
		if !strings.Contains(message, want) {
			t.Fatalf("error does not say %q, so it cannot be acted on: %v", want, err)
		}
	}
}

// TestInteractivePossibleAgreesWithTheHeadlessCheck: one process must not be
// headless for a question and interactive for a picker, so this uses the same
// test the prompt bridge uses. Under `go test` there is no terminal, which is the
// case that matters.
func TestInteractivePossibleAgreesWithTheHeadlessCheck(t *testing.T) {
	if interactivePossible() {
		t.Fatal("a process with no controlling terminal reported that it can show a selector")
	}
}
