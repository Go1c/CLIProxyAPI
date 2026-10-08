//go:build cgo && (darwin || linux || freebsd)

package pluginhost

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCodexHTTP2PluginRealABIUsesCodexOAuthProvider(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and invokes the example shared library")
	}
	_, currentFile, _, okCaller := runtime.Caller(0)
	if !okCaller {
		t.Fatal("runtime.Caller() failed")
	}
	pluginDir := filepath.Join(filepath.Dir(currentFile), "..", "..", "examples", "plugin", "codex-http2-keepalive", "go")
	outputDir := t.TempDir()
	pluginBase := filepath.Join(outputDir, "codex-http2-keepalive")
	pluginPath := pluginBase + PluginExtension(runtime.GOOS)
	cmdBuild := exec.Command("go", "build", "-buildmode=c-shared", "-o", pluginPath, ".")
	cmdBuild.Dir = pluginDir
	if output, errBuild := cmdBuild.CombinedOutput(); errBuild != nil {
		t.Fatalf("build example plugin: %v\n%s", errBuild, output)
	}

	harnessPath := filepath.Join(outputDir, "codex-plugin-harness")
	harnessSource := filepath.Join(filepath.Dir(currentFile), "testdata", "codex_plugin_harness.c")
	cmdCompile := exec.Command("cc", harnessSource, pluginPath, "-I", outputDir, "-o", harnessPath)
	if output, errCompile := cmdCompile.CombinedOutput(); errCompile != nil {
		t.Fatalf("compile C harness: %v\n%s", errCompile, output)
	}

	cmdHarness := exec.Command(harnessPath)
	cmdHarness.Env = append(os.Environ(), "DYLD_LIBRARY_PATH="+outputDir, "LD_LIBRARY_PATH="+outputDir)
	output, errHarness := cmdHarness.CombinedOutput()
	if errHarness != nil {
		t.Fatalf("run C harness: %v\n%s", errHarness, output)
	}
	text := string(output)
	if !strings.Contains(text, `"identifier":"codex"`) {
		t.Fatalf("real plugin identifier response missing codex provider:\n%s", text)
	}
	if strings.Contains(text, "auth_not_found") {
		t.Fatalf("real plugin rejected selected Codex OAuth:\n%s", text)
	}
	if !strings.Contains(strings.ToLower(text), "connect") {
		t.Fatalf("real plugin execution did not reach the configured test endpoint:\n%s", text)
	}
}
