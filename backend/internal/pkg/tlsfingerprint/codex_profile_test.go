//go:build unit

package tlsfingerprint

import (
	"reflect"
	"testing"
)

func TestCodexCLIRustlsProfileUsesSourceDerivedTLS13Defaults(t *testing.T) {
	profile := codexCLIRustlsProfileForSeed(0)

	if profile.Name != BuiltInCodexCLIProfileName {
		t.Fatalf("name = %q, want %q", profile.Name, BuiltInCodexCLIProfileName)
	}
	if profile.EnableGREASE {
		t.Fatal("Codex rustls profile should not add uTLS GREASE bookends")
	}

	wantCiphers := []uint16{0x1302, 0x1301, 0x1303}
	if !reflect.DeepEqual(profile.CipherSuites, wantCiphers) {
		t.Fatalf("cipher suites = %#v, want %#v", profile.CipherSuites, wantCiphers)
	}

	wantCurves := []uint16{29, 23, 24, 0x11ec}
	if !reflect.DeepEqual(profile.Curves, wantCurves) {
		t.Fatalf("curves = %#v, want %#v", profile.Curves, wantCurves)
	}

	wantSigAlgs := []uint16{0x0503, 0x0403, 0x0603, 0x0807, 0x0806, 0x0805, 0x0804, 0x0601, 0x0501, 0x0401}
	if !reflect.DeepEqual(profile.SignatureAlgorithms, wantSigAlgs) {
		t.Fatalf("signature algorithms = %#v, want %#v", profile.SignatureAlgorithms, wantSigAlgs)
	}

	if !reflect.DeepEqual(profile.SupportedVersions, []uint16{0x0304}) {
		t.Fatalf("supported versions = %#v, want TLS 1.3 only", profile.SupportedVersions)
	}
	if !reflect.DeepEqual(profile.KeyShareGroups, []uint16{29}) {
		t.Fatalf("key share groups = %#v, want X25519 only", profile.KeyShareGroups)
	}

	wantExtensionsForSeed0 := []uint16{5, 43, 11, 10, 0, 13, 23, 51, 45}
	if !reflect.DeepEqual(profile.Extensions, wantExtensionsForSeed0) {
		t.Fatalf("extensions = %#v, want %#v", profile.Extensions, wantExtensionsForSeed0)
	}
}
