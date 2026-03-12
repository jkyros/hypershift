package v1beta1

import (
	"encoding/json"
	"strings"

	"sigs.k8s.io/yaml"
)

// ToKubeletConfigManifest converts the KubeletConfiguration spec into a KubeletConfig
// machineconfiguration.openshift.io/v1 YAML manifest string suitable for injection
// into a ConfigMap "config" key. Returns empty string if KubeletConfiguration is nil.
// The name argument is used as the KubeletConfig CR name.
func (kc *KubeletConfiguration) ToKubeletConfigManifest(name string) (string, error) {
	if kc == nil {
		return "", nil
	}

	// Marshal via JSON first (respects json tags and omitempty), then convert to a
	// map so the full CR can be serialised as clean YAML by sigs.k8s.io/yaml.
	raw, err := json.Marshal(kc)
	if err != nil {
		return "", err
	}
	var kubeletConfigMap map[string]interface{}
	if err := json.Unmarshal(raw, &kubeletConfigMap); err != nil {
		return "", err
	}

	cr := map[string]interface{}{
		"apiVersion": "machineconfiguration.openshift.io/v1",
		"kind":       "KubeletConfig",
		"metadata":   map[string]interface{}{"name": name},
		"spec":       map[string]interface{}{"kubeletConfig": kubeletConfigMap},
	}

	out, err := yaml.Marshal(cr)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(out), "\n"), nil
}
