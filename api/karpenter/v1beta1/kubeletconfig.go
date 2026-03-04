package v1beta1

import (
	"encoding/json"
	"fmt"
)

// ToKubeletConfigManifest converts the KubeletConfiguration spec into a KubeletConfig
// machineconfiguration.openshift.io/v1 YAML manifest string suitable for injection
// into a ConfigMap "config" key. Returns empty string if KubeletConfiguration is nil.
// The name argument is used as the KubeletConfig CR name.
func (kc *KubeletConfiguration) ToKubeletConfigManifest(name string) (string, error) {
	if kc == nil {
		return "", nil
	}

	// Marshal the KubeletConfig struct directly — the JSON tags and omitempty on each field
	// handle nil/empty filtering, and metav1.Duration marshals as a duration string automatically.
	// This mirrors how upstream Karpenter serializes KubeletConfiguration in nodeadm.go.
	kubeletConfigJSON, err := json.Marshal(kc)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(`apiVersion: machineconfiguration.openshift.io/v1
kind: KubeletConfig
metadata:
  name: %s
spec:
  kubeletConfig: %s`, name, string(kubeletConfigJSON)), nil
}
