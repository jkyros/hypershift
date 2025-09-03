package karpenter

import (
	component "github.com/openshift/hypershift/support/controlplane-component"

	corev1 "k8s.io/api/core/v1"
)

func adaptSidecarConfig(cpContext component.WorkloadContext, cm *corev1.ConfigMap) error {

	config := `# Skroobapi configuration for HyperShift
# Management cluster kubeconfig (in-cluster service account)
defaultKubeconfig: "incluster"

mappings:
  # Route Karpenter resources to the guest cluster
  - group: "karpenter.sh"
    version: "v1"
    kind: "NodePool"
    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
    priority: 200
    
  - group: "karpenter.sh"
    version: "v1" 
    kind: "NodeClaim"
    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
    priority: 200
    
  # - group: "karpenter.k8s.aws"
  #  version: "v1"
  #  kind: "EC2NodeClass"
  #  kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
  #  priority: 200
    
  # Route Nodes to guest cluster
  - group: ""
    version: "v1"
    kind: "Node"
    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
    priority: 150
`

	cm.Data["skroobapi.yaml"] = config
	return nil
}
