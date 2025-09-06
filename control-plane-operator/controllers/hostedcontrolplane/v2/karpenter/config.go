package karpenter

import (
	"fmt"

	component "github.com/openshift/hypershift/support/controlplane-component"

	corev1 "k8s.io/api/core/v1"
)

func adaptSidecarConfig(cpContext component.WorkloadContext, cm *corev1.ConfigMap) error {
	namespace := cpContext.HCP.Namespace

	config := fmt.Sprintf(`# Skroobapi configuration for HyperShift
# Management cluster kubeconfig (in-cluster service account)
defaultKubeconfig: "/mnt/kubeconfig/target-kubeconfig"

mappings:
  # ec2nodeclasses need to live in the management cluster (incluster is management cluster kubeconfig)
  - group: "karpenter.k8s.aws"
    version: "v1"
    kind: "EC2NodeClass"
    kubeconfig: "incluster"
    forceClusterScopedToNamespace: "%s"
    priority: 200
`, namespace)
	/*config := fmt.Sprintf(`# Skroobapi configuration for HyperShift
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
	  - group: "karpenter.k8s.aws"
	    version: "v1"
	    kind: "EC2NodeClass"
	    kubeconfig: "incluster"
	    forceClusterScopedToNamespace: "%s"
	    priority: 200
	  # Route Nodes to guest cluster
	  - group: ""
	    version: "v1"
	    kind: "Node"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: ""
	    version: "v1"
	    kind: "Pod"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: "storage.k8s.io"
	    version: "v1"
	    kind: "VolumeAttachment"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: "storage.k8s.io"
	    version: "v1"
	    kind: "CSINode"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: "apps"
	    version: "v1"
	    kind: "Deployment"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: "apps"
	    version: "v1"
	    kind: "DaemonSet"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	  - group: "policy"
	    version: "v1"
	    kind: "PodDisruptionBudget"
	    kubeconfig: "/mnt/kubeconfig/target-kubeconfig"
	    priority: 150
	`, namespace)*/

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data["skroobapi.yaml"] = config
	return nil
}
