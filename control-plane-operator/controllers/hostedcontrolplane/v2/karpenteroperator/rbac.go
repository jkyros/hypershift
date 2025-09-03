package karpenteroperator

import (
	component "github.com/openshift/hypershift/support/controlplane-component"

	rbacv1 "k8s.io/api/rbac/v1"
)

func adaptClusterRole(cpContext component.WorkloadContext, clusterRole *rbacv1.ClusterRole) error {
	hcp := cpContext.HCP

	// ClusterRole is cluster-scoped, so we need to make the name unique per HCP namespace
	// The name template in the YAML uses HCP_NAMESPACE placeholder
	clusterRole.Name = "karpenter-operator-" + hcp.Namespace

	// ClusterRole should not have a namespace (it's cluster-scoped)
	clusterRole.Namespace = ""

	return nil
}

func adaptClusterRoleBinding(cpContext component.WorkloadContext, clusterRoleBinding *rbacv1.ClusterRoleBinding) error {
	hcp := cpContext.HCP

	// ClusterRoleBinding is cluster-scoped, so we need to make the name unique per HCP namespace
	clusterRoleBinding.Name = "karpenter-operator-" + hcp.Namespace

	// ClusterRoleBinding should not have a namespace (it's cluster-scoped)
	clusterRoleBinding.Namespace = ""

	// Update the ClusterRole reference to match the adapted name
	clusterRoleBinding.RoleRef.Name = "karpenter-operator-" + hcp.Namespace

	// Ensure the ServiceAccount subject has the correct namespace
	for i := range clusterRoleBinding.Subjects {
		if clusterRoleBinding.Subjects[i].Kind == "ServiceAccount" {
			clusterRoleBinding.Subjects[i].Namespace = hcp.Namespace
		}
	}

	return nil
}
