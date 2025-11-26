package karpenter

// Default image for Karpenter provider AWS controller
const DefaultKarpenterProviderAWSImage = "public.ecr.aws/karpenter/controller:1.2.3"

// Default EC2NodeClass name
const EC2NodeClassDefault = "default"

const (
	// Karpenter Metrics
	// from: https://github.com/openshift/kubernetes-sigs-karpenter/blob/9ec6578ef19c3d8fdbaeb00d9ea87d8371bdd3d0/pkg/operator/operator.go#L70
	KarpenterBuildInfoMetricName = "karpenter_build_info"

	// Karpenter Operator Metrics
	KarpenterOperatorInfoMetricName = "karpenter_operator_info"
)
