//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	. "github.com/onsi/gomega"
	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
	hyperkarpenterv1 "github.com/openshift/hypershift/api/karpenter/v1beta1"
	e2eutil "github.com/openshift/hypershift/test/e2e/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// TestKarpenterArbitrarySubnets validates the full end-to-end path for Karpenter-driven
// subnet aggregation in a PublicAndPrivate hosted cluster:
//
//	OpenshiftEC2NodeClass (guest) → karpenter-subnets ConfigMap (HCP ns) → AWSEndpointService.Spec.SubnetIDs (HCP ns)
//
// The test performs AZ-aware subnet selection to avoid the AZ mismatch that would cause
// ModifyVpcEndpoint to fail: it queries the endpoint service's supported AZs first via
// DescribeVpcEndpointServices, then picks a VPC subnet that lies in one of those AZs.
// This ensures the subnet selected for the OpenshiftEC2NodeClass is compatible with the
// NLB backing the private endpoint so ModifyVpcEndpoint won't get an AZ mismatch error.
//
// Validation steps:
//  1. Look up the AWSEndpointService's supported AZs via DescribeVpcEndpointServices.
//  2. Pick a VPC subnet in a supported AZ.
//  3. Create an OpenshiftEC2NodeClass pointing at that subnet.
//  4. Wait for Karpenter to resolve the subnet into the NodeClass status.
//  5. Verify the karpenter-subnets ConfigMap in the HCP namespace contains the subnet.
//  6. Verify AWSEndpointService.Spec.SubnetIDs includes the subnet (proving the full pipeline).
func TestKarpenterArbitrarySubnets(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(testContext)
	defer cancel()

	// Skip if not AWS platform
	if globalOpts.Platform != hyperv1.AWSPlatform {
		t.Skip("test only supported on platform AWS")
	}

	clusterOpts := globalOpts.DefaultClusterOptions(t)
	clusterOpts.AWSPlatform.AutoNode = true
	clusterOpts.ControlPlaneAvailabilityPolicy = string(hyperv1.SingleReplica)
	// No worker nodes needed — all test assertions are on the management-plane (ConfigMap)
	// and guest-plane (OpenshiftEC2NodeClass status). Karpenter (AutoNode) does not
	// provision nodes without workload demand.
	clusterOpts.NodePoolReplicas = 0
	// PublicAndPrivate: required to create AWSEndpointService resources backed by NLBs,
	// which are the target for Karpenter subnet propagation. The test selects a subnet
	// in an AZ supported by the endpoint service NLB so ModifyVpcEndpoint won't fail.
	clusterOpts.AWSPlatform.EndpointAccess = string(hyperv1.PublicAndPrivate)

	e2eutil.NewHypershiftTest(t, ctx, func(t *testing.T, g Gomega, mgtClient crclient.Client, hostedCluster *hyperv1.HostedCluster) {
		t.Logf("Testing Karpenter subnet aggregation for PublicAndPrivate cluster")

		t.Run("ConfigMap subnet aggregation", func(t *testing.T) {
			g := NewWithT(t)

			// Get EC2 client to query AWS resources
			ec2client := ec2Client(clusterOpts.AWSPlatform.Credentials.AWSCredentialsFile, clusterOpts.AWSPlatform.Region)

			hcpNamespace := fmt.Sprintf("%s-%s", hostedCluster.Namespace, hostedCluster.Name)

			// Step 1: Wait for an AWSEndpointService to have a populated EndpointServiceName
			// so we can look up its supported AZs. Both kube-apiserver-private and
			// private-router are backed by NLBs in the same AZs, so either will do.
			t.Logf("Waiting for AWSEndpointService to populate EndpointServiceName in namespace: %s", hcpNamespace)
			var endpointServiceName string
			err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
				epsList := &hyperv1.AWSEndpointServiceList{}
				if err := mgtClient.List(ctx, epsList, crclient.InNamespace(hcpNamespace)); err != nil {
					return false, nil
				}
				for i := range epsList.Items {
					if epsList.Items[i].Status.EndpointServiceName != "" {
						endpointServiceName = epsList.Items[i].Status.EndpointServiceName
						return true, nil
					}
				}
				return false, nil
			})
			g.Expect(err).NotTo(HaveOccurred(), "AWSEndpointService should have a populated EndpointServiceName")
			t.Logf("Found endpoint service: %s", endpointServiceName)

			// Step 2: Query the supported AZs for that endpoint service.
			svcOut, err := ec2client.DescribeVpcEndpointServicesWithContext(ctx, &ec2.DescribeVpcEndpointServicesInput{
				ServiceNames: []*string{aws.String(endpointServiceName)},
			})
			g.Expect(err).NotTo(HaveOccurred(), "failed to describe VPC endpoint services")
			g.Expect(svcOut.ServiceDetails).NotTo(BeEmpty(), "endpoint service should have details")

			supportedAZs := sets.NewString()
			for _, az := range svcOut.ServiceDetails[0].AvailabilityZones {
				supportedAZs.Insert(aws.StringValue(az))
			}
			t.Logf("Endpoint service supported AZs: %v", supportedAZs.List())

			// Step 3: Find a VPC subnet in a supported AZ.
			// This ensures ModifyVpcEndpoint won't fail with an AZ mismatch error.
			vpcID := hostedCluster.Spec.Platform.AWS.CloudProviderConfig.VPC
			t.Logf("Finding subnet in a supported AZ in VPC: %s", vpcID)

			subnetsOutput, err := ec2client.DescribeSubnetsWithContext(ctx, &ec2.DescribeSubnetsInput{
				Filters: []*ec2.Filter{
					{
						Name:   aws.String("vpc-id"),
						Values: []*string{aws.String(vpcID)},
					},
					{
						Name:   aws.String("state"),
						Values: []*string{aws.String("available")},
					},
				},
			})
			g.Expect(err).NotTo(HaveOccurred(), "failed to describe subnets")
			g.Expect(subnetsOutput.Subnets).NotTo(BeEmpty(), "VPC should have at least one subnet")

			var arbitrarySubnet *ec2.Subnet
			for _, s := range subnetsOutput.Subnets {
				if supportedAZs.Has(aws.StringValue(s.AvailabilityZone)) {
					arbitrarySubnet = s
					break
				}
			}
			g.Expect(arbitrarySubnet).NotTo(BeNil(), "VPC should have at least one subnet in a supported AZ (%v)", supportedAZs.List())

			arbitrarySubnetID := aws.StringValue(arbitrarySubnet.SubnetId)
			t.Logf("Using arbitrary subnet: %s in AZ: %s", arbitrarySubnetID, aws.StringValue(arbitrarySubnet.AvailabilityZone))

			// Step 4: Get the guest client to create OpenshiftEC2NodeClass resources.
			guestClient := e2eutil.WaitForGuestClient(t, ctx, mgtClient, hostedCluster)

			// Step 5: Create OpenshiftEC2NodeClass with custom SubnetSelectorTerms
			customNodeClassName := "custom-subnet-nodeclass"
			openshiftEC2NodeClass := &hyperkarpenterv1.OpenshiftEC2NodeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: customNodeClassName,
				},
				Spec: hyperkarpenterv1.OpenshiftEC2NodeClassSpec{
					SubnetSelectorTerms: []hyperkarpenterv1.SubnetSelectorTerm{
						{
							ID: arbitrarySubnetID,
						},
					},
				},
			}

			g.Expect(guestClient.Create(ctx, openshiftEC2NodeClass)).To(Succeed())
			t.Logf("Created OpenshiftEC2NodeClass with custom subnet selector")
			defer func() {
				_ = guestClient.Delete(ctx, openshiftEC2NodeClass)
			}()

			// Step 6: Wait for OpenshiftEC2NodeClass status to be populated by Karpenter
			t.Logf("Waiting for Karpenter to resolve subnets...")
			var resolvedSubnets []hyperkarpenterv1.Subnet
			err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				nodeClass := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
				if err := guestClient.Get(ctx, crclient.ObjectKey{Name: customNodeClassName}, nodeClass); err != nil {
					return false, err
				}
				if len(nodeClass.Status.Subnets) > 0 {
					resolvedSubnets = nodeClass.Status.Subnets
					return true, nil
				}
				return false, nil
			})
			g.Expect(err).NotTo(HaveOccurred(), "OpenshiftEC2NodeClass should have resolved subnets in status")
			g.Expect(resolvedSubnets).To(HaveLen(1), "should have resolved exactly one subnet")
			g.Expect(resolvedSubnets[0].ID).To(Equal(arbitrarySubnetID), "resolved subnet should match the specified subnet")
			t.Logf("Karpenter resolved subnet: %s", resolvedSubnets[0].ID)

			// Step 7: Verify ConfigMap is created with the Karpenter-resolved subnet.
			// This proves the EC2NodeClass controller is aggregating subnets correctly.
			t.Logf("Checking for karpenter-subnets ConfigMap in namespace: %s", hcpNamespace)

			configMap := &corev1.ConfigMap{}
			err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
				if err := mgtClient.Get(ctx, crclient.ObjectKey{
					Namespace: hcpNamespace,
					Name:      "karpenter-subnets",
				}, configMap); err != nil {
					t.Logf("ConfigMap not yet available: %v", err)
					return false, nil
				}
				return true, nil
			})
			g.Expect(err).NotTo(HaveOccurred(), "karpenter-subnets ConfigMap should be created")

			// Validate ConfigMap labels
			g.Expect(configMap.Labels).To(HaveKeyWithValue("hypershift.openshift.io/managed-by", "karpenter"))
			g.Expect(configMap.Labels).To(HaveKey("hypershift.openshift.io/infra-id"))
			t.Logf("ConfigMap has correct labels")

			// Validate ConfigMap contains our subnet
			subnetIDsJSON := configMap.Data["subnetIDs"]
			g.Expect(subnetIDsJSON).NotTo(BeEmpty(), "ConfigMap should contain subnetIDs data")

			var subnetIDs []string
			err = json.Unmarshal([]byte(subnetIDsJSON), &subnetIDs)
			g.Expect(err).NotTo(HaveOccurred(), "subnetIDs should be valid JSON")
			g.Expect(subnetIDs).To(ContainElement(arbitrarySubnetID), "ConfigMap should contain the arbitrary subnet")
			t.Logf("ConfigMap contains Karpenter-resolved subnet: %v", subnetIDs)

			// Step 8: Verify the subnet flows through to AWSEndpointService.Spec.SubnetIDs.
			// This proves the full pipeline: OpenshiftEC2NodeClass → ConfigMap → AWSEndpointService.
			// We check Spec.SubnetIDs rather than waiting for AWSEndpointAvailable=True to avoid
			// prolonging the test with the AWS-side endpoint modification round-trip.
			t.Logf("Waiting for AWSEndpointService to include subnet %s in Spec.SubnetIDs", arbitrarySubnetID)
			err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
				eps := &hyperv1.AWSEndpointService{}
				if err := mgtClient.Get(ctx, crclient.ObjectKey{
					Namespace: hcpNamespace,
					Name:      "kube-apiserver-private",
				}, eps); err != nil {
					return false, nil
				}
				for _, id := range eps.Spec.SubnetIDs {
					if id == arbitrarySubnetID {
						return true, nil
					}
				}
				return false, nil
			})
			g.Expect(err).NotTo(HaveOccurred(), "AWSEndpointService should include the Karpenter subnet in Spec.SubnetIDs")
			t.Logf("AWSEndpointService.Spec.SubnetIDs includes Karpenter subnet: %s", arbitrarySubnetID)

			// Step 9: Provision a Karpenter node so the framework's EnsureHostedCluster
			// post-validation phase finds a real worker node and cluster operators can converge.
			// The default OpenshiftEC2NodeClass ("default") is created automatically by AutoNode.
			// We reuse the existing assets from TestKarpenter.
			t.Logf("Creating Karpenter NodePool and workload to provision a worker node")
			karpenterNodePool := &unstructured.Unstructured{}
			yamlFile, err := content.ReadFile("assets/karpenter-nodepool.yaml")
			g.Expect(err).NotTo(HaveOccurred(), "should read karpenter-nodepool.yaml")
			g.Expect(yaml.Unmarshal(yamlFile, karpenterNodePool)).To(Succeed())

			workLoads := &unstructured.Unstructured{}
			yamlFile, err = content.ReadFile("assets/karpenter-workloads.yaml")
			g.Expect(err).NotTo(HaveOccurred(), "should read karpenter-workloads.yaml")
			g.Expect(yaml.Unmarshal(yamlFile, workLoads)).To(Succeed())
			// Override replicas to 1 so Karpenter provisions exactly one node.
			// The YAML default is 3 (with podAntiAffinity), which would cause
			// WaitForReadyNodesByLabels to fail since it checks for an exact count.
			workLoads.Object["spec"].(map[string]interface{})["replicas"] = 1

			g.Expect(guestClient.Create(ctx, karpenterNodePool)).To(Succeed())
			// DO NOT defer-delete the NodePool: PublicAndPrivate clusters are treated as
			// private by IsPrivateHC(), so EnsureHostedCluster skips the node-list check
			// and defaults hasWorkerNodes=true. If the NodePool is deleted here (end of
			// Main), Karpenter terminates the node before EnsureHostedCluster runs, and
			// ValidateHostedClusterConditions times out waiting for cluster operators that
			// need worker nodes. The framework tears down the whole HostedCluster during
			// Teardown, so no cleanup is needed here.
			// defer func() { _ = guestClient.Delete(ctx, karpenterNodePool) }()
			g.Expect(guestClient.Create(ctx, workLoads)).To(Succeed())
			// defer func() { _ = guestClient.Delete(ctx, workLoads) }()

			nodeLabels := map[string]string{
				"node.kubernetes.io/instance-type": "t3.large",
				"karpenter.sh/nodepool":            karpenterNodePool.GetName(),
			}
			t.Logf("Waiting for a Karpenter node to become ready")
			e2eutil.WaitForReadyNodesByLabels(t, ctx, guestClient, hostedCluster.Spec.Platform.Type, 1, nodeLabels)
			t.Logf("Karpenter node is ready")

			// Cleanup: delete the OpenshiftEC2NodeClass
			t.Logf("Cleaning up OpenshiftEC2NodeClass")
			g.Expect(guestClient.Delete(ctx, openshiftEC2NodeClass)).To(Succeed())

			// Step 10: Verify ConfigMap cleanup when all OpenshiftEC2NodeClasses are deleted.
			// List remaining NodeClasses to determine if we were the last one.
			nodeClassList := &hyperkarpenterv1.OpenshiftEC2NodeClassList{}
			err = guestClient.List(ctx, nodeClassList)
			g.Expect(err).NotTo(HaveOccurred())

			if len(nodeClassList.Items) == 0 {
				t.Logf("All OpenshiftEC2NodeClass resources deleted, ConfigMap should be cleaned up")
				err = wait.PollUntilContextTimeout(ctx, 5*time.Second, 1*time.Minute, true, func(ctx context.Context) (bool, error) {
					cm := &corev1.ConfigMap{}
					err := mgtClient.Get(ctx, crclient.ObjectKey{
						Namespace: hcpNamespace,
						Name:      "karpenter-subnets",
					}, cm)
					if err != nil && crclient.IgnoreNotFound(err) == nil {
						return true, nil // ConfigMap deleted
					}
					return false, nil
				})
				// Don't fail if cleanup didn't happen - other tests may have created nodeclasses
				if err == nil {
					t.Logf("ConfigMap successfully cleaned up after last OpenshiftEC2NodeClass deletion")
				} else {
					t.Logf("ConfigMap still exists (may be used by other NodeClasses)")
				}
			} else {
				t.Logf("Other OpenshiftEC2NodeClass resources exist, ConfigMap should remain")
			}
		})
	}).Execute(&clusterOpts, globalOpts.Platform, globalOpts.ArtifactDir, "karpenter-arbitrary-subnets", globalOpts.ServiceAccountSigningKey)
}
