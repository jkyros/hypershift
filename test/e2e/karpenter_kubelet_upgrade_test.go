//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	hyperkarpenterv1 "github.com/openshift/hypershift/api/karpenter/v1"
	e2eutil "github.com/openshift/hypershift/test/e2e/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crclient "sigs.k8s.io/controller-runtime/pkg/client"

	hyperv1 "github.com/openshift/hypershift/api/hypershift/v1beta1"
)

const nodeClassCurrentConfigVersionAnnotation = "hypershift.openshift.io/nodeClassCurrentConfigVersion"

// TestKarpenterKubeletConfigUpgrade verifies that promoting a kubelet config field from overflow
// to a typed struct field in the karpenter API does not cause node rolls or break NodeClass updates.
//
// The test requires a HyperShift Operator upgrade scenario (--run-upgrade-test flag):
//   - Cluster starts on the previous HO payload (old karpenter-operator, fields may be overflow)
//   - NodeClass is created with kubelet config values; config hash is captured
//   - HO is upgraded to the new payload (new karpenter-operator, fields now typed)
//   - Hash must remain stable (no roll triggered)
//   - A no-op metadata update must leave the hash unchanged (round-trip safety)
func TestKarpenterKubeletConfigUpgrade(t *testing.T) {
	if !globalOpts.RunUpgradeTest {
		t.Skip("skipping HO upgrade test — set --run-upgrade-test to enable")
	}
	if globalOpts.Platform != hyperv1.AWSPlatform {
		t.Skip("test only supported on platform AWS")
	}
	t.Parallel()

	ctx, cancel := context.WithCancel(testContext)
	defer cancel()

	clusterOpts := globalOpts.DefaultClusterOptions(t)
	clusterOpts.AWSPlatform.AutoNode = true

	g := gomega.NewWithT(t)

	e2eutil.NewHypershiftTest(t, ctx, func(t *testing.T, gg gomega.Gomega, mgtClient crclient.Client, hostedCluster *hyperv1.HostedCluster) {
		guestClient := e2eutil.WaitForGuestClient(t, ctx, mgtClient, hostedCluster)

		// Build a KubeletConfiguration via JSON unmarshal. On the old binary, fields not yet
		// typed land in Overflow; on the new binary they land in typed struct fields. Either way
		// the controller must produce an identical ignition config hash.
		kubeletJSON := `{
			"maxPods": 203,
			"podsPerCore": 11,
			"systemReserved": {"cpu": "510m", "memory": "521Mi"},
			"kubeReserved": {"cpu": "520m", "memory": "531Mi"},
			"evictionHard": {"memory.available": "201Mi", "nodefs.available": "11%"},
			"evictionSoft": {"memory.available": "401Mi", "nodefs.available": "16%"},
			"evictionSoftGracePeriod": {"memory.available": "1m31s", "nodefs.available": "2m5s"},
			"evictionMaxPodGracePeriod": 31,
			"imageGCHighThresholdPercent": 81,
			"imageGCLowThresholdPercent": 71,
			"cpuCFSQuota": false,
			"podPidsLimit": 4096,
			"containerLogMaxSize": "50Mi"
		}`
		var kubeletConfig hyperkarpenterv1.KubeletConfiguration
		g.Expect(json.Unmarshal([]byte(kubeletJSON), &kubeletConfig)).To(gomega.Succeed())

		nc := &hyperkarpenterv1.OpenshiftEC2NodeClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "kubelet-upgrade-test",
			},
			Spec: hyperkarpenterv1.OpenshiftEC2NodeClassSpec{
				Kubelet: kubeletConfig,
			},
		}
		g.Expect(guestClient.Create(ctx, nc)).To(gomega.Succeed())
		t.Logf("Created OpenshiftEC2NodeClass %q with kubelet config", nc.Name)
		t.Cleanup(func() { _ = guestClient.Delete(ctx, nc) })

		// Wait for the karpenterignition controller to issue the ignition token and set the
		// config hash annotation. This guarantees the controller has fully processed the NodeClass.
		t.Logf("Waiting for nodeClassCurrentConfigVersion annotation to be set on %q", nc.Name)
		var preUpgradeHash string
		e2eutil.EventuallyObject(t, ctx, fmt.Sprintf("OpenshiftEC2NodeClass %q to have config version annotation", nc.Name),
			func(ctx context.Context) (*hyperkarpenterv1.OpenshiftEC2NodeClass, error) {
				updated := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
				err := guestClient.Get(ctx, crclient.ObjectKey{Name: nc.Name}, updated)
				return updated, err
			},
			[]e2eutil.Predicate[*hyperkarpenterv1.OpenshiftEC2NodeClass]{
				func(nc *hyperkarpenterv1.OpenshiftEC2NodeClass) (done bool, reasons string, err error) {
					v := nc.GetAnnotations()[nodeClassCurrentConfigVersionAnnotation]
					if v == "" {
						return false, "annotation not yet set", nil
					}
					preUpgradeHash = v
					return true, fmt.Sprintf("annotation set to %q", v), nil
				},
			},
			e2eutil.WithTimeout(3*time.Minute),
			e2eutil.WithInterval(5*time.Second),
		)
		t.Logf("Pre-upgrade config hash: %q", preUpgradeHash)

		// Upgrade the HyperShift Operator. The karpenter-operator ships as part of the HO image,
		// so this upgrades the karpenter-operator to the version where kubelet fields are typed.
		t.Logf("Upgrading HyperShift Operator to %s", globalOpts.HyperShiftOperatorLatestImage)
		installOptions := globalOpts.HOInstallationOptions
		installOptions.HyperShiftOperatorLatestImage = globalOpts.HyperShiftOperatorLatestImage
		g.Expect(e2eutil.InstallHyperShiftOperator(ctx, installOptions)).To(gomega.Succeed())
		t.Logf("HyperShift Operator upgrade complete")

		// The config hash must remain identical after the upgrade. If it changes, the field
		// promotion altered the rendered ignition config and nodes would roll unnecessarily.
		t.Logf("Verifying config hash is stable after HO upgrade (no node roll)")
		g.Consistently(func(g gomega.Gomega) {
			updated := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
			g.Expect(guestClient.Get(ctx, crclient.ObjectKey{Name: nc.Name}, updated)).To(gomega.Succeed())
			hash := updated.GetAnnotations()[nodeClassCurrentConfigVersionAnnotation]
			g.Expect(hash).To(gomega.Equal(preUpgradeHash),
				"config hash changed after HO upgrade — kubelet field promotion triggered an unexpected node roll")
		}, "2m", "5s")
		t.Logf("Config hash stable after upgrade: no node roll triggered")

		// Simulate a user doing `kubectl apply` on their existing YAML (e.g. adding a label).
		// The config hash must not change — round-trip through the new typed API must be identity.
		t.Logf("Performing no-op metadata update to simulate user round-trip")
		err := e2eutil.UpdateObject(t, ctx, guestClient, nc, func(obj *hyperkarpenterv1.OpenshiftEC2NodeClass) {
			if obj.Annotations == nil {
				obj.Annotations = make(map[string]string)
			}
			obj.Annotations["test/updated"] = "true"
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())

		t.Logf("Verifying config hash is stable after no-op update")
		g.Consistently(func(g gomega.Gomega) {
			updated := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
			g.Expect(guestClient.Get(ctx, crclient.ObjectKey{Name: nc.Name}, updated)).To(gomega.Succeed())
			hash := updated.GetAnnotations()[nodeClassCurrentConfigVersionAnnotation]
			g.Expect(hash).To(gomega.Equal(preUpgradeHash),
				"config hash changed after no-op metadata update — round-trip through new API broke config identity")
		}, "1m", "5s")
		t.Logf("Config hash stable after no-op update: round-trip is safe")

		// Sanity check: a real kubelet config change must update the hash.
		// This proves the signal is working and not just stuck.
		t.Logf("Verifying that a real kubelet config change does update the hash (sanity check)")
		err = e2eutil.UpdateObject(t, ctx, guestClient, nc, func(obj *hyperkarpenterv1.OpenshiftEC2NodeClass) {
			obj.Spec.Kubelet.MaxPods++
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())

		e2eutil.EventuallyObject(t, ctx, "config hash to change after real kubelet config update",
			func(ctx context.Context) (*hyperkarpenterv1.OpenshiftEC2NodeClass, error) {
				updated := &hyperkarpenterv1.OpenshiftEC2NodeClass{}
				err := guestClient.Get(ctx, crclient.ObjectKey{Name: nc.Name}, updated)
				return updated, err
			},
			[]e2eutil.Predicate[*hyperkarpenterv1.OpenshiftEC2NodeClass]{
				func(nc *hyperkarpenterv1.OpenshiftEC2NodeClass) (done bool, reasons string, err error) {
					hash := nc.GetAnnotations()[nodeClassCurrentConfigVersionAnnotation]
					if hash == preUpgradeHash {
						return false, "hash not yet updated", nil
					}
					return true, fmt.Sprintf("hash updated to %q", hash), nil
				},
			},
			e2eutil.WithTimeout(2*time.Minute),
			e2eutil.WithInterval(5*time.Second),
		)
		t.Logf("Config hash correctly updated after real kubelet change — signal is working")
	}).WithHOUpgrade().Execute(&clusterOpts, globalOpts.Platform, globalOpts.ArtifactDir, "karpenter-kubelet-upgrade", globalOpts.ServiceAccountSigningKey)
}
