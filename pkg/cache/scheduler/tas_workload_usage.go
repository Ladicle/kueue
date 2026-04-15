package scheduler

import (
	"k8s.io/apimachinery/pkg/util/sets"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/workload"
)

func workloadUsageForCQ(wi *workload.Info, cq *ClusterQueueSnapshot) workload.Usage {
	usage := wi.Usage()
	usage.TAS = workloadTASUsageForFlavorNames(wi, sets.KeySet(cq.TASFlavors))
	return usage
}

func workloadTASUsageForFlavorNames(wi *workload.Info, tasFlavors sets.Set[kueue.ResourceFlavorReference]) workload.TASUsage {
	if !features.Enabled(features.TopologyAwareScheduling) || wi == nil || !wi.IsUsingTAS() {
		return nil
	}

	result := make(workload.TASUsage)
	singleTASFlavor, hasSingleTASFlavor := singleTASFlavor(tasFlavors)
	for _, ps := range wi.TotalRequests {
		if ps.TopologyRequest == nil {
			continue
		}

		flavors := tasFlavorsForPodSet(ps, tasFlavors, singleTASFlavor, hasSingleTASFlavor)
		if len(flavors) == 0 {
			continue
		}

		singlePodRequests := singlePodRequestsForPodSet(wi.Obj.Spec.PodSets, ps.Name, ps.SinglePodRequests())
		for _, tasFlavor := range flavors {
			for _, domain := range ps.TopologyRequest.DomainRequests {
				result[tasFlavor] = append(result[tasFlavor], workload.TopologyDomainRequests{
					Values:            domain.Values,
					SinglePodRequests: singlePodRequests.Clone(),
					Count:             domain.Count,
				})
			}
		}
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func tasFlavorsForPodSet(ps workload.PodSetResources, tasFlavors sets.Set[kueue.ResourceFlavorReference], singleFlavor kueue.ResourceFlavorReference, hasSingleFlavor bool) []kueue.ResourceFlavorReference {
	seen := sets.New[kueue.ResourceFlavorReference]()
	flavors := make([]kueue.ResourceFlavorReference, 0, len(ps.Flavors))
	for _, flavor := range ps.Flavors {
		if tasFlavors.Has(flavor) && !seen.Has(flavor) {
			seen.Insert(flavor)
			flavors = append(flavors, flavor)
		}
	}
	if len(flavors) == 0 && hasSingleFlavor {
		return []kueue.ResourceFlavorReference{singleFlavor}
	}
	return flavors
}

func singleTASFlavor(tasFlavors sets.Set[kueue.ResourceFlavorReference]) (kueue.ResourceFlavorReference, bool) {
	if tasFlavors.Len() != 1 {
		return "", false
	}
	for flavor := range tasFlavors {
		return flavor, true
	}
	return "", false
}

func singlePodRequestsForPodSet(specPodSets []kueue.PodSet, name kueue.PodSetReference, fallback resources.Requests) resources.Requests {
	for i := range specPodSets {
		if specPodSets[i].Name == name {
			return resources.NewRequestsFromPodSpec(&specPodSets[i].Template.Spec)
		}
	}
	return fallback.Clone()
}
