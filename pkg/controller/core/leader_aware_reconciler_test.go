/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type stubReconciler struct {
	calls  int
	result ctrl.Result
	err    error
}

func (r *stubReconciler) Reconcile(context.Context, ctrl.Request) (ctrl.Result, error) {
	r.calls++
	return r.result, r.err
}

func newFollowerLeaderAwareReconciler(cl client.Client, delegate reconcile.Reconciler, requeueAfter time.Duration) *leaderAwareReconciler {
	return &leaderAwareReconciler{
		elected:         make(chan struct{}),
		client:          cl,
		delegate:        delegate,
		object:          &kueue.Workload{},
		requeueDuration: requeueAfter,
	}
}

func TestLeaderAwareReconcilerRequeuesWhileFollowerForExistingObject(t *testing.T) {
	workloadObj := utiltestingapi.MakeWorkload("wl", "ns").Obj()
	cl := utiltesting.NewClientBuilder().WithObjects(workloadObj).Build()
	delegate := &stubReconciler{}
	requeueAfter := time.Minute

	r := newFollowerLeaderAwareReconciler(cl, delegate, requeueAfter)

	gotResult, gotErr := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(workloadObj),
	})

	if gotErr != nil {
		t.Fatalf("Reconcile() error = %v, want nil", gotErr)
	}
	if diff := cmp.Diff(ctrl.Result{RequeueAfter: requeueAfter}, gotResult); diff != "" {
		t.Fatalf("Reconcile() result mismatch (-want,+got):\n%s", diff)
	}
	if delegate.calls != 0 {
		t.Fatalf("delegate calls = %d, want 0", delegate.calls)
	}
}

func TestLeaderAwareReconcilerDelegatesNotFoundToWorkloadCleanup(t *testing.T) {
	clientBuilder := utiltesting.NewClientBuilder().
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
	cl := clientBuilder.Build()
	recorder := &utiltesting.EventRecorder{}
	cqCache := schdcache.New(cl)
	queueOptions := []qcache.Option{qcache.WithPreemptionExpectations(preemptexpectations.New())}
	qManager := qcache.NewManagerForUnitTests(cl, cqCache, queueOptions...)
	reconciler := NewWorkloadReconciler(cl, qManager, cqCache, recorder)

	ctx, log := utiltesting.ContextWithLog(t)

	cq := utiltestingapi.MakeClusterQueue("cq").Obj()
	lq := utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue("cq").Obj()
	if err := errors.Join(cqCache.AddClusterQueue(ctx, cq.DeepCopy()), qManager.AddClusterQueue(ctx, cq.DeepCopy())); err != nil {
		t.Fatalf("add ClusterQueue: %v", err)
	}
	if err := errors.Join(cqCache.AddLocalQueue(lq.DeepCopy()), qManager.AddLocalQueue(ctx, lq.DeepCopy())); err != nil {
		t.Fatalf("add LocalQueue: %v", err)
	}

	pendingWL := utiltestingapi.MakeWorkload("wl", "ns").Queue("lq").Obj()
	if err := qManager.AddOrUpdateWorkload(log, pendingWL.DeepCopy()); err != nil {
		t.Fatalf("add queue workload: %v", err)
	}

	admittedWL := utiltestingapi.MakeWorkload("wl", "ns").
		Queue("lq").
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").Obj(), time.Now()).
		Obj()
	if !cqCache.AddOrUpdateWorkload(log, admittedWL.DeepCopy()) {
		t.Fatal("add scheduler workload returned false")
	}

	wlKey := workload.Key(admittedWL)
	if qManager.GetWorkloadFromCache(wlKey) == nil {
		t.Fatal("workload missing from queue cache before reconcile")
	}
	if cqCache.GetWorkloadFromCache(wlKey) == nil {
		t.Fatal("workload missing from scheduler cache before reconcile")
	}

	r := newFollowerLeaderAwareReconciler(cl, reconciler, time.Minute)

	gotResult, gotErr := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(admittedWL),
	})

	if gotErr != nil {
		t.Fatalf("Reconcile() error = %v, want nil", gotErr)
	}
	if diff := cmp.Diff(ctrl.Result{}, gotResult); diff != "" {
		t.Fatalf("Reconcile() result mismatch (-want,+got):\n%s", diff)
	}
	if qManager.GetWorkloadFromCache(wlKey) != nil {
		t.Fatal("workload still present in queue cache after reconcile")
	}
	if cqCache.GetWorkloadFromCache(wlKey) != nil {
		t.Fatal("workload still present in scheduler cache after reconcile")
	}
}
