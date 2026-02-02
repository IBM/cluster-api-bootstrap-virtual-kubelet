/*
Copyright 2025.

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

package controller

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1 "github.com/metal3-io/cluster-api-provider-metal3/api/v1beta1"
	cluster "sigs.k8s.io/cluster-api/api/core/v1beta1"
)

// MachineReconciler reconciles a Machine object
type MachineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status;machinepools;machinepools/status,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=metal3machines,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Machine object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *MachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	machine := &cluster.Machine{}
	if err := r.Client.Get(ctx, req.NamespacedName, machine); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	annotationKey := "pre-drain.delete.hook.machine.cluster.x-k8s.io"
	annotationVal := "drain-slurm"

	if machine.Status.Phase == "Provisioning" && strings.Contains(machine.ObjectMeta.Name, "slurm-worker") && (machine.Annotations == nil || machine.Annotations[annotationKey] == "") {
		logger.Info("Provision a machine")
		// Add annotation to wait the drainig before deprovisioning
		if machine.Annotations == nil {
			machine.Annotations = make(map[string]string)
		}
		// machine.Annotations["pre-drain.delete.hook.machine.cluster.x-k8s.io"] = "drain-slurm"
		machine.Annotations[annotationKey] = annotationVal

		err := r.Client.Update(context.TODO(), machine)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if machine.ObjectMeta.DeletionTimestamp != nil && machine.Annotations != nil && strings.Contains(machine.ObjectMeta.Name, "slurm-worker") {
		logger.Info("Deprovisioning a machine")
		//TODO: get the metal3machine
		logger.Info("Getting metal3 machine")
		metal3machine := &infrav1.Metal3Machine{}
		if err := r.Client.Get(ctx, req.NamespacedName, metal3machine); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}

		// HTTP request to detch node from the cluster
		regex := regexp.MustCompile(`[0-9]{1,3}\.[0-9]{1,3}\.[2]{3}\.[0-9]{1,3}`)
		var externalNicIndex int
		for i, nic := range metal3machine.Status.Addresses {
			if regex.MatchString(nic.Address) {
				externalNicIndex = i
			}
		}

                // reading from metal3machine and not from machine since a deprovisioning
                // request may arrive before Addresses get propagated to machine
		node := metal3machine.Status.Addresses[externalNicIndex].Address
		mode := "detach"
		url := fmt.Sprintf("http://%s:8090/%s", node, mode)
		resp, err := http.Get(url)

		if err != nil || resp.StatusCode != 200 {
			fmt.Println("Cannot detach")
			fmt.Println(err)
			return ctrl.Result{}, err
		}

		// Remove annotation to continue deprovisioning
		delete(machine.Annotations, "pre-drain.delete.hook.machine.cluster.x-k8s.io")
		err = r.Client.Update(context.TODO(), machine)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MachineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cluster.Machine{}).
		Complete(r)
}
