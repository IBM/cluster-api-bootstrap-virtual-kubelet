/*
Copyright 2024.

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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cluster-api/util/conditions"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	//metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	//"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	bsutil "sigs.k8s.io/cluster-api/bootstrap/util"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"

	//"sigs.k8s.io/cluster-api/util/secret"
	"text/template"
	"time"

	"sigs.k8s.io/cluster-api/controllers/clustercache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bootstrapclusterxk8siov1alpha1 "github.com/ibm/cluster-api-bootstrap-virtual-kubelet/api/v1alpha1"
	bootstrapv1 "sigs.k8s.io/cluster-api/api/bootstrap/kubeadm/v1beta2"
)

const (
	templateCloudInit = `{{.Header}}
{{template "files" .WriteFiles}}
runcmd:
{{- template "commands" .Commands }}
{{- template "users" .Users }}
`
	cloudConfigHeader = `## template: jinja
#cloud-config
`
	filesTemplate = `{{ define "files" -}}
write_files:{{ range . }}
-   path: {{.Path}}
    {{ if ne .Encoding "" -}}
    encoding: "{{.Encoding}}"
    {{ end -}}
    {{ if ne .Owner "" -}}
    owner: {{.Owner}}
    {{ end -}}
    {{ if ne .Permissions "" -}}
    permissions: '{{.Permissions}}'
    {{ end -}}
    {{ if .Append -}}
    append: true
    {{ end -}}
    content: |
{{.Content | Indent 6}}
{{- end -}}
{{- end -}}
`
	commandsTemplate = `{{- define "commands" -}}
{{ range . }}
  - {{printf "%q" .}}
{{- end -}}
{{- end -}}
`
	usersTemplate = `{{ define "users" -}}
{{- if . }}
users:{{ range . }}
  - name: {{ .Name }}
    {{- if .Passwd }}
    passwd: {{ .Passwd }}
    {{- end -}}
    {{- if .Gecos }}
    gecos: {{ .Gecos }}
    {{- end -}}
    {{- if .Groups }}
    groups: {{ .Groups }}
    {{- end -}}
    {{- if .HomeDir }}
    homedir: {{ .HomeDir }}
    {{- end -}}
    {{- if .Inactive }}
    inactive: true
    {{- end -}}
    {{- if .LockPassword }}
    lock_passwd: {{ .LockPassword }}
    {{- end -}}
    {{- if .Shell }}
    shell: {{ .Shell }}
    {{- end -}}
    {{- if .PrimaryGroup }}
    primary_group: {{ .PrimaryGroup }}
    {{- end -}}
    {{- if .Sudo }}
    sudo: {{ .Sudo }}
    {{- end -}}
    {{- if .SSHAuthorizedKeys }}
    ssh_authorized_keys:{{ range .SSHAuthorizedKeys }}
      - {{ . }}
    {{- end -}}
    {{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
`
)

var (
	defaultTemplateFuncMap = template.FuncMap{
		"Indent": templateYAMLIndent,
	}
)

func templateYAMLIndent(i int, input string) string {
	split := strings.Split(input, "\n")
	ident := "\n" + strings.Repeat(" ", i)
	return strings.Repeat(" ", i) + strings.Join(split, ident)
}

// Scope is a scoped struct used during reconciliation.
type Scope struct {
	logr.Logger
	Config      *bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig
	ConfigOwner *bsutil.ConfigOwner
	Cluster     *clusterv1.Cluster
}

// VirtualKubeletConfigReconciler reconciles a VirtualKubeletConfig object
type VirtualKubeletConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=virtualkubeletconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=virtualkubeletconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=virtualkubeletconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status;machinesets;machines;machines/status;machinepools;machinepools/status,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VirtualKubeletConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.15.0/pkg/reconcile
func (r *VirtualKubeletConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	fmt.Println("reconcile called...")
	log := ctrl.LoggerFrom(ctx)

	r.Scheme.AddKnownTypes(clusterv1.GroupVersion,
		&clusterv1.Cluster{},
		&clusterv1.ClusterList{},
		&clusterv1.Machine{},
		&clusterv1.MachineList{})

	metav1.AddToGroupVersion(r.Scheme, clusterv1.GroupVersion)

	config := &bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig{}
	if err := r.Client.Get(ctx, req.NamespacedName, config); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	//fmt.println("name:", config.getname())
	//fmt.Println("namespace:", config.GetNamespace())

	log = log.WithValues(config.GetNamespace(), config.GetName(), "resourceVersion", config.GetResourceVersion())

	/*
				var data []byte
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      config.Name,
						Namespace: config.Namespace,
					},
					Data: map[string][]byte{
						"value":  data,
						"format": []byte(config.Spec.Format),
					},
					Type: clusterv1.ClusterSecretType,
				}

			// as secret creation and scope.Config status patch are not atomic operations
			// it is possible that secret creation happens but the config.Status patches are not applied
			if err := r.Client.Create(ctx, secret); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return ctrl.Result{}, errors.Wrapf(err, "failed to create bootstrap data secret %s/%s", scope.Config.Namespace, scope.Config.Name)
				}
				log.Info("Bootstrap data secret already exists, updating", "Secret", klog.KObj(secret))
				if err := r.Client.Update(ctx, secret); err != nil {
					return ctrl.Result{}, errors.Wrapf(err, "failed to update bootstrap data secret %s/%s", scope.Config.Namespace, scope.Config.Name)
				}
			}
		scope.Config.Status.DataSecretName = ptr.To(secret.Name)
		scope.Config.Status.Ready = true
	*/

	//log := ctrl.LoggerFrom(ctx)
	//log = log.WithValues(configOwner.GetKind(), klog.KRef(configOwner.GetNamespace(), configOwner.GetName()), "resourceVersion", configOwner.GetResourceVersion())
	//ctx = ctrl.LoggerInto(ctx, log)

	configOwner, err := bsutil.GetTypedConfigOwner(ctx, r.Client, config)

	if err != nil {
		if apierrors.IsNotFound(err) {
			// Could not find the owner yet, this is not an error and will rereconcile when the owner gets set.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, errors.Wrapf(err, "failed to get owner")
	}
	if configOwner == nil {
		return ctrl.Result{}, nil
	}

	log.Info("owner", "config namespace: ", fmt.Sprintln(configOwner.GetNamespace()))
	log.Info("owner", "config cluster name: ", config.Labels["cluster.x-k8s.io/cluster-name"])

	log = log.WithValues(configOwner.GetKind(), klog.KRef(configOwner.GetNamespace(), configOwner.GetName()), "resourceVersion", configOwner.GetResourceVersion())
	ctx = ctrl.LoggerInto(ctx, log)

	log = log.WithValues("Cluster", klog.KRef(configOwner.GetNamespace(), configOwner.ClusterName()))
	ctx = ctrl.LoggerInto(ctx, log)

	// Lookup the cluster the config owner is associated with
	cluster, err := util.GetClusterByName(ctx, r.Client, configOwner.GetNamespace(), configOwner.ClusterName())
	if err != nil {
		if errors.Cause(err) == util.ErrNoCluster {
			log.Info(fmt.Sprintf("%s does not belong to a cluster yet, waiting until it's part of a cluster", configOwner.GetKind()))
			return ctrl.Result{}, nil
		}

		if apierrors.IsNotFound(err) {
			log.Info("Cluster does not exist yet, waiting until it is created")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Could not get cluster with metadata")
		return ctrl.Result{}, err
	}

	if annotations.IsPaused(cluster, config) {
		log.Info("Reconciliation is paused for this object")
		return ctrl.Result{}, nil
	}
	scope := &Scope{
		Logger:      log,
		Config:      config,
		ConfigOwner: configOwner,
		Cluster:     cluster,
	}

	res, err := r.reconcile(ctx, scope, cluster, config, configOwner)
	if err != nil && errors.Is(err, clustercache.ErrClusterNotConnected) {
		// Requeue if the reconcile failed because the ClusterCacheTracker was locked for
		// the current cluster because of concurrent access.
		log.V(5).Info("Requeuing because another worker has the lock on the ClusterCacheTracker")
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	return res, err
}

// BaseUserData is shared across all the various types of files written to disk.
type BootstrapInput struct {
	Header string
	//PreKubeadmCommands   []string
	Commands        []string
	AdditionalFiles []bootstrapv1.File
	WriteFiles      []bootstrapv1.File
	Users           []bootstrapv1.User
	//NTP                  *bootstrapv1.NTP
	//DiskSetup            *bootstrapv1.DiskSetup
	//Mounts               []bootstrapv1.MountPoints
	//ControlPlane         bool
	//UseExperimentalRetry bool
	//KubeadmCommand       string
	//KubeadmVerbosity     string
	//SentinelFileCommand  string
}

func (r *VirtualKubeletConfigReconciler) reconcile(ctx context.Context, scope *Scope, cluster *clusterv1.Cluster, config *bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig, configOwner *bsutil.ConfigOwner) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)
	log.Info("running reconcile....")

	switch {
	// Wait for the infrastructure to be ready.
	//case cluster.Status.Conditions[clusterv1.ClusterInfrastructureReadyStatus].Status == metav1.ConditionFalse:
	case !conditions.IsTrue(cluster, clusterv1.ClusterInfrastructureReadyCondition):
		log.Info("Cluster infrastructure is not ready, waiting")
		//conditions.MarkFalse(config, bootstrapv1.DataSecretAvailableCondition, bootstrapv1.WaitingForClusterInfrastructureReason, clusterv1.ConditionSeverityInfo, "")
		return ctrl.Result{}, nil

	//case configOwner.DataSecretName() != nil && (!config.Status.Ready || config.Status.DataSecretName == nil):
	case config.Status.Ready && config.Status.DataSecretName != nil:
		log.Info("datasecret created and config ready")

		//conditions.MarkTrue(config, bootstrapv1.DataSecretAvailableCondition)
		return ctrl.Result{}, nil
	}

	scope.Config.Spec.Files = append(scope.Config.Spec.Files,
		bootstrapv1.File{
			ContentFrom: bootstrapv1.FileSource{
				Secret: bootstrapv1.SecretFileSource{
					//configOwner.ClusterName()-kubeconfig
					Name: cluster.Name + "-kubeconfig",
					Key:  "value",
				},
			},
			Path:        "/etc/kubernetes/kubeconfig",
			Owner:       "root:root",
			Permissions: "0644",
		},
		bootstrapv1.File{
			ContentFrom: bootstrapv1.FileSource{
				Secret: bootstrapv1.SecretFileSource{
					//configOwner.ClusterName()-kubeconfig
					Name: cluster.Name + "-ca",
					Key:  "tls.crt",
				},
			},
			Path:        "/etc/kubernetes/pki/ca.crt",
			Owner:       "root:root",
			Permissions: "0644",
		},
		bootstrapv1.File{
			ContentFrom: bootstrapv1.FileSource{
				Secret: bootstrapv1.SecretFileSource{
					//configOwner.ClusterName()-kubeconfig
					Name: cluster.Name + "-ca",
					Key:  "tls.key",
				},
			},
			Path:        "/etc/kubernetes/pki/ca.key",
			Owner:       "root:root",
			Permissions: "0600",
		},
		bootstrapv1.File{
			Path:        "/etc/virtual-kubelet/vkubelet-cfg.json",
			Owner:       "root:root",
			Permissions: "0644",
			Content: `
      {
        "{{ ds.meta_data.local_hostname }}": {
        "cpu": "0",
        "memory": "0Gi",
        "pods": "0",
        "labels":{
          "metal3.io/uuid":"{{ ds.meta_data.uuid }}"
        }
        }
      }`,
		},
		bootstrapv1.File{
			Path:        "/lib/systemd/system/virtual-kubelet.service",
			Owner:       "root:root",
			Permissions: "0640",
			Content: fmt.Sprintf(`
[Unit]
Description=virtual-kubelet
[Service]
Environment=KUBECONFIG=/etc/kubernetes/kubeconfig
Environment=APISERVER_CERT_LOCATION=/etc/kubernetes/pki/ca.crt
Environment=APISERVER_KEY_LOCATION=/etc/kubernetes/pki/ca.key
Environment=KUBERNETES_SERVICE_HOST=%s
Environment=KUBERNETES_SERVICE_PORT=%d
ExecStart=/usr/local/bin/virtual-kubelet %s --nodename "{{ ds.meta_data.local_hostname }}" --provider-config /etc/virtual-kubelet/vkubelet-cfg.json  --kubeconfig /etc/kubernetes/kubeconfig
[Install]
WantedBy=multi-user.target`,
				cluster.Spec.ControlPlaneEndpoint.Host,
				cluster.Spec.ControlPlaneEndpoint.Port,
				strings.Join(scope.Config.Spec.VirtualKubelet.Args, " ")),
		},
	)

	//files, err := r.resolveFiles(ctx, scope.Config)
	files, err := r.resolveFiles(ctx, scope.Config)
	if err != nil {
		return ctrl.Result{}, err
	}

	users, err := r.resolveUsers(ctx, scope.Config)
	if err != nil {
		return ctrl.Result{}, err
	}

	//cmds
	var commands = scope.Config.Spec.Commands
	if scope.Config.Spec.VirtualKubelet.Url != "" {
		commands = append(commands,
			"curl "+scope.Config.Spec.VirtualKubelet.Url+" --output /usr/local/bin/virtual-kubelet",
			"chmod a+x /usr/local/bin/virtual-kubelet",
			"systemctl enable --now virtual-kubelet",
		)

	}

	input := BootstrapInput{
		WriteFiles: files,
		Users:      users,
		Header:     cloudConfigHeader,
		Commands:   commands,
	}

	//log.Info("log", "files:", files)

	input.Header = cloudConfigHeader
	userData, err := generate("InitConfig", templateCloudInit, input)
	if err != nil {
		return ctrl.Result{}, err
	}

	dataSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: config.Namespace,
		},
		Data: map[string][]byte{
			"value":  userData,
			"format": []byte(config.Spec.Format),
		},
		Type: clusterv1.ClusterSecretType,
	}

	// as secret creation and scope.Config status patch are not atomic operations
	// it is possible that secret creation happens but the config.Status patches are not applied
	err = r.Client.Create(ctx, dataSecret)
	log.Info("datasecret created...")

	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, errors.Wrapf(err, "failed to create bootstrap data secret %s/%s", scope.Config.Namespace, scope.Config.Name)
		}
		//log.Info("Bootstrap data secret already exists, updating", "Secret", klog.KObj(dataSecret))
		if err := r.Client.Update(ctx, dataSecret); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to update bootstrap data secret %s/%s", scope.Config.Namespace, scope.Config.Name)
		}
	}
	config.Status.DataSecretName = ptr.To(dataSecret.Name)
	config.Status.Ready = true
	config.Status.Initialization.DataSecretCreated = true

	log.Info("updating datasecret...")
	err = r.Client.Status().Update(context.TODO(), config)
	log.Info("updated datasecret...")
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}
	return ctrl.Result{}, nil
}

func generate(kind string, tpl string, data interface{}) ([]byte, error) {
	tm := template.New(kind).Funcs(defaultTemplateFuncMap)
	if _, err := tm.Parse(filesTemplate); err != nil {
		return nil, errors.Wrap(err, "failed to parse files template")
	}

	if _, err := tm.Parse(commandsTemplate); err != nil {
		return nil, errors.Wrap(err, "failed to parse commands template")
	}

	if _, err := tm.Parse(usersTemplate); err != nil {
		return nil, errors.Wrap(err, "failed to parse users template")
	}

	/*
			if _, err := tm.Parse(ntpTemplate); err != nil {
				return nil, errors.Wrap(err, "failed to parse ntp template")
			}


			if _, err := tm.Parse(diskSetupTemplate); err != nil {
				return nil, errors.Wrap(err, "failed to parse disk setup template")
			}

			if _, err := tm.Parse(fsSetupTemplate); err != nil {
				return nil, errors.Wrap(err, "failed to parse fs setup template")
			}

		if _, err := tm.Parse(mountsTemplate); err != nil {
			return nil, errors.Wrap(err, "failed to parse mounts template")
		}
	*/

	t, err := tm.Parse(tpl)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse %s template", kind)
	}

	var out bytes.Buffer
	if err := t.Execute(&out, data); err != nil {
		return nil, errors.Wrapf(err, "failed to generate %s template", kind)
	}

	return out.Bytes(), nil
}

// resolveUsers maps .Spec.Users into cloudinit.Users, resolving any object references
// along the way.
func (r *VirtualKubeletConfigReconciler) resolveUsers(ctx context.Context, cfg *bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig) ([]bootstrapv1.User, error) {
	collected := make([]bootstrapv1.User, 0, len(cfg.Spec.Users))

	for i := range cfg.Spec.Users {
		in := cfg.Spec.Users[i]
		if in.PasswdFrom.IsDefined() {
			data, err := r.resolveSecretPasswordContent(ctx, cfg.Namespace, in)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to resolve passwd source")
			}
			//in.PasswdFrom = nil
			passwdContent := string(data)
			in.Passwd = passwdContent
		}
		collected = append(collected, in)
	}

	return collected, nil
}

// resolveFiles maps .Spec.Files into cloudinit.Files, resolving any object references
// along the way.
func (r *VirtualKubeletConfigReconciler) resolveFiles(ctx context.Context, cfg *bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig) ([]bootstrapv1.File, error) {
	collected := make([]bootstrapv1.File, 0, len(cfg.Spec.Files))

	for i := range cfg.Spec.Files {
		in := cfg.Spec.Files[i]
		if in.ContentFrom.IsDefined() {
			data, err := r.resolveSecretFileContent(ctx, cfg.Namespace, in)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to resolve file source")
			}
			//in.ContentFrom = nil
			in.Content = string(data)
		}
		collected = append(collected, in)
	}

	return collected, nil
}

// resolveSecretFileContent returns file content fetched from a referenced secret object.
func (r *VirtualKubeletConfigReconciler) resolveSecretFileContent(ctx context.Context, ns string, source bootstrapv1.File) ([]byte, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: ns, Name: source.ContentFrom.Secret.Name}
	if err := r.Client.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "secret not found: %s", key)
		}
		return nil, errors.Wrapf(err, "failed to retrieve Secret %q", key)
	}
	data, ok := secret.Data[source.ContentFrom.Secret.Key]
	if !ok {
		return nil, errors.Errorf("secret references non-existent secret key: %q", source.ContentFrom.Secret.Key)
	}
	return data, nil
}

// resolveSecretUserContent returns passwd fetched from a referenced secret object.
func (r *VirtualKubeletConfigReconciler) resolveSecretPasswordContent(ctx context.Context, ns string, source bootstrapv1.User) ([]byte, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: ns, Name: source.PasswdFrom.Secret.Name}
	if err := r.Client.Get(ctx, key, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.Wrapf(err, "secret not found: %s", key)
		}
		return nil, errors.Wrapf(err, "failed to retrieve Secret %q", key)
	}
	data, ok := secret.Data[source.PasswdFrom.Secret.Key]
	if !ok {
		return nil, errors.Errorf("secret references non-existent secret key: %q", source.PasswdFrom.Secret.Key)
	}
	return data, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *VirtualKubeletConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bootstrapclusterxk8siov1alpha1.VirtualKubeletConfig{}).
		Complete(r)
}
