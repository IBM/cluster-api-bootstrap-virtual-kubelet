
### Tutorial



Install the Metal3 Dev Env
```bash
git clone https://github.com/metal3-io/metal3-dev-env.gitcd metal3-dev-env/

cat > config_${USER}.sh << EOF
export CLUSTER_NAME="workload-cluster"
export TARGET_NODE_MEMORY=4096
export SSH_PUB_KEY=~/.ssh/id_rsa.pub

export NUM_NODES=5
export CONTROL_PLANE_MACHINE_COUNT=3
export WORKER_MACHINE_COUNT=0
export BMC_DRIVER="redfish"

export EXTRA_NETWORK_NAMES="nmstate1 nmstate2"
export NMSTATE1_NETWORK_SUBNET_V4='192.168.221.0/24'
export NMSTATE1_NETWORK_SUBNET_V6='fd2e:6f44:5dd8:ca56::/120'
export NMSTATE2_NETWORK_SUBNET_V4='192.168.222.0/24'
export NMSTATE2_NETWORK_SUBNET_V6='fd2e:6f44:5dd8:cc56::/120'
EOF

make
```

Provision a Kubernetes cluster and related control plane via Cluster API

```bash
./tests/scripts/provision/cluster.sh
./tests/scripts/provision/controlplane.sh
```


Make the virtual Kubelet available over the network
```bash
git clone  git@github.com:mgazz/virtual-kubelet.git
cd virtual-kubelet
git checkout capi
make build
cp bin/virtual-kubelet /opt/metal3-dev-env/ironic/html/
chmod a+rwx /opt/metal3-dev-env/ironic/html/virtual-kubelet
```



Download and build the Cluster API Bootstrap Provider Virtual Kubelet (CBPV)
```bash
git clone git@github.com:IBM/cluster-api-bootstrap-virtual-kubelet.git
cd cluster-api-bootstrap-virtual-kubelet/

### Build provider
make docker-build
make docker-push
make build-installer
kubectl apply -f dist/install.yaml
```

Install the Slurm Detach Handler
```
cd examples/slurm/slurmDetachHandler/slurm-operator/
make docker-build
make docker-push
make build-installer
kubectl apply -f dist/install.yaml
cd -
```


Add the `sshAuthorizedKeys` to both 
[slurm-head.yaml](./examples/slurm/machineDeployments/slurm-head.yaml)
and
[slurm-worker.yaml](./examples/slurm/machineDeployments/slurm-worker.yaml)

```yaml
users:
- name: hpcuser
    sudo: ALL=(ALL) NOPASSWD:ALL
    sshAuthorizedKeys:
    - <SSH_PUBLIC_KEY>

```


Deploy the Slurm head node
```bash
kubectl apply -f examples/slurm/machineDeployments/slurm-head.yaml
```


Deploy the Slurm worker nodes
```bash
kubectl apply -f examples/slurm/machineDeployments/slurm-worker.yaml
```
