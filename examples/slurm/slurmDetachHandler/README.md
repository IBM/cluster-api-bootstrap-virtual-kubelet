# Slurm Detach Handler 

The deprovisioning process requires more involvement from the control plane side. Scale down the number of replicas in the MachineDeployment controlling the Slurm workers without first cordoning and draining server would leave the cluster in an inconsistent state.

- [client.go](./client/client.go):
An example of the process performed by the operator to communicate with the node selected for deprovisioning.

- [server.go](./server/server.go):
A service running on each Slurm computing node that listens for detaching requests.

- [slurm-operator](./slurm-operator/): 
This is the heart of the deprovisioning. 
1) It adds the `pre-drain.delete.hook.machine.cluster.x-k8s.io` annotation to the Slurm computing nodes when they are provisioning 
2) It uses the `client.go` logic to comunicate with the service running on the node during the deprovisioning phase.  