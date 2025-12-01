package main

import (
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

func hello(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "hello\n")
}

func getHostname() string {
	cmd := exec.Command("hostname")
	stdout, _ := cmd.Output()
	hostname := strings.TrimSuffix(string(stdout), "\n")
	fmt.Printf("Detacting %s from slurm cluster\n", hostname)
	return hostname
}

func deleteNodeFromCluster(hostname string) {
	cmd := exec.Command("sudo", "scontrol", "delete", "nodename", hostname)
	stdout, err := cmd.Output()
	if err != nil {
		fmt.Print(string(stdout))
		fmt.Println("Cannot detach: " + err.Error())
		return
	}
	fmt.Println(string(stdout))
	fmt.Println("Detached from Slurm")
}

func detachFromSlurmcluster(w http.ResponseWriter, req *http.Request) {
	hostname := getHostname()

	fmt.Println("Forcing")
	cmd := exec.Command("sudo", "scontrol", "update", "NodeName="+hostname, "State=DOWN", "Reason=deprovision")
	stdout, err := cmd.Output()
	if err != nil {
		fmt.Print(string(stdout))
		fmt.Println("Cannot Drain: " + err.Error())
		return
	}

	deleteNodeFromCluster(hostname)
}

func drainAndDetachFromSlurmcluster(w http.ResponseWriter, req *http.Request) {
	hostname := getHostname()

	fmt.Println("Draining")
	cmd := exec.Command("sudo", "scontrol", "update", "NodeName="+hostname, "State=DRAIN", "Reason=deprovision")
	stdout, err := cmd.Output()
	if err != nil {
		fmt.Print(string(stdout))
		fmt.Println("Cannot Drain: " + err.Error())
		return
	}

	regex, _ := regexp.Compile("State=IDLE\\+DRAIN")
	for {
		cmd = exec.Command("bash", "-c", "sudo scontrol show node "+hostname)
		stdout, err = cmd.Output()
		if err != nil {
			fmt.Print(string(stdout))
			fmt.Println("Cannot get node status: " + err.Error())
			return
		}
		fmt.Print(string(stdout))

		match := regex.Match(stdout)
		if match {
			break
		}

		fmt.Println("Node is busy, waiting")
		time.Sleep(1 * time.Second)
	}

	deleteNodeFromCluster(hostname)
}

func main() {
	http.HandleFunc("/hello", hello)
	http.HandleFunc("/detach", detachFromSlurmcluster)
	http.HandleFunc("/drain", drainAndDetachFromSlurmcluster)
	http.ListenAndServe(":8090", nil)
}
