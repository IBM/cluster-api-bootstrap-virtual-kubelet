package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	node := os.Args[1]
	mode := os.Args[2]
	fmt.Printf("Detaching node %s from slurm with mode %s\n", node, mode)

	if mode != "drain" && mode != "detach" {
		fmt.Println("Error: Usage client [node-address] [detach|drain]")
		return
	}

	url := fmt.Sprintf("http://%s:8090/%s", node, mode)
	resp, err := http.Get(url)

	if err != nil || resp.StatusCode != 200 {
		fmt.Println("Cannot detach")
		fmt.Println(err)
		return
	}

	fmt.Println("Detached")
}
