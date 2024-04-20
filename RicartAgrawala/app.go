package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parseIps(path string) (lines []string) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

func launchApp(remoteUser, addr string, pid int, wg *sync.WaitGroup) {
	defer wg.Done()
	ip := strings.Split(addr, ":")

	cmd := exec.Command("ssh", remoteUser+"@"+ip[0], 
		"cd /misc/alumnos/sd/sd2324/a838819/; export PATH=$PATH:/usr/local/go/bin; go mod tidy;",
		"go run main.go ", strconv.Itoa(pid))
	
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	checkError(err)
}

func main() {
	wkPath := os.Args[1]
	ips := parseIps(wkPath)
	// remoteUser := os.Args[2]

	var wg sync.WaitGroup
	for i, ip := range ips {
		wg.Add(1)
		fmt.Println("Launching app in ", ip, i+1)
		go launchAppLocal(i+1, wkPath, &wg)
		// go launchApp(remoteUser, ip, i+1, &wg)
	}
	wg.Wait()
}

func launchAppLocal(pid int, wkPath string, wg *sync.WaitGroup) {
	defer wg.Done()

	cmd := exec.Command("go", "run", "main.go", strconv.Itoa(pid), wkPath)
	
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	checkError(err)
}