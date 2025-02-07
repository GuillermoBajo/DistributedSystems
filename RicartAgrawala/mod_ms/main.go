package main

import (
	"fmt"
	"os"
	"p2/gf"
	"p2/ms"
	"p2/ra"
	"strconv"
	"sync"
	"time"
)

// Struct for file update message
type FileUpdate struct {
	Fragment string
}

// Struct for synchronization barrier
type Barrier struct{}

// Check for errors, exit program if there is an error
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// Writer process: this process writes to the file and uses Ricart-Agrawala protocol for mutual exclusion.
func escritor(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, mSys *ms.MessageSystem, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	frag := "a" // Fragment content to write
	for {
		// Pre-protocol of Ricart-Agrawala: Wait until access to the critical section is granted
		rasdb.PreProtocol()

		// Mutual exclusion for writing to the local file
		writeMutex.Lock()
		gestF.EscribirFichero(frag)
		writeMutex.Unlock()

		// Notify other processes of the change
		for i := 1; i <= ra.N; i++ {
			if i != me {
				mSys.Send(i, FileUpdate{frag})
			}
		}

		// Post-protocol of Ricart-Agrawala: Release access to the critical section, other processes can read/write to the file
		rasdb.PostProtocol()
	}
}

// Reader process: this process only reads from the file.
func lector(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	for {
		rasdb.PreProtocol()
		_ = gestF.LeerFichero() // Read the file
		rasdb.PostProtocol()
	}
}

func main() {
	// Define file name based on the passed arguments
	myFile := "p2_" + os.Args[1] + ".txt"
	gFich := gf.New(myFile)

	// Get process ID and check for errors
	myPID, err := strconv.Atoi(os.Args[1])
	checkError(err)
	usersFile := os.Args[2]

	// Initialize channels for message passing
	reqChan := make(chan ra.Request)
	replChan := make(chan ra.Reply)
	mSys := ms.New(myPID, usersFile, []ms.Message{ra.Request{}, ra.Reply{}, FileUpdate{}, Barrier{}})

	// Initialize shared database for Ricart-Agrawala protocol
	ras := ra.New(myPID, usersFile, &mSys, reqChan, replChan)

	// Initialize synchronization barrier and mutex for file writing
	syncBarrier := make(chan bool)
	writeMutex := sync.Mutex{}
	go handleMessages(&mSys, reqChan, replChan, syncBarrier, gFich, &writeMutex)

	// Barrier synchronization before starting processes
	barrera(&mSys, syncBarrier, myPID)

	// WaitGroup for synchronization between processes
	var wg sync.WaitGroup
	wg.Add(1)

	// Choose whether the process is a writer or reader based on its PID
	if ra.N/2 > myPID {
		go escritor(myPID, *gFich, ras, &mSys, &wg, &writeMutex) // Writer process
	} else {
		go lector(myPID, *gFich, ras, &wg, &writeMutex) // Reader process
	}

	// Wait for the processes to complete
	wg.Wait()
	fmt.Println("Done")
}

// Function to handle incoming messages and process them accordingly
func handleMessages(mSys *ms.MessageSystem, reqChan chan ra.Request, replChan chan ra.Reply, syncBarrier chan bool, gFich *gf.GestorFichero, writeMutex *sync.Mutex) {
	for {
		msg := mSys.Receive()
		switch msgType := msg.(type) {
		case ra.Request:
			reqChan <- msgType
		case ra.Reply:
			replChan <- msgType
		case FileUpdate:
			writeMutex.Lock()
			gFich.EscribirFichero(msgType.Fragment)
			writeMutex.Unlock()
		case Barrier:
			syncBarrier <- true
		}
	}
}

// Barrier synchronization function: ensures all processes reach the barrier before continuing
func barrera(mSys *ms.MessageSystem, syncBarrier chan bool, myPID int) {
	time.Sleep(1 * time.Second)
	for i := 1; i <= ra.N; i++ {
		if i != myPID {
			mSys.Send(i, Barrier{}) // Send barrier message to other processes
		}
	}
	num_ack := 0
	for num_ack < ra.N-1 {
		<-syncBarrier // Wait for acknowledgements from other processes
		num_ack++
	}
	fmt.Println(myPID, " - barrier passed") // Barrier passed by this process
}
