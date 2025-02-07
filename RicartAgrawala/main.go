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

// FileUpdate represents the structure of a message sent for updating the file.
type FileUpdate struct {
	Fragment string 
}

// Barrier is used as a synchronization primitive to coordinate processes.
type Barrier struct{}

// checkError handles errors by printing them and exiting the program if any error occurs.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// escritor (writer) simulates a writer process that writes data to the file. 
// It follows the Ricart-Agrawala protocol for mutual exclusion to access the critical section.
func escritor(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, mSys *ms.MessageSystem, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	frag := fmt.Sprintln("process", me, "wrote this") // Message to be written to the file
	for {
		time.Sleep(time.Duration(me) * time.Millisecond) // Simulate different process speeds

		// Pre-protocol of Ricart-Agrawala. Waits for permission to enter the critical section.
		rasdb.PreProtocol()

		// Notifies other processes of the file change
		for i := 1; i <= ra.N; i++ {
			if i != me { // Send update to all other processes except itself
				mSys.Send(i, FileUpdate{frag})
			} else {
				// Write to the file in the critical section
				writeMutex.Lock()
				gestF.EscribirFichero(frag)
				writeMutex.Unlock()
			}
		}
		// Post-protocol of Ricart-Agrawala. Releases access to the critical section. Other processes can now read/write.
		rasdb.PostProtocol()
	}
}

// lector (reader) simulates a reader process that reads data from the file.
// It also uses the Ricart-Agrawala protocol to synchronize access to the file.
func lector(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	for {
		time.Sleep(time.Duration(me) * time.Millisecond) // Simulate different process speeds

		rasdb.PreProtocol() // Pre-protocol to enter critical section (read in this case)
		_ = gestF.LeerFichero() // Read the file (no actual reading is done here for simplicity)
		rasdb.PostProtocol() // Post-protocol to exit critical section
	}
}

// main function initializes and runs the Raft system with different processes (writers and readers).
func main() {
	myFile := "p2_" + os.Args[1] + ".txt" // Define the file name based on the process ID argument
	gFich := gf.New(myFile) // Create a new file manager

	// Convert the process ID from string to integer and handle any errors
	myPID, err := strconv.Atoi(os.Args[1])
	checkError(err)
	usersFile := os.Args[2] // Get the user file (configuration file)

	// Channels for communication between Raft processes
	reqChan := make(chan ra.Request)
	replChan := make(chan ra.Reply)
	mSys := ms.New(myPID, usersFile, []ms.Message{ra.Request{}, ra.Reply{}, FileUpdate{}, Barrier{}})

	// Create the Raft system with the given parameters
	ras := ra.New(myPID, usersFile, &mSys, reqChan, replChan)

	// Synchronization barrier and mutex for writing
	syncBarrier := make(chan bool)
	writeMutex := sync.Mutex{}
	
	// Start the message handling function in a goroutine
	go handleMessages(&mSys, reqChan, replChan, syncBarrier, gFich, &writeMutex)

	// Synchronization barrier to ensure all processes start at the same time
	barrera(&mSys, syncBarrier, myPID)

	var wg sync.WaitGroup
	wg.Add(1) // Add one wait group to ensure main waits for all goroutines to finish

	// If the process ID is less than half of the total number of processes, it's a writer.
	// Otherwise, it's a reader.
	if ra.N/2 >= myPID {
		go escritor(myPID, *gFich, ras, &mSys, &wg, &writeMutex)
	} else {
		go lector(myPID, *gFich, ras, &wg, &writeMutex)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	fmt.Println("Done") // Print when all processes are done
}

// handleMessages handles incoming messages, routing them to appropriate channels or actions.
func handleMessages(mSys *ms.MessageSystem, reqChan chan ra.Request, replChan chan ra.Reply, syncBarrier chan bool, gFich *gf.GestorFichero, writeMutex *sync.Mutex) {
	for {
		msg := mSys.Receive() // Receive a message from the system
		switch msgType := msg.(type) {
		case ra.Request:
			reqChan <- msgType // Send request to request channel
		case ra.Reply:
			replChan <- msgType // Send reply to reply channel
		case FileUpdate:
			writeMutex.Lock() // Lock the mutex before writing to the file
			gFich.EscribirFichero(msgType.Fragment) // Update the file
			writeMutex.Unlock() // Unlock the mutex after writing
		case Barrier:
			syncBarrier <- true // Signal the synchronization barrier
		}
	}
}

// barrera (barrier) is used for synchronizing all processes before they begin executing.
func barrera(mSys *ms.MessageSystem, syncBarrier chan bool, myPID int) {
	time.Sleep(1 * time.Second) // Sleep to ensure all processes are ready
	for i := 1; i <= ra.N; i++ {
		if i != myPID { // Send the barrier message to all other processes
			mSys.Send(i, Barrier{})
		}
	}
	num_ack := 0
	for num_ack < ra.N-1 { // Wait for all other processes to acknowledge the barrier
		<-syncBarrier
		num_ack++
	}
	fmt.Println(myPID, " - barrier passed") // Print when the process has passed the barrier
}
