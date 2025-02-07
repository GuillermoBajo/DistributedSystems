// Write your Raft functionality code in this file
//

package raft

// API
// ===
// This is the API that your implementation should export
//
// nodoRaft = NuevoNodo(...)
//   Create a new server in the election group.
//
// nodoRaft.Para()
//   Request the stop of a server
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Request the state of a node by "yo", its current term, and whether it thinks it is the leader
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"

	"sync"
	"time"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constant to set uninitialized integer value
	IntNOINICIALIZADO = -1

	// false disables debugging logs completely
	// Ensure kEnableDebugLogs is set to false before delivery
	kEnableDebugLogs = true

	// Set to true to log to stdout instead of a file
	kLogToStdout = false

	// Change this to a different directory for log output
	kLogOutputDir = "./logs_raft/"

	// Timeout for re-election
	ReelectionTimer = 2.5
)

// Operation Type: Possible operations are "read" and "write"
type TipoOperacion struct {
	Operacion string // The possible operations are "leer" (read) and "escribir" (write)
	Clave     string // For reading, Clave = ""
	Valor     string // For reading, Valor = ""
}

// As the Raft node learns about committed log entries,
// it sends an AplicaOperacion with each of them to the "canalAplicar" channel (from NuevoNodo function) of the state machine
type AplicaOperacion struct {
	Indice    int           // Log entry index
	Operacion TipoOperacion // The operation to be applied
}

type Entry struct {
	Term      int           // Term in which the entry was created
	Operacion TipoOperacion // The operation to perform
	Index	  int 			// Log entry index
}

// Go data type representing a single Raft node (replica)
type NodoRaft struct {
	Mux sync.Mutex // Mutex to protect access to shared state

	// Host:Port of all Raft nodes (replicas), in the same order
	Nodos   []rpctimeout.HostPort 	// Host:Port of all Raft nodes (replicas), in the same order
	Yo      int 					// Index of this node in the "nodos" array
	IdLider int 					// Index of the leader node in the "nodos" array

	// Optional logger for debugging
	// Each Raft node has its own trace log
	Logger *log.Logger

	// Your custom data here
	FollowerChan chan bool // Channel to switch to follower role
	LeaderChan   chan bool // Channel to switch to leader role
	Heartbeat    chan bool // Channel to send heartbeats

	Rol string // Node's role (Follower, Candidate, Leader)

	// RaftState
	CurrentTerm int 		// Last known term
	VotedFor    int 		// Index of the node voted for in the current term
	Log 		[]Entry		// Log entries

	// NodeState
	CommitIndex int 		// Highest known index of committed log entries
	LastApplied int 		// Highest known index of applied log entries
	NumVotes    int 		// Number of votes received in the current election
	NumSucess   int			// Number of success responses in appendEntries

	// LeaderState
	NextIndex  []int 		// For each node, the index of the next log entry to send, initialized to LastLogIndex + 1
	MatchIndex []int 		// For each node, the index of the last log entry known to be replicated, initialized to 0

	AplicarOperacion chan AplicaOperacion // Channel for operations to be applied to the state machine
	Commited   		 chan string		  // Channel for the value to be returned to the client

	alive bool	// Indicates if the node is alive or not
}

// Creation of a new Raft node
//
// Table of <IP Address:port> of each node, including itself.
//
// The <IP Address:port> of this node is in "nodos[yo]"
//
// All "nodos[]" arrays of the nodes have the same order

// canalAplicar is a channel where, in practice 5, operations to apply to the state machine will be received.
// It can be assumed that this channel will be consumed continuously.
//
// NuevoNodo() should return quickly, so long-running tasks should be started in Goroutines
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	// Setting up the logger for debugging
	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Initializing Raft node attributes
	nr.FollowerChan = make(chan bool)
	nr.LeaderChan = make(chan bool)
	nr.Heartbeat = make(chan bool)
	nr.Rol = "Follower"
	nr.CurrentTerm = 0
	nr.VotedFor = -1	
	nr.Log = []Entry{}
	nr.alive = true

	nr.CommitIndex = 0
	nr.LastApplied = 0
	nr.NumVotes = 0
	nr.NumSucess = 0

	nr.LastApplied = 0
	nr.NextIndex = make([]int, len(nr.Nodos))
	nr.MatchIndex = make([]int, len(nr.Nodos))

	nr.AplicarOperacion = canalAplicarOperacion
	nr.Commited = make(chan string)

	// Delay to ensure proper initialization
	time.Sleep(3 * time.Second)
	go nr.automataRaft()

	return nr
}

// "para" is a method that stops the current node execution.
// It creates a Goroutine that sleeps for 5 milliseconds and then exits the program.
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// obtenerEstado (Get Node State) returns the current state of the Raft node.
// It returns the following values:
// - yo: the node's identifier (index in the node list).
// - mandato: the current term of the node.
// - esLider: a boolean indicating whether the node is the leader.
// - idLider: the identifier of the current leader.
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var esLider bool
	var idLider int = nr.IdLider

	// Check if the current node is the leader
	if nr.Yo == nr.IdLider {
		esLider = true
	} else {
		esLider = false
	}
	// Return the node state
	return yo, mandato, esLider, idLider
}

// someterOperacion (Submit Operation) is a method that submits an operation
// (such as a read or write operation on a key-value store) to the Raft node.
// If the node is the leader, the operation is added to the log and committed.
// It returns the index of the log entry, the current term, whether the node is the leader,
// the leader's ID, and the result value.
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int, bool, int, string) {
	// If the node is not active, wait and return a result indicating that it's dead
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return -1, -1, false, -1, "dead"
	}

	// Initialize variables
	indice := -1
	mandato := nr.CurrentTerm
	EsLider := nr.Yo == nr.IdLider
	idLider := nr.IdLider
	valorADevolver := ""

	// If the current node is the leader, process the operation
	if EsLider {
		// Lock access to the log while performing the operation
		nr.Mux.Lock()

		// Get the updated index after the operation
		indice = nr.CommitIndex
		// Create a new log entry with the provided operation
		entry := Entry{Term: mandato, Operacion: operacion, Index: indice}
		// Add the entry to the log
		nr.Log = append(nr.Log, entry)

		// Log the operation and the updated log state
		nr.Logger.Printf("New log entry added, ENTRY: (Index: %d, Term: %d, Operation: %s, Value: %s, Key: %s)\n"+
			"Log updated: %v\n", entry.Index, entry.Term, entry.Operacion.Operacion, entry.Operacion.Valor, entry.Operacion.Clave, nr.Log)

		// Unlock access to the log
		nr.Mux.Unlock()

		// Wait for the operation to be committed and get the returned value
		valorADevolver = <-nr.Commited
		// Update the leader ID
		idLider = nr.Yo
	}

	// Return the results of the operation submission
	return indice, mandato, EsLider, idLider, valorADevolver
}

// ------------------------------------------
// RPC CALLS TO RAFT API

// The structure for empty arguments or response, when no arguments or empty response structures are used (size zero).
type Vacio struct{}

// ParaNodo (Stop Node) is an RPC method to stop the Raft node.
func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

// EstadoParcial (Partial State) contains the current term, whether the node is the leader, and the leader's ID.
type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

// EstadoRemoto (Remote State) contains the ID of the node and the partial state.
type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

// ResultadoRemoto (Remote Result) contains the result of an operation along with the current state.
type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

// ResultadoRegistro (Log Entry Result) contains the index and term of a log entry.
type ResultadoRegistro struct {
	Index int
	Term  int
}

// ObtenerEstadoNodo (Get Node State) retrieves and returns the current state of the Raft node.
func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	// Get the node's state and assign it to the remote response
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

// ObtenerEstadoRegistro (Get Log State) retrieves and returns the current state of the node's log.
func (nr *NodoRaft) ObtenerEstadoRegistro(args Vacio, reply *ResultadoRegistro) error {
	// Lock the log while retrieving the current state
	nr.Mux.Lock()
	reply.Term, reply.Index = nr.getEstadoRegistro()
	nr.Mux.Unlock()
	return nil
}

// getEstadoRegistro (Get Log State) returns the term and index of the current log entry state.
func (nr *NodoRaft) getEstadoRegistro() (int, int) {
	if len(nr.Log) != 0 {
		return nr.Log[nr.CommitIndex-1].Term, nr.CommitIndex
	}
	return 0, -1
}

// SometerOperacionRaft (Submit Raft Operation) carries out the process of submitting an operation to the Raft node.
func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion, reply *ResultadoRemoto) error {
	// Get the results of the operation submission process and assign them to the remote response
	reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// ------------------------------------------
// RAFT PROTOCOL RPC CALLS

// ArgsPeticionVoto (Request Vote Arguments) contains the information needed for a vote request.
type ArgsPeticionVoto struct {
	// Your data here
	Term         int // Term of the candidate
	CandidateId  int // ID of the candidate requesting the vote
	LastLogIndex int // Index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

// RespuestaPeticionVoto (Vote Request Response) contains the response to a vote request.
type RespuestaPeticionVoto struct {
	// Your data here
	Term        int  // Current term of the node so that the candidate can update
	VoteGranted bool // True if the vote was granted
}

// solicitarVotos (Request Votes) is a function that sends vote requests to all nodes in the cluster.
// If the node is not active, the function waits and exits.
// It uses goroutines to send vote requests concurrently.
func solicitarVotos(nr *NodoRaft) {
	// If the node is not active, wait and exit
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return
	}

	// Declare variables for the response and the mutex
	var reply RespuestaPeticionVoto
	var mx sync.Mutex

	// Iterate through all nodes in the cluster
	for i := 0; i < len(nr.Nodos); i++ {
		// Avoid sending a vote request to itself
		if i != nr.Yo {
			// Start a goroutine to send the vote request to node i
			go enviarPeticionVoto(nr, i, &mx, &reply, &ArgsPeticionVoto{
				Term:         nr.CurrentTerm,
				CandidateId:  nr.Yo,
				LastLogIndex: len(nr.Log) - 1,
				LastLogTerm:  0,
			})
			// Log the sent vote request
			nr.Logger.Printf("Vote request sent to %d. Arguments: Term: %d, Yo: %d, LastLogIndex: %d, LastLogTerm: %d\n",
				i, nr.CurrentTerm, nr.Yo, len(nr.Log), 0)
		}
	}
}

// enviarPeticionVoto sends a vote request to the specified node in the Raft cluster.
// It returns true if the request was successfully sent and processed, false otherwise.
func enviarPeticionVoto(nr *NodoRaft, node int, mx *sync.Mutex, reply *RespuestaPeticionVoto, args *ArgsPeticionVoto) bool {
	// If the node is not active, wait and return false
	if !nr.alive {
		time.Sleep(400 * time.Millisecond)
		return false
	}

	// Update LastLogTerm if the log is not empty
	if len(nr.Log) > 0 {
		args.LastLogTerm = nr.Log[len(nr.Log)-1].Term
	}

	// Make the remote call to send the vote request to the node
	err := nr.Nodos[node].CallTimeout("NodoRaft.PedirVoto", args, reply, 300*time.Millisecond)

	// Check if there was an error with the remote call
	if err != nil {
		return false
	}

	// Lock access to critical sections using the mutex
	mx.Lock()

	// Update the CurrentTerm if the term in the reply is higher
	if reply.Term > nr.CurrentTerm {
		nr.CurrentTerm = reply.Term
		nr.Logger.Printf("Their term is greater: My term is: %d, theirs is: %d (so, I return to follower)\n", nr.CurrentTerm, reply.Term)
		nr.FollowerChan <- true
	} else if reply.VoteGranted {
		// If the vote is granted, increment the vote counter and check if the majority is reached
		nr.NumVotes++
		nr.Logger.Printf("They granted me their vote %d\n", node)
		// If majority is reached, the node becomes the leader
		if nr.NumVotes > len(nr.Nodos)/2 {
			nr.IdLider = nr.Yo
			nr.LeaderChan <- true
		}
	} else {
		// If the vote is not granted, return to follower state
		nr.FollowerChan <- true
	}

	// Unlock the access to critical sections
	mx.Unlock()

	// Return true indicating the request was sent and processed successfully
	return true
}

// PedirVoto (Request Vote) processes a vote request received from a candidate in the Raft cluster.
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error {
	// If the node is not active, wait and exit the function without processing the request
	if !nr.alive {
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	// Lock access to the critical state of the node with the mutex
	nr.Mux.Lock()

	// Respond with false if the term in the request is less than the current term
	if peticion.Term < nr.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = nr.CurrentTerm
		nr.Logger.Printf("Vote denied to %d because their term is lower than mine\n", peticion.CandidateId)

	// If the term in the request is greater, and additional checks to grant the vote
	} else if nr.CurrentTerm < peticion.Term && shouldGrantVote(nr, peticion) {
		// Update the term, grant the vote, and notify the state change to follower
		nr.CurrentTerm = peticion.Term
		reply.Term = nr.CurrentTerm
		nr.VotedFor = peticion.CandidateId
		nr.Logger.Printf("Vote granted to %d\n", peticion.CandidateId)
		nr.FollowerChan <- true
		reply.VoteGranted = true
	}

	// Unlock access to the critical state of the node
	nr.Mux.Unlock()

	return nil
}

// If voted for is null or candidateId, and candidate's log is at least as up-to-date as the receiver's log, grant vote
func shouldGrantVote(nr *NodoRaft, peticion *ArgsPeticionVoto) bool {
	if nr.VotedFor == -1 || nr.VotedFor == peticion.CandidateId || len(nr.Log) == 0 || peticion.LastLogTerm >= nr.Log[len(nr.Log)-1].Term || 
	(peticion.LastLogTerm == nr.Log[len(nr.Log)-1].Term && peticion.LastLogIndex >= len(nr.Log)-1) {
		return true
	}
	return false
}

// automataRaft is the implementation of the Raft protocol state machine for the Raft node.
// It controls the behavior of the Raft node in different roles: Follower, Candidate, and Leader.
func (nr *NodoRaft) automataRaft() {
	// Node identification for clarity in logs
	nr.Logger.Printf("I am node %d\n", nr.Yo)

	for {
		// Check if the node is active
		if nr.alive {
			// Apply operations if there are changes in the CommitIndex
			if nr.CommitIndex > nr.LastApplied {
				nr.Mux.Lock()
				nr.LastApplied++
				nr.AplicarOperacion <- AplicaOperacion{Indice: nr.LastApplied, Operacion: nr.Log[nr.LastApplied-1].Operacion}
				nr.Mux.Unlock()
			}

			// If the node is a follower;
			if nr.Rol == "Follower" {
				select {
				case <-nr.Heartbeat:
					nr.Rol = "Follower"
				case <-nr.FollowerChan:
					nr.Rol = "Follower"
				
				// If the timeout expires, the node becomes a candidate
				case <-time.After(generarTimeout()):
					if nr.alive {
						nr.Mux.Lock()
						nr.Rol = "Candidate"
						nr.IdLider = -1
						nr.Mux.Unlock()
						nr.Logger.Printf("Timeout expired. I am now running as a candidate\n")
					}
				}

			// If the node is a candidate;
			} else if nr.Rol == "Candidate" {
				// Start the election process as a candidate
				nr.Mux.Lock()
				nr.VotedFor = nr.Yo
				nr.NumVotes = 1
				nr.CurrentTerm++
				nr.Mux.Unlock()
				solicitarVotos(nr)

				// Handle events based on the election result
				select {
				case <-nr.FollowerChan:
					nr.Rol = "Follower"

				case <-nr.Heartbeat:
					nr.Logger.Printf("I am a candidate, but I received a heartbeat, so I return to follower\n")
					nr.Rol = "Follower"

				// If elected leader;
				case <-nr.LeaderChan:
					nr.Rol = "Leader"
					nr.Logger.Printf("I was a candidate, but I was just elected leader\n")

					// Set NextIndex and MatchIndex for log replication
					for i := 0; i < len(nr.Nodos); i++ {
						if i != nr.Yo {
							nr.Mux.Lock()
							nr.NextIndex[i] = len(nr.Log) + 1
							nr.MatchIndex[i] = 0
							nr.Mux.Unlock()
						}
					}

				// If timeout expires, the node runs again as a candidate
				case <-time.After(generarTimeout()):
					nr.Rol = "Candidate"
					nr.Logger.Printf("Timeout expired, I am presenting myself again as a candidate\n")
				}

			// If the node is a leader;
			} else if nr.Rol == "Leader" {
				// Set the node as the leader and send heartbeats
				nr.Mux.Lock()
				nr.IdLider = nr.Yo
				nr.Mux.Unlock()
				enviaHeartbeats(nr)

				// Handle events based on the current role
				select {
				case <-nr.FollowerChan:
					nr.Rol = "Follower"
				case <-nr.Heartbeat:
					nr.Rol = "Follower"
					nr.Logger.Printf("I am the leader, but I received a heartbeat, so I return to follower\n")
				case <-time.After(50 * time.Millisecond):
					nr.Rol = "Leader"
				}
			}
		}
	}
}


// enviaHeartbeats sends heartbeats to all nodes in the Raft cluster.
// If the node is not active, it waits for 400 milliseconds before sending heartbeats.
// For each node in the cluster, a goroutine is started to send heartbeats.
// If there are new log entries to send, they are sent via AppendEntries.
// If no new entries, just send a heartbeat.
func enviaHeartbeats(nr *NodoRaft) {
    // Check if the node is active before sending heartbeats
    if !nr.alive {
        time.Sleep(400 * time.Millisecond)
        return
    }

    // Iterate over all nodes in the Raft cluster
    for i := 0; i < len(nr.Nodos); i++ {
        // Ensure the current index is not the same as the node's own index
        if i != nr.Yo {
            // Start a goroutine to send heartbeats to the current node
            go func(node int) {
                var result ResultsAE
                var entradas []Entry

                // If there are new entries that haven't been sent to the follower node:
                if len(nr.Log) >= nr.NextIndex[node] {
                    for i := nr.NextIndex[node] - 1; i < len(nr.Log); i++ {
                        entrada := nr.Log[i]
                        nuevaEntrada := Entry{Term: entrada.Term, Operacion: entrada.Operacion, Index: entrada.Index}
                        entradas = append(entradas, nuevaEntrada)
                    }

                    // If there are log entries, append them to the node
                    if nr.NextIndex[node] > 1 {
                        entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, nr.NextIndex[node] - 1, nr.Log[nr.NextIndex[node]-1].Term, entradas, nr.CommitIndex}
                        sendEntries(nr, node, &result, &entrada)
                    } else {
                        entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, -1, 0, entradas, nr.CommitIndex}
                        sendEntries(nr, node, &result, &entrada)
                    }
                // If no new entries, send a heartbeat
                } else {
                    nr.Logger.Printf("Sending heartbeat to %d\n", node)
                    if nr.NextIndex[node] > 2 {
                        entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, nr.NextIndex[node] - 1, nr.Log[nr.NextIndex[node]-2].Term, entradas, nr.CommitIndex}
                        sendHeartbeat(nr, node, &result, &entrada)
                    } else {
                        entrada := ArgAppendEntries{nr.CurrentTerm, nr.Yo, -1, 0, entradas, nr.CommitIndex}
                        sendHeartbeat(nr, node, &result, &entrada)
                    }
                }
            }(i)
        }
    }
}

// sendEntries sends log entries to the node identified by 'node' using a remote call.
// Returns true if the sending was successful, otherwise returns false.
func sendEntries(nr *NodoRaft, node int, result *ResultsAE, entrada *ArgAppendEntries) bool {
    // Check if the node is alive before attempting to send log entries
    if !nr.alive {
        time.Sleep(400 * time.Millisecond)
        nr.Logger.Printf("Could not send entries to %d because it is dead\n", node)
        return false
    }

    // Perform a remote call to send log entries to the node identified by 'node'
    err := nr.Nodos[node].CallTimeout("NodoRaft.AppendEntries", entrada, result, 700*time.Millisecond)

    // If the remote call is successful, update NextIndex and MatchIndex values of the destination node.
    if result.Success {
        nr.Mux.Lock()

        // Update the NextIndex and MatchIndex of the destination node
        nr.NextIndex[node] += len(entrada.Entries)
        nr.MatchIndex[node] = result.MatchIndex

        if nr.CommitIndex < nr.MatchIndex[node] {
            nr.NumSucess++

            // If the majority of nodes have replicated the entry, commit and update CommitIndex.
            if nr.NumSucess >= len(nr.Nodos)/2 {
                nr.CommitIndex += len(entrada.Entries)
                nr.NumSucess = 0
                nr.Logger.Printf("CommitIndex updated, now it is: %d.\n", nr.CommitIndex)
            }
        }

        nr.Mux.Unlock()

    // If the remote call is unsuccessful and the result contains a term greater than zero, decrement NextIndex.
    } else if result.Term > 0 {
        nr.NextIndex[node]--
        nr.Logger.Printf("Failed, decrementing NextIndex. New value: %d\n", nr.NextIndex[node])
    }

    return err == nil
}

// sendHeartbeat sends a "heartbeat" to the specified node.
// Returns true if the sending was successful, otherwise returns false.
func sendHeartbeat(nr *NodoRaft, node int, result *ResultsAE, entrada *ArgAppendEntries) bool {
    // Check if the node is alive before sending the heartbeat
    if !nr.alive {
        time.Sleep(400 * time.Millisecond)
        nr.Logger.Printf("Could not send heartbeat to %d because it is dead\n", node)
        return false
    }

    // Perform a remote call to send the heartbeat
    err := nr.Nodos[node].CallTimeout("NodoRaft.AppendEntries", entrada, result, 700*time.Millisecond)

    // Revert to follower if a larger term is received in the heartbeat
    if nr.CurrentTerm < result.Term {
        nr.CurrentTerm = result.Term
        nr.IdLider = -1
        nr.FollowerChan <- true
        nr.Logger.Printf("Larger term received: My term is: %d, theirs is: %d (reverting to follower)\n", nr.CurrentTerm, result.Term)
    }

    return err == nil
}

// AppendEntries handles the AppendEntries RPC call.
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries, results *ResultsAE) error {
    if !nr.alive {
        time.Sleep(400 * time.Millisecond)
        return nil
    }

    nr.Mux.Lock()
    nr.Logger.Printf("Received AppendEntries, args: %v\n", args)

    // 1: Reply false if term < currentTerm
    if nr.CurrentTerm > args.LeaderTerm {
        nr.Logger.Printf("Their term is: %d, mine is: %d. Replying false", args.LeaderTerm, nr.CurrentTerm)
        results.Success = false
        results.Term = nr.CurrentTerm
        results.MatchIndex = 0
        nr.Mux.Unlock()
        return nil
    }

    // 2: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
    if len(nr.Log) > 0 && args.PrevLogIndex > -1 && len(nr.Log) < args.PrevLogIndex {
        nr.Logger.Printf("Their LastApplied is: %d, mine is: %d", args.PrevLogIndex, nr.LastApplied)
        results.Success = false
        results.Term = nr.CurrentTerm
        results.MatchIndex = 0
        nr.Mux.Unlock()
        return nil
    }

    // 3: If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
    if len(nr.Log) > 0 && args.PrevLogIndex > -1 && nr.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
        results.Success = false
        results.Term = nr.CurrentTerm
        results.MatchIndex = 0
        nr.Log = nr.Log[:args.PrevLogIndex]
        nr.LastApplied = max(0, len(nr.Log)-1)
        nr.Logger.Printf("Conflict entry. Entry: %v\n", args)
        nr.Mux.Unlock()
        return nil
    }

    results.Success = true
    results.Term = args.LeaderTerm
    nr.IdLider = args.LeaderId
    nr.CurrentTerm = args.LeaderTerm

    // 4: Append any new entries not already in the log
    if len(args.Entries) > 0 {
        nr.Log = append(nr.Log, args.Entries...)
        nr.Logger.Printf("New entries added to log: %v\n", args.Entries)
    }
    results.MatchIndex = len(nr.Log)

    // 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if nr.CommitIndex < args.LeaderCommit {
        nr.CommitIndex = min(args.LeaderCommit, len(nr.Log))
        nr.Logger.Printf("CommitIndex updated to: %d\n", nr.CommitIndex)
    }

    nr.Heartbeat <- true
    nr.Mux.Unlock()

    return nil
}

// max returns the maximum between two integer numbers.
func max(a, b int) int {
    if a > b {
        return a
    }
    return b
}

// min returns the minimum between two integer numbers.
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

// generateTimeout generates a random time duration between 200 and 500 milliseconds.
// This time range is recommended in the paper.
func generateTimeout() time.Duration {
    return time.Duration(rand.Intn(200)+300) * time.Millisecond
}

// KillNode stops the current node and marks its state as inactive.
func (nr *NodoRaft) KillNode(args Vacio, reply *Vacio) error {
    nr.alive = false
    nr.Logger.Printf("Node %d killed\n", nr.Yo)
    return nil
}

// ReviveNode revives a Raft node that was previously marked as inactive.
// This method sets the "alive" attribute to true and logs a message indicating that the node has been revived.
func (nr *NodoRaft) ReviveNode(args Vacio, reply *Vacio) error {
    nr.alive = true
    nr.Logger.Printf("Node %d revived\n", nr.Yo)
    return nil
}
