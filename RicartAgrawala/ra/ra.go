/*
* FILE: ricart-agrawala.go
* DESCRIPTION: Implementation of the Generalized Ricart-Agrawala Algorithm in Go
 */
 package ra

 import (
	 "fmt"
	 "p2/ms"
	 "strconv"
	 "sync"
 
	 "github.com/DistributedClocks/GoVector/govec"
	 "github.com/DistributedClocks/GoVector/govec/vclock"
 )
 
 const (
	 N = 6 // Number of processes
 )
 
 type Request struct {
	 VClock []byte // Logical clock of the request
	 Pid    int    // Process ID that is making the request
	 Op     string // Operation being requested (Read or Write)
 }
 
 type Reply struct{} // Reply struct for the message system
 
 type Exclusion struct {
	 type1 string
	 type2 string
 }
 
 type RASharedDB struct {
	 OutRepCnt int           // Counter for outstanding replies
	 ReqCS     bool          // Indicates if the process is requesting the critical section
	 RepDefd   []bool        // Array indicating whether a reply is deferred
	 mSys      *ms.MessageSystem
	 ReqChan   chan Request  // Channel for receiving requests
	 ReplChan  chan Reply    // Channel for receiving replies
	 done      chan bool     // Channel to stop the process
	 chrep     chan bool     // Channel to signal when all replies are received
	 Mutex     sync.Mutex    // Mutex for synchronizing access to shared variables
	 myPID     int           // Process ID
	 operation string        // The operation being performed (Read or Write)
	 exclude   map[Exclusion]bool // Map for operation exclusions
	 logger    *govec.GoLog // Logger for the vector clock
	 currentClk vclock.VClock // Current vector clock of the process
 }
 
 // Pre: Valid arguments
 // Post: Returns a pointer to a newly initialized RASharedDB structure. Initializes the message system,
 //       the variables, and launches concurrent processes for waiting for messages.
 func New(me int, usersFile string, mSys *ms.MessageSystem, reqChan chan Request, replChan chan Reply) *RASharedDB {
 
	 // Initialize the logger using GoVector
	 logger := govec.InitGoVector(strconv.Itoa(me), fmt.Sprintf("log/%d", me), govec.GetDefaultConfig())
 
	 // Initialize the RASharedDB structure
	 ra := RASharedDB{
		 0, 
		 false, 
		 make([]bool, N), 
		 mSys, 
		 reqChan, 
		 replChan, 
		 make(chan bool), 
		 make(chan bool),
		 sync.Mutex{}, 
		 me, 
		 getOp(me), 
		 initExclude(), 
		 logger, 
		 logger.GetCurrentVC().Copy(),
	 }
 
	 // Start the concurrent processes for receiving requests and replies
	 go ReceiveRequest(&ra)
	 go ReceiveReply(&ra)
	 return &ra
 }
 
 // Pre: Valid arguments
 // Post: Performs the PreProtocol for the Generalized Ricart-Agrawala algorithm
 func (ra *RASharedDB) PreProtocol() {
	 ra.Mutex.Lock()
	 ra.ReqCS = true // Mark the process as requesting the critical section
	 ra.OutRepCnt = N - 1 // Set the expected number of replies
 
	 // Prepare the request message with the vector clock
	 context := fmt.Sprint("Process ", ra.myPID, " is requesting CS")
	 msgPayload := ra.logger.GetCurrentVC().Copy()
	 msgPayload.Tick(strconv.Itoa(ra.myPID))
	 payload := msgPayload.Bytes()
	 clk := ra.logger.PrepareSend(context, payload, govec.GetDefaultLogOptions())
	 ra.currentClk = ra.logger.GetCurrentVC().Copy()
 
	 ra.Mutex.Unlock()
 
	 // Send request to all other processes
	 for j := 1; j <= N; j++ {
		 if j != ra.myPID {
			 ra.mSys.Send(j, Request{clk, ra.myPID, ra.operation})
		 }
	 }
	 <-ra.chrep // Wait for replies
 }
 
 // Pre: Valid arguments
 // Post: Performs the PostProtocol for the Generalized Ricart-Agrawala algorithm
 func (ra *RASharedDB) PostProtocol() {
	 ra.Mutex.Lock()
	 ra.ReqCS = false // Mark the process as no longer requesting the critical section
 
	 // Send replies to deferred processes
	 for j := 0; j < N; j++ {
		 if ra.RepDefd[j] {
			 ra.RepDefd[j] = false
			 ra.mSys.Send(j+1, Reply{})
		 }
	 }
	 ra.Mutex.Unlock()
 }
 
 func (ra *RASharedDB) Stop() {
	 ra.done <- true
 }
 
 // Pre: ra is an initialized pointer to a RASharedDB structure
 // Post: Launches a concurrent process that waits for incoming messages. It waits for requests and,
 //       if appropriate, sends replies and updates the shared variables.
 func ReceiveRequest(ra *RASharedDB) {
	 for {
		 req := <-ra.ReqChan // Receive a request from the channel
		 payload := []byte("sample") // Sample payload (this can be updated with actual data)
 
		 ra.Mutex.Lock()
		 // Log the reception of the request
		 context := fmt.Sprint("Receiver ", ra.myPID, " got petition from ", req.Pid, " i am requesting CS: ", ra.ReqCS)
		 ra.logger.UnpackReceive(context, req.VClock, &payload, govec.GetDefaultLogOptions())
 
		 // Compare vector clocks to decide if the request should be deferred or not
		 otherVClock, _ := vclock.FromBytes(payload)
		 postergar := ra.exclude[Exclusion{ra.operation, req.Op}] && ra.ReqCS && iHappenBefore(ra.currentClk, otherVClock, ra.myPID, req.Pid)
 
		 if postergar {
			 ra.RepDefd[req.Pid-1] = true // Defer the reply if necessary
		 } else {
			 ra.mSys.Send(req.Pid, Reply{}) // Send reply immediately if no exclusion
		 }
		 ra.Mutex.Unlock()
	 }
 }
 
 func ReceiveReply(ra *RASharedDB) {
	 for {
		 <-ra.ReplChan // Wait for a reply from the channel
 
		 ra.Mutex.Lock()
		 ra.OutRepCnt-- // Decrement the count of outstanding replies
		 if ra.OutRepCnt == 0 {
			 ra.chrep <- true // Signal that all replies have been received
		 }
		 ra.Mutex.Unlock()
	 }
 }
 
 // --------------------UTILS-------------------- //
 // iHappenBefore checks if the request from the current process happens before the other process.
 func iHappenBefore(myVC, otherVC vclock.VClock, myPID, otherPID int) bool {
	 if myVC.Compare(otherVC, vclock.Descendant) {
		 return true
	 } else if myVC.Compare(otherVC, vclock.Concurrent) {
		 return myPID < otherPID // If the clocks are concurrent, lower PID process gets precedence
	 } else {
		 return false
	 }
 }
 
 // initExclude initializes the exclusion map, which controls which operations can be executed together.
 func initExclude() map[Exclusion]bool {
	 exclude := make(map[Exclusion]bool)
	 exclude[Exclusion{"Read", "Read"}] = false // Read-Read is allowed
	 exclude[Exclusion{"Read", "Write"}] = true  // Read-Write is not allowed
	 exclude[Exclusion{"Write", "Read"}] = true  // Write-Read is not allowed
	 exclude[Exclusion{"Write", "Write"}] = true // Write-Write is not allowed
	 return exclude
 }
 
 // getOp determines the type of operation (Read or Write) based on the process ID.
 func getOp(me int) string {
	 if (N / 2) >= me {
		 return "Write"
	 } else {
		 return "Read"
	 }
 }
 