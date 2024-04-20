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

type FileUpdate struct {
	Fragment string 
}

type Barrier struct{}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func escritor(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, mSys *ms.MessageSystem, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	frag := fmt.Sprintln("proccess", me, "wrote this")
	for {
		time.Sleep(time.Duration(me) * time.Millisecond)

		// Preprotocol de Ricart-Agrawala. Espera a que se le conceda el acceso a la sección crítica.
		rasdb.PreProtocol()

		// Notifica a los procesos el cambio
		for i := 1; i <= ra.N; i++ {
			if i != me {
				mSys.Send(i, FileUpdate{frag})
			} else {
				writeMutex.Lock()
				gestF.EscribirFichero(frag)
				writeMutex.Unlock()
			}
		}
		// Postprotocol de Ricart-Agrawala. Libera el acceso a la sección crítica. Los otros procesos podran
		// leer-escribir en el fichero.
		rasdb.PostProtocol()
	}
}

func lector(me int, gestF gf.GestorFichero, rasdb *ra.RASharedDB, wg *sync.WaitGroup, writeMutex *sync.Mutex) {
	for {
		time.Sleep(time.Duration(me) * time.Millisecond)

		rasdb.PreProtocol()
		_ = gestF.LeerFichero()
		rasdb.PostProtocol()
	}
}

func main() {
	myFile := "p2_" + os.Args[1] + ".txt"
	gFich := gf.New(myFile)

	myPID, err := strconv.Atoi(os.Args[1])
	checkError(err)
	usersFile := os.Args[2]

	reqChan := make(chan ra.Request)
	replChan := make(chan ra.Reply)
	mSys := ms.New(myPID, usersFile, []ms.Message{ra.Request{}, ra.Reply{}, FileUpdate{}, Barrier{}})

	ras := ra.New(myPID, usersFile, &mSys, reqChan, replChan)

	syncBarrier := make(chan bool)
	writeMutex := sync.Mutex{}
	go handleMessages(&mSys, reqChan, replChan, syncBarrier, gFich, &writeMutex)

	barrera(&mSys, syncBarrier, myPID)

	var wg sync.WaitGroup
	wg.Add(1)
	//
	if ra.N/2 >= myPID {
		go escritor(myPID, *gFich, ras, &mSys, &wg, &writeMutex)
	} else {
		go lector(myPID, *gFich, ras, &wg, &writeMutex)
	}

	wg.Wait()
	fmt.Println("Done")
}

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

func barrera(mSys *ms.MessageSystem, syncBarrier chan bool, myPID int) {
	time.Sleep(1 * time.Second)
	for i := 1; i <= ra.N; i++ {
		if i != myPID {
			mSys.Send(i, Barrier{})
		}
	}
	num_ack := 0
	for num_ack < ra.N-1 {
		<-syncBarrier
		num_ack++
	}
	fmt.Println(myPID, " - barrera superada")
}

// type MessageHandler struct {
// 	SendRequest 	chan ra.Request
// 	SendReply   	chan ra.Reply
// 	ReceiveRequest 	chan ra.Request
// 	ReceiveReply   	chan ra.Reply

// 	SendFileUpdate 	chan FileUpdate
// 	ReceiveFileUpdate 	chan FileUpdate

// 	SendBarrier 	chan Barrier
// 	ReceiveBarrier 	chan Barrier
// }

// func NewMessageHandler(mSys *ms.MessageSystem) *MessageHandler {
// 	return &MessageHandler{
// 		SendRequest: 	make(chan ra.Request),
// 		SendReply:   	make(chan ra.Reply),
// 		ReceiveRequest: make(chan ra.Request),
// 		ReceiveReply:   make(chan ra.Reply),

// 		SendFileUpdate: 	make(chan FileUpdate),
// 		ReceiveFileUpdate: 	make(chan FileUpdate),

// 		SendBarrier: 	make(chan Barrier),
// 		ReceiveBarrier: 	make(chan Barrier),
// 	}
// }
