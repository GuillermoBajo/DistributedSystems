/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
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
	N = 6 // Número de procesos
)

type Request struct {
	VClock []byte
	Pid    int
	Op     string
}

type Reply struct{}

type Exclusion struct {
	type1 string
	type2 string
}

type RASharedDB struct {
	OutRepCnt int
	ReqCS     bool // Indica si estamos pidiendo sección crítica
	RepDefd   []bool
	mSys      *ms.MessageSystem
	ReqChan   chan Request
	ReplChan  chan Reply
	done      chan bool
	chrep     chan bool
	Mutex     sync.Mutex // Bloqueo para proteger concurrencia sobre las variables
	myPID     int
	operation string             // Operación que realiza el proceso (Read o Write)
	exclude   map[Exclusion]bool // Mapa de exclusiones, indica si se puede ejecutar una operación u otra
	logger    *govec.GoLog
	currentClk vclock.VClock
}

// Pre: Verdad
// Post: Devuelve un puntero a una estructura RASharedDB inicializada. Crea un sistema de mensajes,
//
//	inicializa las variables y lanza un proceso concurrente que se queda esperando mensajes.
func New(me int, usersFile string, mSys *ms.MessageSystem, reqChan chan Request, replChan chan Reply) *RASharedDB {

	logger := govec.InitGoVector(strconv.Itoa(me), fmt.Sprintf("log/%d", me), govec.GetDefaultConfig())

	ra := RASharedDB{0, false, make([]bool, N), mSys, reqChan, replChan, make(chan bool), make(chan bool),
		sync.Mutex{}, me, getOp(me), initExclude(), logger, logger.GetCurrentVC().Copy()}

	// TODO completar
	go ReceiveRequest(&ra)
	go ReceiveReply(&ra)
	return &ra
}

// Pre: Verdad
// Post: Realiza  el  PreProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol() {
	// TODO completar
	ra.Mutex.Lock()
	ra.ReqCS = true
	ra.OutRepCnt = N-1
	
	context := fmt.Sprint("Process ", ra.myPID, " is requesting CS")
	msgPayload := ra.logger.GetCurrentVC().Copy()
	msgPayload.Tick(strconv.Itoa(ra.myPID))
	payload := msgPayload.Bytes()
	clk := ra.logger.PrepareSend(context, payload, govec.GetDefaultLogOptions())
	ra.currentClk = ra.logger.GetCurrentVC().Copy()

	ra.Mutex.Unlock()

	for j := 1; j <= N; j++ {
		if j != ra.myPID {
			ra.mSys.Send(j, Request{clk, ra.myPID, ra.operation})
			// ra.SendReq <- SendRequest{j, Request{clk, ra.myPID, ra.operation}}
		}
	}
	<-ra.chrep
}

// Pre: Verdad
// Post: Realiza  el  PostProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	// TODO completar
	ra.Mutex.Lock()
	ra.ReqCS = false

	for j := 0; j < N; j++ {
		if ra.RepDefd[j] {
			ra.RepDefd[j] = false
			ra.mSys.Send(j+1, Reply{})
		}
	}
	ra.Mutex.Unlock()
}

func (ra *RASharedDB) Stop() {
	// ra.ms.Stop()
	ra.done <- true
}

// Pre: ra es un puntero a una estructura RASharedDB inicializada
// Post: Lanza un proceso concurrente que se queda esperando mensajes. Espera mensajes de tipo Request,
//
//	envía mensajes Reply si procede y actualiza las variables compartidas.
func ReceiveRequest(ra *RASharedDB) {
	for {
		// req := ra.mSys.Receive()
		req := <-ra.ReqChan
		payload := []byte("sample")

		ra.Mutex.Lock()
		context := fmt.Sprint("Receiver ", ra.myPID, " got petition from ", req.Pid, " i am requesting CS: ", ra.ReqCS)
		ra.logger.UnpackReceive(context, req.VClock, &payload, govec.GetDefaultLogOptions())
		
		otherVClock, _ := vclock.FromBytes(payload)
		postergar := ra.exclude[Exclusion{ra.operation, req.Op}] && ra.ReqCS && iHappenBefore(ra.currentClk, otherVClock, ra.myPID, req.Pid)

		if postergar {
			ra.RepDefd[req.Pid-1] = true
		} else {
			ra.mSys.Send(req.Pid, Reply{})
		}
		ra.Mutex.Unlock()
	}
}

func ReceiveReply(ra *RASharedDB) {
	for {
		// _ = ra.ms.Receive().(Reply)
		<-ra.ReplChan

		ra.Mutex.Lock()
		ra.OutRepCnt--
		if ra.OutRepCnt == 0 {
			ra.chrep <- true
		}
		ra.Mutex.Unlock()
	}
}

// --------------------UTILS-------------------- //
func iHappenBefore(myVC, otherVC vclock.VClock, myPID, otherPID int) bool {
	// TODO completar
	if myVC.Compare(otherVC, vclock.Descendant) {
		return true
	} else if myVC.Compare(otherVC, vclock.Concurrent) {
		return myPID < otherPID
	} else {
		return false
	}
}

func initExclude() map[Exclusion]bool {
	exclude := make(map[Exclusion]bool)
	exclude[Exclusion{"Read", "Read"}] = false
	exclude[Exclusion{"Read", "Write"}] = true
	exclude[Exclusion{"Write", "Read"}] = true
	exclude[Exclusion{"Write", "Write"}] = true
	return exclude
}

func getOp(me int) string {
	if (N / 2) >= me {
		return "Write"
	} else {
		return "Read"
	}
}
