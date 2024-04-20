package main

import (
	"p2/ms"
	"p2/ra"
)

type MessageHandler struct {
	SendChan chan ms.Message

	GetReqChan  chan ra.Request
	GetReplChan chan ra.Reply
	GetFileChan chan FileUpdate
	GetBarrChan chan Barrier

	mSys *ms.MessageSystem
}

func NewMessageHandler(whoIam int, usersFile string, messageTypes []ms.Message) *MessageHandler {

	mSys := ms.New(whoIam, usersFile, []ms.Message{ra.Request{}, ra.Reply{}, FileUpdate{}, Barrier{}})

	return &MessageHandler{
		SendChan: make(chan ms.Message),

		GetReqChan:  make(chan ra.Request),
		GetReplChan: make(chan ra.Reply),
		GetFileChan: make(chan FileUpdate),
		GetBarrChan: make(chan Barrier),

		mSys: &mSys,
	}
}

func (mh *MessageHandler) Start() {
	go func() {
		for {
			msg := mh.mSys.Receive()
			switch msgType := msg.(type) {
			case ra.Request:
				mh.GetReqChan <- msgType
			case ra.Reply:
				mh.GetReplChan <- msgType
			case FileUpdate:
				mh.GetFileChan <- msgType
			case Barrier:
				mh.GetBarrChan <- msgType
			}
		}
	}()
}

func (mh *MessageHandler) Send(pid int, msg ms.Message) {
	mh.mSys.Send(pid, msg)
}
