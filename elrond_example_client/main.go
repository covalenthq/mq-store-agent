package main

import (
	"encoding/hex"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/ElrondNetwork/covalent-indexer-go/process/utility"
	"github.com/ElrondNetwork/covalent-indexer-go/schema"
	"github.com/gorilla/websocket"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	urlReceiveData := url.URL{Scheme: "ws", Host: "127.0.0.1:21111", Path: "/block"}
	log.Printf("connecting to %s", urlReceiveData.String())
	connectionReceiveData, _, err := websocket.DefaultDialer.Dial(urlReceiveData.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer connectionReceiveData.Close()

	urlAcknowledgeData := url.URL{Scheme: "ws", Host: "127.0.0.1:21111", Path: "/acknowledge"}
	log.Printf("connecting to %s", urlAcknowledgeData.String())
	connectionAcknowledgeData, _, err := websocket.DefaultDialer.Dial(urlAcknowledgeData.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer connectionAcknowledgeData.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := connectionReceiveData.ReadMessage()
			if err != nil {
				log.Println("error read message:", err)
				return
			}
			res := &schema.BlockResult{}

			errDecode := utility.Decode(res, message)
			if errDecode != nil {
				log.Println("could not decode block", errDecode)
			}
			log.Printf("received block hash: %v, nonce: %v", hex.EncodeToString(res.Block.Hash), res.Block.Nonce)
			log.Println("sending back acknowledged hash...")

			errAcknowledgeData := connectionAcknowledgeData.WriteMessage(websocket.BinaryMessage, res.Block.Hash)
			if errAcknowledgeData != nil {
				log.Println("could not send acknowledged hash :(", errAcknowledgeData)
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := connectionReceiveData.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			_ = connectionAcknowledgeData.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}