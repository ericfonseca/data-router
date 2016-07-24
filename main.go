package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

type EdisonMessage struct {
	ID        string  `json:id`
	Timestamp uint64  `json:ts`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Z         float64 `json:"z"`
}

var (
	messageTypeText = 1
	connMap         = make(map[string]*websocket.Conn)
	upgrader        = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
)

func receive(w http.ResponseWriter, r *http.Request) {
	var msg EdisonMessage
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("ERROR: could not read body")
		return
	}

	//print for now, eventually ingest to apm ts
	fmt.Printf("URL: %s, BODY: %s\n", r.URL, string(body))
	io.WriteString(w, "OK")

	err = json.Unmarshal(body, &msg)
	if err != nil {
		fmt.Println("ERROR: could not unmarshal body")
		return
	}
	conn, found := connMap[msg.ID]
	if !found {
		fmt.Printf("ERROR: could not find connection with id %s\n", msg.ID)
		return
	}
	if err := conn.WriteMessage(1, body); err != nil {
		return
		fmt.Println("ERROR: could not write message")
	}
}

func listen(w http.ResponseWriter, r *http.Request) {
	carID := r.URL.Query().Get("carId")
	var err error
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	connMap[carID] = conn
}

func main() {
	http.HandleFunc("/", receive)
	http.HandleFunc("/listen", listen)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}
