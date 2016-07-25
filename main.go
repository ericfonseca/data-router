package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/websocket"
)

type EdisonWrapper struct {
	Form EdisonMessage `json:"form"`
}

type EdisonMessage struct {
	ID        string  `json:"id"`
	Timestamp uint64  `json:"ts"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Z         float64 `json:"z"`
}

var (
	conn            *websocket.Conn
	accThreshold    = 1.2
	lifetimeMax     = 100000
	scalingFactor   = 500
	messageTypeText = 1
	decMap          = make(map[string]int)
	accMap          = make(map[string]int)
	startMap        = make(map[string]uint64)
	upgrader        = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
)

func detectAccelerations(msg EdisonMessage) {
	var err error
	msgId := msg.ID

	if (msg.X > accThreshold) || (msg.Y > accThreshold) {
		accMap[msgId]++
		if conn == nil {
			fmt.Println("ERROR: no active conns")
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"hardAcc\": %d, \"lifetime\": %d}", msgId, accMap[msgId], calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard acc to ws")
		}
	}

	if (msg.X < -1*accThreshold) || (msg.Y < -1*accThreshold) {
		decMap[msg.ID]++
		if conn == nil {
			fmt.Println("ERROR: no active conns")
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"hardBreak\": %d, \"lifetime\": %d}", msgId, decMap[msgId], calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard break to ws")
		}
	}
}

func calcLifetime(carId string) int {
	return lifetimeMax - scalingFactor*(accMap[carId]+decMap[carId])
}

func receive(w http.ResponseWriter, r *http.Request) {
	var wrapper EdisonWrapper
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("ERROR: could not read body")
		return
	}

	err = json.Unmarshal(body, &wrapper)
	if err != nil {
		fmt.Println("ERROR: could not unmarshal wrapper body")
		return
	}
	io.WriteString(w, "OK")

	msg := wrapper.Form
	_, found := startMap[msg.ID]
	if !found {
		startMap[msg.ID] = msg.Timestamp
	}

	detectAccelerations(msg)
}

func listen(w http.ResponseWriter, r *http.Request) {
	var err error
	conn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func all(w http.ResponseWriter, r *http.Request) {
	response := "["
	for key, value := range startMap {
		response += fmt.Sprintf("{\"carId\":\"%s\", \"startTime\": %d,\"hardAcc\": %d, \"hardBreak\": %d, \"lifetime\": %d}", key, value, accMap[key], decMap[key], calcLifetime(key))
		response += ","
	}
	response = strings.TrimSuffix(response, ",")
	response += "]"
	io.WriteString(w, response)
}

func main() {
	http.HandleFunc("/", receive)
	http.HandleFunc("/listen", listen)
	http.HandleFunc("/all", all)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}
