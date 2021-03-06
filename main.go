package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type EdisonWrapper struct {
	Form EdisonMessage `json:"form"`
}

type EdisonMessage struct {
	ID        string  `json:"id"`
	Timestamp uint64  `json:"ts"`
	Miles     float64 `json:"miles"`
	X         float64 `json:"x"`
	Y         float64 `json:"y"`
	Z         float64 `json:"z"`
}

var (
	conn                *websocket.Conn
	accThreshold        = 1.2
	lifetimeMax         = 150000
	scalingFactor       = 500
	mobileScalingFactor = 25.00
	messageTypeText     = 1
	assetIds            = []string{"320I-UID1", "320I-UID2", "320I-UID3", "320I-UID4", "320I-UID5", "320I-UID6", "320I-UID7", "320I-UID8", "320I-UID9", "320I-UID10", "320I-UID11", "320I-UID12"}
	assetIdMap          = make(map[string]string)
	decMap              = make(map[string]int)
	accMap              = make(map[string]int)
	milesMap            = make(map[string]float64)
	startMap            = make(map[string]uint64)
	upgrader            = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	assetIdMapMutex = &sync.Mutex{}
	decMapMutex     = &sync.Mutex{}
	accMapMutex     = &sync.Mutex{}
	milesMapMutex   = &sync.Mutex{}
	startMapMutex   = &sync.Mutex{}
	token           = os.Getenv("TOKEN")
)

func storeEvent(ts uint64, val float64, tag string, apmId string, lifetime int, accs int) {
	url := "https://apm-timeseries-services-hackapm.run.aws-usw02-pr.ice.predix.io/v2/time_series?file_type=json"
	body := "{\"tags\": ["
	body += fmt.Sprintf("{\"tagId\": \"%s\",\"errorCode\": null,\"errorMessage\": null,\"data\": [{\"ts\": %d,\"v\": \"%v\",\"q\": \"3\"}]},", tag, ts, val)
	body += fmt.Sprintf("{\"tagId\": \"%s\",\"errorCode\": null,\"errorMessage\": null,\"data\": [{\"ts\": %d,\"v\": \"%v\",\"q\": \"3\"}]},", fmt.Sprintf("%s.%s", apmId, "lifespan"), ts, lifetime)
	body += fmt.Sprintf("{\"tagId\": \"%s\",\"errorCode\": null,\"errorMessage\": null,\"data\": [{\"ts\": %d,\"v\": \"%v\",\"q\": \"3\"}]}", fmt.Sprintf("%s.%s", apmId, tag), ts, accs)
	body += "]}"

	fmt.Println("BODY: ", body)
	payload := strings.NewReader(body)

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("authorization", token)
	req.Header.Add("tenant", "E1AB6F7711A5403FB2B607EA1306D94F")
	req.Header.Add("content-type", "application/json")
	req.Header.Add("accept", "application/json")
	req.Header.Add("cache-control", "no-cache")
	req.Header.Add("postman-token", "357e82d4-fc97-3895-df56-9ff67b8a4a98")

	res, _ := http.DefaultClient.Do(req)
	fmt.Println("%v", res)
	if res.StatusCode > 299 {
		fmt.Println("ERROR: bad status code while posting to apm ts")
	}
}

func detectAccelerations(msg EdisonMessage) {
	var err error
	msgId := msg.ID

	if (msg.X > accThreshold) || (msg.Y > accThreshold) {
		accMapMutex.Lock()
		accMap[msgId] = accMap[msgId] + 1
		accMapMutex.Unlock()
		go storeEvent(msg.Timestamp, math.Max(msg.X, msg.Y), "Tag_Hard_Acceleration_1", assetIdMap[msgId], calcLifetime(msgId), accMap[msgId])
		if conn == nil {
			fmt.Println("ERROR: no active conns")
			return
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"apmId\":\"%s\", \"hardAcc\": %d, \"miles\": %d, \"lifetime\": %d}", msgId, assetIdMap[msgId], accMap[msgId], int(msg.Miles), calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard acc to ws")
		}
		time.Sleep(10 * time.Millisecond) //to stop concurrent ws writes
	}

	if (msg.X < -1*accThreshold) || (msg.Y < -1*accThreshold) {
		decMapMutex.Lock()
		decMap[msgId] = decMap[msgId] + 1
		decMapMutex.Unlock()

		go storeEvent(msg.Timestamp, math.Max(msg.X, msg.Y), "Tag_Hard_Breaks_1", assetIdMap[msgId], calcLifetime(msgId), decMap[msgId])
		if conn == nil {
			fmt.Println("ERROR: no active conns")
			return
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"apmId\": \"%s\", \"hardBreak\": %d, \"miles\": %d, \"lifetime\": %d}", msgId, assetIdMap[msgId], decMap[msgId], int(msg.Miles), calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard break to ws")
		}
	}
}

func calcLifetime(carId string) int {
	return int(math.Min(float64(lifetimeMax-scalingFactor*(accMap[carId]+decMap[carId])), float64(200000)))
}

func receive(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Access-Control-Allow-Origin", "*")
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
		startMapMutex.Lock()
		startMap[msg.ID] = msg.Timestamp
		startMapMutex.Unlock()

		if len(assetIds) > 0 {
			headId := assetIds[0]
			assetIds = assetIds[1:]
			assetIdMapMutex.Lock()
			assetIdMap[msg.ID] = headId
			assetIdMapMutex.Unlock()
		}
	}
	milesMapMutex.Lock()
	milesMap[msg.ID] = msg.Miles
	milesMapMutex.Unlock()

	detectAccelerations(msg)
}

func listen(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var err error
	conn, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func all(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	response := "["
	for key, value := range startMap {
		response += fmt.Sprintf("{\"carId\":\"%s\", \"apmId\": \"%s\", \"startTime\": %d,\"miles\":%d, \"hardAcc\": %d, \"hardBreak\": %d, \"lifetime\": %d}", key, assetIdMap[key], value, int(milesMap[key]), accMap[key], decMap[key], calcLifetime(key))
		response += ","
	}
	response = strings.TrimSuffix(response, ",")
	response += "]"
	io.WriteString(w, response)
}

func mobile(w http.ResponseWriter, r *http.Request) {
	fmt.Println("HELLO MOBILE")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	var msg EdisonMessage
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("ERROR: could not read body")
		return
	}
	fmt.Println("MOBILE BODY: ", string(body))
	err = json.Unmarshal(body, &msg)
	if err != nil {
		fmt.Println("ERROR: could not unmarshal wrapper body")
		fmt.Printf("BODY: %s\n", string(body))
		return
	}
	io.WriteString(w, "OK")

	_, found := startMap[msg.ID]
	if !found {
		startMapMutex.Lock()
		startMap[msg.ID] = msg.Timestamp
		startMapMutex.Unlock()

		if len(assetIds) > 0 {
			headId := assetIds[0]
			assetIds = assetIds[1:]
			assetIdMapMutex.Lock()
			assetIdMap[msg.ID] = headId
			assetIdMapMutex.Unlock()
		}
	}
	milesMapMutex.Lock()
	milesMap[msg.ID] = msg.Miles
	milesMapMutex.Unlock()

	msg.X = msg.X / mobileScalingFactor
	msg.Y = msg.Y / mobileScalingFactor
	detectAccelerations(msg)
}

func queryAPMTS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", fmt.Sprintf("*"))

	tag := r.URL.Query().Get("tag")
	tenant := r.URL.Query().Get("tenant")

	if len(tag) < 1 || len(tenant) < 1 {
		w.WriteHeader(400)
	}

	url := fmt.Sprintf("https://apm-timeseries-services-hackapm.run.aws-usw02-pr.ice.predix.io/v2/time_series?operation=raw&tagList=%s&startTime=2010-12-31T00:28:03.000Z&endTime=2017-04-05T00:28:03.000Z&responseFormat=KAIROSDB", tag)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Failed to create GET request")
		w.WriteHeader(500)

	}

	req.Header.Add("authorization", token)
	req.Header.Add("tenant", tenant)
	req.Header.Add("content-type", "application/json")
	req.Header.Add("cache-control", "no-cache")
	req.Header.Add("postman-token", "958d1a58-20f4-5361-fe72-aa149e73edf2")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("HTTP Request failed to execute")
		w.WriteHeader(500)
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Failed to read response body")
		w.WriteHeader(500)
	}
	w.Write(body)
}

func gradualImprovement() {
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			lifetimeMax += 250
			for key, _ := range startMap {
				msg := fmt.Sprintf("{\"carId\":\"%s\", \"apmId\":\"%s\", \"hardAcc\": %d, \"miles\": %d, \"lifetime\": %d}", key, assetIdMap[key], accMap[key], int(milesMap[key]), calcLifetime(key))
				if conn == nil {
					break
				}
				err := conn.WriteMessage(1, []byte(msg))
				if err != nil {
					fmt.Println("ERROR: could not write hard acc to ws")
				}
			}
		}
	}
}

func clearMap(m map[string]string) {
	for k := range m {
		delete(m, k)
	}
}

func clear(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", fmt.Sprintf("*"))

	lifetimeMax = 150000
	assetIds = []string{"320I-UID1", "320I-UID2", "320I-UID3", "320I-UID4", "320I-UID5", "320I-UID6", "320I-UID7", "320I-UID8", "320I-UID9", "320I-UID10", "320I-UID11", "320I-UID12"}
	assetIdMapMutex.Lock()
	decMapMutex.Lock()
	accMapMutex.Lock()
	milesMapMutex.Lock()
	startMapMutex.Lock()

	for k := range assetIdMap {
		delete(assetIdMap, k)
	}

	for k := range decMap {
		delete(decMap, k)
	}

	for k := range accMap {
		delete(accMap, k)
	}

	for k := range milesMap {
		delete(milesMap, k)
	}

	for k := range startMap {
		delete(startMap, k)
	}

	assetIdMapMutex.Unlock()
	decMapMutex.Unlock()
	accMapMutex.Unlock()
	milesMapMutex.Unlock()
	startMapMutex.Unlock()
}

func main() {
	go gradualImprovement()

	http.HandleFunc("/", receive)
	http.HandleFunc("/listen", listen)
	http.HandleFunc("/all", all)
	http.HandleFunc("/mobile", mobile)
	http.HandleFunc("/queryTS", queryAPMTS)
	http.HandleFunc("/clear", clear)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
	// test ingest
	// storeEvent(1469437879000, 1, "Tag_Hard_Breaks_2")
}
