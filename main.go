package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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
	lifetimeMax         = 200000
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
)

func storeEvent(ts uint64, val float64, tag string) {
	url := "https://apm-timeseries-services-hackapm.run.aws-usw02-pr.ice.predix.io/v2/time_series?file_type=json"
	//"{\n  \"tags\": [\n    {\n      \"tagId\": \"TAG_HARD_BREAKS\",\n      \"errorCode\": null,\n      \"errorMessage\": null,\n      \"data\": [\n        {\n          \"ts\": 1469431390000,\n          \"v\": \"150.0\",\n          \"q\": \"3\"\n        }\n      ]\n    }\n  ]\n}"
	//"{\n  \"tags\": [\n    {\n      \"tagId\": \"%s\",\n      \"errorCode\": null,\n      \"errorMessage\": null,\n      \"data\": [\n        {\n          \"ts\": %d,\n          \"v\": \"%f\",\n          \"q\": \"3\"\n        }\n      ]\n    }\n  ]\n}", tag, ts, val))
	// payload := strings.NewReader("{\n  \"tags\": [\n    {\n      \"tagId\": \"Tag_Hard_Acceleration_1\",\n      \"errorCode\": null,\n      \"errorMessage\": null,\n      \"data\": [\n        {\n          \"ts\": 1469437375000,\n          \"v\": \"155.0\",\n          \"q\": \"3\"\n        }\n      ]\n    }\n  ]\n}")
	payload := strings.NewReader(fmt.Sprintf("{\n  \"tags\": [\n    {\n      \"tagId\": \"%s\",\n      \"errorCode\": null,\n      \"errorMessage\": null,\n      \"data\": [\n        {\n          \"ts\": %d,\n          \"v\": \"%v\",\n          \"q\": \"3\"\n        }\n      ]\n    }\n  ]\n}", tag, ts, val))

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("authorization", "bearer eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiI0ZDJkMGYzMy1jYjAzLTRjMGQtYTgwNC0zNjBmY2NjMjYyMDMiLCJzdWIiOiJlNDNkNjlmMC01NjlkLTQxOGEtYjMzYi05ZjAxYjY1NGNkOTMiLCJzY29wZSI6WyJwYXNzd29yZC53cml0ZSIsIm9wZW5pZCJdLCJjbGllbnRfaWQiOiJpbmdlc3Rvci45Y2YzM2NlMzdiZjY0YzU2ODFiNTE1YTZmNmFhZGY0NyIsImNpZCI6ImluZ2VzdG9yLjljZjMzY2UzN2JmNjRjNTY4MWI1MTVhNmY2YWFkZjQ3IiwiYXpwIjoiaW5nZXN0b3IuOWNmMzNjZTM3YmY2NGM1NjgxYjUxNWE2ZjZhYWRmNDciLCJncmFudF90eXBlIjoicGFzc3dvcmQiLCJ1c2VyX2lkIjoiZTQzZDY5ZjAtNTY5ZC00MThhLWIzM2ItOWYwMWI2NTRjZDkzIiwib3JpZ2luIjoidWFhIiwidXNlcl9uYW1lIjoiZ2VuZXNpc191c2VyMSIsImVtYWlsIjoiam9lQGdlLmNvbSIsImF1dGhfdGltZSI6MTQ2OTQyNjUxNCwicmV2X3NpZyI6IjNjMzE2YjM3IiwiaWF0IjoxNDY5NDI2NTE0LCJleHAiOjE0Njk1MTI5MTQsImlzcyI6Imh0dHBzOi8vZDllZjEwNmMtNzA0OC00ODZlLWE3OWYtOWM4MDgyN2I4YTE0LnByZWRpeC11YWEucnVuLmF3cy11c3cwMi1wci5pY2UucHJlZGl4LmlvL29hdXRoL3Rva2VuIiwiemlkIjoiZDllZjEwNmMtNzA0OC00ODZlLWE3OWYtOWM4MDgyN2I4YTE0IiwiYXVkIjpbImluZ2VzdG9yLjljZjMzY2UzN2JmNjRjNTY4MWI1MTVhNmY2YWFkZjQ3IiwicGFzc3dvcmQiLCJvcGVuaWQiXX0.Nl1hD5-pTtehZbmhxA8BXO97Qi39ml_-Tn8vqeoSHIPpqTWkCWHAk8k9vtc5_WWxIfj1V6tBfRQ5f6S5dEqh78jjR099xOOrrcGi9yhob7MydjaE5VvCk5hqwU87w8vOLwhI7cRSruHLij8zClgWEJd-LwcRXXK7zpgBBqBN5AK3DZ5HaoP0VHZJaJsQykEUk6EM8G_BeUxQEaZr6TxXyd-C2flxxq9tqDWyCTu1YZ-KRnXKbdtU5Hw0ONfPK3951EHwLUxvSMbIXalDLcf1UQTL7yRsnaBt4s7R40CBei9wl3LtejluZ34CCthT1y67nUL7uZ7Ay-l4xu1E4Mp2zw")
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
		accMap[msgId] = accMap[msgId] + 1
		go storeEvent(msg.Timestamp, msg.X, "HA_3")
		if conn == nil {
			fmt.Println("ERROR: no active conns")
			return
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"hardAcc\": %d, \"miles\": %d, \"lifetime\": %d}", msgId, accMap[msgId], int(msg.Miles), calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard acc to ws")
		}
		time.Sleep(10 * time.Millisecond) //to stop concurrent ws writes
	}

	if (msg.X < -1*accThreshold) || (msg.Y < -1*accThreshold) {
		decMap[msgId] = decMap[msgId] + 1
		go storeEvent(msg.Timestamp, msg.Y, "Tag_Hard_Acceleration_1")
		if conn == nil {
			fmt.Println("ERROR: no active conns")
			return
		}
		err = conn.WriteMessage(1, []byte(fmt.Sprintf("{\"carId\":\"%s\", \"hardBreak\": %d, \"miles\": %d, \"lifetime\": %d}", msgId, decMap[msgId], int(msg.Miles), calcLifetime(msgId))))
		if err != nil {
			fmt.Println("ERROR: could not write hard break to ws")
		}
	}
}

func calcLifetime(carId string) int {
	return lifetimeMax - scalingFactor*(accMap[carId]+decMap[carId])
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
		startMap[msg.ID] = msg.Timestamp
	}
	milesMap[msg.ID] = msg.Miles
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
		response += fmt.Sprintf("{\"carId\":\"%s\", \"startTime\": %d,\"miles\":%d, \"hardAcc\": %d, \"hardBreak\": %d, \"lifetime\": %d}", key, value, int(milesMap[key]), accMap[key], decMap[key], calcLifetime(key))
		response += ","
	}
	response = strings.TrimSuffix(response, ",")
	response += "]"
	io.WriteString(w, response)
}

func mobile(w http.ResponseWriter, r *http.Request) {
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
		startMap[msg.ID] = msg.Timestamp
		headId := assetIds[0]
		assetIds = assetIds[1:]
		assetIdMap[msg.ID] = headId
	}
	milesMap[msg.ID] = msg.Miles
	msg.X = msg.X / mobileScalingFactor
	msg.Y = msg.Y / mobileScalingFactor
	detectAccelerations(msg)
}

func queryAPMTS(w http.ResponseWriter, r *http.Request) {

	tag := r.URL.Query().Get("tag")
	tenant := r.URL.Query().Get("tenant")

	fmt.Println("TAG & TENANT", tag, tenant)

	url := fmt.Sprintf("https://apm-timeseries-services-hackapm.run.aws-usw02-pr.ice.predix.io/v2/time_series?operation=raw&tagList=%s&startTime=2010-12-31T00:28:03.000Z&endTime=2017-04-05T00:28:03.000Z&responseFormat=KAIROSDB", tag)

	fmt.Println(url)

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("authorization", "bearer eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiJjYWViYTM4Mi1mZjcwLTQ0MmItOTIwZS1iNGRmZjI3MjQ4ZGIiLCJzdWIiOiI2YWQ5Nzg3Ny0zMTQ3LTQyYzUtOGIwNi1iY2U3NTU5OTNmMzMiLCJzY29wZSI6WyJwYXNzd29yZC53cml0ZSIsIm9wZW5pZCJdLCJjbGllbnRfaWQiOiJpbmdlc3Rvci45Y2YzM2NlMzdiZjY0YzU2ODFiNTE1YTZmNmFhZGY0NyIsImNpZCI6ImluZ2VzdG9yLjljZjMzY2UzN2JmNjRjNTY4MWI1MTVhNmY2YWFkZjQ3IiwiYXpwIjoiaW5nZXN0b3IuOWNmMzNjZTM3YmY2NGM1NjgxYjUxNWE2ZjZhYWRmNDciLCJncmFudF90eXBlIjoicGFzc3dvcmQiLCJ1c2VyX2lkIjoiNmFkOTc4NzctMzE0Ny00MmM1LThiMDYtYmNlNzU1OTkzZjMzIiwib3JpZ2luIjoidWFhIiwidXNlcl9uYW1lIjoic2siLCJlbWFpbCI6ImphbmVAZ2UuY29tIiwiYXV0aF90aW1lIjoxNDY5NDM5NDUyLCJyZXZfc2lnIjoiYjc0NzUwYmQiLCJpYXQiOjE0Njk0Mzk0NTIsImV4cCI6MTQ2OTUyNTg1MiwiaXNzIjoiaHR0cHM6Ly9kOWVmMTA2Yy03MDQ4LTQ4NmUtYTc5Zi05YzgwODI3YjhhMTQucHJlZGl4LXVhYS5ydW4uYXdzLXVzdzAyLXByLmljZS5wcmVkaXguaW8vb2F1dGgvdG9rZW4iLCJ6aWQiOiJkOWVmMTA2Yy03MDQ4LTQ4NmUtYTc5Zi05YzgwODI3YjhhMTQiLCJhdWQiOlsiaW5nZXN0b3IuOWNmMzNjZTM3YmY2NGM1NjgxYjUxNWE2ZjZhYWRmNDciLCJwYXNzd29yZCIsIm9wZW5pZCJdfQ.wC0Tfbq1m9W6OOmFTcJ0THJohRgV7SEdwH3tyoWX2by8MrbWGWT1Ne5Y4iioPkNeS0y987yKljp7YVghWcpyovKFknG_0RNttsf55u4lMiWTgxHaHidZU_UETLGS-byJYO5Bkn-xP-hG5-BNjpNDw4-u-xdUU_DCmY4XTR6QNA3uregJIGt-v8-dDej_z1fAN01Bw-MjrdD4zvHPU4UoYug4bBqw2ckYB9MWF94qRqj5iOsCALXOlJHaisAoPHQVgsQgJ8fFuh8DX9eEziQ1Bew5rEMzjPRtRzUDbwkEmjyEn9YPGSpp1kfCvloxZ6fLQcwuMbtJnqxCKvyU_bS6ow")
	req.Header.Add("tenant", tenant)
	req.Header.Add("content-type", "application/json")
	req.Header.Add("cache-control", "no-cache")
	req.Header.Add("postman-token", "958d1a58-20f4-5361-fe72-aa149e73edf2")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := ioutil.ReadAll(res.Body)

	fmt.Println(res)

	w.Write(body)
}

func main() {
	http.HandleFunc("/", receive)
	http.HandleFunc("/listen", listen)
	http.HandleFunc("/all", all)
	http.HandleFunc("/mobile", mobile)
	http.HandleFunc("/queryTS", queryAPMTS)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)

	// test ingest
	// storeEvent(1469437879000, 1, "Tag_Hard_Breaks_2")
}
