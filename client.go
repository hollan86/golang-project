package main

import (
	"encoding/json"
	"bytes"
	"fmt"
	"net/http"
	"time"
	//"math/rand"
)

const (
	MAX_COUNT = 1000000001
	MIN_CLIENT_ID = 1
	MAX_CLIENT_ID = 10
	URL = "http://localhost:8000/json"
)

type Data struct {
	Text string	`json:"text"`
	Content_id int	`json:"content_id"`
	Client_id int	`json:"client_id"`
	Timestamp time.Time	`json:"timestamp"`
}

func main() {
	client := &http.Client{}

	for i := 1;i < MAX_COUNT ; i++ {
		payload := Data{"hello world",i,rand.Intn(MAX_CLIENT_ID - MIN_CLIENT_ID) + MIN_CLIENT_ID,time.Now()}
		//payload := Data{"hello world",i,1,time.Now()}
		jsonData,_ := json.Marshal(payload)
		
		req, err := http.NewRequest("POST", URL, bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		
		//defer resp.Body.Close()
		fmt.Println("Response Status:", resp.Status)
		resp.Body.Close()
	}
}
