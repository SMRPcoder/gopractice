package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RequestData map[string]string

type ReAssign map[string]string

type AssignFormat struct {
	VALUE string `json:"value"`
	TYPE  string `json:"type"`
}

type ReAssignStruct struct {
	ATTRIBUTES map[string]AssignFormat `json:"attributes"`
	TRAITS     map[string]AssignFormat `json:"traits"`
}

type ResponseData struct {
	Event           string `json:"event"`
	EventType       string `json:"event_type"`
	AppID           string `json:"app_id"`
	UserID          string `json:"user_id"`
	MessageID       string `json:"message_id"`
	PageTitle       string `json:"page_title"`
	PageURL         string `json:"page_url"`
	BrowserLanguage string `json:"browser_language"`
	ScreenSize      string `json:"screen_size"`
	ReAssignStruct
}

type RootWorker struct {
	Resmap      map[string]string `json:"resmap"`
	Attributtes map[string]string `json:"attributtes"`
	Traits      map[string]string `json:"traits"`
}

type RemainingKeyVal struct {
	KEY   string
	VALUE string
	TYPE  string
}

func (R RemainingKeyVal) checkValues() bool {
	if len(R.KEY) == 0 {
		return false
	} else if len(R.VALUE) == 0 {
		return false
	} else if len(R.TYPE) == 0 {
		return false
	}
	return true
}

func main() {

	requestChannel := make(chan *http.Request)
	responseChannel := make(chan RootWorker)
	var wg sync.WaitGroup

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go worker(requestChannel, responseChannel, &wg)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		if r.Method == http.MethodGet {
			fmt.Fprint(w, "Hello World")
		} else if r.Method == http.MethodPost {
			requestChannel <- r
			go func() {
				wg.Wait()
				close(responseChannel)
			}()
			Resresult, ok := <-responseChannel
			if !ok {
				http.Error(w, "error channel not present or closed", http.StatusNoContent)
				return
			}

			resmap := Resresult.Resmap
			attributtes := Resresult.Attributtes
			traits := Resresult.Traits

			attr_arrs := colapsmap(attributtes)
			trait_arrs := colapsmap(traits)
			var allRemain ReAssignStruct
			allRemain.ATTRIBUTES = make(map[string]AssignFormat)
			allRemain.TRAITS = make(map[string]AssignFormat)
			newattrformatt := AssignFormat{}
			newtrformatt := AssignFormat{}

			go setValuesTo(attr_arrs, &allRemain.ATTRIBUTES, newattrformatt)
			go setValuesTo(trait_arrs, &allRemain.TRAITS, newtrformatt)
			resChannel := make(chan ResponseData)
			wg.Add(1)

			go finalResponse(resmap, allRemain, resChannel, &wg)

			fnResponse, ok := <-resChannel
			if !ok {
				fmt.Println(errors.New("response data is missing or channel closed"))
			}

			ch := make(chan string)
			wg.Add(1)
			go sentToWebHook(fnResponse, ch, &wg, startTime)
			go func() {
				wg.Wait()
				close(ch)
				close(resChannel)
				close(requestChannel)

			}()
			result, ok := <-ch
			if ok {
				fmt.Println(result)
			}

			jsonData, err := json.Marshal(fnResponse)
			if err != nil {
				http.Error(w, "Error marshaling JSON", http.StatusInternalServerError)
				return
			}
			w.Write(jsonData)

		}
	})

	fmt.Println("Server Started")
	http.ListenAndServe(":8000", nil)

}

func colapsmap(actual_arr map[string]string) map[int]RemainingKeyVal {
	attr_with_index := make(map[int]RemainingKeyVal)
	for key, val := range actual_arr {
		var lastkey int
		lastChar := key[len(key)-1]
		if lastDigit, err := strconv.Atoi(string(lastChar)); err == nil {
			lastkey = lastDigit
		}
		switch {
		case key[len(key)-2] == 'k':
			if v, ok := attr_with_index[lastkey]; ok {
				v.KEY = val
				attr_with_index[lastkey] = v
			} else {
				newStruct := RemainingKeyVal{KEY: val}
				attr_with_index[lastkey] = newStruct
			}
		case key[len(key)-2] == 'v':
			if v, ok := attr_with_index[lastkey]; ok {
				v.VALUE = val
				attr_with_index[lastkey] = v
			} else {
				newStruct := RemainingKeyVal{VALUE: val}
				attr_with_index[lastkey] = newStruct
			}
		case key[len(key)-2] == 't':
			if v, ok := attr_with_index[lastkey]; ok {
				v.TYPE = val
				attr_with_index[lastkey] = v
			} else {
				newStruct := RemainingKeyVal{TYPE: val}
				attr_with_index[lastkey] = newStruct
			}
		}
	}

	return attr_with_index
}

func setValuesTo(target map[int]RemainingKeyVal, allremain *map[string]AssignFormat, formatter AssignFormat) {
	for _, obj := range target {
		if obj.checkValues() {
			formatter.VALUE = obj.VALUE
			formatter.TYPE = obj.TYPE
			(*allremain)[obj.KEY] = formatter
		}
	}
}

func finalResponse(resmap map[string]string, allremain ReAssignStruct, channel chan ResponseData, wg *sync.WaitGroup) {
	defer wg.Done()
	var fnResponse ResponseData

	fnResponse.Event = resmap["event"]
	fnResponse.EventType = resmap["event_type"]
	fnResponse.AppID = resmap["app_id"]
	fnResponse.UserID = resmap["user_id"]
	fnResponse.MessageID = resmap["message_id"]
	fnResponse.PageTitle = resmap["page_title"]
	fnResponse.PageURL = resmap["page_url"]
	fnResponse.BrowserLanguage = resmap["browser_language"]
	fnResponse.ScreenSize = resmap["screen_size"]
	fnResponse.ATTRIBUTES = allremain.ATTRIBUTES
	fnResponse.TRAITS = allremain.TRAITS

	channel <- fnResponse
}

func sentToWebHook(fnResponse ResponseData, ch chan string, wg *sync.WaitGroup, timing time.Time) {
	defer wg.Done()

	dataToSend, err := json.Marshal(fnResponse)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
		return
	}
	buffer := bytes.NewBuffer(dataToSend)
	url := "https://webhook.site/8e8baf25-4de3-497d-8048-d7f8d14a4b9e"

	response, err := http.Post(url, "application/json", buffer)
	if err != nil {
		fmt.Println("Error making POST request:", err)
		return
	}
	defer response.Body.Close()
	endTime := time.Now()
	executionTime := endTime.Sub(timing)
	ch <- fmt.Sprintf("Response Sent to https://webhook.site/8e8baf25-4de3-497d-8048-d7f8d14a4b9e And Status was %s in a Time of %v ms", response.Status, executionTime.Milliseconds())
}

func worker(req chan *http.Request, res chan RootWorker, wg *sync.WaitGroup) {
	defer wg.Done()
	r, ok := <-req
	if !ok {
		fmt.Println("Channel is closed or error")
	}

	switch r.URL.Path {
	case "/":
		reassign := ReAssign{
			"ev":  "event",
			"et":  "event_type",
			"id":  "app_id",
			"uid": "user_id",
			"mid": "message_id",
			"t":   "page_title",
			"p":   "page_url",
			"l":   "browser_language",
			"sc":  "screen_size",
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			fmt.Println(errors.New("error on body reading"))
			return
		}
		var data RequestData
		json.Unmarshal(body, &data)
		// w.Write(body)
		resmap := make(map[string]string)
		attributtes := make(map[string]string)
		traits := make(map[string]string)
		for k, v := range data {

			if _, ok := reassign[k]; ok {
				resmap[reassign[k]] = v
			} else {
				switch {
				case strings.HasPrefix(k, "atr"):
					attributtes[k] = v

				case strings.HasPrefix(k, "uatr"):
					traits[k] = v
				}
			}
		}

		res <- RootWorker{Resmap: resmap, Attributtes: attributtes, Traits: traits}

	}

}
