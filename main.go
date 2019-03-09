package main

import (
	"github.com/labstack/echo"
	"net"
	"encoding/json"
	"strings"
	"bytes"
	"encoding/gob"
	"net/http"
	"os"
	"math"
	"io/ioutil"
	"fmt"
	"sync"
)

func main() {
	t := os.Getenv("TYPE")

	switch t {
	case "MAP":
		mapper()
	case "MASTER":
		master()
	case "REDUCE":
		reducer()
	}
}

func min(a, b int) int { if a <= b { return a }; return b }

func master() {
	e := echo.New()

	var client = &http.Client{
	}

	e.GET("/compute", func(c echo.Context) error {
		text := c.QueryParam("text")

		words := strings.Split(text, " ")

		// MAPPING

		mapperHost := os.Getenv("MAPPER_HOST")

		var mapperIps []string
		ips, _ := net.LookupIP(mapperHost)
		for _, ip := range ips {
			mapperIps = append(mapperIps, ip.String())
		}

		mapSplitCount := int(math.Ceil(float64(len(words)) / float64(len(mapperIps))))

		var mapSplits = map[string][]string{}

		for idx, mapperIp := range mapperIps {
			if idx*mapSplitCount >= len(words) { break }
			mapSplits[mapperIp] = words[idx*mapSplitCount:min(idx*mapSplitCount+mapSplitCount, len(words))]
		}

		var mapping = map[string]map[string]int{}

		var wgm sync.WaitGroup
		wgm.Add(len(mapSplits))

		for host, split := range mapSplits {
			go func(host string, split []string) {
				defer wgm.Done()

				req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s:%s/map", host, os.Getenv("MAPPER_PORT")), nil)

				q := req.URL.Query()
				q.Add("str", strings.Join(split, " "))
				req.URL.RawQuery = q.Encode()

				res, _ := client.Do(req)
				body, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()

				buf := bytes.NewBuffer(body)


				var decodedMap map[string]int
				decoder := gob.NewDecoder(buf)
				_ = decoder.Decode(&decodedMap)

				mapping[host] = decodedMap
			}(host, split)
		}

		wgm.Wait()

		//SHUFFLING

		var shuffling = map[string][]int{}

		for _, host := range mapping {
			for word, count := range host {
				shuffling[word] = append(shuffling[word], count)
			}
		}

		//REDUCING

		reducerHost := os.Getenv("REDUCER_HOST")

		var reducerIps []string
		ips, _ = net.LookupIP(reducerHost)
		for _, ip := range ips {
			reducerIps = append(reducerIps, ip.String())
		}

		var shuffleWords []string
		for word := range shuffling {
			shuffleWords = append(shuffleWords, word)
		}

		reduceSplitCount := int(math.Ceil(float64(len(shuffleWords)) / float64(len(reducerIps))))

		var reduceSplits = map[string]map[string][]int{}

		for idx, reducerIp := range reducerIps {
			if idx*reduceSplitCount >= len(shuffleWords) { break }
			reduceWords := shuffleWords[idx*reduceSplitCount:min(idx*reduceSplitCount+reduceSplitCount, len(shuffleWords))]

			reduceSplits[reducerIp] = map[string][]int{}
			for _, reduceKey := range reduceWords {
				reduceSplits[reducerIp][reduceKey] = shuffling[reduceKey]
			}
		}


		var wgr sync.WaitGroup
		wgr.Add(len(reduceSplits))

		var reducing = map[string]map[string]int{}

		for host, split := range reduceSplits {
			func (host string, split map[string][]int) {
				defer wgr.Done()
				req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s:%s/reduce", host, os.Getenv("REDUCER_PORT")), nil)

				buf := new(bytes.Buffer)
				encoder := gob.NewEncoder(buf)
				_ = encoder.Encode(split)

				q := req.URL.Query()
				q.Add("body", string(buf.Bytes()))
				req.URL.RawQuery = q.Encode()

				res, _ := client.Do(req)
				body, _ := ioutil.ReadAll(res.Body)
				_ = res.Body.Close()

				buf = bytes.NewBuffer(body)


				var decodedReduce = map[string]int{}
				decoder := gob.NewDecoder(buf)
				_ = decoder.Decode(&decodedReduce)

				reducing[host] = decodedReduce
			}(host, split)
		}

		wgr.Wait()

		return json.NewEncoder(c.Response()).Encode(&reducing)
	})

	e.Logger.Fatal(e.Start(":8080"))
}

func mapper() {
	e := echo.New()

	e.GET("/map", func(c echo.Context) error {
		str := c.QueryParam("str")

		words := strings.Split(str, " ")

		mapping := map[string]int{}

		for _, word := range words {
			if _, prs := mapping[word]; prs {
				mapping[word] += 1
			} else {
				mapping[word] = 1
			}
		}

		buf := new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		_ = encoder.Encode(mapping)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}

func reducer() {
	e := echo.New()

	e.GET("/reduce", func(c echo.Context) error {
		body := c.QueryParam("body")

		buf := bytes.NewBuffer([]byte(body))

		var reduceData = map[string][]int{}

		decoder := gob.NewDecoder(buf)
		_ = decoder.Decode(&reduceData)

		var reducing = map[string]int{}

		for key, value := range reduceData {
			reducing[key] = 0
			for _, count := range value {
				reducing[key] += count
			}
		}

		buf = new(bytes.Buffer)
		encoder := gob.NewEncoder(buf)
		_ = encoder.Encode(reducing)

		return c.Blob(http.StatusOK, "application/octet-stream", buf.Bytes())
	})

	e.Logger.Fatal(e.Start(":8080"))
}
