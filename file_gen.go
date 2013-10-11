package main

import (
	"fmt"
	"log"
	"io/ioutil"
	"bufio"
	"encoding/json"
	"os"
) 


type FileDataSource struct {
    cacheReady bool
    cacheCurPos int
    fileInfoSlice []os.FileInfo
    currFileContent []string
    currFileRead int
    fileDir string
}


func (f *FileDataSource) Init(fileDir string) {

	f.fileDir = fileDir
	var err error
    f.fileInfoSlice, err = ioutil.ReadDir(fileDir)
    if err != nil {
    	log.Fatalf("Reading Data Dir: %v", err)
    }
    
    f.cacheReady = false
    f.cacheCurPos = 0
    f.currFileRead = 0
}

func (f *FileDataSource) Next() (string, string) {

	var err error
    if f.cacheReady == false {

		if f.currFileRead == len(f.fileInfoSlice) {
			log.Println("No more files to read")
			return "", ""
		}
		fileInfo := f.fileInfoSlice[f.currFileRead]
        f.currFileContent, err = readLines( f.fileDir + "/" + fileInfo.Name())
        if err != nil {
       		log.Fatalf("Unable to read file %s, Error: %v", fileInfo.Name(), err)
        }
	    if len(f.currFileContent) == 0 {
    		log.Fatalf("Data File Empty %s", fileInfo.Name())
    	}
	    f.cacheReady = true
     	f.currFileRead++
     	f.cacheCurPos = 0
    }


	if f.cacheCurPos == len(f.currFileContent) - 1 {
		f.cacheReady = false
	}
	
	jsonLine := f.currFileContent[f.cacheCurPos]
	f.cacheCurPos++
	
	var j interface{}
	err = json.Unmarshal([]byte(jsonLine), &j)
	if err != nil {
		log.Fatalf("Unable to decode json %v", err)
	}
	m := j.(map[string]interface{})
	p := m["profile_details"].(map[string] interface{})
	k := p["user_id"].(string)
	
	return k, jsonLine
	    
}


// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
  file, err := os.Open(path)
  if err != nil {
    return nil, err
  }
  defer file.Close()
 
  var lines []string
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    lines = append(lines, scanner.Text())
  }
  return lines, scanner.Err()
}
 
// writeLines writes the lines to the given file.
func writeLines(lines []string, path string) error {
  file, err := os.Create(path)
  if err != nil {
    return err
  }
  defer file.Close()
 
  w := bufio.NewWriter(file)
  for _, line := range lines {
    fmt.Fprintln(w, line)
  }
  return w.Flush()
}

