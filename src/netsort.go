package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"log"
	"os"
	"strconv"
	"net"
	"math"
	"io/ioutil"
	"sort"
	"bytes"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func handleConnection(conn net.Conn, ch chan <- []byte){
	record := []byte{}
	for{
		for{
			buffer := make([]byte, 100)
			bytes, err := conn.Read(buffer)
			if err != nil{
				if err == io.EOF {
					conn.Close()
					return
				} else {
					log.Panicln(err)
				}
			}
			record = append(record, buffer[:bytes]...)
			if len(record)>=100{
				ch <- record[:100]
				record = record[100:]
				break
			}
		}
	}
	
}


func listenforData(ch chan <- []byte, host string, port string){
	l, err := net.Listen("tcp", host+":"+port)
	if err != nil{
		log.Panic(err)
	}
	defer l.Close()

	for{
		conn, err := l.Accept()
		if err != nil{
			log.Panicln(err)
		}
		go handleConnection(conn, ch)
	}
}


func sendData(conn net.Conn, data [][]byte){
	for _, record := range data{
		conn.Write(record)
	}
	all_zero_record := make([]byte, 100)
	conn.Write(all_zero_record)
	conn.Close()
}

func dialToServers(serverId int, scs ServerConfigs, records_by_server map[int][][]byte){
	for _, serv := range scs.Servers {
		if serv.ServerId == serverId {
			continue
		}
		data := records_by_server[serv.ServerId]
		for{
			conn, err := net.Dial("tcp", serv.Host+":"+serv.Port)
			if err == nil{
				go sendData(conn, data)
				break
			}
			continue
		}
	}
}

func consolidateData(ch <-chan []byte, numOfClients int) [][]byte{
	all_zero_record := make([]byte, 100)
	records := [][]byte{}
	num_received := 0
	for{
		record := <- ch
		if bytes.Equal(all_zero_record, record){
			num_received += 1
			if num_received == numOfClients {
				break
			}
		} else{
			records = append(records, record)
		}
	}
	return records
}


func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	// get my host and port
	var my_host string
	var my_port string
	for _, serv := range scs.Servers {
		if serv.ServerId == serverId {
			my_host = serv.Host
			my_port = serv.Port
		}
	}

	// Read from input file
	fin, err := os.Open(os.Args[2])
	if err != nil{
		log.Fatal(err)
	}
	defer fin.Close()

	records := [][]byte{}
	
	for {
		buf := make([]byte, 100)
		n, err := fin.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}	
		records = append(records, buf[0:n])
	}
	
	// Partition by server id

	n := int(math.Log2(float64(len(scs.Servers))))
	//n := int(math.Ceil(math.Log2(float64(len(scs.Servers)))))
	records_by_server := make(map[int] [][]byte)
	
	for _, record := range records {
		server_to_go := int(record[0] >> (8-n))
		records_by_server[server_to_go] = append(records_by_server[server_to_go], record)
	}


	// send to and listen from other severs 
	ch := make(chan []byte)
	go listenforData(ch, my_host, my_port)
	go dialToServers(serverId, scs, records_by_server)
	
	numOfClients := len(scs.Servers)-1
	received_records := consolidateData(ch, numOfClients)
	all_records := append(received_records, records_by_server[serverId]...)

	// Custom sort by key
	sort.Slice(all_records, func(i, j int) bool {
		return string(all_records[i][:10]) < string(all_records[j][:10])
	}) 

	// Write to output file
	fout, err := os.Create(os.Args[3])
	if err != nil{
		log.Fatal(err)
	}
	defer fout.Close()
	
	for _, record := range records {
		fout.Write(record)
	}


}
