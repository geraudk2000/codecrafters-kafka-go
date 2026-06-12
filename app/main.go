package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	buf := make([]byte, 1024)
	// TODO: Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("error reading:", err)
		return
	}
	if n < 12 {
		fmt.Println("Request too short")
		return
	}
	response := make([]byte, 23)
	//body := make([]byte, 15)

	correleationID := binary.BigEndian.Uint32(buf[8:12])
	request_api_version := binary.BigEndian.Uint16(buf[6:8])

	binary.BigEndian.PutUint32(response[0:4], 19)
	binary.BigEndian.PutUint32(response[4:8], correleationID)

	if request_api_version <= 4 {

		binary.BigEndian.PutUint16(response[8:10], 0)

	} else {
		binary.BigEndian.PutUint16(response[8:10], 35)
	}

	response[10] = 2

	binary.BigEndian.PutUint16(response[11:13], 18)
	binary.BigEndian.PutUint16(response[13:15], 0)
	binary.BigEndian.PutUint16(response[15:17], 4)

	response[17] = 0

	binary.BigEndian.PutUint16(response[18:22], 0)

	response[22] = 0

	conn.Write(response)

}
