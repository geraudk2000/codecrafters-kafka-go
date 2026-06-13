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

	// TODO: Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {

		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConn(conn)

	}

}

func handleConn(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("error reading:", err)
			return
		}
		if n < 12 {
			fmt.Println("Request too short")
			return
		}
		response := make([]byte, 33)
		//body := make([]byte, 15)

		// --- Parse the REQUEST header ---
		// Request layout: size[0:4] api_key[4:6] api_version[6:8] correlation_id[8:12] ...
		// correlation_id: the client's tag for this request; we must echo it back so the
		// client can match our response to the request it sent.
		correleationID := binary.BigEndian.Uint32(buf[8:12])
		// request_api_version: which version of the ApiVersions API the client is speaking.
		request_api_version := binary.BigEndian.Uint16(buf[6:8])

		// --- Build the RESPONSE ---
		// response[0:4]  message_size: number of bytes that follow this field (23 total - 4 = 19).
		binary.BigEndian.PutUint32(response[0:4], 29)
		// response[4:8]  correlation_id: echoed straight back from the request.
		binary.BigEndian.PutUint32(response[4:8], correleationID)

		// response[8:10] error_code: 0 = no error. 35 = UNSUPPORTED_VERSION,
		// returned when the client asks for an api_version we don't support (>4).
		if request_api_version <= 4 {

			binary.BigEndian.PutUint16(response[8:10], 0)

		} else {
			binary.BigEndian.PutUint16(response[8:10], 35)
		}

		// response[10] num_api_keys: this is a COMPACT array, whose length is encoded as N+1.
		// We advertise 1 ApiKey, so we write 2.
		response[10] = 3

		// --- One entry in the api_keys array ---
		// response[11:13] api_key   = 18 (this is the ApiVersions API itself)
		binary.BigEndian.PutUint16(response[11:13], 18)
		// response[13:15] min_version we support = 0
		binary.BigEndian.PutUint16(response[13:15], 0)
		// response[15:17] max_version we support = 4
		binary.BigEndian.PutUint16(response[15:17], 4)

		// response[17] tagged_fields for this array entry: 0 = none (end of the entry).
		response[17] = 0

		//
		binary.BigEndian.PutUint16(response[18:20], 75)
		binary.BigEndian.PutUint16(response[20:22], 0)
		binary.BigEndian.PutUint16(response[22:24], 0)
		response[24] = 0

		// response[18:22] throttle_time_ms = 0 (no throttling).
		// NOTE: this field is an int32 (4 bytes) — see the question below about PutUint16 here.
		binary.BigEndian.PutUint32(response[25:29], 0)

		// response[22] tagged_fields for the whole response: 0 = none.
		response[29] = 0

		conn.Write(response)
	}

}
