package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
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
			fmt.Println("request too short")
			return
		}

		apiKey := binary.BigEndian.Uint16(buf[4:6])
		apiVersion := binary.BigEndian.Uint16(buf[6:8])
		correlationID := binary.BigEndian.Uint32(buf[8:12])

		var response []byte

		switch apiKey {
		case 18:
			response = buildApiVersionsResponse(correlationID, apiVersion)

		case 75:
			topicName := parseDescribeTopicPartitionsTopicName(buf[:n])
			response = buildDescribeTopicPartitionsUnknownTopicResponse(correlationID, topicName)

		default:
			fmt.Println("unsupported api key:", apiKey)
			return
		}

		_, err = conn.Write(response)
		if err != nil {
			fmt.Println("error writing:", err)
			return
		}
	}
}

func buildApiVersionsResponse(correlationID uint32, apiVersion uint16) []byte {
	body := []byte{}

	// Response header v0
	body = binary.BigEndian.AppendUint32(body, correlationID)

	// error_code
	if apiVersion <= 4 {
		body = binary.BigEndian.AppendUint16(body, 0)
	} else {
		body = binary.BigEndian.AppendUint16(body, 35)
	}

	// api_keys compact array: 2 entries => 2 + 1 = 3
	body = append(body, 3)

	// API key 18: ApiVersions
	body = binary.BigEndian.AppendUint16(body, 18)
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, 0) // TAG_BUFFER

	// API key 75: DescribeTopicPartitions
	body = binary.BigEndian.AppendUint16(body, 75)
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint16(body, 0)
	body = append(body, 0) // TAG_BUFFER

	// throttle_time_ms
	body = binary.BigEndian.AppendUint32(body, 0)

	// response TAG_BUFFER
	body = append(body, 0)

	response := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)

	return response
}

func parseDescribeTopicPartitionsTopicName(buf []byte) string {
	// Request layout:
	// 0:4   message_size
	// 4:6   api_key
	// 6:8   api_version
	// 8:12  correlation_id
	// 12:14 client_id length
	clientIDLen := int(binary.BigEndian.Uint16(buf[12:14]))

	pos := 14 + clientIDLen

	// request header TAG_BUFFER
	pos++

	// topics compact array length
	topicCount := int(buf[pos]) - 1
	pos++

	if topicCount <= 0 {
		return ""
	}

	// topic_name compact string length
	nameLen := int(buf[pos]) - 1
	pos++

	topicName := string(buf[pos : pos+nameLen])

	return topicName
}

func buildDescribeTopicPartitionsUnknownTopicResponse(correlationID uint32, topicName string) []byte {
	body := []byte{}

	// Response header v1
	body = binary.BigEndian.AppendUint32(body, correlationID)
	body = append(body, 0) // TAG_BUFFER

	// throttle_time_ms
	body = binary.BigEndian.AppendUint32(body, 0)

	// topics compact array: 1 topic => 1 + 1 = 2
	body = append(body, 2)

	// error_code: UNKNOWN_TOPIC_OR_PARTITION = 3
	body = binary.BigEndian.AppendUint16(body, 3)

	// topic_name as COMPACT_STRING
	body = append(body, byte(len(topicName)+1))
	body = append(body, []byte(topicName)...)

	// topic_id: zero UUID, 16 bytes
	body = append(body, make([]byte, 16)...)

	// is_internal: false
	body = append(body, 0)

	// partitions compact array: 0 partitions => 0 + 1 = 1
	body = append(body, 1)

	// topic_authorized_operations
	body = binary.BigEndian.AppendUint32(body, 0)

	// topic TAG_BUFFER
	body = append(body, 0)

	// next_cursor: null => -1 => 0xff
	body = append(body, 0xff)

	// response TAG_BUFFER
	body = append(body, 0)

	response := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)

	return response
}
