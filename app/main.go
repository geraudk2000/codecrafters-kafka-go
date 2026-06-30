package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
)

type UUID [16]byte

type Partition struct {
	id          int32
	leaderID    int32
	leaderEpoch int32
	replicas    []int32
	isr         []int32
}

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
			names := parseDescribeTopicPartitionsTopicNames(buf[:n])

			data, err := getData()
			if err != nil {
				fmt.Println("error reading metadata:", err)
				return
			}
			meta := parseClusterMetadata(data)

			response = buildDescribeTopicPartitionsTopicResponse(correlationID, names, meta)

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

func parseDescribeTopicPartitionsTopicNames(buf []byte) []string {
	// Request layout:
	// 0:4   message_size
	// 4:6   api_key
	// 6:8   api_version
	// 8:12  correlation_id
	// 12:14 client_id length  (normal string: int16 length prefix)
	clientIDLen := int(binary.BigEndian.Uint16(buf[12:14]))

	pos := 14 + clientIDLen

	// request header TAG_BUFFER
	pos++

	// topics COMPACT_ARRAY: stored as (realCount + 1)
	topicCount := int(buf[pos]) - 1
	pos++

	names := make([]string, 0, max(topicCount, 0))

	// One iteration per requested topic.
	for i := 0; i < topicCount; i++ {
		// topic name: COMPACT_STRING, length stored as (len + 1)
		nameLen := int(buf[pos]) - 1
		pos++

		names = append(names, string(buf[pos:pos+nameLen]))
		pos += nameLen

		// each topic entry ends with its own TAG_BUFFER
		pos++
	}

	return names
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

// appendCompactInt32Array emits a COMPACT_ARRAY of int32: the byte (len+1),
// then each element as a 4-byte big-endian int32.
func appendCompactInt32Array(body []byte, xs []int32) []byte {
	body = append(body, byte(len(xs)+1))
	for _, x := range xs {
		body = binary.BigEndian.AppendUint32(body, uint32(x))
	}
	return body
}

func buildDescribeTopicPartitionsTopicResponse(correlationID uint32, names []string, meta ClusterMetadata) []byte {

	sort.Strings(names)

	body := []byte{}

	// Response header v1
	body = binary.BigEndian.AppendUint32(body, correlationID)
	body = append(body, 0) // TAG_BUFFER

	// throttle_time_ms
	body = binary.BigEndian.AppendUint32(body, 0)

	// topics compact array: 1 topic => 1 + 1 = 2
	body = append(body, byte(len(names)+1))

	for _, name := range names {

		id, ok := meta.topicUUIDByName[name]
		partitions := meta.partitionsByUUID[id]

		if ok {
			body = binary.BigEndian.AppendUint16(body, 0) // exists
		} else {
			body = binary.BigEndian.AppendUint16(body, 3) // unknown
		}

		// topic_name as COMPACT_STRING
		body = append(body, byte(len(name)+1))
		body = append(body, []byte(name)...)

		// topic_id: the real UUID from the metadata log
		body = append(body, id[:]...)

		// is_internal: false
		body = append(body, 0)

		// partitions compact array: len(partitions) + 1
		body = append(body, byte(len(partitions)+1))
		for _, p := range partitions {
			body = binary.BigEndian.AppendUint16(body, 0)            // error_code
			body = binary.BigEndian.AppendUint32(body, uint32(p.id)) // partition_index
			body = binary.BigEndian.AppendUint32(body, uint32(p.leaderID))
			body = binary.BigEndian.AppendUint32(body, uint32(p.leaderEpoch))
			body = appendCompactInt32Array(body, p.replicas) // replica_nodes
			body = appendCompactInt32Array(body, p.isr)      // isr_nodes
			body = append(body, 1)                           // eligible_leader_replicas: empty
			body = append(body, 1)                           // last_known_elr: empty
			body = append(body, 1)                           // offline_replicas: empty
			body = append(body, 0)                           // partition TAG_BUFFER
		}

		// topic_authorized_operations
		body = binary.BigEndian.AppendUint32(body, 0)

		// topic TAG_BUFFER
		body = append(body, 0)

		// error_code: 0 (topic exists)
		body = binary.BigEndian.AppendUint16(body, 0)

	}

	// next_cursor: null => 0xff
	body = append(body, 0xff)

	// response TAG_BUFFER
	body = append(body, 0)

	response := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)

	return response
}

func getData() ([]byte, error) {

	filepath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

	data, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatalf("can't read metadata: %v", err)
	}
	return data, nil
}

// ClusterMetadata is the result of scanning the metadata log: the two lookups
// needed to answer a DescribeTopicPartitions request.
type ClusterMetadata struct {
	topicUUIDByName  map[string]UUID      // "foo" → UUID  (from Topic records, type 2)
	partitionsByUUID map[UUID][]Partition // UUID  → parts (from Partition records, type 3)
}

// parseClusterMetadata walks the metadata log and COLLECTS what we need.
// It does no serialization — it just turns raw bytes into the two maps.
func parseClusterMetadata(data []byte) ClusterMetadata {
	meta := ClusterMetadata{
		topicUUIDByName:  map[string]UUID{},
		partitionsByUUID: map[UUID][]Partition{},
	}

	c := &Cursor{data: data, pos: 0}

	// LAYER 1: iterate over record batches until we run out of bytes.
	for c.pos < len(data) {
		batchStart := c.pos

		// --- batch header (fixed-size fields) ---
		c.readInt64()                // baseOffset
		batchLength := c.readInt32() // byte count of everything AFTER this field

		// 12 = baseOffset(8) + batchLength(4), the bytes NOT counted by batchLength.
		batchEnd := batchStart + 12 + int(batchLength)

		c.readInt32() // partitionLeaderEpoch
		c.readInt8()  // magic
		c.readInt32() // crc
		c.readInt16() // attributes
		c.readInt32() // lastOffsetDelta
		c.readInt64() // baseTimestamp
		c.readInt64() // maxTimestamp
		c.readInt64() // producerID
		c.readInt16() // producerEpoch
		c.readInt32() // baseSequence
		recordCount := c.readInt32()

		// LAYER 2: iterate over the records inside this batch.
		for i := int32(0); i < recordCount; i++ {
			c.readVarint() // record length (we walk every field, so unused)

			c.readInt8()    // attributes
			c.readVarlong() // timestampDelta
			c.readVarint()  // offsetDelta

			// key: varint length, -1 means null.
			if keyLength := c.readVarint(); keyLength >= 0 {
				c.readBytes(int(keyLength))
			}

			// value: the metadata payload. Isolate the bytes, decode below.
			valueLength := c.readVarint()
			var value []byte
			if valueLength >= 0 {
				value = c.readBytes(int(valueLength))
			}

			// headers: read past them or the cursor drifts.
			headerCount := c.readVarint()
			for h := int32(0); h < headerCount; h++ {
				headerKeyLength := c.readVarint()
				c.readBytes(int(headerKeyLength))
				if headerValueLength := c.readVarint(); headerValueLength >= 0 {
					c.readBytes(int(headerValueLength))
				}
			}

			// LAYER 3: decode the value into a metadata record and collect it.
			if valueLength >= 0 {
				meta.collectRecord(value)
			}
		}

		c.pos = batchEnd
	}

	return meta
}

// collectRecord decodes one metadata-record value and stores the bits we care
// about into the lookup maps.
func (meta *ClusterMetadata) collectRecord(value []byte) {
	vc := &Cursor{data: value, pos: 0}
	vc.readInt8()               // frameVersion
	recordType := vc.readInt8() // 2=Topic, 3=Partition, 12=FeatureLevel
	vc.readInt8()               // version

	switch recordType {
	case 2: // ---- Topic record ----
		nameLen := int(vc.readUvarint()) - 1 // compact string: stored len+1
		name := string(vc.readBytes(nameLen))

		var id UUID
		copy(id[:], vc.readBytes(16))

		vc.readUvarint() // tagged_fields count

		meta.topicUUIDByName[name] = id

	case 3: // ---- Partition record ----
		p := Partition{}
		p.id = vc.readInt32()

		var topicID UUID
		copy(topicID[:], vc.readBytes(16))

		p.replicas = vc.readCompactInt32Array()
		p.isr = vc.readCompactInt32Array()
		vc.skipCompactArray(4) // removing_replicas
		vc.skipCompactArray(4) // adding_replicas

		p.leaderID = vc.readInt32()
		p.leaderEpoch = vc.readInt32()
		vc.readInt32()          // partition_epoch
		vc.skipCompactArray(16) // directories (UUIDs)
		vc.readUvarint()        // tagged_fields count

		meta.partitionsByUUID[topicID] = append(meta.partitionsByUUID[topicID], p)
	}
}
