package data

import (
	"errors"
	"flowlogs-merger/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

/*
LogEntry describes a single line of log data
*/
type LogEntry struct {
	/*
		flowID is an identifier for a FLOW.
		It is used when collecting multiple log records for the same flow together
		Built as: srcaddr + ":" + srcport + "-" + dstaddr + ":" + dstport + "-" + InterfaceID + "-" + action
	*/
	FlowID        string // Ignored for Parquet...
	Version       int32  // Ignore for Parquet - we only support version 2 right now
	AccountID     string `parquet:"name=account, type=UTF8, encoding=PLAIN_DICTIONARY"`
	InterfaceID   string `parquet:"name=interfaceId, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcAddress    string `parquet:"name=srcAddress, type=UTF8, encoding=PLAIN_DICTIONARY"`
	DstAddress    string `parquet:"name=dstAddress, type=UTF8, encoding=PLAIN_DICTIONARY"`
	SrcPort       int32  `parquet:"name=srcPort, type=INT32"`
	DstPort       int32  `parquet:"name=dstPort, type=INT32"`
	Protocol      int32  `parquet:"name=protocol, type=INT32"`
	ProtocolName  string `parquet:"name=protocolName, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Packets       int64  `parquet:"name=packets, type=INT64"`
	Bytes         int64  `parquet:"name=bytes, type=INT64"`
	Start         time.Time
	StartVal      int64 `parquet:"name=start, type=TIMESTAMP_MILLIS"`
	End           time.Time
	EndVal        int64  `parquet:"name=end, type=TIMESTAMP_MILLIS"`
	Action        bool   `parquet:"name=action, type=BOOLEAN"`
	LogStatus     int32  `parquet:"name=logStatus, type=INT32"` // 0 = OK, 1 = NODATA, 2 = SKIP
	NumLogEntries int32  `parquet:"name=ofRawlogEntries, type=INT32"`
	OriginHash    uint32 `parquet:"name=originHash, type=UINT_32"`
}

/*
ParseLogEntry parses a raw FlowLog Entry line and returns a LogEntry.FileToProcessInfo*/
func ParseLogEntry(line *string) (*LogEntry, error) {
	arr := strings.Split(*line, " ")
	if len(arr) < 13 {
		// Invalid Format....
		msg := fmt.Sprintf("Log entry is in wrong format - it has the wrong number of columns [Got: %d, Expected: 13], will ignore log line", len(arr))
		return nil, errors.New(msg)
	} else if arr[0] != "2" {
		// Wrong version - not in a known format - ignore line...
		msg := fmt.Sprintf("Log entry is wrong version [%s], will ignore log line", arr[0])
		return nil, errors.New(msg)
	}

	var status int32
	switch arr[13] {
	case "OK":
		status = 0
	case "NODATA":
		status = 1
	default:
		status = 2
	}
	version, _ := strconv.ParseInt(arr[0], 10, 32)

	startTime, _ := strconv.ParseInt(arr[10], 10, 64)
	start := time.Unix(startTime, 0).UTC()

	endTime, _ := strconv.ParseInt(arr[11], 10, 64)
	end := time.Unix(endTime, 0).UTC()

	var log LogEntry
	if status == 0 {
		srcPort, _ := strconv.ParseInt(arr[5], 10, 32)
		dstPort, _ := strconv.ParseInt(arr[6], 10, 32)
		protocol, _ := strconv.ParseInt(arr[7], 10, 32)
		packets, _ := strconv.ParseInt(arr[8], 10, 64)
		bytes, _ := strconv.ParseInt(arr[9], 10, 64)

		flowID := arr[3] + ":" + arr[5] + "-" + arr[4] + ":" + arr[6] + "-" + arr[2] + "-" + arr[12]

		log = LogEntry{
			FlowID:        flowID,
			Version:       int32(version),
			AccountID:     arr[1],
			InterfaceID:   arr[2],
			SrcAddress:    arr[3],
			DstAddress:    arr[4],
			SrcPort:       int32(srcPort),
			DstPort:       int32(dstPort),
			Protocol:      int32(protocol),
			ProtocolName:  util.ProtocolMap[int(protocol)],
			Packets:       packets,
			Bytes:         bytes,
			Start:         start,
			StartVal:      start.Unix() * 1000,
			End:           end,
			EndVal:        end.Unix() * 1000,
			Action:        arr[12] == "ACCEPT",
			LogStatus:     status,
			NumLogEntries: 1,
		}
	} else {
		// Partial Log Entry, is either a SKIP or NODATA line
		flowID := arr[2] + "-" + arr[12]
		log = LogEntry{
			FlowID:        flowID,
			Version:       int32(version),
			AccountID:     arr[1],
			InterfaceID:   arr[2],
			SrcAddress:    "",
			DstAddress:    "",
			SrcPort:       -1,
			DstPort:       -1,
			Protocol:      -1,
			ProtocolName:  "",
			Packets:       0,
			Bytes:         0,
			Start:         start,
			StartVal:      start.Unix() * 1000,
			End:           end,
			EndVal:        end.Unix() * 1000,
			Action:        false,
			LogStatus:     status,
			NumLogEntries: 1,
		}
	}

	return &log, nil

}
