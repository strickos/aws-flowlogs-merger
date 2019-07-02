package data

import (
	"sync"
)

/*
LogToProcess holds a log entry and a reference to the associated wait group that needs to be makred complete when done.
*/
type LogToProcess struct {
	Log        *LogEntry
	waitGroups []*sync.WaitGroup
}

/*
MakeLogToProcess creates a new LogToProcess and returns it.
*/
func MakeLogToProcess(log *LogEntry, file *FileToProcessInfo) *LogToProcess {
	file.wg.Add(1)
	entry := LogToProcess{Log: log}
	entry.waitGroups = append(entry.waitGroups, file.wg)
	return &entry
}

/*
Mark this LogEntry as processed.
*/
func (f *LogToProcess) Done() {
	for _, wg := range f.waitGroups {
		wg.Done()
	}
}

/*
Combine will merge to LogToProcess structs together
*/
func (f *LogToProcess) Combine(record *LogToProcess) {
	f.Log.NumLogEntries += record.Log.NumLogEntries
	f.Log.Bytes += record.Log.Bytes
	f.Log.Packets += record.Log.Packets
	if record.Log.Start.Before(f.Log.Start) {
		f.Log.Start = record.Log.Start
	}
	if record.Log.End.After(f.Log.End) {
		f.Log.End = record.Log.End
	}
	f.waitGroups = append(f.waitGroups, record.waitGroups...)
}
