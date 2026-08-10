package main

import (
	"encoding/json"
	"fmt"
	"os"
)

const defaultEventsFile = "events.jsonl"

func main() {
	path := defaultEventsFile
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot read events:", err)
		os.Exit(1)
	}
	defer file.Close()

	events, stats := parse(file)
	summary := summarize(events, stats)

	out, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot write summary:", err)
		os.Exit(1)
	}
	fmt.Println(string(out))
}
