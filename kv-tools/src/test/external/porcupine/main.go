// Checks a history of the external fault suite with Porcupine, a linearizability checker
// that shares no code with the checker of the project (docs/spec/verification.md, section 4).
//
// usage: porcupine-check HISTORY-FILE
//
// The file holds one operation per line:
//
//	client key kind a b invoke response result
//
// kind is r (read), w (write a), or c (set b if the value is a). response is -1 for an
// operation that never returned. A register starts with the value 0.
//
// Exit code 0: linearizable. 1: not linearizable. 2: wrong usage or an unreadable file.
// 3: the check did not finish in its time.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type input struct {
	key  string
	kind string
	a    int64
	b    int64
}

type output struct {
	known  bool
	result int64
}

var model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			key := op.Input.(input).key
			if _, seen := byKey[key]; !seen {
				keys = append(keys, key)
			}
			byKey[key] = append(byKey[key], op)
		}
		parts := make([][]porcupine.Operation, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, byKey[key])
		}
		return parts
	},
	Init: func() interface{} {
		return int64(0)
	},
	Step: func(state, in, out interface{}) (bool, interface{}) {
		value := state.(int64)
		i := in.(input)
		o := out.(output)
		switch i.kind {
		case "r":
			return o.result == value, value
		case "w":
			return true, i.a
		default:
			succeeds := value == i.a
			next := value
			if succeeds {
				next = i.b
			}
			if o.known && (o.result == 1) != succeeds {
				return false, value
			}
			return true, next
		}
	},
	Equal: func(a, b interface{}) bool {
		return a.(int64) == b.(int64)
	},
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: porcupine-check HISTORY-FILE")
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	defer file.Close()

	type line struct {
		client           int
		in               input
		invoke, response int64
		result           int64
	}
	var lines []line
	var last int64
	scanner := bufio.NewScanner(file)
	for number := 1; scanner.Scan(); number++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		var l line
		if _, err := fmt.Sscan(text, &l.client, &l.in.key, &l.in.kind, &l.in.a, &l.in.b, &l.invoke, &l.response,
			&l.result); err != nil {
			fmt.Fprintf(os.Stderr, "error: line %d: %v\n", number, err)
			os.Exit(2)
		}
		if l.in.kind != "r" && l.in.kind != "w" && l.in.kind != "c" {
			fmt.Fprintf(os.Stderr, "error: line %d: unknown kind %q\n", number, l.in.kind)
			os.Exit(2)
		}
		if l.invoke > last {
			last = l.invoke
		}
		if l.response > last {
			last = l.response
		}
		lines = append(lines, l)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	var history []porcupine.Operation
	open := 0
	for _, l := range lines {
		known := l.response >= 0
		if !known {
			if l.in.kind == "r" {
				continue // a read that never returned constrains nothing
			}
			// It may take effect at any time after its invocation, or never: it returns
			// after everything else, each at a time of its own.
			open++
			l.response = last + int64(open)
		}
		history = append(history, porcupine.Operation{
			ClientId: l.client,
			Input:    l.in,
			Call:     l.invoke,
			Output:   output{known: known, result: l.result},
			Return:   l.response,
		})
	}

	switch porcupine.CheckOperationsTimeout(model, history, 10*time.Minute) {
	case porcupine.Ok:
		fmt.Printf("linearizable: %d operations, %d without a response\n", len(history), open)
	case porcupine.Illegal:
		fmt.Printf("NOT linearizable: %d operations\n", len(history))
		os.Exit(1)
	default:
		fmt.Println("the check did not finish in its time")
		os.Exit(3)
	}
}
