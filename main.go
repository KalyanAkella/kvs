package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	s "strings"
)

func load_db(file string) map[string]string {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}
	return read_kvs(string(data))
}

func kvs_to_bytes(kvs map[string]string) []byte {
	var buffer bytes.Buffer
	for k, v := range kvs {
		buffer.WriteString(fmt.Sprintf("%s:%s\n", k, v))
	}
	return buffer.Bytes()
}

func save_db(file string, kvs map[string]string) {
	err := ioutil.WriteFile(file, kvs_to_bytes(kvs), 0644)
	if err != nil {
		panic(err)
	}
}

func read_kvs(data string) map[string]string {
	kvs := make(map[string]string)
	scanner := bufio.NewScanner(s.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			kv := s.Split(line, ":")
			kvs[kv[0]] = kv[1]
		}
	}
	return kvs
}

func get_handler(command string, kvs map[string]string) (string, error) {
	parts := get_pattern.FindStringSubmatch(command)
	key := parts[1]
	if len(key) == 0 {
		return "", errors.New("get operation takes non-empty key strings")
	}
	value, present := kvs[key]
	if present {
		return fmt.Sprintf("%s", value), nil
	} else {
		return fmt.Sprintf("No such key %s found", key), nil
	}
}

func set_handler(command string, kvs map[string]string) (string, error) {
	parts := set_pattern.FindStringSubmatch(command)
	key, value := parts[1], parts[2]
	if len(key) == 0 {
		return "", errors.New("set operation takes non-empty key strings")
	}
	kvs[key] = value
	return fmt.Sprintf("Successfully set %s:%s", key, value), nil
}

var set_pattern, get_pattern *regexp.Regexp

func execute(command string, kvs map[string]string) (string, error) {
	switch command {
	case "exit":
		return "", errors.New("Exiting... Bye")
	case "quit":
		return "", errors.New("Exiting... Bye")
	case "bye":
		return "", errors.New("Exiting... Bye")
	case "help":
		return "Available commands: exit|quit|bye|help|set|get|list", nil
	case "list":
		return string(kvs_to_bytes(kvs)), nil
	default:
		if set_pattern.MatchString(command) {
			return set_handler(command, kvs)
		} else if get_pattern.MatchString(command) {
			return get_handler(command, kvs)
		}
		return "", errors.New(fmt.Sprintf("Unknown command: %s", command))
	}
}

func shell(db_file string) {
	kvs := load_db(db_file)
	fmt.Printf("Loaded %d key value pairs...\n", len(kvs))
	fmt.Println("Entering shell. Type 'help' for list of available commands")
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("> ")
	for scanner.Scan() {
		command := scanner.Text()
		result, err := execute(command, kvs)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(result)
		fmt.Printf("\n> ")
	}
	save_db(db_file, kvs)
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Usage: kvs <db-file-path>")
		os.Exit(1)
	}
	set_pattern = regexp.MustCompile("^set\\s+(\\w+)\\s+(\\S+)\\s*$")
	get_pattern = regexp.MustCompile("^get\\s+(\\w+)\\s*$")
	shell(args[0])
}
