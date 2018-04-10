package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"kvs/db"
	"os"
	"regexp"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func get_handler(command string, client db.KeyValueStoreClient) (string, error) {
	parts := get_pattern.FindStringSubmatch(command)
	key := parts[1]
	if len(key) == 0 {
		return "", errors.New("get operation takes non-empty key strings")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	get_res, err := client.Get(ctx, &db.GetRequest{key})
	if err != nil {
		return "", errors.New(fmt.Sprintf("An error occurred: %s", err.Error()))
	}
	return fmt.Sprintf("Value -> %s, Err -> %s", get_res.GetValue(), get_res.GetError()), nil
}

func set_handler(command string, client db.KeyValueStoreClient) (string, error) {
	parts := set_pattern.FindStringSubmatch(command)
	key, value := parts[1], parts[2]
	if len(key) == 0 {
		return "", errors.New("set operation takes non-empty key strings")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	set_res, err := client.Set(ctx, &db.KeyValuePair{key, value})
	if err != nil {
		return "", errors.New(fmt.Sprintf("An error occurred: %s", err.Error()))
	}
	return fmt.Sprintf("Value -> %s, Err -> %s", set_res.GetValue(), set_res.GetError()), nil
}

var set_pattern, get_pattern *regexp.Regexp

func execute(command string, client db.KeyValueStoreClient) (string, error) {
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		stream, err := client.List(ctx, &db.ListRequest{})
		if err != nil {
			return "", err
		}
		var buffer bytes.Buffer
		for {
			if kv, err := stream.Recv(); err != nil {
				if err == io.EOF {
					break
				} else {
					return "", err
				}
			} else {
				buffer.WriteString(fmt.Sprintf("%s:%s\n", kv.Key, kv.Value))
			}
		}
		return string(buffer.Bytes()), nil
	default:
		if set_pattern.MatchString(command) {
			return set_handler(command, client)
		} else if get_pattern.MatchString(command) {
			return get_handler(command, client)
		}
		return "", errors.New(fmt.Sprintf("Unknown command: %s", command))
	}
}

func shell(client db.KeyValueStoreClient) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Printf("> ")
	for scanner.Scan() {
		command := scanner.Text()
		result, err := execute(command, client)
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(result)
		fmt.Printf("> ")
	}
}

var (
	serverAddr = flag.String("server_addr", "127.0.0.1:9090", "Key Value DB server address in the format host:port")
)

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := db.NewKeyValueStoreClient(conn)
	set_pattern = regexp.MustCompile("^set\\s+(\\w+)\\s+(\\S+)\\s*$")
	get_pattern = regexp.MustCompile("^get\\s+(\\w+)\\s*$")
	shell(client)
}
