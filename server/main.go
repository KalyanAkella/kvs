package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"kvs/db"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*
Run with:
go run kvs/server/main.go --db_file=kvs/data.db
*/

var (
	dbFile      = ""
	port        = 9090
	dbFileError = errors.New("db_file is a mandatory parameter")
	logger      = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	infoLog     = func(msg string) {
		logger.SetPrefix("INFO:")
		logger.Println(msg)
	}
	errorLog = func(msg string) {
		logger.SetPrefix("ERROR:")
		logger.Println(msg)
	}
)

type keyValueServer struct {
	kv_file *os.File
}

func (server *keyValueServer) newScanner() *bufio.Scanner {
	infoLog(fmt.Sprintf("Creating reader for DB file: %s", server.kv_file.Name()))
	kv_reader := bufio.NewReader(server.kv_file)
	return bufio.NewScanner(kv_reader)
}

func (server *keyValueServer) Close() error {
	infoLog("Shutting down")
	return server.kv_file.Close()
}

func (server *keyValueServer) List(list_req *db.ListRequest, list_server db.KeyValueStore_ListServer) error {
	infoLog("Executing List command")
	if _, err := server.kv_file.Seek(0, 0); err != nil {
		errorLog(fmt.Sprintf("An error occurred while seeking: %s", err.Error()))
	}
	scanner := server.newScanner()
	for scanner.Scan() {
		kv := strings.Split(scanner.Text(), ":")
		kv_pair := &db.KeyValuePair{kv[0], kv[1]}
		if err := list_server.Send(kv_pair); err != nil {
			return err
		}
	}
	return nil
}

func (server *keyValueServer) Get(ctx context.Context, get_req *db.GetRequest) (*db.GetResponse, error) {
	infoLog("Executing Get command")
	if _, err := server.kv_file.Seek(0, 0); err != nil {
		errorLog(fmt.Sprintf("An error occurred while seeking: %s", err.Error()))
	}
	scanner := server.newScanner()
	var result string
	for scanner.Scan() {
		kv := strings.Split(scanner.Text(), ":")
		if strings.EqualFold(kv[0], get_req.Key) {
			result = kv[1]
		}
	}
	if len(result) > 0 {
		return &db.GetResponse{&db.GetResponse_Value{result}}, nil
	}
	return &db.GetResponse{&db.GetResponse_Error{fmt.Sprintf("Given key:%s not found", get_req.Key)}}, nil
}

func (server *keyValueServer) Set(ctx context.Context, kv_pair *db.KeyValuePair) (*db.SetResponse, error) {
	infoLog("Executing Set command")
	infoLog(fmt.Sprintf("Creating writer for DB file: %s", server.kv_file.Name()))
	kv_writer := bufio.NewWriter(server.kv_file)
	line := fmt.Sprintf("%s:%s\n", kv_pair.Key, kv_pair.Value)
	num, err := kv_writer.WriteString(line)
	if err != nil {
		return &db.SetResponse{&db.SetResponse_Error{err.Error()}}, err
	}
	err = kv_writer.Flush()
	if err != nil {
		return &db.SetResponse{&db.SetResponse_Error{err.Error()}}, err
	}
	err = server.kv_file.Sync()
	if err != nil {
		return &db.SetResponse{&db.SetResponse_Error{err.Error()}}, err
	}
	return &db.SetResponse{&db.SetResponse_Value{fmt.Sprintf("Number of bytes written: %d", num)}}, nil
}

func (server *keyValueServer) Remove(ctx context.Context, remove_req *db.RemoveRequest) (*db.RemoveResponse, error) {
	infoLog("Executing Remove command")
	return &db.RemoveResponse{&db.RemoveResponse_Error{"Unsupported operation: Remove"}}, nil
}

func newKeyValueServer(db_file string) *keyValueServer {
	infoLog(fmt.Sprintf("Initialising from DB file: %s", db_file))
	kv_file, err := os.OpenFile(db_file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0400)
	if err != nil {
		panic(err)
	}
	return &keyValueServer{kv_file}
}

func init() {
	flag.StringVar(&dbFile, "db_file", dbFile, "The DB file for the key value store")
	flag.IntVar(&port, "port", port, "The port used for binding the DB server")
	flag.Parse()
}

func main() {
	if len(dbFile) == 0 {
		panic(dbFileError)
	}
	infoLog(fmt.Sprintf("Starting up on %d", port))
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		panic(err)
	}
	kv_server := newKeyValueServer(dbFile)
	infoLog("Starting GRPC service")
	server := grpc.NewServer()
	infoLog("Registering Key Value server with GRPC service")
	db.RegisterKeyValueStoreServer(server, kv_server)
	infoLog("Ready to serve clients...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	go func() {
		<-quit
		kv_server.Close()
		server.GracefulStop()
	}()
	server.Serve(lis)
}
