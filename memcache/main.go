package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func (store *Store) Set(key, val string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = val
}

func (store *Store) Get(key string) (string, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	val, exists := store.data[key]
	return val, exists
}

func main() {
	//create store
	store := &Store{
		data: make(map[string]string),
	}

	//define flags
	port := flag.String("port", "8080", "Port to listen on")
	host := flag.String("host", "localhost", "Host to bind to")

	//parse flags
	flag.Parse()

	address := fmt.Sprintf("%s:%s", *host, *port)

	listener, err := net.Listen("tcp", address)

	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server running at :", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		go handleConnection(conn, store)
	}
}

func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	fmt.Println("New connection from:", conn.RemoteAddr())

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Connection closed:", conn.RemoteAddr(), err)
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			conn.Write([]byte("ERROR: Empty command\n"))
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: GET requires key\n"))
				continue
			}

			key := parts[1]
			value, exists := store.Get(key)

			if exists {
				fmt.Printf("GET %s -> %s\n", key, value)
				conn.Write([]byte(value + "\n"))
			} else {
				fmt.Printf("GET %s -> (not found)\n", key)
				conn.Write([]byte("(nil)\n"))
			}

		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: SET requires key and value\n"))
				continue
			}

			key := parts[1]
			value := strings.Join(parts[2:], " ") //Join everything from index 2 to the end, seperating with spaces
			store.Set(key, value)

			fmt.Printf("SET %s = %s\n", key, value)
			conn.Write([]byte("OK\n"))

		case "QUIT":
			conn.Write([]byte("Goodbye\n"))
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
			conn.Write([]byte("ERROR: Unknown command. Use GET, SET, or QUIT\n"))
		}
	}
}
