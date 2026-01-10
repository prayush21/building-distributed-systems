package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (client *Client) Connect(address string) error {
	if strings.TrimSpace(address) == "" {
		return fmt.Errorf("address cannot be empty")
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", address, err)
	}

	client.conn = conn
	client.reader = bufio.NewReader(conn)
	return nil
}

func (client *Client) Close() error {
	if client.conn != nil {
		return client.conn.Close()
	}
	return nil
}

func (client *Client) Get(key string) (string, error) {
	//its a Get method Client can use to fetch any value from store at server.
	cleanKey := strings.TrimSpace(key)
	if cleanKey == "" {
		return "", fmt.Errorf("key cannot be empty")
	}

	_, err := fmt.Fprintf(client.conn, "GET %s\n", cleanKey)
	if err != nil {
		return "", err
	}

	response, err := client.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(response), nil
}

func (client *Client) Set(key, value string) error {
	cleanKey := strings.TrimSpace(key)
	if cleanKey == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if value == "" {
		return fmt.Errorf("value cannot be empty")
	}

	_, err := fmt.Fprintf(client.conn, "SET %s %s\n", cleanKey, value)
	if err != nil {
		return fmt.Errorf("failed to send SET command: %w", err)
	}

	response, err := client.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read SET response: %w", err)
	}

	if strings.TrimSpace(response) != "OK" {
		return fmt.Errorf("SET failed: %s", strings.TrimSpace(response))
	}
	return nil
}

// func main() {
// 	proxyClient := Client{}
// 	// err := proxyClient.Connect("localhost:8080")
// 	// if err != nil {
// 	// 	fmt.Println("Error connecting to server:", err)
// 	// 	return
// 	// }
// 	// fmt.Println(proxyClient.Get("number"))
// 	// fmt.Println(proxyClient.Get("name"))
// 	// fmt.Println(proxyClient.Get("code"))
// 	// proxyClient.Set("number", "eleven")
// 	// fmt.Println(proxyClient.Get("number2"))
// 	proxyClient.Close()

// 	sampleRing := NewRing()

// 	sampleRing.AddNode("localhost:8082")
// 	sampleRing.AddNode("localhost:8081")
// 	sampleRing.AddNode("localhost:8080")

// 	hash := crc32.ChecksumIEEE([]byte("localhost:8080"))
// 	fmt.Printf("8080 lands at: %d\n", hash)
// 	hash = crc32.ChecksumIEEE([]byte("localhost:8081"))
// 	fmt.Printf("8081 lands at: %d\n", hash)
// 	hash = crc32.ChecksumIEEE([]byte("localhost:8082"))
// 	fmt.Printf("8082 lands at: %d\n", hash)

// 	// Test with 15 keys
// 	for i := 1; i <= 15; i++ {
// 		key := fmt.Sprintf("key%d", i)
// 		node, err := sampleRing.GetNode(key)
// 		if err != nil {
// 			fmt.Printf("Error getting node for %s: %v\n", key, err)
// 			continue
// 		}
// 		fmt.Printf("Key '%s' is mapped to node: %s\n", key, node)
// 	}

// 	return
// }

// A distributed cache client based on the implementation of Ring,
// laid out by the concept of Consistent Hashing
type DistributedCache struct {
	ring       *Ring
	clientPool map[string]*Client
}

func NewDistributedCache() *DistributedCache {
	return &DistributedCache{
		ring:       NewRing(),
		clientPool: make(map[string]*Client),
	}
}

func (dc *DistributedCache) AddNode(address string) error {
	client := &Client{}
	if err := client.Connect(address); err != nil {
		return fmt.Errorf("failed to connect to node %s: %w", address, err)
	}

	if err := dc.ring.AddNode(address); err != nil {
		client.Close() // cleanup on failure
		return err
	}

	dc.clientPool[address] = client
	return nil
}

func (dc *DistributedCache) Set(key, value string) error {
	node, err := dc.ring.GetNode(key)

	if err != nil {
		return fmt.Errorf("Error in getting the node: %s", err)
	}

	client, exists := dc.clientPool[node]
	if !exists {
		return fmt.Errorf("no client for node %s", node)
	}

	return client.Set(key, value)
}

func (dc *DistributedCache) Get(key string) (string, error) {
	node, err := dc.ring.GetNode(key)

	if err != nil {
		return "", fmt.Errorf("failed to get node: %w", err)
	}

	client, exists := dc.clientPool[node]
	if !exists {
		return "", fmt.Errorf("no client for node %s", node)
	}

	value, err := client.Get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key from node %s: %w", node, err)
	}

	return value, nil
}

// Close closes all client connections in the pool
func (dc *DistributedCache) Close() {
	for _, client := range dc.clientPool {
		client.Close()
	}
}

func main() {
	cache := NewDistributedCache()
	defer cache.Close()

	nodes := []string{"localhost:8080", "localhost:8081", "localhost:8082"}
	for _, node := range nodes {
		if err := cache.AddNode(node); err != nil {
			fmt.Printf("Warning: failed to add node %s: %v\n", node, err)
		}
	}

	fmt.Println(cache.Get("name"))
	fmt.Println(cache.Get("code"))
	cache.Set("name1", "prayush")
	cache.Set("code1", "DAVE")
	fmt.Println(cache.Get("name1"))
	fmt.Println(cache.Get("code1"))
}
