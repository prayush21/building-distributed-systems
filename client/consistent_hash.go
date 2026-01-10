package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

// Consistent Hashing:
// a mechanism for mapping keys to different servers,
// offering the flexibility to retaining keys on addition & removal of servers

// a data structure to implement the concept of Consistent Hashing
type Ring struct {
	nodes   []string
	hashes  []uint32 //sorted list of hashes
	hashMap map[uint32]string
}

// Constructor
func NewRing() *Ring {
	return &Ring{
		nodes:   make([]string, 0),
		hashes:  make([]uint32, 0),
		hashMap: make(map[uint32]string),
	}
}

const VirtualNodes = 20 // its generally quite big

// AddNode adds a server to the Ring
func (ring *Ring) AddNode(nodeAddress string) error {
	if strings.TrimSpace(nodeAddress) == "" {
		return fmt.Errorf("node address cannot be empty")
	}

	for i := 0; i < VirtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeAddress, i)

		hashValue := crc32.ChecksumIEEE([]byte(virtualKey))
		ring.nodes = append(ring.nodes, nodeAddress)
		ring.hashes = append(ring.hashes, hashValue)
		ring.hashMap[hashValue] = nodeAddress
	}

	sort.Slice(ring.hashes, func(i, j int) bool { return ring.hashes[i] < ring.hashes[j] })

	return nil
}

// GetNode returns the address of server responsible for this key
func (ring *Ring) GetNode(key string) (string, error) {
	if len(ring.hashes) == 0 {
		return "", fmt.Errorf("ring is empty: no nodes available")
	}

	keyHash := crc32.ChecksumIEEE([]byte(key))

	// search ring.hash for first hash >= keyHash
	idx := sort.Search(len(ring.hashes), func(i int) bool {
		return ring.hashes[i] >= keyHash
	})

	if idx == len(ring.hashes) {
		idx = 0
	}

	return ring.hashMap[ring.hashes[idx]], nil
}
