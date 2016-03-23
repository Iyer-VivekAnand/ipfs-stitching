package main

import (
	"bytes"
	"math"
	//	"encoding/json"
	"fmt"
	ipfs "github.com/RealImage/go-ipfs-api"
	//	log "github.com/Sirupsen/logrus"
	b58 "github.com/btcsuite/btcutil/base58"
	proto "github.com/golang/protobuf/proto"
	mk "ipfs-stitching/merkledag"
	"ipfs-stitching/pb"
)

/*
type (
	PBLink struct {
		Hash  string `json:"Hash"`
		Name  string `json:"Name"`
		Tsize uint64 `json:"Tsize"`
	}

	// An IPFS MerkleDAG Node
	PBNode struct {
		Links []PBLink `json:"Links"`
		Data  string   `protobuf:"bytes,2,opt" json:"Data"`
	}
)
*/

type Node struct {
	Hash      []byte
	BlockSize uint64
	DataSize  uint64
}

const (
	MaxNodesCount = int64(174)
)

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int64) int64 {
	if x > y {
		return x
	}
	return y
}

func stitchNodes(nodes []*Node) (*Node, error) {
	data := unixfs_pb.Data{}
	data.Type = unixfs_pb.Data_File.Enum()
	pbnode := mk.PBNode{}

	nodeName := ""
	totalSize := uint64(0)
	for _, node := range nodes {
		data.Blocksizes = append(data.Blocksizes, node.DataSize)
		pbnode.Links = append(pbnode.Links, &mk.PBLink{
			Hash:  node.Hash,
			Tsize: &node.BlockSize,
			Name:  &nodeName,
		})

		totalSize += node.DataSize
	}

	data.Filesize = proto.Uint64(totalSize)
	protoData, err := proto.Marshal(&data)
	pbnode.Data = protoData

	pbnodeString, err := pbnode.Marshal()
	if err != nil {
		panic(err.Error)
	}

	sh := ipfs.NewShell(":5001")

	newHash, err := sh.PutObject(bytes.NewBufferString(string(pbnodeString)))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &Node{Hash: b58.Decode(newHash), BlockSize: uint64(pbnode.Size()) + totalSize, DataSize: totalSize}, nil
}

func createLink(nodes []*Node) (*Node, error) {

	if len(nodes) == 1 {
		return nodes[0], nil
	}

	iterations := int64(math.Ceil(float64(len(nodes)) / float64(MaxNodesCount)))
	if iterations <= 1 {
		return stitchNodes(nodes)
	}

	linkNodes := []*Node{}
	startOffset := int64(0)
	endOffset := Min(int64(len(nodes)), (MaxNodesCount - 1))
	for i := int64(0); i < iterations; i++ {
		fmt.Println("Calling createLink- startOffset: ", startOffset, " endOffset: ", endOffset)

		node, err := createLink(nodes[startOffset:endOffset])
		if err != nil {
			return nil, err
		}

		linkNodes = append(linkNodes, node)
		startOffset = endOffset + 1
		endOffset = endOffset + Min(int64(len(nodes))-endOffset, MaxNodesCount)
	}

	return createLink(linkNodes)
}

func main() {

	var childNodes []*Node

	hash := "QmRk1rduJvo5DfEYAaLobS2za9tDszk35hzaNSDCJ74DA7"
	hashBytes := b58.Decode(hash)

	for i := int64(0); i < 40; i++ {
		childNodes = append(childNodes,
			&Node{
				Hash:      hashBytes,
				BlockSize: 262158,
				DataSize:  262144,
			})
	}

	parentNode, err := createLink(childNodes)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("FileHash := ", b58.Encode(parentNode.Hash), " BlockSize: ", parentNode.BlockSize, parentNode.DataSize)
}

/*func main() {
	blocksCount := 1000

	dataHash := "QmRk1rduJvo5DfEYAaLobS2za9tDszk35hzaNSDCJ74DA7"
	size := uint64(256) * 1024

	data := unixfs_pb.Data{}
	data.Type = unixfs_pb.Data_File.Enum()
	data.Blocksizes = append(data.Blocksizes, size)

	protoData, err := proto.Marshal(&data)
	if err != nil {
		// This really shouldnt happen, i promise
		// The only failure case for marshal is if required fields
		// are not filled out, and they all are. If the proto object
		// gets changed and nobody updates this function, the code
		// should panic due to programmer error
		panic(err)
	}

	pbnode := PBNode{}
	pbnode.Links = append(pbnode.Links, PBLink{
		Hash:  dataHash,
		Tsize: size,
		Name:  "node",
	})

	pbnode.Data = string(protoData)
	pbnodeString, err := json.Marshal(pbnode)
	if err != nil {
		panic(err.Error)
	}

	sh := ipfs.NewShell(":5001")
	fmt.Println("adding node ", string(pbnodeString))
	newHash, err := sh.PutObject(bytes.NewBufferString(string(pbnodeString)))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(newHash)
}
*/
