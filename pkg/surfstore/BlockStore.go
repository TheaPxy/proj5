package surfstore

import (
	context "context"
	"sync"
)

// server struct
type BlockStore struct {
	mu       sync.Mutex
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//return &Block{
	//	BlockData: []byte("abcd"),
	//	BlockSize: 4,
	//}, nil
	// check whether hashlist contains the hash

	//panic("todo")
	bs.mu.Lock()
	hash := blockHash.GetHash()
	ret := (*bs).BlockMap[hash]
	bs.mu.Unlock()
	return ret, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	//hashBytes := GetBlockHashBytes(block.GetBlockData())
	bs.mu.Lock()
	hashString := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hashString] = block
	succ := &Success{
		Flag: true,
	}
	bs.mu.Unlock()
	return succ, nil

}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	bs.mu.Lock()
	var hashOut []string

	for _, hashIn := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hashIn]; ok {
			hashOut = append(hashOut, hashIn)
		}
	}
	blockHashesOut := &BlockHashes{
		Hashes: hashOut,
	}
	bs.mu.Unlock()
	return blockHashesOut, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
