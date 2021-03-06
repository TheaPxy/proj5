package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"

	grpc "google.golang.org/grpc"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	//fmt.Println("client getBlockData: ", b.GetBlockData())
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block) // write the block from client to the server

	if err != nil {
		return err
	}
	// ????????????????
	// what is succ for?  for sync?
	*succ = success.Flag // ????????
	//fmt.Println("upload test: putblock")
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	inHashes := &BlockHashes{
		Hashes: blockHashesIn,
	}
	out, err := c.HasBlocks(ctx, inHashes)
	if err != nil {
		return err
	}
	blockHashesOut = &out.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	//panic("todo")
	for idx, _ := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// remoteIndex : FileInfoMap
		//remoteIndex := *FileInfoMap{}
		remoteIndex, e := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if e != nil {

			//fmt.Println("---------Error: err_server_crash 1----------", e, e.Error() == "Majority Server Down")
			//if e.Error() == "Majority Server Down" {
			//	fmt.Println("---------Error: err_server_crash 1.1----------")
			//	return e
			//}
			fmt.Printf("--------GetFileInfoMap--------- server %v is not normal", idx)
			continue
		}
		fmt.Printf("--------GetFileInfoMap--------- server %v is leader", idx)
		*serverFileInfoMap = (*remoteIndex).FileInfoMap

		//fmt.Println("Client func test GetFileInfoMap")
		//PrintMetaMap(*serverFileInfoMap)

		return conn.Close()
	}
	return ERR_SERVER_CRASHED
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//fmt.Println("enter client updatefile")
	for idx, _ := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		fileMetaData.Version = *latestVersion

		_, e := c.UpdateFile(ctx, fileMetaData)

		if e != nil {
			//fmt.Println("---------Error: err_server_crash 2----------", e)
			//if e.Error() == "Majority Server Down" {
			//	fmt.Println("---------Error: err_server_crash 2.1----------")
			//	return e
			//}
			continue
			//return err
		}

		//fmt.Println("upload test: update file")
		// close connection
		return conn.Close()
	}
	return ERR_SERVER_CRASHED
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for idx, _ := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[idx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addr, e := c.GetBlockStoreAddr(ctx, &emptypb.Empty{}) // addr: message type struct
		if e != nil {
			//conn.Close()
			////fmt.Println("---------Error: err_server_crash 3----------", e)
			////if e.Error() == "Majority Server Down" {
			////	fmt.Println("---------Error: err_server_crash 3.1----------")
			////	return e
			////}
			continue
			//return err
		}
		//fmt.Println("blockStoreAddr: ", addr.Addr)
		*blockStoreAddr = addr.Addr

		//fmt.Println("upload test: get block store addr")
		// close the connection
		return conn.Close()
	}
	return ERR_SERVER_CRASHED
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
