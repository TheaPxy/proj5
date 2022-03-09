package surfstore

import (
	context "context"
	"errors"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type MetaStore struct {
	mu             sync.Mutex
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	m.mu.Lock()
	ret := &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}
	m.mu.Unlock()

	return ret, nil

}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	//fmt.Println("enter mateStore updatefile")
	//PrintMetaMap(m.FileMetaMap)
	m.mu.Lock()
	filename := fileMetaData.GetFilename()
	serverVersion := m.FileMetaMap[filename].GetVersion()
	latestVersion := fileMetaData.GetVersion()
	//fmt.Println("serverVersion, local latestVersion", serverVersion, latestVersion)
	if serverVersion+1 == latestVersion {
		m.FileMetaMap[filename] = fileMetaData
		//fmt.Println("After update file")
		//PrintMetaMap(m.FileMetaMap)
		newVersion := &Version{
			Version: latestVersion,
		}
		m.mu.Unlock()
		return newVersion, nil
	} else {

		newVersion := &Version{
			Version: -1,
		}
		m.mu.Unlock()
		return newVersion, errors.New("wrong version metastore")
	}

}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//panic("todo")
	m.mu.Lock()
	addr := &BlockStoreAddr{Addr: m.BlockStoreAddr}
	m.mu.Unlock()
	return addr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {

	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
