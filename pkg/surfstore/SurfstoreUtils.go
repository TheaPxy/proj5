package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//////////////////////////////////////////////////////////////////////////

	baseDirFileInfoMap, baseDirFileBlockMap := BaseDirMetaMap(client)

	//fmt.Println("--------BaseDir Map--------")
	//PrintMetaMap(baseDirFileInfoMap)

	// consult with local index file with hash list
	localIndexMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatalf("Read local index.txt failed: %s", err)
	}

	updateBaseDirVersion(baseDirFileInfoMap, localIndexMetaMap)
	//fmt.Println("BaseDir Map ", client.BaseDir)
	//PrintMetaMap(baseDirFileInfoMap)
	//fmt.Println("Local Index Map", client.BaseDir)
	//PrintMetaMap(localIndexMetaMap)

	// client download updated FileInfoMap (remote index) from server
	//serverFileInfoMap := make(map[string]*FileMetaData)
	var serverFileInfoMap map[string]*FileMetaData
	if err := client.GetFileInfoMap(&serverFileInfoMap); err != nil {
		log.Fatalf("Fetch server FileInfoMap failed: %s", err)
	}
	//fmt.Println("Server Index Map ", client.BaseDir)
	//PrintMetaMap(serverFileInfoMap)

	for filename := range serverFileInfoMap {
		localIndexMetaData, isOnLocalIndex := localIndexMetaMap[filename]
		baseDirMetaData, isOnBaseDir := baseDirFileInfoMap[filename]
		//isModified := modifiedFileMap[filename]
		isModified := false
		if !reflect.DeepEqual(localIndexMetaData.GetBlockHashList(), baseDirMetaData.GetBlockHashList()) {
			isModified = true
		}
		localVersion := localIndexMetaData.GetVersion()
		serverVersion := serverFileInfoMap[filename].GetVersion()
		log.Println(filename, " isOnlocal: ", isOnLocalIndex, "inOnBaseDir: ", isOnBaseDir, "isModified", isModified)

		if !isOnBaseDir && !isOnLocalIndex {
			if err = download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); err != nil {
				log.Fatalf("Download %v failed: %s", filename, err)
			}
			log.Println("After download localIndexMetaMap ", client.BaseDir)
			PrintMetaMap(localIndexMetaMap)
		} else if isOnLocalIndex && isOnBaseDir {
			if serverVersion > localVersion {
				if err = download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); err != nil {
					log.Fatalf("Download %v failed: %s", filename, err)
				}
			}
		} else if isOnLocalIndex && !isOnBaseDir {
			if serverVersion > localVersion {
				if err = download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); err != nil {
					log.Fatalf("Download %v failed: %s", filename, err)
				}
			} else if serverVersion == localVersion {
				// file delete locally, server no newer version
				// upload tombstone version
				// todo if trying to delete the deleted file

				//if !(len(serverFileInfoMap[filename].BlockHashList) == 1 && serverFileInfoMap[filename].BlockHashList[0] == "0") {
				//	// remote not deleted
				//	if err = uploadTombstone(client, localVersion, filename, serverFileInfoMap, localIndexMetaMap); err != nil {
				//		log.Fatalf("Upload Tombstone Failed: %s", err)
				//	}
				//}

				// todo logic: no matter server delete or not, delete and update to sever
				if err = uploadTombstone(client, localVersion, filename, serverFileInfoMap, localIndexMetaMap); err != nil {
					log.Fatalf("Upload Tombstone Failed: %s", err)
				}
				// todo if trying to create/modify the deleted file

			}
		} else if isOnBaseDir && !isOnLocalIndex {
			if err = download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); err != nil {
				log.Fatalf("Download %v failed: %s", filename, err)
			}
		}
	}

	for filename := range baseDirFileInfoMap {
		localIndexMetaData, isOnLocalIndex := localIndexMetaMap[filename]
		serverMetaData, isOnServer := serverFileInfoMap[filename]
		//isModified := modifiedFileMap[filename]
		isModified := false
		if !reflect.DeepEqual(localIndexMetaData.GetBlockHashList(), baseDirFileInfoMap[filename].GetBlockHashList()) {
			isModified = true
		}
		// not on local, not on server, upload file
		log.Println(filename, " isOnlocal: ", isOnLocalIndex, "inOnServer: ", isOnServer, "isModified", isModified)
		if !isOnServer && !isOnLocalIndex {
			// case 1
			// upload new file, most base case
			if err1 := upload(client, int32(0), filename, baseDirFileInfoMap, baseDirFileBlockMap, localIndexMetaMap); err1 != nil {
				//var VersionErr = errors.New("wrong version metastore")
				//if errors.Is(err1, VersionErr) {
				//	if e := download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); e != nil {
				//		log.Fatalf("Download %v failed: %s", filename, e)
				//	}
				//} else {
				//	log.Fatalf("Upload failed: %s", err1)
				//}
				if e := download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); e != nil {
					log.Fatalf("Download %v failed: %s", filename, e)
				}
				//}
			}
		} else if isOnServer && isOnLocalIndex {
			//fmt.Println("isModified: ", isModified)
			serverVersion := serverMetaData.GetVersion()
			localVersion := localIndexMetaData.GetVersion()
			if isModified {
				// locally modified the file
				log.Println(filename, " serverVersion, localVersion: ", serverVersion, localVersion)
				if serverVersion == localVersion {
					// case 2:
					// locally modified the file no remote changes
					if err1 := upload(client, localVersion, filename, baseDirFileInfoMap, baseDirFileBlockMap, localIndexMetaMap); err1 != nil {
						//var VersionErr = errors.New("wrong version metastore")
						//if errors.Is(err1, VersionErr) {
						//	if e := download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); e != nil {
						//		log.Fatalf("Download %v failed: %s", filename, e)
						//	}
						//} else {
						//	log.Fatalf("Upload failed: %s", err1)
						//}
						if e := download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); e != nil {
							log.Fatalf("Download %v failed: %s", filename, e)
						}
					}

				}
			}

			// server version > local version
			// download or delete
			// todo: delete
			if serverVersion > localVersion {
				// case 3
				// download from server
				if err := download(client, filename, serverFileInfoMap, localIndexMetaMap, baseDirFileInfoMap); err != nil {
					log.Fatalf("Download %v failed: %s", filename, err)
				}
			}
		}
	}

	//fmt.Println("--------Server Index Map After Upload--------")
	//PrintMetaMap(serverFileInfoMap)

}

func uploadTombstone(client RPCClient, version int32, filename string, serverFileInfoMap map[string]*FileMetaData, localIndexMetaMap map[string]*FileMetaData) error {
	version += 1
	serverFileInfoMap[filename].BlockHashList = []string{"0"}
	if err := client.UpdateFile(serverFileInfoMap[filename], &version); err != nil {
		return err
	}
	localIndexMetaMap[filename].Version = version
	localIndexMetaMap[filename].BlockHashList = []string{"0"}
	if err := WriteMetaFile(localIndexMetaMap, client.BaseDir); err != nil {
		return err
	}
	return nil
}

func updateBaseDirVersion(baseDirFileInfoMap map[string]*FileMetaData, localIndexMetaMap map[string]*FileMetaData) {
	for filename := range baseDirFileInfoMap {
		if _, isOnIndex := localIndexMetaMap[filename]; isOnIndex {
			baseDirFileInfoMap[filename].Version = localIndexMetaMap[filename].GetVersion()
		}
	}
}

//
func download(client RPCClient, k string, serverFileInfoMap map[string]*FileMetaData, localIndexMetaMap map[string]*FileMetaData, baseDirFileInfoMap map[string]*FileMetaData) error {
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		return err
	}

	remoteBlockHashList := serverFileInfoMap[k].GetBlockHashList()
	isServerDeleted := len(remoteBlockHashList) == 1 && remoteBlockHashList[0] == "0"

	localIndexMetaData, _ := localIndexMetaMap[k]
	////.Println("file is on local", isOnLocal)
	localIndexHashList := localIndexMetaData.GetBlockHashList()

	if isServerDeleted {
		localIndexMetaMap[k].Version = serverFileInfoMap[k].GetVersion()
		localIndexMetaMap[k].BlockHashList = []string{"0"}
		path := client.BaseDir + "/" + k
		log.Println("Download: delete file path: ", path)
		if err := WriteMetaFile(localIndexMetaMap, client.BaseDir); err != nil {
			return err
		}
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err == nil {
			// file exist, delete file
			err = os.Remove(path)
			if err != nil {
				return err
			}
		} else {
			return err
		}

	} else {
		// download blocks associate with the file
		// reconstitute that file in the base directory
		// update FileInfo to local index
		currFileWriteAtPosition := int32(0)
		currHashPosition := 0
		path := client.BaseDir + "/" + k
		//path := k
		for i, bh := range remoteBlockHashList {
			b := new(Block)
			if err := client.GetBlock(bh, blockStoreAddr, b); err != nil {
				return err
			}
			// write file
			if i < len(localIndexHashList) && bh == localIndexHashList[i] {
				continue
			} else {
				if currHashPosition == 0 {
					currHashPosition = i
				}
				if _, err := os.Stat(path); os.IsNotExist(err) {
					os.Create(path)
				}
				file, err := os.OpenFile(path, os.O_RDWR, 0644)
				if err != nil {
					log.Printf("Failed open file: %s\n", err)
					return err
				}
				log.Println(b)
				log.Println(currFileWriteAtPosition)
				if _, err := file.WriteAt(b.BlockData, int64(currFileWriteAtPosition)); err != nil {
					log.Printf("Failed write file: %s\n", err)
					return err
				}
				if err := file.Truncate(int64(b.BlockSize + currFileWriteAtPosition)); err != nil {
					log.Printf("Failed write file: %s\n", err)
					return err
				}
				if err := file.Close(); err != nil {
					log.Printf("Failed close file: %s\n", err)
					return err
				}
			}
			currFileWriteAtPosition += b.BlockSize
		}

		// rewrite local index.txt
		localIndexMetaMap[k] = serverFileInfoMap[k]
		if err := WriteMetaFile(localIndexMetaMap, client.BaseDir); err != nil {
			return err
		}
		baseDirFileInfoMap[k] = serverFileInfoMap[k]
	}
	return nil
}

func upload(client RPCClient, lastestVersion int32, filename string, baseDirFileInfoMap map[string]*FileMetaData, baseDirFileBlockMap map[string]*Block, localIndexMetaMap map[string]*FileMetaData) error {
	// upload blocks based on the file to server
	// update server with new FileInfo (may have version error)
	// if update success, update local index
	blockHashList := baseDirFileInfoMap[filename].GetBlockHashList()
	//lastestVersion := baseDirFileInfoMap[filename].GetVersion()
	for _, hash := range blockHashList {
		block := baseDirFileBlockMap[hash]
		var BlockStoreAddr string
		if err := client.GetBlockStoreAddr(&BlockStoreAddr); err != nil {
			return err
		}
		var succ bool
		if err := client.PutBlock(block, BlockStoreAddr, &succ); err != nil {
			return err
		}
		if !succ {
			// put block failed
			log.Fatalf("Put block failed")
		}
	}
	lastestVersion += 1
	localIndexMetaMap[filename] = baseDirFileInfoMap[filename]
	if err := client.UpdateFile(localIndexMetaMap[filename], &lastestVersion); err != nil {
		return err
	}

	// update local index.txt
	localIndexMetaMap[filename].Version = lastestVersion
	if err := WriteMetaFile(localIndexMetaMap, client.BaseDir); err != nil {
		return err
	}

	return nil

}

func BaseDirMetaMap(client RPCClient) (map[string]*FileMetaData, map[string]*Block) {
	// scan base dir
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatalf("Read baseDir failed: %s", err)
	}
	baseDirFileInfoMap := make(map[string]*FileMetaData)
	baseDirFileBlockMap := make(map[string]*Block) // hash: *block
	localindexFlag := false
	for _, fileInfo := range files {
		if fileInfo.Name() == "index.txt" {
			// check if local index file exists
			localindexFlag = true
		} else {
			// compute hash list for each file
			// * split the file based on client.blocksize
			localBlockHashes := &BlockHashes{
				Hashes: []string{},
			}

			file, err := os.Open(client.BaseDir + "/" + fileInfo.Name())
			defer func(file *os.File) {
				err := file.Close()
				if err != nil {
					log.Fatalf("File close failed: %s", err)
				}
			}(file)

			for {
				partBuffer := make([]byte, client.BlockSize)

				if err != nil {
					log.Fatal(err)
				}
				num, e := file.Read(partBuffer)
				if e != nil && e == io.EOF {
					break
				} else if e != nil {
					log.Fatal(e)
				}
				var block Block
				block.BlockData = partBuffer[:num]
				block.BlockSize = int32(num)
				// func for create hash of a block return *blockhashes
				// * compute hash for each block
				hashBytes := sha256.Sum256(block.BlockData)
				hashString := hex.EncodeToString(hashBytes[:])
				// * append hash to hashlist
				localBlockHashes.Hashes = append(localBlockHashes.Hashes, hashString)

				baseDirFileBlockMap[hashString] = &block
			}
			//baseDirFileHashMap[fileInfo.Name()] = localBlockHashes
			baseDirFileInfoMap[fileInfo.Name()] = &FileMetaData{
				Filename:      fileInfo.Name(),
				Version:       0,
				BlockHashList: localBlockHashes.Hashes,
			}
		}
	}
	// create local index if not exist
	if err := CreateIndexFile(localindexFlag, client); err != nil {
		log.Fatalf("Create Index.txt failed: %s", err)
	}

	return baseDirFileInfoMap, baseDirFileBlockMap
}

func CreateIndexFile(localindexFlag bool, client RPCClient) error {
	if localindexFlag == false {
		localIndexFile, err := os.Create(client.BaseDir + "/index.txt")
		if err != nil {
			return err
		}
		return localIndexFile.Close()

	}
	return nil
}
