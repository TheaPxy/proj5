package surfstore

import (
	context "context"
	"fmt"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"math"
	"sync"
	"time"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool
	//nextIndex      sync.Map
	nextIndex map[string]int64

	lastApplied int64

	//server info
	ip       string
	ipList   []string
	serverId int64

	//leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	nextIndexMapMutex sync.RWMutex
	nextIndexMapCond  *sync.Cond

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// s is crashed
	// s.isCrashedMutex.Lock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.Unlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.Unlock()
	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	// majority of nodes are not working, block
	for {
		count, err := s.CountFollowers(ctx, empty)
		if err != nil {
			return nil, err
		}
		if count > len(s.ipList)/2 {
			break
		}
	}
	fileInfoMap, err := s.metaStore.GetFileInfoMap(ctx, empty)
	if err != nil {
		return nil, err
	}
	return fileInfoMap, nil

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// s is crashed
	// s.isCrashedMutex.Lock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.Unlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.Unlock()
	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	// majority of nodes are not working, block
	for {
		count, err := s.CountFollowers(ctx, empty)
		if err != nil {
			return nil, err
		}
		if count > len(s.ipList)/2 {
			break
		}
	}
	addr, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
	if err != nil {
		return nil, err
	}
	return addr, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//mutex here
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &op)
	//committed := make(chan bool)
	//s.pendingCommits = append(s.pendingCommits, committed)

	//go s.attemptCommit()
	//success := <-committed
	//if success {
	//	return s.metaStore.UpdateFile(ctx, filemeta)
	//}
	ok := make(chan bool, len(s.ipList)-1)

	for i, p := range s.ipList {
		if p == s.ip {
			continue
		}
		go s.AppendFollowerEntry(i, ok)
	}
	count := 1
	for {
		succ := <-ok
		if succ {
			count++
		}
		if count > len(s.ipList)/2 {
			// majority of nodes are alive
			// change leader's commit index
			s.commitIndex++
			break
		}
	}
	return s.metaStore.UpdateFile(ctx, filemeta)
}

func (s *RaftSurfstore) AppendFollowerEntry(serverIdx int, ok chan bool) {
	// should similar to commitEntry
	for {
		//mutex here
		s.isCrashedMutex.Lock()
		if s.isCrashed {
			s.isCrashedMutex.Unlock()
			return
		}
		s.isCrashedMutex.Unlock()

		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		input := &AppendEntryInput{
			Term: s.term,
			//PrevLogTerm: s.log[s.nextIndex[idx]-1].Term,
			PrevLogTerm: 0,
			//PrevLogIndex: s.nextIndex[idx] - 1,
			PrevLogIndex: -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		// if s.log is not empty
		//s.nextIndexMapMutex.Lock()
		if s.nextIndex[addr] >= 1 {

			input.PrevLogTerm = s.log[s.nextIndex[addr]-1].Term
			input.PrevLogIndex = s.nextIndex[addr] - 1
			//s.nextIndexMapMutex.Unlock()
		}
		//s.nextIndexMapMutex.Unlock()

		//s.nextIndexMapMutex.Lock()
		if s.nextIndex[addr] < int64(len(s.log)) {

			input.Entries = s.log[s.nextIndex[addr]:]
		}
		//s.nextIndexMapMutex.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		fmt.Println("--AppendFollowerEntry-- output: ", output, " error: ", err)
		if err != nil {
			continue
		}
		if output.Success {
			// todo update nextIndex for followers ???????????
			// rule 4, rule 5
			// s.term = output.Term
			//s.nextIndexMapMutex.Lock()
			s.nextIndex[addr] += int64(len(input.Entries))
			//s.nextIndexMapMutex.Unlock()
			ok <- true
			return
		} else {
			// failed cases, desc nextIndex based on output
			// violate rule 1:
			if output.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				ok <- false
				return
			} else {
				// violate rule 2|| violate rule 3
				//s.nextIndexMapMutex.Lock()
				s.nextIndex[addr]--
				//s.nextIndexMapMutex.Unlock()
			}
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	output := &AppendEntryOutput{
		ServerId: s.serverId,
		Term:     s.term,
		Success:  false,
	}
	// rule 1
	if input.Term < s.term {
		//output.Term =
		return output, nil
	}

	// rule2 || rule3
	if len(s.log) <= int(input.PrevLogIndex) || input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		return output, nil
	}

	// input.PrevLogIndex < len(s.log) && (< 0 || term ==)
	//todo: how to deal with match situation
	s.term = input.Term
	output.Term = s.term

	// rule 4
	s.log = append(s.log, input.Entries...)

	// todo: what if len(input.entries) == 0
	// rule 5
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	output.Success = true
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	s.term++
	// set all nextIndex = num of logs+1
	for i, _ := range s.nextIndex {
		s.nextIndex[i] = s.commitIndex + 1
	}
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	//todo set all others isleader to false
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	if s.isCrashed {
		s.isCrashedMutex.Unlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.Unlock()

	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	fmt.Printf("--SendHeartBeat-- Server Id: %d term: %d \n", s.serverId, s.term)
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		//fmt.Println("sendHeartBeat nextIndex: ", s.nextIndex[addr], " addr: ", addr)
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  0,
			PrevLogIndex: -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		// if s.log is not empty
		if s.nextIndex[addr] >= 1 {
			input.PrevLogTerm = s.log[s.nextIndex[addr]-1].Term
			input.PrevLogIndex = s.nextIndex[addr] - 1
		}
		if s.nextIndex[addr] < int64(len(s.log)) {
			input.Entries = s.log[s.nextIndex[addr]:]
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)

		fmt.Println("--SendHeartBeat-- AppendEntries output: ", output, " s term: ", s.term)
		if output != nil && !output.Success && output.Term > s.term {
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
		}
	}

	return &Success{Flag: true}, nil

}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

func (s *RaftSurfstore) CountFollowers(ctx context.Context, empty *emptypb.Empty) (int, error) {
	count := 0
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return -1, nil
		}
		client := NewRaftSurfstoreClient(conn)

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if output != nil {
			// server is alive
			count++
		}
	}
	return count, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
