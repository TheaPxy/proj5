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

	commitIndexMutex sync.RWMutex
	commitIndexCond  *sync.Cond

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
	//fmt.Println("--GetFileInfoMap--")
	//fmt.Println("iscrashed, isleader: ", s.serverId, s.serverId, s.isCrashed, s.isLeader)

	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	// majority of nodes are not working, block

	//s.isCrashedMutex.Lock()
	//if s.isCrashed {
	//	s.isCrashedMutex.Unlock()
	//	//return nil, ERR_SERVER_CRASHED
	//	return nil, ERR_NOT_LEADER
	//}
	//s.isCrashedMutex.Unlock()
	for {
		count, err := s.CountFollowers(ctx, empty)
		if err != nil {
			return nil, err
		}
		if count > len(s.ipList)/2 {
			break
		}
	}
	fileInfoMap, e := s.metaStore.GetFileInfoMap(ctx, empty)
	if e != nil {
		return nil, e
	}
	return fileInfoMap, nil

}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// s is crashed

	s.isLeaderMutex.Lock()
	if !s.isLeader {
		s.isLeaderMutex.Unlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.Unlock()

	////s.isCrashedMutex.Lock()
	////if s.isCrashed {
	////	s.isCrashedMutex.Unlock()
	////	return nil, ERR_NOT_LEADER////
	////}
	////s.isCrashedMutex.Unlock()

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
	//count, err := s.CountFollowers(ctx, empty)
	//if err != nil {
	//	return nil, err
	//}
	//if count > len(s.ipList)/2 {
	//	addr, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return addr, nil
	//} else {
	//	return nil, errors.New("Majority Server Down")
	//}

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
	//todo find the right place to update s.log
	s.log = append(s.log, &op)

	ok := make(chan bool, len(s.ipList)-1)

	for i, addr := range s.ipList {
		if addr == s.ip {
			continue
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  0,
			PrevLogIndex: -1,
			Entries:      make([]*UpdateOperation, 0),
			LeaderCommit: s.commitIndex,
		}

		// if s.log is not empty
		//s.nextIndexMapMutex.Lock()
		//todo CorrectLog: check input.entries
		fmt.Printf("--UpdateFile-- %v \n Leader's Log: %v, NextIndex %v", s.serverId, s.log, s.nextIndex)
		//fmt.Println("  NextIndex ", s.nextIndex)
		//fmt.Println("  CommitIndex ", s.commitIndex)
		s.nextIndexMapMutex.Lock()
		if s.nextIndex[addr] >= 1 {
			input.PrevLogTerm = s.log[s.nextIndex[addr]-1].Term
			input.PrevLogIndex = s.nextIndex[addr] - 1
		}
		if s.nextIndex[addr] < int64(len(s.log)) {
			input.Entries = []*UpdateOperation{s.log[s.nextIndex[addr]]}
		}
		s.nextIndexMapMutex.Unlock()
		fmt.Println("  Input ", input)
		go s.AppendFollowerEntry(i, ok, input)
	}
	count := 1
	for {
		succ := <-ok
		if succ {
			fmt.Println("-----------succ count", count)
			count++
		}
		if count > len(s.ipList)/2 {
			// majority of nodes are alive
			// change leader's commit index
			//todo how much commitIndex incre?
			s.commitIndexMutex.Lock()
			s.commitIndex++
			s.commitIndexMutex.Unlock()
			fmt.Println("--Update File-- Commit++: Server ", s.serverId, " CommitIndex ", s.commitIndex)
			break
		}
	}
	return s.metaStore.UpdateFile(ctx, filemeta)
}

func (s *RaftSurfstore) AppendFollowerEntry(serverIdx int, ok chan bool, input *AppendEntryInput) {
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

		//fmt.Printf("--AppendFollowerEntr--\n %v input: %v \n", s.serverId, input)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		//internalState, err := s.GetInternalState(ctx, &emptypb.Empty{})
		//fmt.Println(internalState)
		if err != nil {
			//fmt.Println("--AppendFollowerEntry-- output: ", output, " error: ", err)
			continue
		}
		if output.Success {
			// rule 4, rule 5
			if len(input.Entries) != 0 {
				s.nextIndexMapMutex.Lock()
				s.nextIndex[addr]++
				s.nextIndexMapMutex.Unlock()
			}
			fmt.Printf("--AppendFollowerEntr-- return ok true from %v to %v\n %v input: %v \n", s.serverId, serverIdx, input)
			ok <- true
			return
		} else {
			// failed cases, desc nextIndex based on output
			// violate rule 1:
			fmt.Printf("--AppendFollowerEntry-- %v output fail \n", s.serverId)
			if output.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				s.term = output.Term
				ok <- false
				return
			} else {
				// violate rule 2|| violate rule 3
				s.nextIndexMapMutex.Lock()
				s.nextIndex[addr]--
				fmt.Printf("--AppendFollowerEntry-- violate rule 2,3 from %v to %v \n  nextIndex: %v \n", s.serverId, serverIdx, s.nextIndex[addr])
				s.nextIndexMapMutex.Unlock()
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

	fmt.Printf("--AppendEntries-- %v \n Input Received: %v \n s prev log: %v \n", s.serverId, input, s.log)
	//fmt.Println("  ", s.ip, " Follower input ", input, " s prev log ", s.log)
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
	s.isLeaderMutex.Lock()
	if s.isLeader {
		s.isLeaderMutex.Unlock()
		s.isLeader = false
	} else {
		s.isLeaderMutex.Unlock()
	}

	// rule2 || rule3
	// todo never entered
	if len(s.log) <= int(input.PrevLogIndex) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		return output, nil
	}

	// input.PrevLogIndex < len(s.log) && (< 0 || term ==)
	s.term = input.Term
	output.Term = s.term

	//todo overwrite log
	if input.PrevLogIndex < 0 {
		s.log = make([]*UpdateOperation, 0)
	} else {
		s.log = s.log[:input.PrevLogIndex+1]
	}

	// rule 4
	// todo concurrent safe
	s.log = append(s.log, input.Entries...)
	fmt.Printf("--AppendEntries-- %v \n s.commitIndex %v \n s.lastApplied %v \n leader.CommitIndex %v \n s modi log: %v \n", s.serverId, s.commitIndex, s.lastApplied, input.LeaderCommit, s.log)
	// todo: what if len(input.entries) == 0
	// rule 5
	s.commitIndexMutex.Lock()
	//fmt.Println("  325 s.commitIndex ", s.commitIndex, " s.lastApplied ", s.lastApplied, " leader.commitIndex ", input.LeaderCommit, " s.log ", s.log)
	if input.LeaderCommit > s.commitIndex {
		//todo last append new entry?
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	s.commitIndexMutex.Unlock()

	s.commitIndexMutex.Lock()
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		//fmt.Println("  s.commitIndex ", s.commitIndex, " s.lastApplied ", s.lastApplied, " s.log ", s.log)
		fmt.Printf("--AppendEntries-- %v \n s.commitIndex %v \n s.lastApplied %v \n leader.CommitIndex %v \n s modi log: %v \n", s.serverId, s.commitIndex, s.lastApplied, input.LeaderCommit, s.log)
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}
	s.commitIndexMutex.Unlock()
	fmt.Println("  Append Success", s.serverId, " log ", s.log)
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
	s.nextIndexMapMutex.Lock()
	for i, _ := range s.nextIndex {
		s.commitIndexMutex.Lock()
		//todo ?????????????????????
		s.nextIndex[i] = s.commitIndex + 1
		s.commitIndexMutex.Unlock()
	}
	s.nextIndexMapMutex.Unlock()

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	fmt.Println("--set leader-- ", s.serverId, " is leader now")
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

	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		fmt.Printf("--SendHeartBeat-- leader Id: %v, Receiver Id: %v, term: %v \n nextIndex %v \n leaderLog %v \n ", s.serverId, idx, s.term, s.nextIndex, s.log)
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
		s.nextIndexMapMutex.Lock()
		//fmt.Println("  NextIndex ", s.nextIndex)
		//fmt.Println("  s log ", s.log)
		// if s.log is not empty
		if s.nextIndex[addr] >= 1 {
			input.PrevLogTerm = s.log[s.nextIndex[addr]-1].Term
			input.PrevLogIndex = s.nextIndex[addr] - 1
		}
		if s.nextIndex[addr] < int64(len(s.log)) {
			//input.Entries = s.log[s.nextIndex[addr]:]
			input.Entries = []*UpdateOperation{s.log[s.nextIndex[addr]]}
		}
		s.nextIndexMapMutex.Unlock()

		//fmt.Println("  Leader's Log ", s.ip, " ", s.log)
		fmt.Println("  input.entry ", input.Entries)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)

		if err != nil {
			//fmt.Println("--AppendFollowerEntry-- output: ", output, " error: ", err)
			continue
		}
		if output.Success {
			// todo update nextIndex for followers ???????????
			// rule 4, rule 5
			if len(input.Entries) != 0 {
				s.nextIndexMapMutex.Lock()
				s.nextIndex[addr]++
				s.nextIndexMapMutex.Unlock()
			}
		} else {
			// failed cases, desc nextIndex based on output
			// violate rule 1:
			fmt.Println("--AppendFollowerEntry--", s.serverId, " output fail")
			if output.Term > s.term {
				s.isLeaderMutex.Lock()
				s.isLeader = false
				s.isLeaderMutex.Unlock()
				s.term = output.Term

			} else {
				// violate rule 2|| violate rule 3
				s.nextIndexMapMutex.Lock()
				s.nextIndex[addr]--
				fmt.Println("--AppendFollowerEntry-- violate rule 2,3 addr: ", idx, " nextIndex: ", s.nextIndex[addr])
				s.nextIndexMapMutex.Unlock()
			}
		}

	}
	for idx, addr := range s.ipList {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)
		internalState, err := client.GetInternalState(ctx, &emptypb.Empty{})
		isCrash, err := client.IsCrashed(ctx, &emptypb.Empty{})
		fmt.Println("idx ", idx, internalState, isCrash.IsCrashed)
	}
	return &Success{Flag: true}, nil

}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("server %d Crash\n", s.serverId)
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("server %d Restore\n", s.serverId)
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
	count := 1
	//s.isCrashedMutex.Lock()
	//if s.isCrashed {
	//	s.isCrashedMutex.Unlock()
	//	return -1, ERR_SERVER_CRASHED
	//}
	//s.isCrashedMutex.Unlock()
	//
	//s.isLeaderMutex.Lock()
	//if !s.isLeader {
	//	s.isLeaderMutex.Unlock()
	//	return -1, ERR_NOT_LEADER
	//}
	//s.isLeaderMutex.Unlock()

	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		//fmt.Println("--CountFollowers-- leader Id: Receiver Id: term: ", s.ip, addr, s.term)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		client := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		isCrash, err := client.IsCrashed(ctx, &emptypb.Empty{})

		//fmt.Printf("--count follower is crash-- %v %v \n", idx, isCrash)
		if err != nil || isCrash == nil {
			return -1, err
		}
		if !isCrash.IsCrashed {
			count++
		}

	}
	//return &Success{Flag: true}, nil
	return count, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
