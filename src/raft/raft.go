package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"math/rand"
	"time"
)

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	STATUS_FOLLOWER  = 0
	STATUS_CANDIDATE = 1
	STATUS_LEADER    = 2
	HBINTERVAL       = 100 * time.Millisecond
)

type LogEntry struct {
	Command interface{}
	Term    int
	//Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status int // 0 follower, 1 candidate, 2 leader

	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  []int
	matchIndex []int

	//statusChan   chan int
	commitedChan chan bool

	// channels
	hearBeatChan    chan bool
	requestVoteChan chan bool

	voteCount int
	winChan   chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == STATUS_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
	//NextIndex int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// returnned before unlock caused a deadlock
	defer rf.mu.Unlock()
	defer rf.persist()
	DPrintf("%d vote %d in term %d", rf.me, args.CandidateId, args.Term)
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("RequestVote args term %d current term %d ", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = STATUS_FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	// If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	//$ 5.4.1  Election restriction
	if rf.log[len(rf.log)-1].Term > args.LastLogTerm {
		reply.VoteGranted = false
	} else if len(rf.log)-1 > args.LastLogIndex &&
		rf.log[len(rf.log)-1].Term == args.LastLogTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.status = STATUS_FOLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
	DPrintf("vote: %t ", reply.VoteGranted)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("%d sendRequestVote to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%d sendRequestVote %d Ok? %t", rf.me, server, ok)
	// need a lock here to prevert multiple true into winChan
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// host lost connection for a while
		//If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = STATUS_FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}

		if rf.currentTerm == reply.Term && reply.VoteGranted {
			rf.voteCount++
			// host could be follower when got response.
			if rf.status == STATUS_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				DPrintf("%d win the selected", rf.me)
				// change status to follower so winChan will not be pushed in
				// multiple true, otherwise it will win without vote in the future
				rf.status = STATUS_FOLLOWER
				rf.winChan <- true
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	DPrintf("%d Enters broadcastRequestVote", rf.me)
	rf.mu.Lock()
	var reqVoteArgs RequestVoteArgs
	reqVoteArgs.Term = rf.currentTerm
	reqVoteArgs.CandidateId = rf.me
	reqVoteArgs.LastLogIndex = len(rf.log) - 1
	reqVoteArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func(i int, reqVoteArgs RequestVoteArgs) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, &reqVoteArgs, &reply)
			}(i, reqVoteArgs)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	DPrintf("%d got append entry from  %d in term %d", rf.me, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("AppendEntries args term %d current term %d ", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = STATUS_FOLLOWER
		rf.votedFor = -1
	}
	rf.hearBeatChan <- true
	reply.Term = args.Term

	//2. Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm (§5.3)
	DPrintf("rf.log %v args.PrevLogIndex %d", rf.log, args.PrevLogIndex)
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("AppendEntries 263")
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	//	if rf.log[args.PrevLogIndex+1].Term != args.Term {
	//		reply.Success = false
	//		reply.Term = 1
	//		return
	//	}

	// 4. Append any new entries not already in the log
	if len(args.Entries) != 0 {
		DPrintf("AppendEntries 279")
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Term = args.Term
		reply.Success = true
	}

	if args.LeaderCommit > rf.commitIndex {
		lastIdx := len(rf.log) - 1
		if args.LeaderCommit < lastIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIdx
		}
		rf.commitedChan <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("server %d send append entry to %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.status != STATUS_LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		// host lost connection for a while
		//If RPC request or response contains term T > currentTerm:
		//set currentTerm = T, convert to follower (§5.1)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = STATUS_FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		//If successful: update nextIndex and matchIndex for follower (§5.3)
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {

			}
		} else {
			//If AppendEntries fails because of log inconsistency:
			//decrement nextIndex and retry (§5.3)
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server]--
			}
		}
	} else {
		DPrintf("Send append entry failed %v", ok)
	}
	return ok
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("%d Enters broadcastAppendEntries", rf.me)
	rf.mu.Lock()
	var args AppendEntriesArgs
	defer rf.mu.Unlock()
	//• If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	N := rf.commitIndex
	size := len(rf.log)
	for i := rf.commitIndex + 1; i < size; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.commitedChan <- true
	}

	for i := range rf.peers {
		if i != rf.me {
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			if rf.nextIndex[i] >= len(rf.log) {
				args.PrevLogIndex = len(rf.log) - 1
			} else {
				args.PrevLogIndex = rf.nextIndex[i] - 1
			}
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = make([]LogEntry, len(rf.log)-args.PrevLogIndex-1)
			copy(args.Entries, rf.log[args.PrevLogIndex+1:])
			args.LeaderCommit = rf.commitIndex
			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, &args, &reply)
			}(i, args)
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.status == STATUS_LEADER
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		rf.persist()
	}
	// Your code here (2B).
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = STATUS_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0}) // add dummy value, index start from 1
	rf.hearBeatChan = make(chan bool, 100)
	rf.requestVoteChan = make(chan bool, 100)
	rf.winChan = make(chan bool, 100)
	rf.commitedChan = make(chan bool, 100)
	DPrintf("rf %d, state %d", rf.me, rf.status)
	rf.readPersist(persister.ReadRaftState())
	go func() {
		for {
			switch rf.status {
			case STATUS_FOLLOWER:
				DPrintf("%d stat is follower in term %d logs: %v", rf.me, rf.currentTerm, rf.log)
				select {
				case <-rf.hearBeatChan:
					DPrintf("%d got heart beat", rf.me)
				case <-rf.requestVoteChan:
					DPrintf("%d got request vote", rf.me)
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					DPrintf("%d time out change to candidate", rf.me)
					rf.status = STATUS_CANDIDATE
				}
			case STATUS_CANDIDATE:
				DPrintf("%d stat is candidate in term %d logs: %v", rf.me, rf.currentTerm, rf.log)
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				//send request vote to all peers
				go rf.broadcastRequestVote()
				select {
				case <-rf.hearBeatChan:
					rf.mu.Lock()
					rf.status = STATUS_FOLLOWER
					rf.mu.Unlock()
				case <-rf.winChan:
					rf.mu.Lock()
					DPrintf("%d win leader in term %d", rf.me, rf.currentTerm)
					//					rf.broadcastAppendEntries()
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					rf.status = STATUS_LEADER
					rf.mu.Unlock()
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				}
			case STATUS_LEADER:
				DPrintf("%d stat is leader in term %d logs: %v nextIndex %v",
					rf.me, rf.currentTerm, rf.log, rf.nextIndex)
				rf.broadcastAppendEntries()
				time.Sleep(HBINTERVAL)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.commitedChan:
				//	println(rf.me,rf.lastApplied,rf.commitIndex)
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
