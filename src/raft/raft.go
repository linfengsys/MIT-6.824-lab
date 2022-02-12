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

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	LEADER    = "leader"
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	nodeState string // LEAER, FOLLOWER, CANDIDATE
	term      int    // need persistent
	votefor   int    // need persistent
	logs      []LogEntry

	commitIndex      int   // all nodes
	lastAppliedIndex int   // all nodes
	nextIndex        []int // not include leader
	matchIndex       []int // not include leader

	// for leader election
	heartbeatTimer *time.Timer
	electionTimer  *time.Timer

	// for async replication
	replicateCond []*sync.Cond

	// for apply committed log
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.nodeState == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
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

/* 1: local log win, 0: outside log win */
func (rf *Raft) CompareLog(logIndex int, logTerm int) bool {
	logSize := len(rf.logs)
	return rf.logs[logSize-1].Index > logIndex
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/* refuse directly when term is small than local's */
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	/* same term, but already vote for other node */
	if args.Term == rf.term && rf.votefor != -1 && rf.votefor != args.CandidateId {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.votefor = -1
		rf.ModifyNodeState(FOLLOWER)
	}

	if rf.CompareLog(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	rf.votefor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.term, true
}

/* for snapshot */
func (rf *Raft) getFirstLog() LogEntry {
	if len(rf.logs) <= 1 {
		return LogEntry{-1, 0, nil}
	}
	return rf.logs[1]
}

func (rf *Raft) getLastLog() LogEntry {
	logSize := len(rf.logs)
	return rf.logs[logSize-1]
}

func (rf *Raft) getLogByIndex(index int) LogEntry {
	if index == 0 {
		return LogEntry{-1, 0, nil}
	}

	snapshot := rf.logs[0]
	return rf.logs[index-snapshot.Index]
}

func (rf *Raft) appendNewLog(command interface{}) (int, int) {
	fmt.Printf("Append new log, log len:%v\n", len(rf.logs))
	lastLog := rf.getLastLog()
	rf.logs = append(rf.logs, LogEntry{rf.term, lastLog.Index + 1, command})
	fmt.Printf("Append new log, log len:%v\n", len(rf.logs))
	return rf.term, lastLog.Index + 1
}

func (rf *Raft) ModifyNodeState(state string) {
	rf.nodeState = state
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

/* append entries */
type RequestAppendEntriesArgs struct {
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int
	Logs        []LogEntry
	CommitIndex int
}

type RequestAppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		DPrintf("ignore invalid append entries from %v to %v, leader term:%v, follower term:%v", args.LeaderId, rf.me, args.Term, rf.term)
		reply.Term = rf.term
		reply.Success = false
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.votefor = -1
	}

	rf.ModifyNodeState(FOLLOWER)
	rf.electionTimer.Reset(RandomElectionTimeout())

	if len(args.Logs) == 0 {
		/* heartbeat, no need compare prelog */
	} else {
		/* append log entries */
		if args.PreLogIndex < rf.getFirstLog().Index {
			/* cond snapshot, invalid for log catchup */
			reply.Term, reply.Success = rf.term, false
			return
		}

		/* check log index & term, wether match */
		lastLog := rf.getLastLog()
		if lastLog.Index < args.PreLogIndex {
			reply.Term, reply.Success = rf.term, false
			reply.ConflictIndex = lastLog.Index + 1
			return
		}

		checkLog := rf.getLogByIndex(args.PreLogIndex)
		if checkLog.Term != args.PreLogTerm {
			reply.Term, reply.Success = rf.term, false
			reply.ConflictIndex = args.PreLogIndex - 1
			/* todo, can batch rollback ?*/
			return
		}

		/* append logs from index:prelogIndex + 1*/
		rf.logs = rf.logs[0 : args.PreLogIndex-rf.logs[0].Index+1]
		rf.logs = append(rf.logs, args.Logs...)
		fmt.Printf("log in rpc, command:%v, term:%v, index:%v\n", args.Logs[0].Command, args.Logs[0].Term, args.Logs[0].Index)
		fmt.Printf("peer:%v, append log success, log len:%v\n", rf.me, len(rf.logs))
		fmt.Printf("log info for index = 1, command:%v, term:%v, index:%v\n", rf.logs[1].Command, rf.logs[1].Term, rf.logs[1].Index)
	}

	/* update last commit index for apply */
	if args.CommitIndex > rf.commitIndex {
		rf.commitIndex = args.CommitIndex
		rf.applyCond.L.Lock()
		rf.applyCond.Signal()
		rf.applyCond.L.Unlock()
	}
	fmt.Printf("node:%v, commit index:%v\n", rf.me, rf.commitIndex)

	reply.Term, reply.Success = rf.term, true
}

func (rf *Raft) sendAppendEntires(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nodeState != LEADER {
		return -1, -1, false
	}

	newLogTerm, newLogIndex := rf.appendNewLog(command)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.replicateCond[peer].L.Lock()
		fmt.Printf("Wakeup thread:%v\n", peer)
		rf.replicateCond[peer].Signal()
		rf.replicateCond[peer].L.Unlock()
	}
	fmt.Printf("Start new raft log, linfengsys, term:%v, index:%v\n", newLogTerm, newLogIndex)
	return newLogIndex, newLogTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func HeartbeatTimeout() time.Duration {
	HEARTBEAT_TIMEOUT_IN_MS := 100
	return time.Duration(HEARTBEAT_TIMEOUT_IN_MS) * time.Millisecond
}

/* Election timeout is randomize to forbid livelock,
* and it is larger than heartbeat timeout */
func RandomElectionTimeout() time.Duration {
	ELECTION_TIMEOUT_IN_MS := 150
	randRange := 100
	electionTimeout := ELECTION_TIMEOUT_IN_MS + rand.Intn(randRange)
	return time.Duration(electionTimeout) * time.Millisecond
}

func (rf *Raft) getCommitIndex() int {
	firstLog := rf.getFirstLog()
	index := rf.getLastLog().Index
	for index >= firstLog.Index {
		commits := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= index {
				commits++
			}
			if commits > len(rf.peers)/2 {
				return index
			}
		}
		index--
	}
	return firstLog.Index
}

func (rf *Raft) SendHeartbeat() {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if rf.nodeState != LEADER {
		return
	}

	DPrintf("Heartbeat start, node:%v\n", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		commitIndex := rf.getCommitIndex()
		fmt.Printf("Heartbeat start, leader:%v, commitIndex:%v\n", rf.me, commitIndex)
		// send heartbeat via append-entries interface
		go func(peer int) {
			rf.mu.Lock()
			hbRequest := &RequestAppendEntriesArgs{
				Term:        rf.term,
				LeaderId:    rf.me,
				PreLogIndex: 0,
				PreLogTerm:  0,
				Logs:        nil,
				CommitIndex: commitIndex,
			}

			hbResponse := new(RequestAppendEntriesReply)
			rf.mu.Unlock()
			fmt.Printf("Send Append Entry for Heartbeat from: %v to %v\n", rf.me, peer)
			if rf.sendAppendEntires(peer, hbRequest, hbResponse) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.term != hbRequest.Term || rf.nodeState != LEADER {
					DPrintf("Ignore heartbeat rpc response from %v to %v,  currentTerm:%v, currentState:%v, sendTerm:%v",
						rf.me, peer, rf.term, rf.nodeState, hbRequest.Term)
					return
				}

				if hbResponse.Success {
					// do nothing
				} else if rf.term > hbResponse.Term {
					rf.term = hbResponse.Term
					rf.ModifyNodeState(FOLLOWER)
					rf.electionTimer.Reset(RandomElectionTimeout())
				}
			}
		}(peer)
	}
	fmt.Println("Reset timeout for heartbeat")
	rf.heartbeatTimer.Reset(HeartbeatTimeout())
}

func (rf *Raft) StartLeaderElection() {
	fmt.Printf("node:%v start election, %v\n", rf.me, rf.nodeState)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nodeState == LEADER {
		return
	}

	DPrintf("Election start, node:%v\n", rf.me)
	rf.term += 1
	rf.ModifyNodeState(CANDIDATE)
	rf.votefor = rf.me
	grantVotes := 1

	lastLog := rf.getLastLog()
	voteRequest := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		/* async call for rpc */
		go func(peer int) {
			voteResponse := new(RequestVoteReply)
			if rf.sendRequestVote(peer, voteRequest, voteResponse) {
				DPrintf("vote success, from:%v to:%v\n", peer, rf.me)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				/* check current node state, forbid obsolete operation */
				if rf.term != voteRequest.Term || rf.nodeState != CANDIDATE {
					DPrintf("Ignore vote rpc response from %v to %v,  currentTerm:%v, currentState:%v, sendTerm:%v",
						rf.me, peer, rf.term, rf.nodeState, voteRequest.Term)
					return
				}

				if voteResponse.VoteGranted {
					grantVotes++
					DPrintf("node:%v grantvotes:%v\n", rf.me, grantVotes)
					if grantVotes > len(rf.peers)/2 {
						DPrintf("Quorurm votes have been granted by %v\n", rf.me)
						fmt.Printf("Quorurm votes have been granted by %v\n", rf.me)

						rf.heartbeatTimer.Reset(HeartbeatTimeout())
						rf.ModifyNodeState(LEADER)
						/* todo send heartbeat */
					}
				} else if voteResponse.Term > rf.term {
					rf.term = voteResponse.Term
					rf.votefor = -1
					rf.ModifyNodeState(FOLLOWER)
				}
			}
		}(peer)
	}

	rf.electionTimer.Reset(RandomElectionTimeout())
}

/* do not worry about one log may be synced multi times */
/* receieve node will handle this case */
func (rf *Raft) needReplicate(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLog := rf.getLastLog()
	//    fmt.Printf("node:%v last log index:%v, matchIndex for this peer:%v\n", peer, lastLog.Index, rf.matchIndex[peer])
	return rf.nodeState == LEADER && lastLog.Index > rf.matchIndex[peer]
}

func (rf *Raft) handleAppendEntriesResponse(peer int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	if reply.Success {
		rf.nextIndex[peer] = args.Logs[len(args.Logs)-1].Index + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1

		commitIndex := rf.getCommitIndex()
		if commitIndex > rf.commitIndex {
			rf.applyCond.L.Lock()
			rf.applyCond.Signal()
			rf.applyCond.L.Unlock()
			rf.commitIndex = commitIndex
		}
		fmt.Printf("leader:%v -> node:%v, append entries success, matchindex:%v\n", rf.me, peer, rf.matchIndex[peer])
		return
	}

	rf.nextIndex[peer] = reply.ConflictIndex
	rf.matchIndex[peer] = rf.nextIndex[peer] - 1
	return
}

func (rf *Raft) startReplicate(peer int) {
	rf.mu.Lock()

	/* todo: lab2d, lag node catchup by snpashot */

	/* replicate logs to follower */
	preSentLog := rf.getLogByIndex(rf.nextIndex[peer] - 1)
	lastLog := rf.getLastLog()

	if lastLog.Index == preSentLog.Index {
		rf.mu.Unlock()
		return
	}
	replicateLogs := make([]LogEntry, lastLog.Index-preSentLog.Index)
	snapshotIndex := rf.logs[0].Index
	fmt.Printf("leader log len:%v, from:%v -> %v\n", len(rf.logs), preSentLog.Index-snapshotIndex+1, lastLog.Index-snapshotIndex+1)
	copy(replicateLogs, rf.logs[preSentLog.Index-snapshotIndex+1:lastLog.Index-snapshotIndex+1])

	commitIndex := rf.getCommitIndex()
	request := &RequestAppendEntriesArgs{
		Term:        rf.term,
		LeaderId:    rf.me,
		PreLogIndex: preSentLog.Index,
		PreLogTerm:  preSentLog.Term,
		Logs:        replicateLogs,
		CommitIndex: commitIndex,
	}

	response := new(RequestAppendEntriesReply)
	fmt.Printf("Start send entries, from leader:%v to follower:%v, log len:%v\n", rf.me, peer, len(replicateLogs))
	rf.mu.Unlock()
	if rf.sendAppendEntires(peer, request, response) {
		fmt.Printf("leader:%v append entries rpc success\n", rf.me)
		rf.mu.Lock()
		rf.handleAppendEntriesResponse(peer, request, response)
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicateThread(peer int) {
	rf.replicateCond[peer].L.Lock()
	for rf.killed() == false {
		/* if no log needs to append, just wait */
		if rf.needReplicate(peer) {
			fmt.Printf("find peer:%v need replicate log\n", peer)
			rf.startReplicate(peer)
		} else {
			rf.replicateCond[peer].Wait()
		}
	}
	rf.replicateCond[peer].L.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimer.C:
			rf.SendHeartbeat()
		case <-rf.electionTimer.C:
			rf.StartLeaderElection()
		}
	}
}

func (rf *Raft) applier() {
	rf.applyCond.L.Lock()
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastAppliedIndex >= rf.commitIndex {
			rf.mu.Unlock()
			rf.applyCond.Wait()
			rf.mu.Lock()
		}

		snapshotIndex := rf.logs[0].Index
		lastAppliedIndex := rf.lastAppliedIndex
		commitIndex := rf.commitIndex
		fmt.Printf("first index:%v, lastapplideindex:%v, commitIndex:%v\n", snapshotIndex, lastAppliedIndex, commitIndex)
		logEntries := make([]LogEntry, commitIndex-lastAppliedIndex)
		copy(logEntries, rf.logs[lastAppliedIndex+1-snapshotIndex:commitIndex-snapshotIndex+1])
		rf.mu.Unlock()

		for _, log := range logEntries {
			fmt.Printf("XXXX node %v start to apply raft log, index:%v command:%v\n", rf.me, log.Index, log.Command)

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandTerm:  log.Term,
				CommandIndex: log.Index,
			}
		}

		rf.mu.Lock()
		rf.lastAppliedIndex = commitIndex
		rf.mu.Unlock()
	}
	rf.applyCond.L.Unlock()
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

	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		nodeState:        FOLLOWER,
		term:             0,
		votefor:          -1,
		logs:             make([]LogEntry, 0),
		commitIndex:      0,
		lastAppliedIndex: 0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		heartbeatTimer:   time.NewTimer(HeartbeatTimeout()),
		electionTimer:    time.NewTimer(RandomElectionTimeout()),
		replicateCond:    make([]*sync.Cond, len(peers)),
		applyCh:          applyCh,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// todo, read from snapshot
	// log index start with 1
	rf.logs = append(rf.logs, LogEntry{-1, 0, nil})

	// election
	go rf.ticker()

	// log replication
	lastLog := rf.getLastLog()
	for peer := range peers {
		rf.nextIndex[peer] = lastLog.Index + 1
		rf.matchIndex[peer] = 0
		if peer != rf.me {
			rf.replicateCond[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicateThread(peer)
		}
	}

	// applier
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	go rf.applier()

	return rf
}
