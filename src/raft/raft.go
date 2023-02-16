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
	"bytes"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"6.824/labgob"
	"6.824/labrpc"
	
)

type NodeState uint8

const (
	Follower  = NodeState(1)
	Candidate = NodeState(2)
	Leader    = NodeState(3)
)

const (
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

type LogEntry struct {
	Command interface{}
	// Index   int
	Term    int
}

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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	state          NodeState

	currentTerm int
	votedFor    int
	logs        []LogEntry // the first entry is a dummy entry which contains LastSnapshotTerm, LastSnapshotIndex and nil Command

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
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
	// Your data here (2A, 2B).
	Term        int // 2A
	CandidateId int // 2A

	LastLogIndex int // 2B
	LastLogTerm  int // 2B
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 2A
	VoteGranted bool // 2A
}

type AppendEntriesArgs struct {
	Term     int // 2A
	LeaderId int // 2A

	PrevLogIndex int        // 2B
	PrevLogTerm  int        // 2B
	LogEntries   []LogEntry // 2B
	LeaderCommit int        // 2B
}

type AppendEntriesReply struct {
	Term    int  // 2A
	Success bool // 2A

	// OPTIMIZE: see thesis section 5.3
	ConflictTerm  int // 2C
	ConflictIndex int // 2C
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteRequest %v and reply requestVotereply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, reply)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = request.Term, -1
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = request.CandidateId
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.StartElection()
			rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(HeartbeatInterval)
			}
			rf.mu.Unlock()
		}

	}
}
func (rf *Raft) ChangeState(s NodeState) {
	if s == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n",
		rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop()
		resetTimer(rf.electionTimer,
			randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
		rf.votedFor = -1

	case Candidate:
		rf.startElection()

	case Leader:
		for i := range rf.nextIndex {
			// initialized to leader last log index + 1
			rf.nextIndex[i] = rf.getAbsoluteLogIndex(len(rf.logs))
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = rf.snapshottedIndex
		}

		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		resetTimer(rf.heartbeatTimer, HeartbeatInterval)
	}
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied {
		DPrintf("%v apply from index %d to %d", rf, rf.lastApplied+1, rf.commitIndex)
		entriesToApply := append([]LogEntry{},
			rf.logs[rf.getRelativeLogIndex(rf.lastApplied+1):rf.getRelativeLogIndex(rf.commitIndex+1)]...)

		go func(startIdx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = startIdx + idx
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // execute before rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { //If RPC request or response contains term T > currentTerm:
		rf.currentTerm = args.Term  //set currentTerm = T, convert to follower (§5.1)
		rf.ChangeState(Follower)
		// do not return here.
	}
	resetTimer(rf.electionTimer,
		randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //conflict with leader
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
		index := args.PrevLogIndex-1;
		for index>=1 && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
			index--
			reply.ConflictIndex = index
		}
		return
	}

	unmatch_idx := -1
	for idx := range args.LogEntries { // append the entry
		if len(rf.logs) < rf.getRelativeLogIndex(args.PrevLogIndex+2+idx) ||
			rf.logs[rf.getRelativeLogIndex(args.PrevLogIndex+1+idx)].Term != args.LogEntries[idx].Term {
			// unmatch log found
			unmatch_idx = idx
			break
		}
	}
	if unmatch_idx!= -1 { // locate the first unmatched log and copy from the leader from there
		rf.logs = rf.logs[:rf.getRelativeLogIndex(args.PrevLogIndex+1+unmatch_idx)]
		rf.logs = append(rf.logs, args.LogEntries[unmatch_idx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		absoluteLastLogIndex := rf.getAbsoluteLogIndex(len(rf.logs) - 1)
		if args.LeaderCommit <= absoluteLastLogIndex {
			rf.setCommitIndex(args.LeaderCommit)
		} else {
			rf.setCommitIndex(absoluteLastLogIndex)
		}
	}

	reply.Success = true

}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1

			if prevLogIndex < rf.snapshottedIndex {
				// leader has discarded log entries the follower needs
				// send snapshot to follower and retry later
				rf.mu.Unlock()
				rf.syncSnapshotWith(server)
				return
			}
			// use deep copy to avoid race condition
			// when override log in AppendEntries()
			entries := make([]LogEntry, len(rf.logs[rf.getRelativeLogIndex(prevLogIndex+1):]))
			copy(entries, rf.logs[rf.getRelativeLogIndex(prevLogIndex+1):])

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.logs[rf.getRelativeLogIndex(prevLogIndex)].Term,
				LogEntries:   entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					// successfully replicated args.LogEntries
					rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1

					// check if we need to update commitIndex
					// from the last log entry to committed one
					for i := rf.getAbsoluteLogIndex(len(rf.logs) - 1); i > rf.commitIndex; i-- {
						count := 0
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= i {
								count += 1
							}
						}

						if count > len(rf.peers)/2 {
							// most of nodes agreed on rf.logs[i]
							rf.setCommitIndex(i)
							break
						}
					}

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					} else {
						// log unmatch, update nextIndex[server] for the next trial
						rf.nextIndex[server] = reply.ConflictIndex

						// if term found, override it to
						// the first entry after entries in ConflictTerm
						if reply.ConflictTerm != -1 {
							DPrintf("%v conflict with server %d, prevLogIndex %d, log length = %d",
								rf, server, args.PrevLogIndex, len(rf.logs))
							for i := args.PrevLogIndex; i >= rf.snapshottedIndex+1; i-- {
								if rf.logs[rf.getRelativeLogIndex(i-1)].Term == reply.ConflictTerm {
									// in next trial, check if log entries in ConflictTerm matches
									rf.nextIndex[server] = i
									break
								}
							}
						}

						// TODO: retry now or in next RPC?
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}
func (rf *Raft) getRelativeLogIndex(index int) int {
	// index of rf.logs
	return index - rf.snapshottedIndex
}

func (rf *Raft) getAbsoluteLogIndex(index int) int {
	// index of log including snapshotted ones
	return index + rf.snapshottedIndex
}

func (rf *Raft) StartElection() {
	request := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getAbsoluteLogIndex(lastLogIndex),
		LastLogTerm:  rf.logs[lastLogIndex].Term,
	}
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	// use Closure
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVotereply)
			if rf.sendRequestVote(peer, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVotereply %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, reply, peer, request, rf.currentTerm)
				if rf.currentTerm == request.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
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
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          Follower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper)),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}

func randTimeDuration(lower, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C: //try to drain from the channel
		default:
		}
	}
	timer.Reset(d)
}