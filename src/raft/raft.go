package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

// Constants for better readability
const (
	ELECTION_TIMEOUT_MIN = 300 // milliseconds
	ELECTION_TIMEOUT_MAX = 600 // milliseconds
	HEARTBEAT_INTERVAL   = 100 // milliseconds
)

// Debug colors for logging
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
)

/*
Raft Properties:
- Election Safety: at most one leader can be elected in a given term. §5.2
- Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3
- Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
- Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
- State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3
*/

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// Helper function to get state color
func getStateColor(state ServerState) string {
	switch state {
	case Leader:
		return colorGreen
	case Candidate:
		return colorYellow
	case Follower:
		return colorBlue
	default:
		return colorReset
	}
}

func (s ServerState) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Unknown"
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       ServerState

	// Volatile state
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  []int
	matchIndex []int

	// Channels for coordination
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
}

// -- Helper Functions --

func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
	timeout := ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)
	rf.electionTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) becomeFollower(term int) {
	DPrintf("%s[%s][Node %d][Term %d] Converting to Follower (new term: %d)%s",
		getStateColor(rf.state), rf.state, rf.me, rf.currentTerm, term, colorReset)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	DPrintf("%s[%s][Node %d][Term %d] Converting to Candidate%s",
		getStateColor(rf.state), rf.state, rf.me, rf.currentTerm, colorReset)
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}

	DPrintf("%s[%s][Node %d][Term %d] Converting to Leader%s",
		colorGreen, rf.state, rf.me, rf.currentTerm, colorReset)

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	go rf.sendHeartbeats()
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (3C).
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// -- RPC Handlers --

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s[%s][Node %d][Term %d] Received vote request from Node %d (Term: %d)%s",
		getStateColor(rf.state), rf.state, rf.me, rf.currentTerm, args.CandidateId, args.Term, colorReset)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Stepping down: request term (%d) > current term (%d)%s",
			colorRed, rf.state, rf.me, rf.currentTerm, args.Term, rf.currentTerm, colorReset)
		rf.becomeFollower(args.Term)
	}

	canVote := args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId)

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if canVote && logIsUpToDate {
		DPrintf("%s[%s][Node %d][Term %d] Granting vote to Node %d%s",
			colorGreen, rf.state, rf.me, rf.currentTerm, args.CandidateId, colorReset)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	} else {
		DPrintf("%s[%s][Node %d][Term %d] Rejecting vote request from Node %d (canVote: %v, logUpToDate: %v)%s",
			colorRed, rf.state, rf.me, rf.currentTerm, args.CandidateId, canVote, logIsUpToDate, colorReset)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%s[%s][Node %d][Term %d] Received AppendEntries from Leader %d (Term: %d)%s",
		getStateColor(rf.state), rf.state, rf.me, rf.currentTerm, args.LeaderId, args.Term, colorReset)

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Rejected AppendEntries: term too old%s",
			colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Converting to follower: leader term (%d) > current term (%d)%s",
			colorYellow, rf.state, rf.me, rf.currentTerm, args.Term, rf.currentTerm, colorReset)
		rf.becomeFollower(args.Term)
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%s[%s][Node %d][Term %d] Log consistency check failed%s",
			colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		return
	}

	if len(args.Entries) > 0 {
		DPrintf("%s[%s][Node %d][Term %d] Appending %d entries to log%s",
			colorCyan, rf.state, rf.me, rf.currentTerm, len(args.Entries), colorReset)
		newEntries := args.Entries
		rf.log = append(rf.log[:args.PrevLogIndex+1], newEntries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("%s[%s][Node %d][Term %d] Updated commitIndex: %d -> %d%s",
			colorCyan, rf.state, rf.me, rf.currentTerm, oldCommitIndex, rf.commitIndex, colorReset)
	}

	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Function to send periodical heartbeats
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		DPrintf("%s[%s][Node %d][Term %d] Sending heartbeats to all peers%s",
			colorCyan, rf.state, rf.me, rf.currentTerm, colorReset)

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
				Entries:      rf.log[rf.nextIndex[peer]:],
				LeaderCommit: rf.commitIndex,
			}

			go func(peer int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.Term > rf.currentTerm {
						DPrintf("%s[%s][Node %d][Term %d] Stepping down: peer %d has higher term %d%s",
							colorRed, rf.state, rf.me, rf.currentTerm, peer, reply.Term, colorReset)
						rf.becomeFollower(reply.Term)
						return
					}

					if reply.Success {
						rf.nextIndex[peer] = len(rf.log)
						rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						DPrintf("%s[%s][Node %d][Term %d] Successfully updated peer %d (nextIndex: %d)%s",
							colorGreen, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)
					} else {
						rf.nextIndex[peer]--
						DPrintf("%s[%s][Node %d][Term %d] Failed to update peer %d, decreasing nextIndex to %d%s",
							colorYellow, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)
					}
				}
			}(peer, args)
		}
		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()

	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// -- Leader Election --

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.becomeCandidate()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	DPrintf("%s[%s][Node %d][Term %d] Starting election%s",
		colorYellow, rf.state, rf.me, rf.currentTerm, colorReset)

	rf.mu.Unlock()

	votes := 1 // Vote for self
	needed := len(rf.peers)/2 + 1
	responses := make(chan bool, len(rf.peers)-1)

	timeout := time.After(time.Duration(ELECTION_TIMEOUT_MAX) * time.Millisecond)

	// Send vote request to all the peers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("%s[%s][Node %d][Term %d] Discovered higher term from peer %d, stepping down%s",
						colorRed, rf.state, rf.me, rf.currentTerm, peer, colorReset)
					rf.becomeFollower(reply.Term)
					rf.mu.Unlock()
					responses <- false
					return
				}
				rf.mu.Unlock()
				responses <- reply.VoteGranted
				if reply.VoteGranted {
					DPrintf("%s[%s][Node %d][Term %d] Received vote from peer %d%s",
						colorGreen, rf.state, rf.me, rf.currentTerm, peer, colorReset)
				}
			} else {
				responses <- false
			}
		}(peer)
	}

	// Count the votes
	go func() {
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case voteGranted := <-responses:
				if voteGranted {
					votes++
					if votes >= needed {
						rf.mu.Lock()
						if rf.state == Candidate && rf.currentTerm == args.Term {
							DPrintf("%s[%s][Node %d][Term %d] Won election with %d/%d votes%s",
								colorGreen, rf.state, rf.me, rf.currentTerm, votes, len(rf.peers), colorReset)
							rf.becomeLeader()
						}
						rf.mu.Unlock()
						return
					}
				}
			case <-timeout:
				rf.mu.Lock()
				if rf.state == Candidate && rf.currentTerm == args.Term {
					DPrintf("%s[%s][Node %d][Term %d] Election timeout. Starting new election",
						colorGreen, rf.state, rf.me, rf.currentTerm)
					// rf.currentTerm++
					// go rf.startElection()
				}
				rf.mu.Unlock()
				return
			}
		}
		DPrintf("%s[%s][Node %d][Term %d] Lost election: only got %d/%d votes%s",
			colorRed, rf.state, rf.me, rf.currentTerm, votes, len(rf.peers), colorReset)
	}()
}

// The ticker go routine starts a new election if this peer hasnTest (3A): election after network failure ...'t received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.resetElectionTimer()
				rf.mu.Unlock()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		applyCh:       applyCh,
		currentTerm:   0,
		votedFor:      -1,
		log:           []LogEntry{{Term: 0}}, // Initialize with dummy entry
		state:         Follower,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		electionTimer: time.NewTimer(time.Duration(ELECTION_TIMEOUT_MIN) * time.Millisecond),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.resetElectionTimer()
	go rf.ticker()

	return rf
}
