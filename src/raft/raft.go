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
TODO:
- Start by implementing Start(), then write the code to send and receive new log
entries via AppendEntries RPCs, following Figure 2.
- You will need to implement the election restriction (section 5.4.1 in the paper).
*/

/*
References:
- https://thesquareplanet.com/blog/students-guide-to-raft/#the-importance-of-details
- https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections/
- https://notes.eatonphil.com/2023-05-25-raft.html
*/

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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       ServerState

	// Volatile state on all servers
	// Highest log entry known to be committed
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	// For each server, index of the next log entry to send to that server
	nextIndex []int
	// For each server, index of highest log entry known to
	// be replicated on server
	matchIndex []int

	// Channels for coordination
	electionTimer     *time.Timer
	lastElectionReset time.Time
	applyCh           chan ApplyMsg
}

// -- Helper Functions --

func (rf *Raft) resetElectionTimer() {
	if rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	timeout := ELECTION_TIMEOUT_MIN + rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)
	rf.electionTimer.Reset(time.Duration(timeout) * time.Millisecond)
	rf.lastElectionReset = time.Now()
	DPrintf("Node %d resetting election timer with timeout %dms", rf.me, timeout)
}

func (rf *Raft) logTimerState() {
	elapsed := time.Since(rf.lastElectionReset).Milliseconds()
	DPrintf("%sNode %d timer elapsed: %dms since last reset%s", colorCyan, rf.me, elapsed, colorReset)
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

	// Candidates (§5.2):
	// On conversion to candidate, start election:
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	DPrintf("%s[%s][Node %d][Term %d] Converting to Leader%s",
		colorGreen, rf.state, rf.me, rf.currentTerm, colorReset)

	rf.state = Leader
	rf.resetElectionTimer()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
		select {
		case <-rf.electionTimer.C:
		default:
		}
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

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Rejected AppendEntries: term too old%s",
			colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		return
	}

	//  If commitIndex > lastApplied: increment lastApplied,
	//  apply log[lastApplied] to state machine (§5.3)
	// TODO: Impelement this logic.

	// If RPC request or response contains term T > currentTerm: set
	// currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Converting to follower: leader term (%d) > current term (%d)%s",
			colorYellow, rf.state, rf.me, rf.currentTerm, args.Term, rf.currentTerm, colorReset)
		rf.becomeFollower(args.Term)
	}

	canVote := args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId)

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// If votedFor is null or candidateId, and candidates log is at
	// least as up-to-date as receivers log, grant vote (§5.2, §5.4)
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

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Rejected AppendEntries: term too old%s",
			colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		return
	}

	rf.resetElectionTimer()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		DPrintf("%s[%s][Node %d][Term %d] Converting to follower: leader term (%d) > current term (%d)%s",
			colorYellow, rf.state, rf.me, rf.currentTerm, args.Term, rf.currentTerm, colorReset)
		rf.becomeFollower(args.Term)
	}

	// Reply false if log doesnt contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%s[%s][Node %d][Term %d] Log consistency check failed%s",
			colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		return
	}

	// 1. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 2. Append any new entries not already in the log
	if len(args.Entries) > 0 {
		DPrintf("%s[%s][Node %d][Term %d] Appending %d entries to log%s",
			colorCyan, rf.state, rf.me, rf.currentTerm, len(args.Entries), colorReset)
		newEntries := args.Entries
		rf.log = append(rf.log[:args.PrevLogIndex+1], newEntries...)
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
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

func (rf *Raft) updateCommitIndex() {
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}

		count := 1
		for peer := range rf.peers {
			if rf.me != peer && rf.matchIndex[peer] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			return
		}

	}
}

// Function to replicate log entries
func (rf *Raft) replicateLog() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	DPrintf("%s[%s][Node %d][Term %d] Replicating log entries...%s",
		colorCyan, rf.state, rf.me, rf.currentTerm, colorReset)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			// AppendEntries RPC with log entries starting at nextIndex
			entries := []LogEntry{}
			ni := rf.nextIndex[peer]

			if len(rf.log) > ni {
				entries = rf.log[ni:]
			}

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: ni - 1,
				PrevLogTerm:  rf.log[ni-1].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok || rf.state != Leader {
				DPrintf("%s[%s][Node %d][Term %d] Failed to send append entry to the peer %d, retrying...%s",
					colorRed, rf.state, rf.me, rf.currentTerm, peer, colorReset)
				return
			}

			if reply.Term > rf.currentTerm {
				DPrintf("%s[%s][Node %d][Term %d] Stepping down: peer %d has higher term %d%s",
					colorRed, rf.state, rf.me, rf.currentTerm, peer, reply.Term, colorReset)
				rf.becomeFollower(reply.Term)
				return
			}

			// If successful: update nextIndex and matchIndex for follower (§5.3)
			if reply.Success {
				rf.nextIndex[peer] = len(rf.log)
				rf.matchIndex[peer] = rf.nextIndex[peer] - 1
				DPrintf("%s[%s][Node %d][Term %d] Successfully updated peer %d (nextIndex: %d)%s",
					colorGreen, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)

				rf.updateCommitIndex()

				// If AppendEntries fails because of log inconsistency:
				// decrement nextIndex and retry (§5.3)
			} else {
				rf.nextIndex[peer]--
				DPrintf("%s[%s][Node %d][Term %d] Failed to update peer %d, decreasing nextIndex to %d%s",
					colorYellow, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)
			}
		}(peer)
	}
	rf.mu.Unlock()
}

// Function to send periodical heartbeats with no log entries
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		DPrintf("%s[%s][Node %d][Term %d] Sending heartbeats to all peers%s",
			colorCyan, rf.state, rf.me, rf.currentTerm, colorReset)
		rf.logTimerState()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
			// repeat during idle periods to prevent election timeouts (§5.2)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[peer]-1].Term,
				Entries:      []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}

			go func(peer int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !ok {
					DPrintf("%s[%s][Node %d][Term %d] Failed to send a heartbeat to the peer %d, retrying...%s",
						colorRed, rf.state, rf.me, rf.currentTerm, peer, colorReset)
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("%s[%s][Node %d][Term %d] Stepping down: peer %d has higher term %d%s",
						colorRed, rf.state, rf.me, rf.currentTerm, peer, reply.Term, colorReset)
					rf.becomeFollower(reply.Term)
					return
				}

				// If successful: update nextIndex and matchIndex for follower (§5.3)
				if reply.Success {
					rf.nextIndex[peer] = len(rf.log)
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1
					DPrintf("%s[%s][Node %d][Term %d] Successfully updated peer %d (nextIndex: %d)%s",
						colorGreen, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)

					// If AppendEntries fails because of log inconsistency:
					// decrement nextIndex and retry (§5.3)
				} else {
					rf.nextIndex[peer]--
					DPrintf("%s[%s][Node %d][Term %d] Failed to update peer %d, decreasing nextIndex to %d%s",
						colorYellow, rf.state, rf.me, rf.currentTerm, peer, rf.nextIndex[peer], colorReset)
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

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	index := len(rf.log)
	rf.log = append(rf.log, entry)
	rf.persist()

	DPrintf("%s[%s][Node %d][Term %d] Received new command at index %d%s",
		colorGreen, rf.state, rf.me, rf.currentTerm, index, colorReset)

	go rf.replicateLog()

	return index, rf.currentTerm, true
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

	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	votes := 1 // Vote for self
	voteMutex := sync.Mutex{}
	finished := false
	needed := len(rf.peers)/2 + 1

	// Send vote request to all the peers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if finished || rf.state != Candidate || rf.currentTerm != currentTerm {
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("%s[%s][Node %d][Term %d] Discovered higher term from peer %d, stepping down%s",
						colorRed, rf.state, rf.me, rf.currentTerm, peer, colorReset)
					rf.becomeFollower(reply.Term)
					finished = true
					return
				}

				if reply.VoteGranted {
					voteMutex.Lock()
					votes++
					currVotes := votes
					voteMutex.Unlock()
					if currVotes >= needed && !finished {
						finished = true
						rf.becomeLeader()
					}
				}
			}
		}(peer)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimer.C
		rf.mu.Lock()
		rf.resetElectionTimer()
		if rf.state != Leader {
			DPrintf("%s[%s][Node %d][Term %d] Starting a new election... %s",
				colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
			go rf.startElection()
			DPrintf("%s[%s][Node %d][Term %d] Resetting the election timer... %s",
				colorRed, rf.state, rf.me, rf.currentTerm, colorReset)
		}
		rf.mu.Unlock()
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

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				entry := rf.log[rf.lastApplied]
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied,
				}
				DPrintf("%s[%s][Node %d][Term %d] Applying command at index %d%s",
					getStateColor(rf.state), rf.state, rf.me, rf.currentTerm, rf.lastApplied, colorReset)

				rf.mu.Unlock()
				rf.applyCh <- applyMsg
				rf.mu.Lock()
			}

			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond) // Prevent tight loop
		}
	}()
	return rf
}
