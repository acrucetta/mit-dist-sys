package raft

/*
Logs
- Add greppable logs in parts of the process

StartElection:
- No election timer reset when starting new election
*/

/*
Raft Properties:
- Election Safety: at most one leader can be elected in a given term. §5.2
- Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries. §5.3
- Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index. §5.3
- Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms. §5.4
- State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index. §5.4.3
*/

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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type PeerState int

const (
	Leader PeerState = iota
	Candidate
	Follower
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	logs        []LogEntry
	state       PeerState

	// volatile state on all
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// channels
	heartbeatCh chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Logs         []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[Node: %d][Term: %d] AppendEntries called with args: %+v", rf.me, rf.currentTerm, args)

	// If we're outdated, step down from candidate/leader and reset voting status
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// If its just an empty message, update the heartbeats channel.
	if len(args.Logs) == 0 {
		DPrintf("[Node: %d][Term: %d] Received a heartbeat.", rf.me, rf.currentTerm)
		rf.heartbeatCh <- struct{}{}
		return
	}

	// False if the requester's term is less than the current term
	currTerm, _ := rf.GetState()
	if args.Term < currTerm {
		DPrintf("[Node: %d][Term: %d] AppendEntries failed: args.Term (%d) < currTerm (%d)", rf.me, rf.currentTerm, args.Term, currTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// False if the log index is more than the one in the current peer
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// False if log doesn't contain an entry at prevLogIndex whose term
	// matches prevLogTerm
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[Node: %d][Term: %d] AppendEntries failed: log term at prevLogIndex (%d) does not match prevLogTerm (%d)", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Append any new entries from leader log not already in the follower's log
	// Iterate the two logs until one of the entries doesn't agree, then delete all
	// subsequent entries
	for i, entry := range rf.logs {
		// Found mismatch; delete all logs after i in the follower
		// replace them with the leader's logs
		if i >= len(args.Logs) || entry != args.Logs[i] {
			DPrintf("[Node: %d][Term: %d] AppendEntries: log mismatch at index %d, replacing follower logs with leader logs", rf.me, rf.currentTerm, i)
			rf.logs = rf.logs[:i]
			rf.logs = append(rf.logs, args.Logs[i:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
		DPrintf("[Node: %d][Term: %d] AppendEntries: updated commitIndex to %d", rf.me, rf.currentTerm, rf.commitIndex)
	}

	reply.Term = args.Term
	reply.Success = true

	DPrintf("[Node: %d][Term: %d] AppendEntries succeeded: reply: %+v", rf.me, rf.currentTerm, reply)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[Node: %d][Term: %d] Node %d is being requested a vote", rf.me, rf.currentTerm, rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A, 3B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If we're outdated, step down from candidate/leader and reset voting status
	if args.Term > rf.currentTerm {
		DPrintf("\t[Node: %d][Term: %d] Node %d did not grant a vote to %d: request term (%d) more than curr term (%d), becoming follower\n", rf.me, rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	currTerm, _ := rf.GetState()
	if args.Term < currTerm {
		DPrintf("\t[Node: %d][Term: %d] Node %d did not grant a vote to %d: request term (%d) less than curr term (%d)\n", rf.me, rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// Have que voted for someone else?
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("\t[Node: %d][Term: %d] Node %d voted alredy, did not grant a vote to %d\n", rf.me, rf.currentTerm, rf.me, args.CandidateId)
		return
	}

	// If votedFor is null or candidateId, and candidate's log is at
	// least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	// Check candidate's log
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term

	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		DPrintf("[Node: %d][Term: %d] Node %d rejected vote: candidate log not up to date",
			rf.me, rf.currentTerm, rf.me)
		return
	}

	DPrintf("[Node: %d][Term: %d] Node %d granting vote to %d", rf.me, rf.currentTerm, rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Function to send periodical heartbeats to all followers of the
// leader node.
func (rf *Raft) sendHeartbeats() {
	// Every certain amount of time, send a heartbeat to all the followers of
	// this particular node
	for rf.killed() == false {
		for peer := range rf.peers {
			go func(peer int) {
				DPrintf("%d becoming leader, sending heartbeats to: %d", rf.me, peer)
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peer, &args, &reply)
			}(peer)
		}
		time.Sleep(time.Millisecond * 150)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// If the current peer hasn't heard from the leader within
		// a certain period of time, run the election.
		timeout := time.After(RandomizedDuration())
		select {
		case <-rf.heartbeatCh:
			DPrintf("[Node: %d][Term: %d] Received heartbeat, resetting timeout", rf.me, rf.currentTerm)
			timeout = time.After(RandomizedDuration())
		case <-timeout:
			rf.mu.Lock()
			if rf.state != Leader {
				DPrintf("[Node: %d][Term: %d] Election timeout, starting election", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.StartElection()
			} else {
				rf.mu.Unlock()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func RandomizedDuration() time.Duration {
	// random amount of time between 500 and 1000ms
	randomDuration := 150 + (rand.Int63() % 300)
	return time.Duration(randomDuration) * time.Millisecond
}

// Base function for the peer to become leader.
func (rf *Raft) BecomeLeader() {
	/*
		Once a candidate wins an election, it becomes leader.
		It then sends heartbeat messages to all of the other
		servers to establish its authority and prevent new elections.
	*/

	rf.mu.Lock()
	DPrintf("Node %d is becoming the leader", rf.me)
	rf.state = Leader
	rf.votedFor = -1
	lenLogs := len(rf.logs)
	for peer := range rf.peers {
		if peer != rf.me {
			rf.nextIndex[peer] = lenLogs
			rf.matchIndex[peer] = 0
		}
	}
	rf.mu.Unlock()

	go rf.sendHeartbeats()
}

func (rf *Raft) StartElection() {
	DPrintf("Starting elections...")
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	// Reset election timer
	go func() {
		rf.heartbeatCh <- struct{}{}
	}()

	// Election tasks:
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	// 5. Request votes from all peers
	// 5.1 Issue RequestVote RPCs in parallel to
	// the other servers
	// 5.2 We continue in this state until
	// we win, someone else wins, time runs out
	votes := make(chan bool, len(rf.peers))
	if rf.killed() == false {
		DPrintf("[Node: %d][Term: %d] Node %d is requesting votes...", rf.me, rf.currentTerm, rf.me)
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func(peer int) {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				}
				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Candidate {
						return
					}
					if reply.VoteGranted {
						DPrintf("[Node: %d][Term: %d] Node %d was granted a vote...", rf.me, rf.currentTerm, rf.me)
						votes <- true
						// If we didn't receive a vote, check if we're outdated
						// if so, transition to Follower state.
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						return
					}
				}
			}(peer)
		}

		DPrintf("[Node: %d][Term: %d] Node %d is counting votes...", rf.me, rf.currentTerm, rf.me)
		tally := 1 // self vote
		needed := (len(rf.peers) / 2) + 1
		for i := 0; i < len(rf.peers)-1; i++ {
			vote := <-votes
			if vote {
				tally++
				if tally >= needed {
					DPrintf("[Node: %d][Term: %d] Node %d found the votes neeeded...", rf.me, rf.currentTerm, rf.me)
					break
				}
			}
			DPrintf("%d is counting...", rf.me)
		}
		DPrintf("[Node: %d][Term: %d] Node %d is closing the channel", rf.me, rf.currentTerm, rf.me)
		close(votes)

		rf.mu.Lock()
		if tally >= needed && rf.state != Leader {
			DPrintf("[Node: %d][Term: %d] Node %d is becoming a leader...", rf.me, rf.currentTerm, rf.me)
			rf.BecomeLeader()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.state = Follower

	rf.commitIndex = 1
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.heartbeatCh = make(chan struct{}, 1)

	// 3A Task: Modify Make() to create a background goroutine that will
	// kick off leader election periodically by sending out RequestVote
	// RPCs when it hasn't heard from another peer for a while

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
