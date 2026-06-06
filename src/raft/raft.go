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
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

var debugStart = time.Now()

func (rf *Raft) dlog(format string, a ...interface{}) {
	elapsed := time.Since(debugStart).Milliseconds()

	state := "Unknown"
	if rf.isLeader {
		state = "leader"
	} else {
		state = "NonLeader"
	}

	prefix := fmt.Sprintf("[%04dms] S%d T%d %s ",
		elapsed, rf.me, rf.currentTerm, state)

	DPrintf(prefix+format, a...)
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // persistent
	votedFor    int // persistent
	// log: command and term
	// every log entry contains [command] for state machine, and [term] when entry was received by leader (first index is 1)
	// how to organize log? log[i] is the log entry with index i+1, so log[0] is the log entry with index 1
	// but how to split term and command? using a struct LogEntry?
	log         []logEntry //
	commitIndex int        // Volatile  log's index
	lastApplied int        // Volatile  log's index

	// only leaders
	// how to [qufen] leaders and other peers?
	isLeader   bool
	nextIndex  []int
	matchIndex []int

	state string // follower, candidate, leader

	electionTime time.Duration
	lastHeard    time.Time
}

type logEntry struct {
	Command string
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.isLeader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestSolution(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
		rf.isLeader = false
		ms := 200 + (rand.Int63() % 200)
		rf.electionTime = time.Duration(ms) * time.Millisecond
		rf.lastHeard = time.Now()
	}
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//rf.dlog("recv RequestVote from S%d term=%d", args.CandidateId, args.Term)
	//defer rf.dlog("finish RequestVote from S%d vote=%v", args.CandidateId, reply.VoteGranted)
	// 先看看我是谁？
	// 不管是leader/follower/candidate，对待所来的大于自己的term，都是同一个处理方法

	rf.RequestSolution(args)

	// 处理完之后，此时rf就是一个普普通通的rf
	// 要么是term更大了，变成了follower，要么就是term不变，还是原来的状态

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.votedFor == -1 || (rf.votedFor == args.CandidateId)) && rf.upToDate(args) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) upToDate(args *RequestVoteArgs) bool {

	// 判断candidate是不是比follower新
	// 前面已经判断过了,Term <= currentTerm
	if args.Term == rf.currentTerm {
		if args.LastLogIndex >= len(rf.log) {
			return true
		}
	} else if args.Term < rf.currentTerm {
		return false
	}
	return true
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendSolution(args *AppendEntriesArgs) {
	rf.dlog("before append entries, currentTerm=%d, argsTerm=%d\n", rf.currentTerm, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
		rf.isLeader = false
		ms := 200 + (rand.Int63() % 200)
		rf.electionTime = time.Duration(ms) * time.Millisecond
		rf.lastHeard = time.Now()
	} else if args.Term == rf.currentTerm {
		rf.state = "follower"
		rf.isLeader = false
		ms := 200 + (rand.Int63() % 200)
		rf.electionTime = time.Duration(ms) * time.Millisecond
		rf.lastHeard = time.Now()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.AppendSolution(args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
	}
	rf.dlog("after append entries, currentTerm=%d, argsTerm=%d\n", rf.currentTerm, args.Term)
}

// Start
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

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
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

		// Your code here (2A)
		// Check if a leader election should be started.

		_, ok := rf.GetState()

		rf.mu.Lock()
		judge := !ok && (time.Now().Sub(rf.lastHeard) > rf.electionTime)
		rf.mu.Unlock()
		if judge {
			rf.startElection()
		}

		//for !ok { // 不是leader
		//	// 如果超时，发起选举
		//	// electionTime是不是rf
		//	if time.Now().Sub(rf.lastHeard) > rf.electionTime {
		//		// 确实超时了
		//		rf.dlog("startElection\n")
		//		rf.startElection()
		//	}
		//	_, ok = rf.GetState()
		//}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 200 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// candidate
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.isLeader = false
	rf.lastHeard = time.Now()
	ms := 200 + (rand.Int63() % 200)
	rf.electionTime = time.Duration(ms) * time.Millisecond
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
	}
	if len(rf.log) == 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	count := 1
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		x := i
		if x == rf.me {
			continue
		}
		go func() {
			reply := RequestVoteReply{}
			tempTerm := args.Term
			check := rf.sendRequestVote(x, &args, &reply)
			rf.dlog("check=%v\n", check)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if check {
				if tempTerm != rf.currentTerm {
					return
				}
				if reply.VoteGranted {
					if reply.Term != rf.currentTerm {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = "follower"
							rf.isLeader = false
							ms := 200 + (rand.Int63() % 200)
							rf.electionTime = time.Duration(ms) * time.Millisecond
						}
						return
					}
					count++
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = "follower"
						rf.isLeader = false
						ms := 200 + (rand.Int63() % 200)
						rf.electionTime = time.Duration(ms) * time.Millisecond
					}
				}
			}
			rf.dlog("count=%d\n", count)
			if count > len(rf.peers)/2 {
				rf.state = "leader"
				rf.isLeader = true
				rf.dlog("selection success")
				// 当选之后需要发送心跳
				ms := 200 + (rand.Int63() % 200)
				rf.electionTime = time.Duration(ms) * time.Millisecond
				rf.broadcastHeartbeat(rf.currentTerm)
			}
		}()
	}

}
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		time.Sleep(150 * time.Millisecond)

		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			continue
		}

		term := rf.currentTerm
		rf.mu.Unlock()

		rf.broadcastHeartbeat(term)
	}
}

func (rf *Raft) broadcastHeartbeat(term int) {
	rf.dlog("broadcastHeartbeat")
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			args := AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
			}

			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = "follower"
				rf.votedFor = -1
				rf.isLeader = false
				ms := 200 + (rand.Int63() % 200)
				rf.electionTime = time.Duration(ms) * time.Millisecond
				return
			}
		}(server)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeard = time.Now()
	rf.state = "follower"
	rf.isLeader = false
	rf.log = make([]logEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, 0)
	ms := 200 + (rand.Int63() % 200)
	rf.electionTime = time.Duration(ms) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}
