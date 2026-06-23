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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg
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

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	applyCh    chan ApplyMsg
	commitCond *sync.Cond
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm       int
	votedFor          int
	log               []logEntry
	lastIncludedIndex int
	lastIncludedTerm  int

	state       State
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeard       time.Time
	electionTimeout time.Duration

	leaderTimeout time.Duration
}

type logEntry struct {
	Term    int
	Command interface{}
}

type State string

const (
	StateCandidate = "candidate"
	StateFollower  = "follower"
	StateLeader    = "leader"
)

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) + rf.lastIncludedIndex - 1
}

func (rf *Raft) lastLogTerm() int {
	if rf.lastLogIndex() < 1 {
		return 0
	}
	return rf.log[rf.localLogIndex(rf.lastLogIndex())].Term
}

func (rf *Raft) globalLogIndex(localLogIndex int) int {
	return rf.lastIncludedIndex + localLogIndex
}

func (rf *Raft) localLogIndex(globalLogIndex int) int {
	return globalLogIndex - rf.lastIncludedIndex
}

func (rf *Raft) resetElectionTimeout() {
	ms := 500 + (rand.Int63() % 500)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == StateLeader
	term = rf.currentTerm
	return term, isleader
}

// persist
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

// readPersist
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []logEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	localLogIndex := rf.localLogIndex(index)
	if index <= rf.lastIncludedIndex || localLogIndex <= 0 || index > rf.lastApplied || localLogIndex >= len(rf.log) {
		return
	}
	localLogTerm := rf.log[localLogIndex].Term
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = localLogTerm
	newLog := make([]logEntry, 1)
	newLog[0] = logEntry{
		Term: localLogTerm,
	}
	newLog = append(newLog, rf.log[localLogIndex+1:]...)
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = localLogTerm
	rf.persistSnapshot(snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	} else if rf.state != StateFollower {
		rf.changeToFollower(args.Term)
	}

	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	newLog := make([]logEntry, 1)
	newLog[0] = logEntry{
		Term: args.LastIncludedTerm,
	}

	if args.LastIncludedIndex <= rf.lastLogIndex() {
		localIndex := rf.localLogIndex(args.LastIncludedIndex)

		if localIndex >= 0 && localIndex < len(rf.log) && rf.log[localIndex].Term == args.LastIncludedTerm {
			newLog = append(newLog, rf.log[localIndex+1:]...)
		}
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	snapshot := append([]byte(nil), args.Data...)
	rf.persistSnapshot(snapshot)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.commitCond.Broadcast()

	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	}
	if (rf.votedFor == -1 || (rf.votedFor == args.CandidateId)) && rf.rfUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.lastHeard = time.Now()
		rf.resetElectionTimeout()
		reply.Term, reply.VoteGranted = rf.currentTerm, true
		return
	}
	reply.Term, reply.VoteGranted = rf.currentTerm, false
}

func (rf *Raft) rfUpToDate(index int, term int) bool {
	lastLogTerm := rf.lastLogTerm()
	lastLogIndex := rf.lastLogIndex()

	if term != lastLogTerm {
		return term > lastLogTerm
	}

	return index >= lastLogIndex
}

// sendRequestVote
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.changeToFollower(args.Term)
	} else if rf.state != StateFollower {
		rf.changeToFollower(args.Term)
	}

	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.XTerm, reply.XLen = -1, rf.lastLogIndex()+1
		return
	}

	index := rf.localLogIndex(args.PrevLogIndex)
	if rf.log[index].Term != args.PrevLogTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.XTerm = rf.log[index].Term
		for index > 0 && rf.log[index-1].Term == reply.XTerm {
			index--
		}
		reply.XIndex = rf.globalLogIndex(index)
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if insertIndex+i <= rf.lastLogIndex() && rf.log[rf.localLogIndex(insertIndex+i)].Term != entry.Term {
			rf.log = rf.log[:rf.localLogIndex(insertIndex+i)] // 发生冲突，截断
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		} else if insertIndex+i > rf.lastLogIndex() {
			// 直接将超出的部分追加
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minval(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.commitCond.Broadcast()
	}

	reply.Term, reply.Success = rf.currentTerm, true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadCastAppendEntries() {
	rf.mu.Lock()
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for server := 0; server < len(rf.peers); server++ {
		x := server
		if x == rf.me {
			continue
		}
		go rf.broadCastOrReplicate(x)
	}
}

func (rf *Raft) broadCastOrReplicate(server int) {
	rf.mu.Lock()

	if rf.state != StateLeader {
		rf.mu.Unlock()
		return
	}

	term := rf.currentTerm
	leaderId := rf.me
	nextIndex := rf.nextIndex[server]

	if nextIndex > rf.lastLogIndex()+1 {
		nextIndex = rf.lastLogIndex() + 1
		rf.nextIndex[server] = nextIndex
	}

	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(server, args, reply)
		if !ok {
			return
		}
		rf.handleInstallSnapshotReply(server, args, reply)
		return
	}

	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[rf.localLogIndex(prevLogIndex)].Term
	entries := append([]logEntry(nil), rf.log[rf.localLogIndex(nextIndex):]...)
	leaderCommit := rf.commitIndex

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}
	rf.handleAppendEntriesReply(server, args, reply)
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	originalTerm := args.Term
	replyTerm := reply.Term
	rf.persist()

	if replyTerm > currentTerm {
		rf.changeToFollower(reply.Term)
		return
	}

	if currentTerm != originalTerm || rf.state != StateLeader {
		return
	}

	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)

		if matchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = matchIndex
			rf.nextIndex[server] = matchIndex + 1
		}

		rf.commit()
		return
	}

	if rf.nextIndex[server] != args.PrevLogIndex+1 {
		return
	}
	if reply.XTerm == -1 {
		rf.nextIndex[server] = reply.XLen
	} else {
		last := -1
		for i := rf.localLogIndex(rf.lastLogIndex()); i >= 1; i-- {
			if rf.log[i].Term == reply.XTerm {
				last = i
				break
			}
		}
		if last >= 0 {
			rf.nextIndex[server] = rf.globalLogIndex(last + 1)
		} else {
			rf.nextIndex[server] = reply.XIndex
		}
	}
	minNextIndex := rf.lastIncludedIndex
	if minNextIndex < 1 {
		minNextIndex = 1
	}

	if rf.nextIndex[server] < minNextIndex {
		rf.nextIndex[server] = minNextIndex
	}
}

func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.changeToFollower(reply.Term)
		return
	}

	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}

	if rf.matchIndex[server] < rf.lastIncludedIndex {
		rf.matchIndex[server] = rf.lastIncludedIndex
	}

	if rf.nextIndex[server] < rf.lastIncludedIndex+1 {
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
	}
}

func (rf *Raft) commit() {
	for N := rf.lastLogIndex(); N > rf.commitIndex; N-- {
		if rf.log[rf.localLogIndex(N)].Term != rf.currentTerm {
			continue
		}

		count := 1
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.commitCond.Broadcast()
			return
		}
	}
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
	rf.mu.Lock()
	// Your code here (2B).
	if rf.state != StateLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	logEntry := logEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.lastLogTerm()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	rf.broadCastAppendEntries()
	return index, term, true
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
	rf.mu.Lock()
	rf.commitCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	// startElection() is invoked by ticker without holding the lock.
	// The server state may have changed before the election actually starts,
	// so the election conditions must be revalidated.
	ok := (rf.state != StateLeader) && (time.Since(rf.lastHeard) > rf.electionTimeout)
	if rf.killed() || !ok {
		rf.mu.Unlock()
		return
	}
	rf.changeToCandidate()
	term := rf.currentTerm
	candidateid := rf.me
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  candidateid,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	votes := 1
	rf.mu.Unlock()

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		x := server
		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(x, &args, reply)
			if !ok {
				return
			}
			if rf.handleRequestVoteReply(args, reply, &votes) {
				rf.broadCastAppendEntries()
			}
		}()
	}
}
func (rf *Raft) handleRequestVoteReply(args RequestVoteArgs, reply *RequestVoteReply, votes *int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	originTerm := args.Term
	currentTerm := rf.currentTerm
	replyTerm := reply.Term

	if replyTerm > currentTerm {
		rf.changeToFollower(replyTerm)
		return false
	}
	// replyTerm currentTerm相等或小于
	// originTerm != currentTerm这证明你改过了
	if originTerm != currentTerm || rf.state != StateCandidate {
		return false
	}

	if !reply.VoteGranted {
		return false
	}
	*votes++

	if *votes > len(rf.peers)/2 {
		rf.changeToLeader()
		return true
	}
	return false
}

func (rf *Raft) changeToLeader() {
	rf.state = StateLeader
	rf.lastHeard = time.Now()

	next := rf.lastLogIndex() + 1

	for server := 0; server < len(rf.peers); server++ {
		rf.nextIndex[server] = next
		rf.matchIndex[server] = 0
	}

	rf.nextIndex[rf.me] = next
	rf.matchIndex[rf.me] = next - 1

}

func (rf *Raft) changeToFollower(term int) {
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = term
		rf.persist()
	}
	rf.state = StateFollower
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
}

// changeToCandidate transitions this server to the Candidate state.
// It advances to a new term and votes for itself.
// Since the term changes, the election timer must be reset by updating
// the last-heard timestamp and generating a new election timeout.
func (rf *Raft) changeToCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.state = StateCandidate
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
}

func (rf *Raft) leaderTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == StateLeader {
			rf.broadCastAppendEntries()
		}
		time.Sleep(rf.leaderTimeout)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// If this server is not the leader and the election timeout elapses, start a new election.
		ok := (rf.state != StateLeader) && (time.Since(rf.lastHeard) > rf.electionTimeout)
		rf.mu.Unlock()
		if ok {
			rf.startElection()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 没有新提交的日志就等待
		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.commitCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		// 拷贝需要 apply 的日志
		var msgs []ApplyMsg
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.localLogIndex(i)].Command,
				CommandIndex: i,
			})
			rf.lastApplied = i
		}
		rf.mu.Unlock()

		// 在锁外发送，避免阻塞
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// Make
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
	rf := &Raft{
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]logEntry, 1),
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		state:             StateFollower,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
		lastHeard:         time.Now(),
		leaderTimeout:     150 * time.Millisecond,
		peers:             peers,
		persister:         persister,
		me:                me,
		applyCh:           applyCh,
	}
	rf.commitCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	rf.resetElectionTimeout()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()
	go rf.applier()

	return rf
}
