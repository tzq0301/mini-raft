package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"

	"labrpc"
)

// import "bytes"
// import "encoding/gob"

type state int

const (
	stateLeader    state = 0
	stateFollower  state = 1
	stateCandidate state = 2
)

const (
	checkTimeout = 5 * time.Millisecond
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Term    int
	Command any
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       state   // leader / follower / candidate
	currentTerm int     // 当前 Term value
	entries     []Entry // log entries; each Entry contains Command for state machine, and Term when Entry was received by leader (first index is 1)

	votedFor int // candidateId that received vote in current Term (or null if none)
	nVote    int // read via nVoteAsFollower

	commitIndex int // index of highest log Entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log Entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log Entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log Entry known to be replicated on server (initialized to 0, increases monotonically)

	timer *time.Timer
}

func (rf *Raft) lastEntry() Entry {
	return rf.entries[len(rf.entries)-1]
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = rf.state == stateLeader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
}

// RequestVoteArgs example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term        int // 自己当前的 Term value
	CandidateID int // 自己的 ID

	LastLogIndex int // 自己的最后一个 log Entry 的 log index
	LastLogTerm  int // 自己的最后一个 log Entry 的 Term value
}

// RequestVoteReply example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int  // 自己当前的 Term value
	VoteGranted bool // 自己是否给 request 中的 candidateID 进行 vote
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	success := func() {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	fail := func() {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

	rf.followIfHigher(args.Term)

	if args.Term < rf.currentTerm { // 忽略任期小于自己的
		fail()
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID { // 已投过票，拒绝后续的请求
		fail()
		return
	}

	if args.LastLogTerm > rf.lastEntry().Term ||
		args.LastLogTerm == rf.lastEntry().Term &&
			args.LastLogIndex >= len(rf.entries)-1 { // 对比日志，term value 更大 or log index 更大 or log index 相等
		rf.votedFor = args.CandidateID
		rf.timer.Reset(electionTimeout())
		success()
		return
	}

	fail()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int) {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.entries) - 1,
		LastLogTerm:  rf.lastEntry().Term,
	}

	reply := &RequestVoteReply{}

	if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.followIfHigher(reply.Term)

	if reply.VoteGranted {
		rf.nVote += 1
	}
}

func (rf *Raft) followIfHigher(term int) {
	if term <= rf.currentTerm {
		return
	}

	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = stateFollower
}

type AppendEntriesArgs struct {
	Term         int     // 自己当前的 Term value
	LeaderID     int     // Leader 的 ID（也就是自己的 ID）
	PrevLogIndex int     // 前一个日志的 log index
	PrevLogTerm  int     // 前一个日志的 Term value
	Entries      []Entry // Command
	LeaderCommit int     // Leader 的已提交的 log index
}

type AppendEntriesReply struct {
	Term    int  // 自己当前的 Term value
	Success bool // 如果 Follower 的 Local Log 中包括前一个 Log Entry，则返回 true
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	success := func() {
		reply.Term = rf.currentTerm
		reply.Success = true
	}

	fail := func() {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term == rf.currentTerm {
		rf.state = stateFollower
		rf.timer.Reset(electionTimeout())
	}

	rf.followIfHigher(args.Term)

	if args.Term < rf.currentTerm { // 忽略任期小于自己的（不一致）
		fail()
		return
	}

	if args.PrevLogIndex >= len(rf.entries) { // 日志不一致
		fail()
		return
	}

	if args.PrevLogTerm != rf.entries[args.PrevLogIndex].Term { // 日志不一致
		fail()
		return
	}

	if args.PrevLogIndex+1 != len(rf.entries) || args.Entries != nil {
		rf.entries = append(rf.entries[:args.PrevLogIndex+1], args.Entries...) // 删除不匹配并添加未持有的日志
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.entries))
	}

	success()
}

func (rf *Raft) entriesToAppend(server int) []Entry {
	if rf.nextIndex[server] < len(rf.entries) {
		return rf.entries[rf.nextIndex[server]:len(rf.entries)]
	}

	return nil
}

func (rf *Raft) sendAppendEntries(server int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.entries[rf.nextIndex[server]-1].Term,
		Entries:      rf.entriesToAppend(server),
		LeaderCommit: rf.commitIndex,
	}

	reply := &AppendEntriesReply{}

	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.followIfHigher(reply.Term)

	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = matchIndex + 1
		rf.matchIndex[server] = matchIndex
	} else {
		for rf.nextIndex[server]--; rf.nextIndex[server] >= 0 &&
			rf.entries[rf.nextIndex[server]].Term == reply.Term; rf.nextIndex[server]-- {
		}
	}
}

func electionTimeout() time.Duration {
	return time.Millisecond * time.Duration(150+rand.Int63n(150))
}

// Start
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.entries)
	term, isLeader := rf.GetState()

	if isLeader {
		rf.entries = append(rf.entries, Entry{
			Term:    term,
			Command: command,
		})
	}

	return index, term, isLeader
}

// Kill
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
}

// Make
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.

	nPeers := len(peers)

	rf.state = stateFollower
	rf.votedFor = -1
	rf.nVote = 0
	rf.entries = make([]Entry, 1)
	rf.nextIndex = make([]int, nPeers)
	rf.matchIndex = make([]int, nPeers)
	rf.timer = time.NewTimer(electionTimeout())

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	go func() {
		ticker := time.NewTicker(checkTimeout)
		for {
			select {
			case <-ticker.C:
				rf.applyUncommittedEntries(applyCh)
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.timer.C:
				rf.handle()
			}
		}
	}()

	return rf
}

func (rf *Raft) applyUncommittedEntries(applyCh chan<- ApplyMsg) {
	for rf.commitIndex > rf.lastApplied {
		rf.applyUncommittedEntry(applyCh)
	}
}

func (rf *Raft) applyUncommittedEntry(applyCh chan<- ApplyMsg) {
	rf.lastApplied++
	applyCh <- ApplyMsg{
		Index:   rf.lastApplied,
		Command: rf.entries[rf.lastApplied].Command,
	}
}

func (rf *Raft) handle() {
	switch rf.state {
	case stateFollower:
		rf.handlerFollower()
	case stateCandidate:
		rf.handlerCandidate()
	case stateLeader:
		rf.handlerLeader()
	}
}

func (rf *Raft) handlerCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.nVote = 1
	rf.timer.Reset(electionTimeout())
	rf.mu.Unlock()

	rf.requestVotePhase1()
	rf.requestVotePhase2()
}

func (rf *Raft) rpcAll(f func(server int)) {
	ch := make(chan bool)
	for i := range rf.peers {
		func(i int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if i == rf.me {
				return
			}

			go func(i int) {
				f(i)
				ch <- true
			}(i)
		}(i)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case <-ch:
		case <-time.After(checkTimeout):
			return
		}
	}
}

func (rf *Raft) requestVotePhase1() {
	rf.rpcAll(rf.sendRequestVote)
}

func (rf *Raft) requestVotePhase2() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.nVote > len(rf.peers)/2 {
		rf.state = stateLeader
		rf.timer.Reset(0)
		fill(rf.nextIndex, len(rf.entries), 0, len(rf.peers))
	}
}

func (rf *Raft) handlerFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = stateCandidate
	rf.timer.Reset(0)
}

func (rf *Raft) handlerLeader() {
	rf.mu.Lock()
	rf.timer.Reset(30 * time.Millisecond)
	rf.mu.Unlock()

	rf.appendEntriesPhase1()

	rf.appendEntriesPhase2()
}

func (rf *Raft) appendEntriesPhase1() {
	rf.rpcAll(rf.sendAppendEntries)
}

func (rf *Raft) appendEntriesPhase2() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	n := len(rf.entries) - 1

	if n <= rf.commitIndex || rf.entries[n].Term != rf.currentTerm {
		return
	}

	count := 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.matchIndex[i] >= n {
			count++
		}
	}

	if count > len(rf.peers)/2 {
		rf.commitIndex = n
	}
}

func fill[T any](s []T, t T, begin, end int) {
	for i := begin; i < end; i++ {
		s[i] = t
	}
}
