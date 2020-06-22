// dmitrime [https://bluedive.site/]
// This code is in the public domain.

package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

type CommitEntry struct {
	Command interface{}
	Index   int
	Term    int
}

type State string

const (
	Follower  State = "follower"
	Candidate       = "candidate"
	Leader          = "leader"
	Killed          = "killed"
)

const MinElectionInterval = 160
const HeartbeatInterval = 80
const ApplyInterval = 50

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu       sync.Mutex         // lock to protect shared access to this peer's state
	peers    []int              // ids of all peers
	server   *Server            // server using this Raft module
	storage  Storage            // persistent storage
	me       int                // this peer's index into peers[]
	dead     int32              // set by Stop()
	commitCh chan<- CommitEntry // channel for sending back applied messages

	state    State     // server state
	lastTime time.Time // the most recent time of received heartbeat from leader

	currentTerm int        // latest term server has seen
	votedFor    int        // candidate id which receives the vote in current term or -1
	log         []LogEntry // log entries

	commitIndex int // index of highest log entry commited
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // for each peer, index of the next log entry to send to that peer
	matchIndex []int // for each peer, index of the highest log entry replicated on that peer
}

type LogEntry struct {
	Term    int         // leader term of log entry
	Index   int         // leader index of log entry
	Command interface{} // the command to apply to state machine
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // current term for leader to inspect
	VoteGranted bool // true if candidate recieved the vote
}

type AppendEntriesArgs struct {
	Term         int // current term
	LeaderId     int // leader's id
	PrevLogIndex int // index of log entry preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // current term for leader to inspect
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	LogLen  int  // length of the log after update

	XTerm  int // term in the conflicting entry, if any
	XIndex int // index of first entry with that term, if any
	XLen   int // log length
}

func (rf *Raft) Report() (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.me, rf.currentTerm, rf.state == Leader
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	voteGranted := false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] (%s) not voting for candidate [%d] because their term is lower [%d<%d]\n",
			rf.me, rf.state, args.CandidateId, args.Term, rf.currentTerm)
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		if args.LastLogTerm > rf.getLastEntryTerm() ||
			args.LastLogTerm == rf.getLastEntryTerm() && args.LastLogIndex >= rf.getLastEntryIndex() {
			voteGranted = true
			rf.becomeFollower(args.Term, args.CandidateId)

			DPrintf("[%d] (%s) votes for candidate [%d] in term [%d]\n", rf.me, rf.state, args.CandidateId, args.Term)
		} else {
			DPrintf("[%d] (%s) not voting for candidate [%d] because their log is not up-to-date, their LL-index=%d, our LL-index=%d, their LL-term=%d, our LL-term=%d\n",
				rf.me, rf.state, args.CandidateId, args.LastLogIndex, rf.getLastEntryIndex(), args.LastLogTerm, rf.getLastEntryTerm())
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted
	return nil
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	success := false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] (%s) got HB from [%d], not following because args.Term=%d < currentTerm=%d.\n",
			rf.me, rf.state, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		rf.becomeFollower(args.Term, -1)
		DPrintf("[%d] (%s) got HB from [%d] with %d entries, our log=%d, our commitIndex=%d, PrevLogIndex=%d, LeaderCommit=%d\n",
			rf.me, rf.state, args.LeaderId, len(args.Entries), len(rf.log), rf.commitIndex, args.PrevLogIndex, args.LeaderCommit)

		if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			success = true
			rf.updateFollowerLog(args)
			reply.LogLen = len(rf.log)
		} else {
			xTerm, xIndex := rf.findReplyXArgs(args.PrevLogIndex, reply)
			reply.XTerm = xTerm
			reply.XIndex = xIndex
			reply.XLen = len(rf.log)

			trm := -1
			if args.PrevLogIndex < len(rf.log) {
				trm = rf.log[args.PrevLogIndex].Term
			}
			DPrintf("[%d] (%s) Log indices do not match: PrevLogIndex=%d, len(log)=%d, PrevLogTerm=%d, our term at PrevLogTerm index=%d.\n",
				rf.me, rf.state, args.PrevLogIndex, len(rf.log), args.PrevLogTerm, trm)
			DPrintf("[%d] (%s) Sending back XTerm=%d, XIndex=%d, XLen=%d\n", rf.me, rf.state, reply.XTerm, reply.XIndex, reply.XLen)
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = success
	return nil
}

func (rf *Raft) findReplyXArgs(prevLogIndex int, reply *AppendEntriesReply) (int, int) {
	term := -1
	index := -1
	if prevLogIndex < len(rf.log) {
		term = rf.log[prevLogIndex].Term
		index = prevLogIndex
		for index >= 0 && rf.log[index].Term == term {
			index--
		}
	}
	return term, index + 1
}

func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.server.Call(peer, "Raft.RequestVote", args, reply)
	return err == nil
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	err := rf.server.Call(peer, "Raft.AppendEntries", args, reply)
	return err == nil
}

func (rf *Raft) Submit(command interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		rf.log = append(rf.log,
			LogEntry{Term: rf.currentTerm, Command: command, Index: len(rf.log)})
		DPrintf("[%d] (%s) Start: added to log, len(log)=%d\n", rf.me, rf.state, len(rf.log))
		rf.persistToStorage()
		return true
	}

	return false
}

func (rf *Raft) Stop() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.state = Killed
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastEntryIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastEntryTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) applyCommands() {
	for {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		for lastApplied < commitIndex {
			rf.mu.Lock()
			rf.lastApplied++
			lastApplied = rf.lastApplied
			entry := rf.log[lastApplied]
			rf.mu.Unlock()

			rf.commitCh <- CommitEntry{
				Command: entry.Command,
				Index:   entry.Index,
				Term:    entry.Term,
			}
		}

		time.Sleep(time.Duration(ApplyInterval) * time.Millisecond)

		if rf.killed() {
			DPrintf("[%d] was killed, stopping apply loop.\n", rf.me)
			return
		}
	}
}

func (rf *Raft) updateFollowerLog(args *AppendEntriesArgs) {
	if len(args.Entries) > 0 {
		DPrintf("[%d] (%s) Starting update to follower log with %d new entries, before len(log)=%d, commitIndex=%d, leaderCommit=%d\n",
			rf.me, rf.state, len(args.Entries), len(rf.log), rf.commitIndex, args.LeaderCommit)
	}

	i := 0
	prevIndex := args.PrevLogIndex + 1
	for ; i < len(args.Entries) && prevIndex < len(rf.log); i++ {
		if args.Entries[i].Term != rf.log[prevIndex].Term {
			break
		}
		prevIndex++
	}
	if prevIndex < len(rf.log) {
		DPrintf("[%d] (%s) Truncated our log from %d to %d entries\n", rf.me, rf.state, len(rf.log), prevIndex)

		rf.log = rf.log[:prevIndex]
	}

	newEntries := len(args.Entries) - i
	if newEntries > 0 {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persistToStorage()

		DPrintf("[%d] (%s) Appended %d new entries to our log, after len(log)=%d\n", rf.me, rf.state, newEntries, len(rf.log))
	}
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastEntryIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		DPrintf("[%d] (%s) Updated follower commitIndex to=%d, len(log)=%d, leaderCommit=%d\n",
			rf.me, rf.state, rf.commitIndex, len(rf.log), args.LeaderCommit)
	}
	DPrintf("[%d] (%s) Sending back update saying we added %d new entries from %d args.Entries, after len(log)=%d",
		rf.me, rf.state, newEntries, len(args.Entries), len(rf.log))
}

func (rf *Raft) becomeFollower(newTerm int, votedFor int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = votedFor
	rf.lastTime = time.Now()

	rf.persistToStorage()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.lastTime = time.Now()

	rf.persistToStorage()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votedFor = -1
	rf.lastTime = time.Now()
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.getLastEntryIndex() + 1
		rf.matchIndex[peer] = 0
	}
}

func (rf *Raft) checkTerm(ourTerm int, theirTerm int) bool {
	if ourTerm < theirTerm {
		DPrintf("[%d] (%s) got a higher term in reply, stepping down.\n", rf.me, rf.state)
		rf.becomeFollower(theirTerm, -1)
		return false
	}
	return ourTerm == theirTerm
}

func (rf *Raft) updateCommitIndex() {
	prevCommitIndex := rf.commitIndex
	for index := len(rf.log) - 1; index > prevCommitIndex; index-- {
		if rf.log[index].Term != rf.currentTerm {
			continue
		}
		count := 1
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer != rf.me && rf.matchIndex[peer] >= index {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			DPrintf("[%d] (%s) Setting commitIndex from %d to %d", rf.me, rf.state, rf.commitIndex, index)
			rf.commitIndex = index
			break
		}
	}
}

func (rf *Raft) processAppendEntriesReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if termOk := rf.checkTerm(args.Term, reply.Term); termOk {
		if reply.Success && len(args.Entries) > 0 && reply.LogLen > 0 {
			DPrintf("[%d] (%s) Got success from peer=[%d] which has %d log entries / we sent %d entries before\n",
				rf.me, rf.state, peer, reply.LogLen, len(args.Entries))

			rf.nextIndex[peer] = reply.LogLen
			if rf.nextIndex[peer] > len(rf.log) {
				DPrintf("[%d] (%s) [WARNING] Setting nextIndex[%d] to %d - follower's log has %d entries, our log=%d\n",
					rf.me, rf.state, peer, rf.nextIndex[peer], reply.LogLen, len(rf.log))
			}
			rf.matchIndex[peer] = args.Entries[len(args.Entries)-1].Index
			rf.updateCommitIndex()
		} else if !reply.Success {
			DPrintf("[%d] (%s) Got failure from peer=%d which has %d total entries / we sent %d entries before\n",
				rf.me, rf.state, peer, reply.LogLen, len(args.Entries))
			if reply.XTerm != -1 {
				// find last entry for args.XTerm, if any
				index := rf.getLastEntryIndex()
				for index >= 0 && rf.log[index].Term > reply.XTerm {
					index--
				}
				if rf.log[index].Term == reply.XTerm {
					// case 1: leader has XTerm
					rf.nextIndex[peer] = index
				} else {
					// case 2: leader does not have XTerm
					rf.nextIndex[peer] = reply.XIndex
				}
			} else {
				// case 3: follower's log is too short
				rf.nextIndex[peer] = reply.XLen
			}
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("[%d] (%s) realised it's not a leader anymore, will stop sending HB.\n", rf.me, rf.state)
			rf.mu.Unlock()
			return
		}

		DPrintf("[%d] (%s) sending out HB to all peers.\n", rf.me, rf.state)
		lastIndex := rf.getLastEntryIndex()
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}
			nextIndex := rf.nextIndex[peer]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.log[nextIndex-1].Term,
				Entries:      make([]LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			if lastIndex >= nextIndex {
				args.Entries = rf.log[nextIndex:]
			}

			go func(p int) {
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(p, &args, &reply); ok {
					rf.processAppendEntriesReply(p, args, reply)
				}
			}(peer)
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) attemptLeaderElection() {
	rf.mu.Lock()
	DPrintf("[%d] becomes a candidate.\n", rf.me)
	rf.becomeCandidate()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastEntryIndex(),
		LastLogTerm:  rf.getLastEntryTerm(),
	}
	rf.mu.Unlock()

	var votes int32 = 1
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(p, &args, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.VoteGranted {
					total := int(atomic.AddInt32(&votes, 1))
					if total > len(rf.peers)/2 && rf.state == Candidate {
						DPrintf("[%d] got %d votes, becomes a leader.\n", rf.me, votes)
						rf.becomeLeader()
						go rf.sendHeartbeats()
					}
				} else {
					rf.checkTerm(args.Term, reply.Term)
				}
			}
		}(peer)
	}
}

func (rf *Raft) periodicLeaderElection() {
	for {
		start := time.Now()
		interval := MinElectionInterval + rand.Intn(200)
		time.Sleep(time.Duration(interval) * time.Millisecond)

		if rf.killed() {
			DPrintf("[%d] was killed, stopping election loop.\n", rf.me)
			return
		}

		rf.mu.Lock()
		if rf.lastTime.Before(start) && rf.state != Leader {
			DPrintf("[%d] (%s) starting elections.\n", rf.me, rf.state)
			go rf.attemptLeaderElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) restoreFromStorage() {
	if termData, found := rf.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&rf.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := rf.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&rf.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := rf.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&rf.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (rf *Raft) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(rf.currentTerm); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(rf.votedFor); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(rf.log); err != nil {
		log.Fatal(err)
	}
	rf.storage.Set("log", logData.Bytes())
}

func DPrintf(format string, args ...interface{}) {
	if Debug > 0 {
		log.Printf(format, args...)
	}
}

func MakeRaft(me int, peers []int, server *Server, storage Storage,
	ready <-chan interface{}, commitCh chan<- CommitEntry) *Raft {
	peers = append(peers, me)
	rf := &Raft{
		me:          me,
		peers:       peers,
		server:      server,
		storage:     storage,
		log:         []LogEntry{LogEntry{Term: 0, Index: 0}},
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		lastTime:    time.Now(),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitCh:    commitCh,
	}
	// initialize from state persisted before a crash
	if rf.storage.HasData() {
		rf.restoreFromStorage()
	}

	go func() {
		<-ready
		rf.periodicLeaderElection()
	}()
	go rf.applyCommands()

	return rf
}
