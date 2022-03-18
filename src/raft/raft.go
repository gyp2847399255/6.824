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
	"context"
	"log"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

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
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int

	LeaderValid bool
	Term        int
}

type LogEntry struct {
	Cmd   interface{}
	Index int
	Term  int
}

type RaftRole int

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mutex     sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	role      RaftRole
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan ApplyMsg

	cancelFunc  context.CancelFunc
	ctx         context.Context
	electTicker *time.Ticker

	currentTerm int
	votedFor    *int
	logs        []LogEntry

	commitIndex      int
	lastApplied      int
	shouldCommitChan chan struct{}

	nextIndex  []int
	matchIndex []int
	logTrigger []chan int

	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	return rf.currentTerm, rf.role == LEADER
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		DPrintf("REQUEST_VOTE receive higher term %d from peer %d",
			args.Term, args.CandidateId)
		rf.updateTerm(args.Term)
		rf.changeRole(FOLLOWER)
	}
	reply.Term = args.Term
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()
		if args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.electTicker.Reset(GenElectTimeout())
			rf.votedFor = new(int)
			*rf.votedFor = args.CandidateId
			rf.persist()
			DPrintf("REQUEST_VOTE vote for peer %d", args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.electTicker.Reset(GenElectTimeout())
	if args.Term > rf.currentTerm {
		DPrintf("AppendEntries receive higher term %d from leader %d", args.Term, args.LeaderId)
		rf.updateTerm(args.Term)
		rf.changeRole(FOLLOWER)
	}
	reply.Term = rf.currentTerm

	lastIncludedIndex := rf.lastIncludedIndex()
	lastIncludedTerm := rf.lastIncludedTerm()
	if args.PrevLogIndex < lastIncludedIndex {
		startPos := lastIncludedIndex - args.PrevLogIndex
		if startPos >= len(args.Entries) {
			args.Entries = []LogEntry{}
			reply.Success = true
			return
		}
		args.Entries = args.Entries[startPos:]
		args.PrevLogIndex = lastIncludedIndex
		args.PrevLogTerm = lastIncludedTerm
	}

	prevLogPos := rf.findLogPosByIndex(args.PrevLogIndex)
	if prevLogPos < 0 {
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	prevLogEntry := &rf.logs[prevLogPos]
	if prevLogEntry.Term != args.PrevLogTerm {
		reply.ConflictTerm = prevLogEntry.Term
		for i := prevLogPos; i >= 0 && rf.logs[i].Term == reply.ConflictTerm; i-- {
			reply.ConflictIndex = i
		}
		return
	}

	reply.Success = true
	hasUpdate := false
	for i, newEntry := range args.Entries {
		pos := prevLogPos + i + 1
		if pos >= len(rf.logs) {
			rf.logs = append(rf.logs, newEntry)
			hasUpdate = true
		} else {
			oldEntry := &rf.logs[pos]
			if oldEntry.Term != newEntry.Term {
				rf.logs = rf.logs[:pos]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				hasUpdate = true
				break
			}
		}
	}
	if hasUpdate {
		rf.persist()
	}
	if args.LeaderCommit > rf.commitIndex {
		newEntryIndex := args.PrevLogIndex
		if len(args.Entries) > 0 {
			newEntryIndex = args.Entries[len(args.Entries)-1].Index
		}
		newCommitIndex := minInt(args.LeaderCommit, newEntryIndex)
		if newCommitIndex > rf.commitIndex {
			DPrintf("APPEND_ENTRIES update commitIndex %d => %d, with leader commit %d",
				rf.commitIndex, newCommitIndex, args.LeaderCommit)
			rf.commitIndex = newCommitIndex
			rf.tickApplier()
		}
	}
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArgs, reply *InstallSnapshotReply) bool {
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
	rf.cancelFunc()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) stepCommitIndex() (int, bool) {
	oldCommitIndex := rf.commitIndex
	matchIndices := make([]int, rf.peerNumber())
	copy(matchIndices, rf.matchIndex)
	sort.Ints(matchIndices)
	index := matchIndices[rf.majority()-1]
	if index > oldCommitIndex {
		logEntry := rf.findLogByIndex(index)
		if logEntry != nil && logEntry.Term == rf.currentTerm {
			rf.commitIndex = index
			return index, true
		}
	}
	return oldCommitIndex, false
}

func (rf *Raft) replicator(ctx context.Context, target int) {
	DPrintf("REPLICATED %d start", target)
	defer DPrintf("REPLICATED %d exit", target)
	ticker := time.NewTicker(HEARTBEAT_TIMEOUT)
	var prevChan chan int = nil
	var fast int32 = 0
	for !rf.killed() {
		tick := false
		if prevChan == nil {
			DPrintf("REPLICATOR %d select on log and ticker", target)
			select {
			case <-ctx.Done():
				DPrintf("REPLICATOR %d exit by context cancel", target)
				return
			case <-rf.logTrigger[target]:
				DPrintf("REPLICATOR %d tick by log", target)
			case <-ticker.C:
				DPrintf("REPLICATOR %d tick by heartbeat 1", target)
				tick = true
			}
		} else {
			DPrintf("REPLICATOR %d select on prev and ticker", target)
			select {
			case <-ctx.Done():
				DPrintf("REPLICATOR %d exit", target)
				return
			case <-ticker.C:
				DPrintf("REPLICATOR %d tick by heartbeat 2", target)
				tick = true
			case n := <-prevChan:
				DPrintf("REPLICATOR %d prev sync finish: %d", target, n)
			}
		}
		rf.mutex.Lock()
		if len(rf.logTrigger[target]) > 0 {
			<-rf.logTrigger[target]
		}
		if !(rf.role == LEADER && (tick || (rf.nextIndex[target] <= rf.lastLogIndex()))) {
			rf.mutex.Unlock()
			prevChan = nil
			continue
		}

		term := rf.currentTerm
		commitIndex := rf.commitIndex

		if rf.nextIndex[target] > rf.lastIncludedIndex() {
			prevLogEntry := rf.findLogByIndex(rf.nextIndex[target] - 1)
			prevLogIndex := prevLogEntry.Index
			prevLogTerm := prevLogEntry.Term
			maxLog := 1
			if f := atomic.LoadInt32(&fast); f > 0 {
				DPrintf("REPLICATOR %d fast send, max step: %d", target, MAX_SEND_STEP)
				maxLog = MAX_SEND_STEP
			}
			logsToSend := rf.findLogSliceByIndex(rf.nextIndex[target], maxLog)
			args := new(AppendEntriesArgs)
			*args = AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      logsToSend,
				LeaderCommit: commitIndex,
			}
			if len(logsToSend) == 0 {
				DPrintf("REPLICATOR %d sent heart beat, prevLogIndex %d, "+
					"prevLogTerm %d, commitIndex %d", target, prevLogIndex, prevLogTerm, commitIndex)
				prevChan = nil
			} else {
				DPrintf("REPLICATOR %d send logs [%d, %d], prevLogIndex %d, prevLogTerm %d, commitIndex %d",
					target, logsToSend[0].Index, logsToSend[len(logsToSend)-1].Index,
					prevLogIndex, prevLogTerm, commitIndex)
				prevChan = make(chan int, 1)
			}
			rf.mutex.Unlock()
			go func(retChan chan int) {
				reply := &AppendEntriesReply{}
				ticker.Reset(HEARTBEAT_TIMEOUT)
				res := rf.sendAppendEntries(target, args, reply)
				if !res {
					if retChan != nil {
						retChan <- 1
					}
					return
				}
				rf.mutex.Lock()
				if args.Term != rf.currentTerm {
					rf.mutex.Unlock()
					if retChan != nil {
						retChan <- 2
					}
					return
				}

				if reply.Term > rf.currentTerm {
					atomic.StoreInt32(&fast, 0)
					rf.changeRole(FOLLOWER)
					rf.updateTerm(reply.Term)
					rf.mutex.Unlock()
					if retChan != nil {
						retChan <- 3
					}
					return
				}

				if reply.Success {
					atomic.StoreInt32(&fast, 1)
					length := len(args.Entries)
					oldNextIndex := rf.nextIndex[target]
					rf.nextIndex[target] = maxInt(rf.nextIndex[target], args.PrevLogIndex+length+1)
					DPrintf("REPLICATOR %d step nextIndex %d => %d",
						target, oldNextIndex, rf.nextIndex[target])

					oldMatchIndex := rf.matchIndex[target]
					rf.matchIndex[target] = maxInt(rf.matchIndex[target], args.PrevLogIndex+length)
					DPrintf("REPLICATOR %d step matchIndex %d => %d",
						target, oldMatchIndex, rf.matchIndex[target])

					oldCommitIndex := rf.commitIndex
					newCommitIndex, ok := rf.stepCommitIndex()
					if ok {
						DPrintf("REPLICATOR %d step commitIndex %d => %d",
							target, oldCommitIndex, newCommitIndex)
						rf.tickApplier()
						args = new(AppendEntriesArgs)
						*args = AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							PrevLogIndex: rf.commitIndex,
							PrevLogTerm:  rf.currentTerm,
							Entries:      []LogEntry{},
							LeaderCommit: rf.commitIndex,
						}
						rf.broadcastAppendEntries(args)
						rf.mutex.Unlock()
						if retChan != nil {
							retChan <- 0
						}
						return
					}
					rf.mutex.Unlock()
					if retChan != nil {
						retChan <- 0
					}
					return
				}
				DPrintf("REPLICATOR %d, sync log failed, ConflictIndex %d, ConflictTerm %d",
					target, reply.ConflictIndex, reply.ConflictTerm)
				if rf.nextIndex[target] == args.PrevLogIndex+1 {
					atomic.StoreInt32(&fast, 0)
					oldNextIndex := rf.nextIndex[target]
					rf.nextIndex[target] = reply.ConflictIndex
					if reply.ConflictTerm < 0 {
						DPrintf("REPLICATOR %d, step down next index %d => %d",
							target, oldNextIndex, rf.nextIndex[target])
					} else {
						for i := len(rf.logs) - 1; i >= 0; i-- {
							if rf.logs[i].Term == reply.ConflictTerm {
								rf.nextIndex[target] = rf.logs[i].Index + 1
								break
							}
						}
						if rf.nextIndex[target] >= oldNextIndex {
							DPrintf("REPLICATOR %d, panic step next index failed %d => %d",
								target, oldNextIndex, rf.nextIndex[target])
							log.Fatalf("step next index failed")
						}
						DPrintf("REPLICATOR %d, step down next index %d => %d",
							target, oldNextIndex, rf.nextIndex[target])
					}
				}
				rf.mutex.Unlock()
				if retChan != nil {
					retChan <- 4
				}
				return
			}(prevChan)
		} else {
			if rf.snapshot == nil || len(rf.snapshot) == 0 {
				DPrintf("REPLICATOR %d can't find snapshot to send", target)
				log.Fatalf("REPLICATOR %d can't find snapshot to send", target)
			}
			lastIncludedIndex := rf.lastIncludedIndex()
			lastIncludedTerm := rf.lastIncludedTerm()
			args := new(InstallSnapShotArgs)
			*args = InstallSnapShotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Snapshot:          rf.snapshot,
			}
			DPrintf("REPLICATOR %d install snapshot lastIncludedIndex %d, lastIncludedTerm %d",
				target, lastIncludedIndex, lastIncludedTerm)
			rf.mutex.Unlock()
			prevChan = make(chan int, 1)
			go func(retChan chan int) {
				reply := new(InstallSnapshotReply)
				ticker.Reset(HEARTBEAT_TIMEOUT)
				ok := rf.sendInstallSnapshot(target, args, reply)
				if !ok {
					retChan <- 0
				}
				rf.mutex.Lock()
				if args.Term != rf.currentTerm {
					rf.mutex.Unlock()
					return
				}
				if args.Term > rf.currentTerm {
					rf.updateTerm(args.Term)
					rf.changeRole(FOLLOWER)
					rf.mutex.Unlock()
					return
				}
				atomic.StoreInt32(&fast, 1)
				oldMatchIndex := rf.matchIndex[target]
				rf.matchIndex[target] = maxInt(rf.matchIndex[target], args.LastIncludedIndex)
				DPrintf("REPLICATOR %d install snapshot step matchIndex %d => %d",
					target, oldMatchIndex, rf.nextIndex[target])
				oldCommitIndex := rf.commitIndex
				if newCommitIndex, ok := rf.stepCommitIndex(); ok {
					DPrintf("REPLICATOR %d install snapshot update commitIndex %d => %d",
						target, oldCommitIndex, newCommitIndex)
					rf.tickApplier()
				}
				rf.mutex.Unlock()
				retChan <- 0
			}(prevChan)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(ctx context.Context) {
	DPrintf("TICKER START")
	defer DPrintf("TICKER EXIT")
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-ctx.Done():
			DPrintf("TICKER exit by context cancel")
			return
		case <-rf.electTicker.C:
		}

		rf.electTicker.Reset(GenElectTimeout())
		rf.mutex.Lock()
		if rf.role == LEADER {
			rf.mutex.Unlock()
			continue
		}

		rf.changeRole(CANDIDATE)
		rf.updateTerm(rf.currentTerm + 1)
		DPrintf("TICKER start election, term %d", rf.currentTerm)
		currentTerm := rf.currentTerm
		lastLogIndex := rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()
		getVote := new(int32)
		if rf.votedFor == nil {
			*getVote = 1
			rf.votedFor = new(int)
			*rf.votedFor = rf.me
		}
		rf.mutex.Unlock()
		rf.electTicker.Reset(GenElectTimeout())
		args := RequestVoteArgs{
			Term:         currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		for i := 0; i < rf.peerNumber(); i++ {
			if i == rf.me {
				continue
			}
			go func(target int) {
				reply := new(RequestVoteReply)
				ok := rf.sendRequestVote(target, &args, reply)
				if !ok {
					return
				}
				rf.mutex.Lock()
				defer rf.mutex.Unlock()
				if currentTerm != rf.currentTerm {
					return
				}
				if reply.Term > currentTerm {
					rf.changeRole(FOLLOWER)
					rf.updateTerm(reply.Term)
					return
				}
				if reply.VoteGranted {
					DPrintf("TICKER receive vote from peer %d for election in term %d",
						target, currentTerm)
					out := atomic.AddInt32(getVote, 1)
					if out == int32(rf.majority()) {
						rf.changeRole(LEADER)
						DPrintf("TICKER become leader")
						/*go func() {
							rf.applyCh <- ApplyMsg{
								LeaderValid: true,
								Term:        currentTerm,
							}
						}()*/
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) applier(ctx context.Context) {
	DPrintf("APPLIER START")
	defer DPrintf("APPLIER EXIT")
	rf.mutex.Lock()
	if rf.snapshot != nil && len(rf.snapshot) > 0 {
		DPrintf("APPLIER apply init snapshot")
		snapshot := rf.snapshot
		lastIncludedIndex := rf.lastIncludedIndex()
		lastIncludedTerm := rf.lastIncludedTerm()
		rf.mutex.Unlock()
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      snapshot,
			SnapshotTerm:  lastIncludedTerm,
			SnapshotIndex: lastIncludedIndex,
		}
	} else {
		rf.mutex.Unlock()
	}

	for !rf.killed() {
		select {
		case <-ctx.Done():
			DPrintf("APPLIER exit by context cancel")
			return
		case <-rf.shouldCommitChan:
			DPrintf("APPLIER wake")
		}
		rf.mutex.Lock()
		rf.lastApplied = maxInt(rf.lastApplied, rf.lastIncludedIndex())
		DPrintf("APPLIER apply to %d, last applied %d", rf.commitIndex, rf.lastApplied)
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			logEntry := rf.findLogByIndex(rf.lastApplied)
			if logEntry == nil {
				DPrintf("can't find logEntry index %d", rf.lastApplied)
				log.Fatalf("can't find logEntry index %d", rf.lastApplied)
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Cmd,
				CommandIndex: logEntry.Index,
			}
			rf.mutex.Unlock()
			DPrintf("APPLIER apply log entry %d, command: %v", applyMsg.CommandIndex, applyMsg.Command)
			rf.applyCh <- applyMsg
			rf.mutex.Lock()
		}
		rf.mutex.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.ctx, rf.cancelFunc = context.WithCancel(context.Background())
	rf.votedFor = nil
	rf.logs = []LogEntry{{Cmd: nil, Term: 0, Index: 0}}
	rf.electTicker = time.NewTicker(GenElectTimeout())
	rf.shouldCommitChan = make(chan struct{}, 1)

	rf.logTrigger = make([]chan int, rf.peerNumber())
	for i := 0; i < rf.peerNumber(); i++ {
		rf.logTrigger[i] = make(chan int, 1)
	}

	// initialize from state persisted before a crash
	/*
		rf.readPersist(persister.ReadRaftState())
		rf.lastApplied = 0
		rf.snapshot = persister.ReadSnapshot()

	*/
	// start ticker goroutine to start elections
	go rf.ticker(rf.ctx)
	//go rf.applier(rf.ctx)
	for target := 0; target < rf.peerNumber(); target++ {
		if target != rf.me {
			go rf.replicator(rf.ctx, target)
		}
	}

	DPrintf("MAKE peer start")
	return rf
}
