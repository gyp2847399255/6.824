package raft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

const HEARTBEAT_TIMEOUT time.Duration = time.Duration(200) * time.Millisecond

const ELECT_TIMEOUT_MIN = 500
const ELECT_TIMEOUT_MAX = 1000

const MAX_SEND_STEP = 100

func GenElectTimeout() time.Duration {
	t, _ := rand.Int(rand.Reader, big.NewInt(ELECT_TIMEOUT_MAX-ELECT_TIMEOUT_MIN))
	return time.Duration(t.Int64()+ELECT_TIMEOUT_MIN) * time.Millisecond
}

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (rf *Raft) peerNumber() int {
	return len(rf.peers)
}

func (rf *Raft) majority() int {
	return (rf.peerNumber() + 1) / 2
}

func (rf *Raft) updateTerm(term int) {
	if rf.currentTerm > term {
		log.Panic("update term to a smaller term")
	}
	if rf.currentTerm == term {
		return
	}

	rf.currentTerm = term
	rf.votedFor = nil
	rf.persist()
}

func (rf *Raft) changeRole(role RaftRole) {
	if rf.role == role {
		return
	}
	if role == LEADER {
		rf.nextIndex = make([]int, rf.peerNumber())
		rf.matchIndex = make([]int, rf.peerNumber())
		lastLogIndex := rf.lastLogIndex()
		nextIndex := lastLogIndex + 1
		for i := 0; i < rf.peerNumber(); i++ {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex

		lastLogIndex = rf.lastLogIndex()
		lastLogTerm := rf.lastLogTerm()

		args := new(AppendEntriesArgs)
		*args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
	}
	rf.role = role
}

func (rf *Raft) tickApplier() {
	if len(rf.shouldCommitChan) == 0 {
		rf.shouldCommitChan <- struct{}{}
	}
}
func (rf *Raft) lastIncludedIndex() int {
	return rf.logs[0].Index
}

func (rf *Raft) lastIncludedTerm() int {
	return rf.logs[0].Term
}

func (rf *Raft) lastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) findLogPosByIndex(index int) int {
	lastIncludedIndex := rf.lastIncludedIndex()
	if index < lastIncludedIndex {
		return -1
	}
	pos := index - lastIncludedIndex
	if pos >= len(rf.logs) {
		return -1
	}
	return pos
}

func (rf *Raft) findLogByIndex(index int) *LogEntry {
	lastIncludedIndex := rf.lastIncludedIndex()
	if index < lastIncludedIndex {
		return nil
	}
	pos := index - lastIncludedIndex
	if pos >= len(rf.logs) {
		return nil
	}
	return &rf.logs[pos]
}

func (rf *Raft) findLogSliceByIndex(startIndex int, maxLength int) []LogEntry {
	lastIncludedIndex := rf.lastIncludedIndex()
	lastLogIndex := rf.lastLogIndex()
	length := len(rf.logs)
	if startIndex > lastLogIndex || maxLength == 0 || length <= 1 ||
		startIndex+maxLength <= lastIncludedIndex+1 {
		return []LogEntry{}
	}
	startPos := maxInt(1, startIndex-lastIncludedIndex)
	endPos := minInt(length, startIndex-lastIncludedIndex+maxLength)
	return rf.logs[startPos:endPos]
}

func (rf *Raft) broadcastAppendEntries(args *AppendEntriesArgs) {
	for i := 0; i < rf.peerNumber(); i++ {
		if i != rf.me {
			reply := new(AppendEntriesReply)
			go rf.sendAppendEntries(i, args, reply)
		}
	}
}
