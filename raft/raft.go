// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	//"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	//
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:                    c.ID,
		Term:                  hs.Term,
		Vote:                  hs.Vote,
		RaftLog:               raftLog,
		Prs:                   make(map[uint64]*Progress),
		State:                 StateFollower,
		votes:                 make(map[uint64]bool),
		msgs:                  nil,
		Lead:                  None,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}
	if len(c.peers) > 0 {
		for _, peer := range c.peers {
			r.Prs[peer] = &Progress{}
		}
	} else {
		for _, id := range cs.Nodes {
			r.Prs[id] = &Progress{}
		}
	}
	if !IsEmptyHardState(hs) {
		r.RaftLog.committed = hs.Commit
	}
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	first, err := r.RaftLog.storage.FirstIndex()
	if err != nil {
		return false
	}
	if r.RaftLog.pendingSnapshot != nil && r.RaftLog.pendingSnapshot.Metadata != nil {
		snapfirst := r.RaftLog.pendingSnapshot.Metadata.Index + 1
		if snapfirst > first {
			first = snapfirst
		}
	}
	if pr.Next < first {
		var snap pb.Snapshot
		if r.RaftLog.pendingSnapshot != nil && r.RaftLog.pendingSnapshot.Metadata != nil {
			snap = *r.RaftLog.pendingSnapshot
		} else {
			var err error
			snap, err = r.RaftLog.storage.Snapshot()
			if err != nil || IsEmptySnap(&snap) {
				return false
			}
		}

		r.msgs = append(r.msgs, pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snap,
		})
		return true
	}

	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	ents := make([]*pb.Entry, 0)
	if pr.Next <= lastIndex {
		for i := pr.Next; i <= lastIndex; i++ {
			pos := i - r.RaftLog.dummyIndex - 1
			e := r.RaftLog.entries[pos]
			e2 := e //
			ents = append(ents, &e2)
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.RaftLog.committed, r.Prs[to].Match)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	case StateFollower, StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionTimeout {
			r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
			r.electionElapsed = 0
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	//log.Infof("[raft %d] becomeFollower term=%d lead=%d state=%v", r.id, term, lead, r.State)
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0
	r.leadTransferee = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	//log.Infof("[raft %d] becomeCandidate term=%d", r.id, r.Term)
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Lead = None
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.heartbeatElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//log.Infof("[raft %d] becomeLeader term=%d", r.id, r.Term)
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	lastIndex := r.RaftLog.LastIndex()
	r.leadTransferee = None
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id].Match = lastIndex
			r.Prs[id].Next = lastIndex + 1
		} else {
			r.Prs[id].Match = 0
			r.Prs[id].Next = lastIndex + 1
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
		Data:  nil,
	})
	newlastIndex := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = newlastIndex
	r.Prs[r.id].Next = newlastIndex + 1
	r.updateCommitIndex()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)
			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
			} else {
				lastIndex := r.RaftLog.LastIndex()
				logTerm, err := r.RaftLog.Term(lastIndex)
				if err != nil {
					return err
				}
				for id := range r.Prs {
					if id != r.id {
						r.msgs = append(r.msgs, pb.Message{
							MsgType: pb.MessageType_MsgRequestVote,
							To:      id,
							Term:    r.Term,
							From:    r.id,
							Index:   lastIndex,
							LogTerm: logTerm,
						})
					}
				}
			}
			return nil
		case pb.MessageType_MsgRequestVote:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			if m.Term < r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
				return nil
			}
			canVote := r.Vote == None || r.Vote == m.From
			lastIndex := r.RaftLog.LastIndex()
			logTerm, err := r.RaftLog.Term(lastIndex)
			if err != nil {
				return err
			}
			upToDate := m.LogTerm > logTerm || (m.LogTerm == logTerm && m.Index >= lastIndex)
			if canVote && upToDate {
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  false,
				})
				r.electionElapsed = 0
			} else {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})

			}
			return nil
		case pb.MessageType_MsgHeartbeat:

			r.handleHeartbeat(m)
			return nil
		case pb.MessageType_MsgAppend:

			r.handleAppendEntries(m)
			return nil
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
			return nil
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			return r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTransferLeader,
					From:    m.From,
					To:      r.Lead,
				})
			}
			return nil
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			r.becomeCandidate()
			if len(r.Prs) == 1 {
				r.becomeLeader()
				return nil
			}
			lastindex := r.RaftLog.LastIndex()
			logterm, err := r.RaftLog.Term(lastindex)
			if err != nil {
				return err
			}
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      id,
					Term:    r.Term,
					From:    r.id,
					Index:   lastindex,
					LogTerm: logterm,
				})
			}
		case pb.MessageType_MsgRequestVoteResponse:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			if m.Term < r.Term {
				return nil
			}
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				return nil
			}
			r.votes[m.From] = !m.Reject
			granted := 0
			rejected := 0
			for _, vote := range r.votes {
				if vote {
					granted += 1
				} else {
					rejected += 1
				}
			}
			if granted > len(r.Prs)/2 {
				r.becomeLeader()
				for id := range r.Prs {
					if id == r.id {
						continue
					}
					r.sendAppend(id)
				}
				return nil
			}
			if rejected > len(r.Prs)/2 {
				r.becomeFollower(r.Term, None)
				return nil
			}
		case pb.MessageType_MsgHeartbeat:
			if m.Term < r.Term {
				return nil
			}
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
			return nil
		case pb.MessageType_MsgAppend:
			if m.Term < r.Term {
				return nil
			}
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
			return nil
		case pb.MessageType_MsgRequestVote:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			if m.Term <= r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
			}
		case pb.MessageType_MsgSnapshot:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
				return nil
			}
		case pb.MessageType_MsgTimeoutNow:
			if _, ok := r.Prs[r.id]; !ok {
				return nil
			}
			return r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTransferLeader,
					From:    m.From,
					To:      r.Lead,
				})
			}
			return nil
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			return nil
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
			return nil
		case pb.MessageType_MsgPropose:
			if len(m.Entries) == 0 {
				return nil
			}
			for i := range m.Entries {
				m.Entries[i].Term = r.Term
				m.Entries[i].Index = r.RaftLog.LastIndex() + 1
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
			}
			pr := r.Prs[r.id]
			pr.Match = r.RaftLog.LastIndex()
			pr.Next = pr.Match + 1

			r.updateCommitIndex()
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
			return nil
		case pb.MessageType_MsgAppendResponse:
			pr := r.Prs[m.From]
			if pr == nil {
				return nil
			}
			if m.Reject {
				pr.Next--
				if pr.Next < 1 {
					pr.Next = 1
				}
				r.sendAppend(m.From)
				return nil
			} else {
				if m.Index > pr.Match {
					pr.Match = m.Index
				}
				pr.Next = pr.Match + 1
				oldCommit := r.RaftLog.committed
				r.updateCommitIndex()
				if r.RaftLog.committed > oldCommit {
					for id := range r.Prs {
						if id == r.id {
							continue
						}
						r.sendAppend(id)
					}
				}
			}
			if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTimeoutNow,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			}
		case pb.MessageType_MsgRequestVote:
			//log.Infof("[raft %d] step msg=%s from=%d term=%d state=%v lead=%d", r.id, m.MsgType, m.From, m.Term, r.State, r.Lead)

			if m.Term <= r.Term {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  true,
				})
				return nil
			}
		case pb.MessageType_MsgHeartbeatResponse:
			r.sendAppend(m.From)
			return nil
		case pb.MessageType_MsgSnapshot:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				r.handleSnapshot(m)
				return nil
			}
		case pb.MessageType_MsgTransferLeader:
			transferee := m.From
			if transferee == r.id {
				return nil
			}
			pr, ok := r.Prs[transferee]
			if !ok {
				return nil
			}
			r.leadTransferee = m.From
			if pr.Match == r.RaftLog.LastIndex() {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTimeoutNow,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
				})
			} else {
				r.sendAppend(transferee)
			}
			return nil
		}
	}
	return nil
}

func (r *Raft) updateCommitIndex() {
	for N := r.RaftLog.committed + 1; N <= r.RaftLog.LastIndex(); N++ {
		t, err := r.RaftLog.Term(N)
		if err != nil || t != r.Term {
			continue
		}
		cnt := 0
		for _, pr := range r.Prs {
			if pr.Match >= N {
				cnt += 1
			}
		}
		if cnt >= len(r.Prs)/2+1 {
			r.RaftLog.committed = N
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.electionElapsed = 0
	localTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || localTerm != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	lastnewindex := m.Index
	for i, entry := range m.Entries {
		if entry.Index <= r.RaftLog.dummyIndex {
			continue
		}
		if entry.Index > r.RaftLog.LastIndex() {
			for _, e := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
				lastnewindex = e.Index
			}
			break
		}
		t, err := r.RaftLog.Term(entry.Index)
		if err != nil {
			return
		}
		if entry.Index <= r.RaftLog.LastIndex() && entry.Term == t {
			lastnewindex = entry.Index
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() && entry.Term != t {
			pos := entry.Index - r.RaftLog.dummyIndex - 1
			// 截断日志需检查stabled
			if r.RaftLog.stabled >= entry.Index {
				r.RaftLog.stabled = entry.Index - 1
			}
			r.RaftLog.entries = r.RaftLog.entries[:pos]
			for _, e := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
				lastnewindex = e.Index
			}
			break
		}

	}

	newCommited := min(m.Commit, lastnewindex)
	if newCommited > r.RaftLog.committed {
		r.RaftLog.committed = newCommited
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Index:   lastnewindex,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.Lead = m.From
	r.electionElapsed = 0
	r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	snap := m.Snapshot
	if snap == nil || snap.Metadata == nil {
		return
	}
	if snap.Metadata.Index <= r.RaftLog.committed {
		return
	}

	term := r.Term
	if m.Term > term {
		term = m.Term
	}
	r.becomeFollower(term, m.From)
	r.Lead = m.From

	r.RaftLog.pendingSnapshot = snap
	r.RaftLog.entries = nil
	r.RaftLog.dummyIndex = snap.Metadata.Index
	r.RaftLog.committed = snap.Metadata.Index
	r.RaftLog.applied = snap.Metadata.Index
	r.RaftLog.stabled = snap.Metadata.Index

	r.Prs = make(map[uint64]*Progress)
	if snap.Metadata.ConfState != nil {
		for _, id := range snap.Metadata.ConfState.Nodes {
			r.Prs[id] = &Progress{}
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		return
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		return
	}
	//fmt.Println("before remove", nodes(r), "remove", id)
	delete(r.Prs, id)
	//fmt.Println("after remove", nodes(r))
	if r.Lead == id {
		r.Lead = None
	}
	if len(r.Prs) == 0 {
		return
	}
	r.updateCommitIndex()
}
