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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstindex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastindex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstindex, lastindex+1)
	if err != nil && err != ErrUnavailable {
		panic(err)
	}

	return &RaftLog{
		storage:    storage,
		entries:    entries,
		committed:  firstindex - 1,
		applied:    firstindex - 1,
		stabled:    lastindex,
		dummyIndex: firstindex - 1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	// ⚠️ 关键修改：不要压缩已提交但未应用的日志
	// 只能安全地压缩已经应用的索引
	compactIdx := first - 1
	if compactIdx > l.applied {
		compactIdx = l.applied
	}
	if compactIdx <= l.dummyIndex {
		return
	}

	offset := compactIdx - l.dummyIndex
	if offset >= uint64(len(l.entries)) {
		l.entries = nil
	} else {
		l.entries = l.entries[offset:]
	}
	l.dummyIndex = compactIdx
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).

	return append([]pb.Entry(nil), l.entries...)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	if l.stabled < l.dummyIndex {
		return nil
	}
	start := l.stabled - l.dummyIndex
	if start >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[start:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	if l.committed <= l.applied {
		return nil
	}

	lo := l.applied + 1
	hi := l.committed

	// 当前内存窗口的绝对范围是 [dummyIndex+1, LastIndex()]
	first := l.dummyIndex + 1
	last := l.LastIndex()

	if hi < first || lo > last {
		return nil
	}
	if lo < first {
		lo = first
	}
	if hi > last {
		hi = last
	}
	if lo > hi {
		return nil
	}

	start := lo - l.dummyIndex - 1
	end := hi - l.dummyIndex
	if end > uint64(len(l.entries)) {
		end = uint64(len(l.entries))
	}
	if start >= end {
		return nil
	}
	return append([]pb.Entry(nil), l.entries[start:end]...)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	lastindex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastindex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil &&
		i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	if i < l.dummyIndex {
		return 0, ErrCompacted
	}
	if i == l.dummyIndex {
		return l.storage.Term(i)
	}

	if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}

	first := l.dummyIndex + 1
	last := l.dummyIndex + uint64(len(l.entries))
	if i < first {
		return 0, ErrCompacted
	}
	if i > last {
		return 0, ErrUnavailable
	}

	offset := i - first
	return l.entries[offset].Term, nil
}
