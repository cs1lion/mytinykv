package server

import (
	"bytes"
	"context"
	"math"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if lock.Ts <= txn.StartTS {
			return &kvrpcpb.GetResponse{

				Error: &kvrpcpb.KeyError{
					Locked: lock.Info(req.Key),
				},
			}, nil
		}
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	} else {
		return &kvrpcpb.GetResponse{

			Value: value,
		}, nil
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp := kvrpcpb.PrewriteResponse{
		Errors: make([]*kvrpcpb.KeyError, 0),
	}

	keys := make([][]byte, len(req.Mutations))
	for i, m := range req.Mutations {
		keys[i] = m.Key
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, mut := range req.Mutations {
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mut.Key),
			})
			continue
		}

		_, committs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if committs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    committs,
					ConflictTs: req.StartVersion,
					Key:        mut.Key,
				},
			})
			continue
		}
		switch mut.Op {
		case 0:
			txn.PutValue(mut.Key, mut.Value)
			txn.PutLock(mut.Key, &mvcc.Lock{
				Kind:    mvcc.WriteKindPut,
				Primary: req.PrimaryLock,
				Ttl:     req.LockTtl,
				Ts:      req.StartVersion,
			})
		case 1:
			txn.DeleteValue(mut.Key)

			txn.PutLock(mut.Key, &mvcc.Lock{
				Kind:    mvcc.WriteKindDelete,
				Primary: req.PrimaryLock,
				Ttl:     req.LockTtl,
				Ts:      req.StartVersion,
			})
		}

	}
	if len(resp.Errors) > 0 {
		return &resp, nil // 有错误，不写
	}
	err = server.storage.Write(req.Context, txn.Writes())

	if err != nil {
		return &resp, err
	}

	return &resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					return &kvrpcpb.CommitResponse{
						Error: &kvrpcpb.KeyError{
							Abort: "rollback",
						},
					}, nil
				}
				return &kvrpcpb.CommitResponse{}, nil
			}
			return &kvrpcpb.CommitResponse{}, nil
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "commit ts not match",
				},
			}, nil
		}
		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			Kind:    lock.Kind,
			StartTS: lock.Ts,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ScanResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)

	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(mvcc.EncodeKey(req.StartKey, math.MaxUint64))

	kvpairs := make([]*kvrpcpb.KvPair, 0)
	lastKey := make([]byte, 0)
	for iter.Valid() && len(kvpairs) < int(req.Limit) {
		item := iter.Item()
		userKey := mvcc.DecodeUserKey(item.Key())
		if !bytes.Equal(userKey, lastKey) {
			lock, err := txn.GetLock(userKey)
			if err != nil {
				return nil, err
			}
			if lock != nil && lock.Ts <= txn.StartTS {
				kvpairs = append(kvpairs, &kvrpcpb.KvPair{
					Key: userKey,
					Error: &kvrpcpb.KeyError{
						Locked: lock.Info(userKey),
					},
				})

				lastKey = userKey
				iter.Next()
				continue
			}
			value, err := txn.GetValue(userKey)
			if err != nil {
				return nil, err
			}
			if value != nil {
				kvpairs = append(kvpairs, &kvrpcpb.KvPair{
					Key:   userKey,
					Value: value,
				})
			}
			lastKey = userKey
			iter.Next()
			continue
		}
		iter.Next()
	}

	return &kvrpcpb.ScanResponse{Pairs: kvpairs}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CheckTxnStatusResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts == req.LockTs {
		if mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(req.LockTs) >= lock.Ttl {
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				Kind:    mvcc.WriteKindRollback,
				StartTS: req.LockTs,
			})
			err = server.storage.Write(req.Context, txn.Writes())

			if err != nil {
				return nil, err
			}

			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl:       0,
				CommitVersion: 0,
				Action:        kvrpcpb.Action_TTLExpireRollback,
			}, nil
		} else {
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl:       lock.Ttl,
				CommitVersion: 0,
				Action:        kvrpcpb.Action_NoAction,
			}, nil

		}
	} else {
		write, committs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.CheckTxnStatusResponse{
					CommitVersion: 0,
					LockTtl:       0,
					Action:        kvrpcpb.Action_NoAction,
				}, nil

			}

			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl:       0,
				CommitVersion: committs,
				Action:        kvrpcpb.Action_NoAction,
			}, nil

		}

		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.LockTs,
		})
		err = server.storage.Write(req.Context, txn.Writes())

		if err != nil {
			return nil, err
		}

		return &kvrpcpb.CheckTxnStatusResponse{
			LockTtl:       0,
			CommitVersion: 0,
			Action:        kvrpcpb.Action_LockNotExistRollback,
		}, nil
	}

}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.BatchRollbackResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			if lock.Ts == req.StartVersion {
				txn.DeleteLock(key)
				txn.DeleteValue(key)
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					Kind:    mvcc.WriteKindRollback,
					StartTS: req.StartVersion,
				})
			} else {
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					Kind:    mvcc.WriteKindRollback,
					StartTS: req.StartVersion,
				})
			}
			continue
		}
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				return &kvrpcpb.BatchRollbackResponse{
					Error: &kvrpcpb.KeyError{
						Retryable: "txn already commit",
					},
				}, nil
			}
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.StartVersion,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())

	if err != nil {
		return nil, err
	}
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionerror, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ResolveLockResponse{
				RegionError: regionerror.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, len(locks))
	for _, l := range locks {
		keys = append(keys, l.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, pair := range locks {
		if req.CommitVersion > 0 {
			txn.DeleteLock(pair.Key)
			txn.PutWrite(pair.Key, req.CommitVersion, &mvcc.Write{
				Kind:    pair.Lock.Kind,
				StartTS: req.StartVersion,
			})
		} else {
			txn.DeleteLock(pair.Key)
			txn.DeleteValue(pair.Key)
			txn.PutWrite(pair.Key, req.StartVersion, &mvcc.Write{
				Kind:    mvcc.WriteKindRollback,
				StartTS: req.StartVersion,
			})
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.ResolveLockResponse{}, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
