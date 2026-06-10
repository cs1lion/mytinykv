// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()
	filters := []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true}}
	sources := filter.SelectSourceStores(stores, filters, cluster)
	targets := filter.SelectTargetStores(stores, filters, cluster)

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	for i := 0; i < len(sources) || i < len(targets); i++ {
		if i < len(sources) {
			source := sources[i]
			sourceID := source.GetID()
			for j := 0; j < balanceRegionRetryLimit; j++ {
				// 随机选一个 region（优先pending, 之后follower，其次 leader）
				region := cluster.RandPendingRegion(sourceID)
				if region == nil {
					region = cluster.RandFollowerRegion(sourceID, core.HealthRegion())
				}
				if region == nil {
					region = cluster.RandLeaderRegion(sourceID, core.HealthRegion())
				}
				if region == nil {
					continue
				}
				if len(region.GetPeers()) < cluster.GetMaxReplicas() {
					continue
				}
				// ... 第④步
				//从全局 targets 中选，排除已有副本的
				for _, target := range targets {
					if region.GetStorePeer(target.GetID()) != nil {
						continue
					}
					// 检查差距是否值得搬
					if source.GetRegionSize()-target.GetRegionSize() < 2*region.GetApproximateSize() {
						continue
					}
					// 分配新 peer
					newPeer, err := cluster.AllocPeer(target.GetID())
					if err != nil {
						continue
					}
					// 创建 MovePeer 操作
					op, err := operator.CreateMovePeerOperator(
						"balance-region", cluster, region,
						operator.OpBalance,
						sourceID, target.GetID(), newPeer.Id,
					)
					if err == nil && op != nil {
						return op
					}
				}

			}
		}
	}

	return nil
}
