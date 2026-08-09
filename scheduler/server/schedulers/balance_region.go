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
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
	filters      []filter.Filter
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
	s.filters = []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true, MoveRegion: true}}
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
	stores := cluster.GetStores()
	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)
	// The store with the largest region size is the source to move regions from,
	// and the one with the smallest region size is the target.
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	for _, source := range sources {
		sourceID := source.GetID()
		for i := 0; i < balanceRegionRetryLimit; i++ {
			if op := s.transferPeerOut(cluster, source, targets); op != nil {
				return op
			}
		}
		log.Debug("no operator created for selected store", zap.String("scheduler", s.GetName()), zap.Uint64("source-store", sourceID))
	}
	return nil
}

// transferPeerOut tries to find a region on the source store that can be moved
// to another store.
func (s *balanceRegionScheduler) transferPeerOut(cluster opt.Cluster, source *core.StoreInfo, targets []*core.StoreInfo) *operator.Operator {
	region := s.selectRegion(cluster, source.GetID())
	if region == nil {
		return nil
	}
	return s.createOperator(cluster, region, source, targets)
}

// selectRegion selects the most suitable region to move out of the store:
// pending region first (it may mean the disk is overloaded), then follower,
// finally leader.
func (s *balanceRegionScheduler) selectRegion(cluster opt.Cluster, storeID uint64) *core.RegionInfo {
	if region := cluster.RandPendingRegion(storeID, core.HealthRegionAllowPending()); region != nil {
		return region
	}
	if region := cluster.RandFollowerRegion(storeID, core.HealthRegion()); region != nil {
		return region
	}
	return cluster.RandLeaderRegion(storeID, core.HealthRegion())
}

// createOperator creates a move peer operator that moves a peer of the region
// from the source store to the most suitable target store.
func (s *balanceRegionScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source *core.StoreInfo, targets []*core.StoreInfo) *operator.Operator {
	for _, target := range targets {
		// The target store must not already hold a peer of the region.
		if _, ok := region.GetStoreIds()[target.GetID()]; ok {
			continue
		}
		if !s.shouldBalance(cluster, source, target, region) {
			continue
		}
		newPeer, err := cluster.AllocPeer(target.GetID())
		if err != nil {
			return nil
		}
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, source.GetID(), target.GetID(), newPeer.GetId())
		if err != nil {
			log.Debug("failed to create move peer operator", zap.Uint64("region-id", region.GetID()), zap.Error(err))
			return nil
		}
		return op
	}
	return nil
}

// shouldBalance checks whether moving the region from the source store to the
// target store is valuable: the region must have full replicas, and the
// difference between the two stores' region sizes must be greater than twice
// the region's approximate size, so that after the movement the target store is
// still smaller than the source store and the scheduler will not move the region
// back immediately.
func (s *balanceRegionScheduler) shouldBalance(cluster opt.Cluster, source, target *core.StoreInfo, region *core.RegionInfo) bool {
	// Only move a region with full replicas; making up for missing replicas is
	// the replica checker's job.
	if len(region.GetPeers()) != cluster.GetMaxReplicas() {
		return false
	}
	if source.GetRegionSize()-target.GetRegionSize() <= 2*region.GetApproximateSize() {
		return false
	}
	return true
}
