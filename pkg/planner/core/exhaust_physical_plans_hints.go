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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

func filterIndexJoinBySessionVars(sc base.PlanContext, indexJoins []base.PhysicalPlan) []base.PhysicalPlan {
	if sc.GetSessionVars().EnableIndexMergeJoin {
		return indexJoins
	}
	return slices.DeleteFunc(indexJoins, func(indexJoin base.PhysicalPlan) bool {
		_, ok := indexJoin.(*physicalop.PhysicalIndexMergeJoin)
		return ok
	})
}

const (
	joinLeft             = 0
	joinRight            = 1
	indexJoinMethod      = 0
	indexHashJoinMethod  = 1
	indexMergeJoinMethod = 2
)

func getIndexJoinSideAndMethod(join base.PhysicalPlan) (innerSide, joinMethod int, ok bool) {
	var innerIdx int
	switch ij := join.(type) {
	case *physicalop.PhysicalIndexJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexJoinMethod
	case *physicalop.PhysicalIndexHashJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexHashJoinMethod
	case *physicalop.PhysicalIndexMergeJoin:
		innerIdx = ij.GetInnerChildIdx()
		joinMethod = indexMergeJoinMethod
	default:
		return 0, 0, false
	}
	ok = true
	innerSide = joinLeft
	if innerIdx == 1 {
		innerSide = joinRight
	}
	return
}

// tryToEnumerateIndexJoin returns all available index join plans, which will require inner indexJoinProp downside
// compared with original tryToGetIndexJoin.
func tryToEnumerateIndexJoin(super base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	// according join type to enumerate index join with inner children's indexJoinProp.
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 1)...)
	}
	// Pre-Handle hints and variables about index join, which try to detect the contradictory hint and variables
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables and rec warns.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}
	// previously we will think about force index join hints here, but we have to wait the inner plans to be a valid
	// physical one/ones. Because indexJoinProp may not be admitted by its inner patterns, so we innovatively move all
	// hint related handling to the findBestTask function when we see the entire inner physical-ized plan tree. See xxx
	// for details.
	//
	// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
	// hint is specified in the query. So we can do it in physic enumeration phase here.
	return handleFilterIndexJoinHints(p, candidates)
}

// tryToGetIndexJoin returns all available index join plans, and the second returned value indicates whether this plan is enforced by hints.
func tryToGetIndexJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty) (indexJoins []base.PhysicalPlan, canForced bool) {
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 1)...)
	}

	// Handle hints and variables about index join.
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables.
	// Handle hints conflict first.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}

	candidates, canForced = handleForceIndexJoinHints(p, prop, candidates)
	if canForced {
		return candidates, canForced
	}
	candidates = handleFilterIndexJoinHints(p, candidates)
	// todo: if any variables banned it, why bother to generate it first?
	return filterIndexJoinBySessionVars(p.SCtx(), candidates), false
}

func enumerationContainIndexJoin(candidates [][]base.PhysicalPlan) bool {
	return slices.ContainsFunc(candidates, func(candidate []base.PhysicalPlan) bool {
		return slices.ContainsFunc(candidate, func(op base.PhysicalPlan) bool {
			_, _, ok := getIndexJoinSideAndMethod(op)
			return ok
		})
	})
}

// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
// hint is specified in the query. So we can do it in physic enumeration phase.
func handleFilterIndexJoinHints(p *logicalop.LogicalJoin, candidates []base.PhysicalPlan) []base.PhysicalPlan {
	if !p.PreferAny(h.PreferNoIndexJoin, h.PreferNoIndexHashJoin, h.PreferNoIndexMergeJoin) {
		return candidates // no filter index join hints
	}
	filtered := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		_, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferNoIndexJoin) && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexHashJoin) && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexMergeJoin) && joinMethod == indexMergeJoinMethod) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

func recordWarnings(lp base.LogicalPlan, prop *property.PhysicalProperty, inEnforce bool) error {
	switch x := lp.(type) {
	case *logicalop.LogicalAggregation:
		return recordAggregationHintWarnings(x)
	case *logicalop.LogicalTopN, *logicalop.LogicalLimit:
		return recordLimitToCopWarnings(lp)
	case *logicalop.LogicalJoin:
		return recordIndexJoinHintWarnings(x, prop, inEnforce)
	default:
		// no warnings to record
		return nil
	}
}

func recordAggregationHintWarnings(la *logicalop.LogicalAggregation) error {
	if la.PreferAggToCop {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint AGG_TO_COP is inapplicable")
	}
	return nil
}

func recordLimitToCopWarnings(lp base.LogicalPlan) error {
	var preferPushDown *bool
	switch lp := lp.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = &lp.PreferLimitToCop
	case *logicalop.LogicalLimit:
		preferPushDown = &lp.PreferLimitToCop
	default:
		return nil
	}
	if *preferPushDown {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint LIMIT_TO_COP is inapplicable")
	}
	return nil
}

// recordIndexJoinHintWarnings records the warnings msg if no valid preferred physic are picked.
// todo: extend recordIndexJoinHintWarnings to support all kind of operator's warnings handling.
func recordIndexJoinHintWarnings(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, inEnforce bool) error {
	// handle mpp join hints first.
	if (p.PreferJoinType&h.PreferShuffleJoin) > 0 || (p.PreferJoinType&h.PreferBCJoin) > 0 {
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
		} else {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
		}
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	// handle index join hints.
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return nil // no force index join hints
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	//
	// but for warnings handle inside findBestTask here, even the not-empty prop
	// will be reset to get the planNeedEnforce plans, but the prop passed down here will
	// still be the same, so here we change the admission to both.
	if prop.IsSortItemEmpty() || inEnforce {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		default:
			// only record warnings for index join hint not working now.
			return nil
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	return nil
}

func applyLogicalHintVarEigen(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	return applyLogicalJoinHint(lp, pp) ||
		applyLogicalTopNAndLimitHint(lp, pp, childTasks) ||
		applyLogicalAggregationHint(lp, pp, childTasks)
}

// Get the most preferred and efficient one by hint and low-cost priority.
// since hint applicable plan may greater than 1, like inl_join can suit for:
// index_join, index_hash_join, index_merge_join, we should chase the most efficient
// one among them.
// applyLogicalJoinHint is used to handle logic hint/prefer/variable, which is not a strong guide for optimization phase.
// It is changed from handleForceIndexJoinHints to handle the preferred join hint among several valid physical plan choices.
// It will return true if the hint can be applied when saw a real physic plan successfully built and returned up from child.
// we cache the most preferred one among this valid and preferred physic plans. If there is no preferred physic applicable
// for the logic hint, we will return false and the optimizer will continue to return the normal low-cost one.
func applyLogicalJoinHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	return preferMergeJoin(lp, physicPlan) || preferIndexJoinFamily(lp, physicPlan) ||
		preferHashJoin(lp, physicPlan)
}

func applyLogicalAggregationHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	la, ok := lp.(*logicalop.LogicalAggregation)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if la.HasDistinct() {
		// TODO: remove after the cost estimation of distinct pushdown is implemented.
		if la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			// when AllowDistinctAggPushDown is true, we will not consider root task type as before.
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			switch childTasks[0].(type) {
			case *physicalop.RootTask:
				// If the distinct agg can't be allowed to push down, we will consider root task type.
				// which is try to get the same behavior as before like types := {RootTask} only.
				return true
			case *physicalop.MppTask:
				// If the distinct agg can't be allowed to push down, we will consider mpp task type too --- RootTask vs MPPTask
				// which is try to get the same behavior as before like types := {RootTask} and appended {MPPTask}.
				return true
			default:
				return false
			}
		}
	} else if la.PreferAggToCop {
		// If the aggregation is preferred to be pushed down to coprocessor, we will prefer it.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
	}
	return false
}

func applyLogicalTopNAndLimitHint(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	hintPrefer, _ := pushLimitOrTopNForcibly(lp, pp)
	if hintPrefer {
		// if there is a user hint control, try to get the copTask as the prior.
		// here we don't assert task itself, because when topN attach 2 cop task, it will become root type automatically.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
		return false
	}
	return false
}

func hasNormalPreferTask(lp base.LogicalPlan, state *enumerateState, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	_, meetThreshold := pushLimitOrTopNForcibly(lp, pp)
	if meetThreshold {
		// previously, we set meetThreshold for pruning root task type but mpp task type. so:
		// 1: when one copTask exists, we will ignore root task type.
		// 2: when one copTask exists, another copTask should be cost compared with.
		// 3: mppTask is always in the cbo comparing.
		// 4: when none copTask exists, we will consider rootTask vs mppTask.
		// the following check priority logic is compatible with former pushLimitOrTopNForcibly prop pruning logic.
		_, isTopN := pp.(*physicalop.PhysicalTopN)
		if isTopN {
			if state.topNCopExist {
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return false
				}
				// peer cop task should compare the cost with each other.
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					return true
				}
			} else {
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					state.topNCopExist = true
					return true
				}
				// when we encounter rootTask type while still see no topNCopExist.
				// that means there is no copTask valid before, we will consider rootTask here.
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return true
				}
			}
			if _, ok := childTasks[0].(*physicalop.MppTask); ok {
				return true
			}
			// shouldn't be here
			return false
		}
		// limit case:
		if state.limitCopExist {
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return false
			}
			// peer cop task should compare the cost with each other.
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				state.limitCopExist = true
				return true
			}
			// when we encounter rootTask type while still see no limitCopExist.
			// that means there is no copTask valid before, we will consider rootTask here.
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return true
			}
		}
		if _, ok := childTasks[0].(*physicalop.MppTask); ok {
			return true
		}
	}
	return false
}

// hash join has two types:
// one is hash join type: normal hash join, shuffle join, broadcast join
// another is the build side hint type: prefer left as build side, prefer right as build side
// the first one is used to control the join type, the second one is used to control the build side of hash join.
// the priority is:
// once the join type is set, we should respect them first, not this type are all ignored.
// after we see all the joins under this type, then we only consider the build side hints satisfied or not.
//
// for the priority among the hash join types, we will respect the join fine-grained hints first, then the normal hash join type,
// that is, the priority is: shuffle join / broadcast join > normal hash join.
func preferHashJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		// for build hint conflict, restore all of them
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	physicalHashJoin, ok := physicPlan.(*physicalop.PhysicalHashJoin)
	if !ok {
		return false
	}
	// If the hint is set, we should prefer MPP shuffle join.
	preferShuffle := (p.PreferJoinType & h.PreferShuffleJoin) > 0
	preferBCJ := (p.PreferJoinType & h.PreferBCJoin) > 0
	if preferShuffle {
		if physicalHashJoin.StoreTp == kv.TiFlash && physicalHashJoin.MppShuffleJoin {
			// first: respect the shuffle join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	if preferBCJ {
		if physicalHashJoin.StoreTp == kv.TiFlash && !physicalHashJoin.MppShuffleJoin {
			// first: respect the broadcast join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	// Respect the join type and join side hints.
	if p.PreferJoinType&h.PreferHashJoin > 0 {
		// first: normal hash join hint are set.
		if forceLeftToBuild || forceRightToBuild {
			// second: respect the join side if join side hints are set.
			return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
				(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
		}
		// second: no join side hints are set, respect the join type is enough.
		return true
	}
	// no hash join type hint is set, we only need to respect the hash join side hints.
	return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
		(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
}

func preferMergeJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	_, ok = physicPlan.(*physicalop.PhysicalMergeJoin)
	return ok && p.PreferJoinType&h.PreferMergeJoin > 0
}

func preferIndexJoinFamily(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return false // no force index join hints
	}
	innerSide, joinMethod, ok := getIndexJoinSideAndMethod(physicPlan)
	if !ok {
		return false
	}
	if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
		// valid physic for the hint
		return true
	}
	return false
}

// handleForceIndexJoinHints handles the force index join hints and returns all plans that can satisfy the hints.
func handleForceIndexJoinHints(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, candidates []base.PhysicalPlan) (indexJoins []base.PhysicalPlan, canForced bool) {
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return candidates, false // no force index join hints
	}
	forced := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		innerSide, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
			forced = append(forced, candidate)
		}
	}

	if len(forced) > 0 {
		return forced, true
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	if prop.IsSortItemEmpty() {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
	}
	return candidates, false
}

func checkChildFitBC(pctx base.PlanContext, stats *property.StatsInfo, schema *expression.Schema) bool {
	if stats.HistColl == nil {
		return pctx.GetSessionVars().BroadcastJoinThresholdCount == -1 || stats.Count() < pctx.GetSessionVars().BroadcastJoinThresholdCount
	}
	avg := cardinality.GetAvgRowSize(pctx, stats.HistColl, schema.Columns, false, false)
	sz := avg * float64(stats.Count())
	return pctx.GetSessionVars().BroadcastJoinThresholdSize == -1 || sz < float64(pctx.GetSessionVars().BroadcastJoinThresholdSize)
}

func calcBroadcastExchangeSize(p base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	size = avg * row
	return row, size, true
}

func calcBroadcastExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcBroadcastExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcBroadcastExchangeSize(p2, mppStoreCnt)

	// broadcast exchange size:
	//   Build: (mppStoreCnt - 1) * sizeof(BuildTable)
	//   Probe: 0
	// choose the child plan with the maximum approximate value as Probe

	if hasSize1 && hasSize2 {
		return math.Min(row1, row2), math.Min(size1, size2), true
	}

	return math.Min(row1, row2), 0, false
}

func calcHashExchangeSize(p base.Plan, mppStoreCnt int) (row float64, sz float64, hasSize bool) {
	s := p.StatsInfo()
	row = float64(s.Count()) * float64(mppStoreCnt-1) / float64(mppStoreCnt)
	if s.HistColl == nil {
		return row, 0, false
	}
	avg := cardinality.GetAvgRowSize(p.SCtx(), s.HistColl, p.Schema().Columns, false, false)
	sz = avg * row
	return row, sz, true
}

func calcHashExchangeSizeByChild(p1 base.Plan, p2 base.Plan, mppStoreCnt int) (row float64, size float64, hasSize bool) {
	row1, size1, hasSize1 := calcHashExchangeSize(p1, mppStoreCnt)
	row2, size2, hasSize2 := calcHashExchangeSize(p2, mppStoreCnt)

	// hash exchange size:
	//   Build: sizeof(BuildTable) * (mppStoreCnt - 1) / mppStoreCnt
	//   Probe: sizeof(ProbeTable) * (mppStoreCnt - 1) / mppStoreCnt

	if hasSize1 && hasSize2 {
		return row1 + row2, size1 + size2, true
	}
	return row1 + row2, 0, false
}

// The size of `Build` hash table when using broadcast join is about `X`.
// The size of `Build` hash table when using shuffle join is about `X / (mppStoreCnt)`.
// It will cost more time to construct `Build` hash table and search `Probe` while using broadcast join.
// Set a scale factor (`mppStoreCnt^*`) when estimating broadcast join in `isJoinFitMPPBCJ` and `isJoinChildFitMPPBCJ` (based on TPCH benchmark, it has been verified in Q9).

func isJoinFitMPPBCJ(p *logicalop.LogicalJoin, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)
	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

func isJoinChildFitMPPBCJ(p *logicalop.LogicalJoin, childIndexToBC int, mppStoreCnt int) bool {
	rowBC, szBC, hasSizeBC := calcBroadcastExchangeSize(p.Children()[childIndexToBC], mppStoreCnt)
	rowHash, szHash, hasSizeHash := calcHashExchangeSizeByChild(p.Children()[0], p.Children()[1], mppStoreCnt)

	if hasSizeBC && hasSizeHash {
		return szBC*float64(mppStoreCnt) <= szHash
	}
	return rowBC*float64(mppStoreCnt) <= rowHash
}

func getJoinChildStatsAndSchema(ge base.GroupExpression, p base.LogicalPlan) (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	if ge != nil {
		g := ge.(*memo.GroupExpression)
		stats0, schema0 = g.Inputs[0].GetLogicalProperty().Stats, g.Inputs[0].GetLogicalProperty().Schema
		stats1, schema1 = g.Inputs[1].GetLogicalProperty().Stats, g.Inputs[1].GetLogicalProperty().Schema
	} else {
		stats1, schema1 = p.Children()[1].StatsInfo(), p.Children()[1].Schema()
		stats0, schema0 = p.Children()[0].StatsInfo(), p.Children()[0].Schema()
	}
	return
}

// If we can use mpp broadcast join, that's our first choice.
func preferMppBCJ(super base.LogicalPlan) bool {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	if len(p.EqualConditions) == 0 && p.SCtx().GetSessionVars().AllowCartesianBCJ == 2 {
		return true
	}

	onlyCheckChild1 := p.JoinType == base.LeftOuterJoin || p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin
	onlyCheckChild0 := p.JoinType == base.RightOuterJoin

	if p.SCtx().GetSessionVars().PreferBCJByExchangeDataSize {
		mppStoreCnt, err := p.SCtx().GetMPPClient().GetMPPStoreCount()

		// No need to exchange data if there is only ONE mpp store. But the behavior of optimizer is unexpected if use broadcast way forcibly, such as tpch q4.
		// TODO: always use broadcast way to exchange data if there is only ONE mpp store.

		if err == nil && mppStoreCnt > 0 {
			if !(onlyCheckChild1 || onlyCheckChild0) {
				return isJoinFitMPPBCJ(p, mppStoreCnt)
			}
			if mppStoreCnt > 1 {
				if onlyCheckChild1 {
					return isJoinChildFitMPPBCJ(p, 1, mppStoreCnt)
				} else if onlyCheckChild0 {
					return isJoinChildFitMPPBCJ(p, 0, mppStoreCnt)
				}
			}
			// If mppStoreCnt is ONE and only need to check one child plan, rollback to original way.
			// Otherwise, the plan of tpch q4 may be unexpected.
		}
	}
	stats0, stats1, schema0, schema1 := getJoinChildStatsAndSchema(ge, p)
	pctx := p.SCtx()
	if onlyCheckChild1 {
		return checkChildFitBC(pctx, stats1, schema1)
	} else if onlyCheckChild0 {
		return checkChildFitBC(pctx, stats0, schema0)
	}
	return checkChildFitBC(pctx, stats0, schema0) || checkChildFitBC(pctx, stats1, schema1)
}

func canExprsInJoinPushdown(p *logicalop.LogicalJoin, storeType kv.StoreType) bool {
	equalExprs := make([]expression.Expression, 0, len(p.EqualConditions))
	for _, eqCondition := range p.EqualConditions {
		if eqCondition.FuncName.L == ast.NullEQ {
			return false
		}
		equalExprs = append(equalExprs, eqCondition)
	}
	pushDownCtx := util.GetPushDownCtx(p.SCtx())
	if !expression.CanExprsPushDown(pushDownCtx, equalExprs, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.LeftConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.RightConditions, storeType) {
		return false
	}
	if !expression.CanExprsPushDown(pushDownCtx, p.OtherConditions, storeType) {
		return false
	}
	return true
}

func tryToGetMppHashJoin(super base.LogicalPlan, prop *property.PhysicalProperty, useBCJ bool) []base.PhysicalPlan {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}

	if !expression.IsPushDownEnabled(p.JoinType.String(), kv.TiFlash) {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is blocked by blacklist, check `table mysql.expr_pushdown_blacklist;` for more information.")
		return nil
	}

	if p.JoinType != base.InnerJoin && p.JoinType != base.LeftOuterJoin && p.JoinType != base.RightOuterJoin && p.JoinType != base.SemiJoin && p.JoinType != base.AntiSemiJoin && p.JoinType != base.LeftOuterSemiJoin && p.JoinType != base.AntiLeftOuterSemiJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because join type `" + p.JoinType.String() + "` is not supported now.")
		return nil
	}

	if len(p.EqualConditions) == 0 {
		if !useBCJ {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variables `tidb_broadcast_join_threshold_size` and `tidb_broadcast_join_threshold_count`.")
			return nil
		}
		if p.SCtx().GetSessionVars().AllowCartesianBCJ == 0 {
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because `Cartesian Product` is only supported by broadcast join, check value and documents of variable `tidb_opt_broadcast_cartesian_join`.")
			return nil
		}
	}
	if len(p.LeftConditions) != 0 && p.JoinType != base.LeftOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `left join` but has left conditions, which is not supported by mpp now, see github.com/pingcap/tidb/issues/26090 for more information.")
		return nil
	}
	if len(p.RightConditions) != 0 && p.JoinType != base.RightOuterJoin {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because there is a join that is not `right join` but has right conditions, which is not supported by mpp now.")
		return nil
	}

	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}
	if !canExprsInJoinPushdown(p, kv.TiFlash) {
		return nil
	}
	lkeys, rkeys, _, _ := p.GetJoinKeys()
	lNAkeys, rNAKeys := p.GetNAJoinKeys()
	// check match property
	baseJoin := physicalop.BasePhysicalJoin{
		JoinType:        p.JoinType,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		DefaultValues:   p.DefaultValues,
		LeftJoinKeys:    lkeys,
		RightJoinKeys:   rkeys,
		LeftNAJoinKeys:  lNAkeys,
		RightNAJoinKeys: rNAKeys,
	}
	// It indicates which side is the build side.
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
			"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are conflicts, please check the hints")
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	preferredBuildIndex := 0
	fixedBuildSide := false // Used to indicate whether the build side for the MPP join is fixed or not.
	stats0, stats1, _, _ := getJoinChildStatsAndSchema(ge, p)
	if p.JoinType == base.InnerJoin {
		if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	} else if p.JoinType.IsSemiJoin() {
		if !useBCJ && !p.IsNAAJ() && len(p.EqualConditions) > 0 && (p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin) {
			// TiFlash only supports Non-null_aware non-cross semi/anti_semi join to use both sides as build side
			preferredBuildIndex = 1
			// MPPOuterJoinFixedBuildSide default value is false
			// use MPPOuterJoinFixedBuildSide here as a way to disable using left table as build side
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide && stats1.Count() > stats0.Count() {
				preferredBuildIndex = 0
			}
		} else {
			preferredBuildIndex = 1
			fixedBuildSide = true
		}
	}
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		// TiFlash does not require that the build side must be the inner table for outer join.
		// so we can choose the build side based on the row count, except that:
		// 1. it is a broadcast join(for broadcast join, it makes sense to use the broadcast side as the build side)
		// 2. or session variable MPPOuterJoinFixedBuildSide is set to true
		// 3. or nullAware/cross joins
		if useBCJ || p.IsNAAJ() || len(p.EqualConditions) == 0 || p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
			if !p.SCtx().GetSessionVars().MPPOuterJoinFixedBuildSide {
				// The hint has higher priority than variable.
				fixedBuildSide = true
			}
			if p.JoinType == base.LeftOuterJoin {
				preferredBuildIndex = 1
			}
		} else if stats0.Count() > stats1.Count() {
			preferredBuildIndex = 1
		}
	}

	if forceLeftToBuild || forceRightToBuild {
		match := (forceLeftToBuild && preferredBuildIndex == 0) || (forceRightToBuild && preferredBuildIndex == 1)
		if !match {
			if fixedBuildSide {
				// A warning will be generated if the build side is fixed, but we attempt to change it using the hint.
				p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
					"Some HASH_JOIN_BUILD and HASH_JOIN_PROBE hints cannot be utilized for MPP joins, please check the hints")
			} else {
				// The HASH_JOIN_BUILD OR HASH_JOIN_PROBE hints can take effective.
				preferredBuildIndex = 1 - preferredBuildIndex
			}
		}
	}

	// set preferredBuildIndex for test
	failpoint.Inject("mockPreferredBuildIndex", func(val failpoint.Value) {
		if !p.SCtx().GetSessionVars().InRestrictedSQL {
			preferredBuildIndex = val.(int)
		}
	})

	baseJoin.InnerChildIdx = preferredBuildIndex
	childrenProps := make([]*property.PhysicalProperty, 2)
	if useBCJ {
		childrenProps[preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.BroadcastType, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		expCnt := math.MaxFloat64
		if prop.ExpectedCnt < p.StatsInfo().RowCount {
			expCntScale := prop.ExpectedCnt / p.StatsInfo().RowCount
			var targetStats *property.StatsInfo
			if 1-preferredBuildIndex == 0 {
				targetStats = stats0
			} else {
				targetStats = stats1
			}
			expCnt = targetStats.RowCount * expCntScale
		}
		if prop.MPPPartitionTp == property.HashType {
			lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
			hashKeys := rPartitionKeys
			if preferredBuildIndex == 1 {
				hashKeys = lPartitionKeys
			}
			matches := prop.IsSubsetOf(hashKeys)
			if len(matches) == 0 {
				return nil
			}
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.HashType, MPPPartitionCols: prop.MPPPartitionCols, CTEProducerStatus: prop.CTEProducerStatus}
		} else {
			childrenProps[1-preferredBuildIndex] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: expCnt, MPPPartitionTp: property.AnyType, CTEProducerStatus: prop.CTEProducerStatus}
		}
	} else {
		lPartitionKeys, rPartitionKeys := p.GetPotentialPartitionKeys()
		if prop.MPPPartitionTp == property.HashType {
			var matches []int
			switch p.JoinType {
			case base.InnerJoin:
				if matches = prop.IsSubsetOf(lPartitionKeys); len(matches) == 0 {
					matches = prop.IsSubsetOf(rPartitionKeys)
				}
			case base.RightOuterJoin:
				// for right out join, only the right partition keys can possibly matches the prop, because
				// the left partition keys will generate NULL values randomly
				// todo maybe we can add a null-sensitive flag in the MPPPartitionColumn to indicate whether the partition column is
				//  null-sensitive(used in aggregation) or null-insensitive(used in join)
				matches = prop.IsSubsetOf(rPartitionKeys)
			default:
				// for left out join, only the left partition keys can possibly matches the prop, because
				// the right partition keys will generate NULL values randomly
				// for semi/anti semi/left out semi/anti left out semi join, only left partition keys are returned,
				// so just check the left partition keys
				matches = prop.IsSubsetOf(lPartitionKeys)
			}
			if len(matches) == 0 {
				return nil
			}
			lPartitionKeys = property.ChoosePartitionKeys(lPartitionKeys, matches)
			rPartitionKeys = property.ChoosePartitionKeys(rPartitionKeys, matches)
		}
		childrenProps[0] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: lPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
		childrenProps[1] = &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, MPPPartitionTp: property.HashType, MPPPartitionCols: rPartitionKeys, CanAddEnforcer: true, CTEProducerStatus: prop.CTEProducerStatus}
	}
	join := physicalop.PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		Concurrency:       uint(p.SCtx().GetSessionVars().CopTiFlashConcurrencyFactor),
		EqualConditions:   p.EqualConditions,
		NAEqualConditions: p.NAEQConditions,
		StoreTp:           kv.TiFlash,
		MppShuffleJoin:    !useBCJ,
		// Mpp Join has quite heavy cost. Even limit might not suspend it in time, so we don't scale the count.
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childrenProps...)
	join.SetSchema(p.Schema())
	return []base.PhysicalPlan{join}
}

// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func exhaustPhysicalPlans4LogicalJoin(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)

	if !isJoinHintSupportedInMPPMode(p.PreferJoinType) {
		if hasMPPJoinHints(p.PreferJoinType) {
			// If there are MPP hints but has some conflicts join method hints, all the join hints are invalid.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now")
			p.PreferJoinType = 0
		} else {
			// If there are no MPP hints but has some conflicts join method hints, the MPP mode will be blocked.
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.")
			if prop.IsFlashProp() {
				return nil, false, nil
			}
		}
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil, false, nil
	}
	joins := make([]base.PhysicalPlan, 0, 8)
	// we lift the p.canPushToTiFlash check here, because we want to generate all the plans to be decided by the attachment layer.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		// prefer hint should be handled in the attachment layer. because the enumerated mpp join may couldn't be built bottom-up.
		if hasMPPJoinHints(p.PreferJoinType) {
			// generate them all for later attachment prefer picking. cause underlying ds may not have tiFlash path.
			// even all mpp join is invalid, they can still resort to root joins as an alternative.
			joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
		} else {
			// join don't have a mpp join hints, only generate preferMppBCJ mpp joins.
			if preferMppBCJ(super) {
				joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			} else {
				joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
			}
		}
	} else {
		hasMppHints := false
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
			hasMppHints = true
		}
		if (p.PreferJoinType & h.PreferBCJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		return joins, true, nil
	}

	if !p.IsNAAJ() {
		// naaj refuse merge join and index join.
		stats0, stats1, _, _ := getJoinChildStatsAndSchema(ge, p)
		mergeJoins := physicalop.GetMergeJoin(p, prop, p.Schema(), p.StatsInfo(), stats0, stats1)
		if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
			return mergeJoins, true, nil
		}
		joins = append(joins, mergeJoins...)

		if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
			indexJoins := tryToEnumerateIndexJoin(super, prop)
			joins = append(joins, indexJoins...)

			failpoint.Inject("MockOnlyEnableIndexHashJoinV2", func(val failpoint.Value) {
				if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
					indexHashJoin := make([]base.PhysicalPlan, 0, len(indexJoins))
					for _, one := range indexJoins {
						if _, ok := one.(*physicalop.PhysicalIndexHashJoin); ok {
							indexHashJoin = append(indexHashJoin, one)
						}
					}
					failpoint.Return(indexHashJoin, true, nil)
				}
			})
		} else {
			indexJoins, forced := tryToGetIndexJoin(p, prop)
			if forced {
				return indexJoins, true, nil
			}
			joins = append(joins, indexJoins...)
		}
	}

	hashJoins, forced := getHashJoins(super, prop)
	if forced && len(hashJoins) > 0 {
		return hashJoins, true, nil
	}
	joins = append(joins, hashJoins...)

	if p.PreferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false, nil
	}
	return joins, true, nil
}

func pushLimitOrTopNForcibly(p base.LogicalPlan, pp base.PhysicalPlan) (preferPushDown bool, meetThreshold bool) {
	switch lp := p.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = lp.PreferLimitToCop
		if _, isPhysicalLimit := pp.(*physicalop.PhysicalLimit); isPhysicalLimit {
			// For query using orderby + limit, the physicalop can be PhysicalLimit
			// when its corresponding logicalop is LogicalTopn.
			// And for PhysicalLimit, it's always better to let it pushdown to tikv.
			meetThreshold = true
		} else {
			meetThreshold = lp.Count+lp.Offset <= uint64(lp.SCtx().GetSessionVars().LimitPushDownThreshold)
		}
	case *logicalop.LogicalLimit:
		preferPushDown = lp.PreferLimitToCop
		// Always push Limit down in this case since it has no side effect
		meetThreshold = true
	default:
		return
	}

	// we remove the child subTree check, each logical operator only focus on themselves.
	// for current level, they prefer a push-down copTask.
	return
}

// GetHashJoin is public for cascades planner.
func GetHashJoin(ge base.GroupExpression, la *logicalop.LogicalApply, prop *property.PhysicalProperty) *physicalop.PhysicalHashJoin {
	return getHashJoin(ge, &la.LogicalJoin, prop, 1, false)
}

// exhaustPhysicalPlans4LogicalApply generates the physical plan for a logical apply.
func exhaustPhysicalPlans4LogicalApply(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, la := base.GetGEAndLogicalOp[*logicalop.LogicalApply](super)
	_, _, schema0, _ := getJoinChildStatsAndSchema(ge, la)
	if !prop.AllColsFromSchema(schema0) || prop.IsFlashProp() { // for convenient, we don't pass through any prop
		la.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Apply` is not supported now.")
		return nil, true, nil
	}
	if !prop.IsSortItemEmpty() && la.SCtx().GetSessionVars().EnableParallelApply {
		la.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("Parallel Apply rejects the possible order properties of its outer child currently"))
		return nil, true, nil
	}
	join := GetHashJoin(ge, la, prop)
	var columns = make([]*expression.Column, 0, len(la.CorCols))
	for _, colColumn := range la.CorCols {
		// fix the liner warning.
		tmp := colColumn
		columns = append(columns, &tmp.Column)
	}
	cacheHitRatio := 0.0
	if la.StatsInfo().RowCount != 0 {
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(la.SCtx(), columns, la.Schema(), la.StatsInfo())
		// for example, if there are 100 rows and the number of distinct values of these correlated columns
		// are 70, then we can assume 30 rows can hit the cache so the cache hit ratio is 1 - (70/100) = 0.3
		cacheHitRatio = 1 - (ndv / la.StatsInfo().RowCount)
	}

	var canUseCache bool
	if cacheHitRatio > 0.1 && la.SCtx().GetSessionVars().MemQuotaApplyCache > 0 {
		canUseCache = true
	} else {
		canUseCache = false
	}

	apply := physicalop.PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
		CanUseCache:      canUseCache,
	}.Init(la.SCtx(),
		la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt),
		la.QueryBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: true},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	apply.SetSchema(la.Schema())
	return []base.PhysicalPlan{apply}, true, nil
}
