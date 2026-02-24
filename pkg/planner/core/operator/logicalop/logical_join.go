// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"bytes"
	"fmt"
	"maps"
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/constraint"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	utilhint "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	LogicalSchemaProducer `hash64-equals:"true"`

	JoinType     base.JoinType `hash64-equals:"true"`
	Reordered    bool
	StraightJoin bool

	// HintInfo stores the join algorithm hint information specified by client.
	HintInfo            *utilhint.PlanHints
	PreferJoinType      uint
	PreferJoinOrder     bool
	LeftPreferJoinType  uint
	RightPreferJoinType uint

	EqualConditions []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	// NAEQConditions means null aware equal conditions, which is used for null aware semi joins.
	NAEQConditions  []*expression.ScalarFunction `hash64-equals:"true" shallow-ref:"true"`
	LeftConditions  expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	RightConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`
	OtherConditions expression.CNFExprs          `hash64-equals:"true" shallow-ref:"true"`

	LeftProperties  [][]*expression.Column
	RightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// FullSchema contains all the columns that the Join can output. It's ordered as [outer schema..., inner schema...].
	// This is useful for natural joins and "using" joins. In these cases, the join key columns from the
	// inner side (or the right side when it's an inner join) will not be in the schema of Join.
	// But upper operators should be able to find those "redundant" columns, and the user also can specifically select
	// those columns, so we put the "redundant" columns here to make them be able to be found.
	//
	// For example:
	// create table t1(a int, b int); create table t2(a int, b int);
	// select * from t1 join t2 using (b);
	// schema of the Join will be [t1.b, t1.a, t2.a]; FullSchema will be [t1.a, t1.b, t2.a, t2.b].
	//
	// We record all columns and keep them ordered is for correctly handling SQLs like
	// select t1.*, t2.* from t1 join t2 using (b);
	// (*PlanBuilder).unfoldWildStar() handles the schema for such case.
	FullSchema *expression.Schema
	FullNames  types.NameSlice

	// EqualCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	EqualCondOutCnt float64

	// allJoinLeaf is used to identify the table where the column is located during constant propagation.
	allJoinLeaf []*expression.Schema
}

// Init initializes LogicalJoin.
func (p LogicalJoin) Init(ctx base.PlanContext, offset int) *LogicalJoin {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeJoin, &p, offset)
	return &p
}

// *************************** start implementation of Plan interface ***************************

// ExplainInfo implements Plan interface.
func (p *LogicalJoin) ExplainInfo() string {
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		fmt.Fprintf(buffer, ", equal:%v", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(evalCtx, p.OtherConditions))
	}
	return buffer.String()
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (p *LogicalJoin) ReplaceExprColumns(replace map[string]*expression.Column) {
	for i, equalExpr := range p.EqualConditions {
		p.EqualConditions[i] = ruleutil.ResolveExprAndReplace(equalExpr, replace).(*expression.ScalarFunction)
	}
	for i, leftExpr := range p.LeftConditions {
		p.LeftConditions[i] = ruleutil.ResolveExprAndReplace(leftExpr, replace)
	}
	for i, rightExpr := range p.RightConditions {
		p.RightConditions[i] = ruleutil.ResolveExprAndReplace(rightExpr, replace)
	}
	for i, otherExpr := range p.OtherConditions {
		p.OtherConditions[i] = ruleutil.ResolveExprAndReplace(otherExpr, replace)
	}
}

// *************************** end implementation of Plan interface ***************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.LogicalPlan.<0th> implementation.

// PredicatePushDown implements the base.LogicalPlan.<1st> interface.
func (p *LogicalJoin) PredicatePushDown(predicates []expression.Expression) (ret []expression.Expression, retPlan base.LogicalPlan, err error) {
	switch p.JoinType {
	case base.AntiLeftOuterSemiJoin, base.LeftOuterSemiJoin, base.AntiSemiJoin:
		// For LeftOuterSemiJoin and AntiLeftOuterSemiJoin, we can actually generate
		// `col is not null` according to expressions in `OtherConditions` now, but we
		// are putting column equal condition converted from `in (subq)` into
		// `OtherConditions`(@sa https://github.com/pingcap/tidb/pull/9051), then it would
		// cause wrong results, so we disable this optimization for outer semi joins now.
	case base.SemiJoin, base.InnerJoin:
		// It will be better to simplify OtherConditions through predicate pushdown for SemiJoin and InnerJoin,
	default:
		// Join ON conditions are not simplified through predicate pushdown.
		// However, we still need to eliminate obvious logical constants in OtherConditions
		// (e.g. "a = b OR 0") to avoid losing join keys.
		p.OtherConditions = ruleutil.ApplyPredicateSimplification(
			p.SCtx(),
			p.OtherConditions,
			false,
			nil,
		)
	}
	simplifyOuterJoin(p, predicates)
	var equalCond []*expression.ScalarFunction
	var leftPushCond, rightPushCond, otherCond, leftCond, rightCond []expression.Expression
	p.allJoinLeaf = getAllJoinLeaf(p)
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin)
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), false, nil)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive left where condition, because right where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, true, false)
		leftCond = leftPushCond
		// Handle join conditions, only derive right join condition, because left join condition cannot be pushed down
		_, derivedRightJoinCond := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), false, true)
		rightCond = append(p.RightConditions, derivedRightJoinCond...)
		p.RightConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, rightPushCond...)
	case base.RightOuterJoin:
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), true, nil)
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForRightOuterJoin)
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
		}
		// Handle where conditions
		predicates = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), predicates)
		// Only derive right where condition, because left where condition cannot be pushed down
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(predicates, false, true)
		rightCond = rightPushCond
		// Handle join conditions, only derive left join condition, because right join condition cannot be pushed down
		derivedLeftJoinCond, _ := DeriveOtherConditions(
			p, p.Children()[0].Schema(), p.Children()[1].Schema(), true, false)
		leftCond = append(p.LeftConditions, derivedLeftJoinCond...)
		p.LeftConditions = nil
		ret = append(expression.ScalarFuncs2Exprs(equalCond), otherCond...)
		ret = append(ret, leftPushCond...)
	case base.SemiJoin, base.InnerJoin:
		tempCond := make([]expression.Expression, 0, len(p.LeftConditions)+len(p.RightConditions)+len(p.EqualConditions)+len(p.OtherConditions)+len(predicates))
		tempCond = append(tempCond, p.LeftConditions...)
		tempCond = append(tempCond, p.RightConditions...)
		tempCond = append(tempCond, expression.ScalarFuncs2Exprs(p.EqualConditions)...)
		tempCond = append(tempCond, p.OtherConditions...)
		tempCond = append(tempCond, predicates...)
		tempCond = expression.ExtractFiltersFromDNFs(p.SCtx().GetExprCtx(), tempCond)
		tempCond = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), tempCond,
			p.Children()[0].Schema(), p.Children()[1].Schema(),
			true, p.isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, tempCond)
		if dual != nil {
			return ret, dual, nil
		}
		equalCond, leftPushCond, rightPushCond, otherCond = p.extractOnCondition(tempCond, true, true)
		p.LeftConditions = nil
		p.RightConditions = nil
		p.EqualConditions = equalCond
		p.OtherConditions = otherCond
		leftCond = leftPushCond
		rightCond = rightPushCond
	case base.AntiSemiJoin:
		predicates = p.outerJoinPropConst(predicates, p.isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin)
		predicates = ruleutil.ApplyPredicateSimplificationForJoin(p.SCtx(), predicates,
			p.Children()[0].Schema(), p.Children()[1].Schema(), true,
			p.isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin)
		// Return table dual when filter is constant false or null.
		dual := Conds2TableDual(p, predicates)
		if dual != nil {
			return ret, dual, nil
		}
		// `predicates` should only contain left conditions or constant filters.
		_, leftPushCond, rightPushCond, _ = p.extractOnCondition(predicates, true, true)
		// Do not derive `is not null` for anti join, since it may cause wrong results.
		// For example:
		// `select * from t t1 where t1.a not in (select b from t t2)` does not imply `t2.b is not null`,
		// `select * from t t1 where t1.a not in (select a from t t2 where t1.b = t2.b` does not imply `t1.b is not null`,
		// `select * from t t1 where not exists (select * from t t2 where t2.a = t1.a)` does not imply `t1.a is not null`,
		leftCond = leftPushCond
		rightCond = append(p.RightConditions, rightPushCond...)
		p.RightConditions = nil
	}
	leftCond = expression.RemoveDupExprs(leftCond)
	rightCond = expression.RemoveDupExprs(rightCond)
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	children := p.Children()
	rightChild := children[1]
	leftChild := children[0]
	rightCond = constraint.DeleteTrueExprsBySchema(evalCtx, rightChild.Schema(), rightCond)
	leftCond = constraint.DeleteTrueExprsBySchema(evalCtx, leftChild.Schema(), leftCond)
	leftRet, lCh, err := leftChild.PredicatePushDown(leftCond)
	if err != nil {
		return nil, nil, err
	}
	rightRet, rCh, err := rightChild.PredicatePushDown(rightCond)
	if err != nil {
		return nil, nil, err
	}
	AddSelection(p, lCh, leftRet, 0)
	AddSelection(p, rCh, rightRet, 1)
	p.updateEQCond()
	ruleutil.BuildKeyInfoPortal(p)
	newnChild, err := p.SemiJoinRewrite()
	return ret, newnChild, err
}

// simplifyOuterJoin transforms "LeftOuterJoin/RightOuterJoin" to "InnerJoin" if possible.
func simplifyOuterJoin(p *LogicalJoin, predicates []expression.Expression) {
	if p.JoinType != base.LeftOuterJoin && p.JoinType != base.RightOuterJoin && p.JoinType != base.InnerJoin {
		return
	}

	innerTable := p.children[0]
	outerTable := p.children[1]
	if p.JoinType == base.LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
	}

	if p.JoinType == base.InnerJoin {
		return
	}
	// then simplify embedding outer join.
	canBeSimplified := false
	for _, expr := range predicates {
		// avoid the case where the expr only refers to the schema of outerTable
		if expression.ExprFromSchema(expr, outerTable.Schema()) {
			continue
		}
		isOk := isNullRejected(p.SCtx(), innerTable.Schema(), expr)
		if isOk {
			canBeSimplified = true
			break
		}
	}
	if canBeSimplified {
		p.JoinType = base.InnerJoin
	}
}

// isNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func isNullRejected(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	exprCtx := exprctx.WithNullRejectCheck(ctx.GetExprCtx())
	expr = expression.PushDownNot(exprCtx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	for _, cond := range expression.SplitCNFItems(expr) {
		if isNullRejectedSpecially(ctx, schema, expr) {
			return true
		}

		result, err := expression.EvaluateExprWithNull(exprCtx, schema, cond, true)
		if err != nil {
			return false
		}
		x, ok := result.(*expression.Constant)
		if !ok {
			continue
		}
		if x.Value.IsNull() {
			return true
		} else if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
			return true
		}
	}
	return false
}

// isNullRejectedSpecially handles some null-rejected cases specially, since the current in
// EvaluateExprWithNull is too strict for some cases, e.g. #49616.
func isNullRejectedSpecially(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	return specialNullRejectedCase1(ctx, schema, expr) // only 1 case now
}

// specialNullRejectedCase1 is mainly for #49616.
// Case1 specially handles `null-rejected OR (null-rejected AND {others})`, then no matter what the result
// of `{others}` is (True, False or Null), the result of this predicate is null, so this predicate is null-rejected.
func specialNullRejectedCase1(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	isFunc := func(e expression.Expression, lowerFuncName string) *expression.ScalarFunction {
		f, ok := e.(*expression.ScalarFunction)
		if !ok {
			return nil
		}
		if f.FuncName.L == lowerFuncName {
			return f
		}
		return nil
	}
	orFunc := isFunc(expr, ast.LogicOr)
	if orFunc == nil {
		return false
	}
	for i := range 2 {
		andFunc := isFunc(orFunc.GetArgs()[i], ast.LogicAnd)
		if andFunc == nil {
			continue
		}
		if !isNullRejected(ctx, schema, orFunc.GetArgs()[1-i]) {
			continue // the other side should be null-rejected: null-rejected OR (... AND ...)
		}
		for _, andItem := range expression.SplitCNFItems(andFunc) {
			if isNullRejected(ctx, schema, andItem) {
				return true // hit the case in the comment: null-rejected OR (null-rejected AND ...)
			}
		}
	}
	return false
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
func (p *LogicalJoin) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	leftCols, rightCols := p.ExtractUsedCols(parentUsedCols)

	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(leftCols)
	if err != nil {
		return nil, err
	}

	p.Children()[1], err = p.Children()[1].PruneColumns(rightCols)
	if err != nil {
		return nil, err
	}

	p.MergeSchema()
	if p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		joinCol := p.Schema().Columns[len(p.Schema().Columns)-1]
		parentUsedCols = append(parentUsedCols, joinCol)
	}
	p.InlineProjection(parentUsedCols)
	return p, nil
}

// FindBestTask inherits the BaseLogicalPlan.LogicalPlan.<3rd> implementation.

// BuildKeyInfo implements the base.LogicalPlan.<4th> interface.
func (p *LogicalJoin) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	p.LogicalSchemaProducer.BuildKeyInfo(selfSchema, childSchema)
	switch p.JoinType {
	case base.SemiJoin, base.LeftOuterSemiJoin, base.AntiSemiJoin, base.AntiLeftOuterSemiJoin:
		selfSchema.PKOrUK = childSchema[0].Clone().PKOrUK
	case base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		// Extract all left and right columns from equal conditions
		leftCols := make([]*expression.Column, 0, len(p.EqualConditions))
		rightCols := make([]*expression.Column, 0, len(p.EqualConditions))
		for _, expr := range p.EqualConditions {
			l, r := expression.ExtractColumnsFromColOpCol(expr)
			leftCols = append(leftCols, l)
			rightCols = append(rightCols, r)
		}
		checkColumnsMatchPKOrUK := func(cols []*expression.Column, pkOrUK []expression.KeyInfo) bool {
			if len(pkOrUK) == 0 {
				return false
			}
			colSet := intset.NewFastIntSet()
			for _, col := range cols {
				colSet.Insert(int(col.UniqueID))
			}
			// Check if any key is a subset of the join key columns
			// A match is considered valid if join key columns contain all columns of any PKOrUK
			for _, key := range pkOrUK {
				allMatch := true
				for _, k := range key {
					if !colSet.Has(int(k.UniqueID)) {
						allMatch = false
						break
					}
				}
				if allMatch {
					return true
				}
			}
			return false
		}
		lOk := checkColumnsMatchPKOrUK(leftCols, childSchema[0].PKOrUK)
		rOk := checkColumnsMatchPKOrUK(rightCols, childSchema[1].PKOrUK)

		// When both sides match different unique keys (e.g., left side matches unique key setA, right side matches unique key setB),
		// we can derive additional functional dependencies through join conditions:
		// 1. Original FDs: setA -> left row, setB -> right row
		// 2. Join condition: setA from left = setA from right
		// 3. By augmentation rule (X->Y implies XZ->YZ): setA from right + setB -> left row + setB
		// 4. By transitivity rule (X->Y and Y->Z implies X->Z): setA from right + setB -> left row + right row
		// This allows us to preserve unique key information from both sides when join keys provide stronger uniqueness guarantees.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != base.LeftOuterJoin {
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, childSchema[1].PKOrUK...)
		}
		if rOk && p.JoinType != base.RightOuterJoin {
			selfSchema.PKOrUK = append(selfSchema.PKOrUK, childSchema[0].PKOrUK...)
		}
	}
}

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalJoin) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	topnEliminated := false
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		p.Children()[0], topnEliminated = p.pushDownTopNToChild(topN, 0)
		p.Children()[1] = p.Children()[1].PushDownTopN(nil)
	case base.RightOuterJoin:
		p.Children()[1], topnEliminated = p.pushDownTopNToChild(topN, 1)
		p.Children()[0] = p.Children()[0].PushDownTopN(nil)
	default:
		return p.BaseLogicalPlan.PushDownTopN(topN)
	}

	// The LogicalJoin may be also a LogicalApply. So we must use self to set parents.
	if topN != nil {
		if topnEliminated {
			// Add a sort if the topN has order by items.
			if len(topN.ByItems) > 0 {
				sort := LogicalSort{ByItems: topN.ByItems}.Init(p.SCtx(), p.QueryBlockOffset())
				sort.SetChildren(p.Self())
				return sort
			}
			// If the topN has no order by items, simply return the join itself.
			return p.Self()
		}
		return topN.AttachChild(p.Self())
	}
	return p.Self()
}

// DeriveTopN inherits the BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits the BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation implements the base.LogicalPlan.<8th> interface.
// about the logic of constant propagation in From List.
// Query: select * from t, (select a, b from s where s.a>1) tmp where tmp.a=t.a
// Origin logical plan:
/*
            +----------------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
//  1. 'PullUpConstantPredicates': Call this function until find selection and pull up the constant predicate layer by layer
//     LogicalSelection: find the s.a>1
//     LogicalProjection: get the s.a>1 and pull up it, changed to tmp.a>1
//  2. 'addCandidateSelection': Add selection above of LogicalJoin,
//     put all predicates pulled up from the lower layer into the current new selection.
//     LogicalSelection: tmp.a >1
//
// Optimized plan:
/*
            +----------------+
            |  Selection     |
            |    tmp.a>1     |
            +-------^--------+
                    |
            +-------+--------+
            |  LogicalJoin   |
            +-------^--------+
                    |
      +-------------+--------------+
      |                            |
+-----+------+              +------+------+
| Projection |              | TableScan   |
+-----^------+              +-------------+
      |
      |
+-----+------+
| Selection  |
|   s.a>1    |
+------------+
*/
// Return nil if the root of plan has not been changed
// Return new root if the root of plan is changed to selection
func (p *LogicalJoin) ConstantPropagation(parentPlan base.LogicalPlan, currentChildIdx int) (newRoot base.LogicalPlan) {
	// step1: get constant predicate from left or right according to the JoinType
	var getConstantPredicateFromLeft bool
	var getConstantPredicateFromRight bool
	switch p.JoinType {
	case base.LeftOuterJoin:
		getConstantPredicateFromLeft = true
	case base.RightOuterJoin:
		getConstantPredicateFromRight = true
	case base.InnerJoin:
		getConstantPredicateFromLeft = true
		getConstantPredicateFromRight = true
	default:
		return
	}
	var candidateConstantPredicates []expression.Expression
	if getConstantPredicateFromLeft {
		candidateConstantPredicates = p.Children()[0].PullUpConstantPredicates()
	}
	if getConstantPredicateFromRight {
		candidateConstantPredicates = append(candidateConstantPredicates, p.Children()[1].PullUpConstantPredicates()...)
	}
	if len(candidateConstantPredicates) == 0 {
		return
	}

	// step2: add selection above of LogicalJoin
	return addCandidateSelection(p, currentChildIdx, parentPlan, candidateConstantPredicates)
}

// PullUpConstantPredicates inherits the BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats inherits the BaseLogicalPlan.LogicalPlan.<10th> implementation.

// DeriveStats implements the base.LogicalPlan.<11th> interface.
// If the type of join is SemiJoin, the selectivity of it will be same as selection's.
// If the type of join is LeftOuterSemiJoin, it will not add or remove any row. The last column is a boolean value, whose NDV should be two.
// If the type of join is inner/outer join, the output of join(s, t) should be N(s) * N(t) / (V(s.key) * V(t.key)) * Min(s.key, t.key).
// N(s) stands for the number of rows in relation s. V(s.key) means the NDV of join key in s.
// This is a quite simple strategy: We assume every bucket of relation which will participate join has the same number of rows, and apply cross join for
// every matched bucket.
func (p *LogicalJoin) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	for _, one := range reloads {
		reload = reload || one
	}
	if !reload && p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
		return p.StatsInfo(), false, nil
	}
	leftProfile, rightProfile := childStats[0], childStats[1]
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	p.EqualCondOutCnt = cardinality.EstimateFullJoinRowCount(p.SCtx(),
		0 == len(p.EqualConditions),
		leftProfile, rightProfile,
		leftJoinKeys, rightJoinKeys,
		childSchema[0], childSchema[1],
		nil, nil)
	if p.JoinType == base.SemiJoin || p.JoinType == base.AntiSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount * cost.SelectionFactor,
			ColNDVs:  make(map[int64]float64, len(leftProfile.ColNDVs)),
		})
		for id, c := range leftProfile.ColNDVs {
			p.StatsInfo().ColNDVs[id] = c * cost.SelectionFactor
		}
		return p.StatsInfo(), true, nil
	}
	if p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		p.SetStats(&property.StatsInfo{
			RowCount: leftProfile.RowCount,
			ColNDVs:  make(map[int64]float64, selfSchema.Len()),
		})
		maps.Copy(p.StatsInfo().ColNDVs, leftProfile.ColNDVs)
		p.StatsInfo().ColNDVs[selfSchema.Columns[selfSchema.Len()-1].UniqueID] = 2.0
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
		return p.StatsInfo(), true, nil
	}
	count := p.EqualCondOutCnt
	if p.JoinType == base.LeftOuterJoin {
		count = math.Max(count, leftProfile.RowCount)
	} else if p.JoinType == base.RightOuterJoin {
		count = math.Max(count, rightProfile.RowCount)
	}
	colNDVs := make(map[int64]float64, selfSchema.Len())
	for id, c := range leftProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	for id, c := range rightProfile.ColNDVs {
		colNDVs[id] = math.Min(c, count)
	}
	p.SetStats(&property.StatsInfo{
		RowCount: count,
		ColNDVs:  colNDVs,
	})
	p.StatsInfo().GroupNDVs = p.getGroupNDVs(childStats)
	return p.StatsInfo(), true, nil
}

// ExtractColGroups implements the base.LogicalPlan.<12th> interface.
func (p *LogicalJoin) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	leftJoinKeys, rightJoinKeys, _, _ := p.GetJoinKeys()
	extracted := make([][]*expression.Column, 0, 2+len(colGroups))
	if len(leftJoinKeys) > 1 && (p.JoinType == base.InnerJoin || p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin) {
		extracted = append(extracted, expression.SortColumns(leftJoinKeys), expression.SortColumns(rightJoinKeys))
	}
	var outerSchema *expression.Schema
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		outerSchema = p.Children()[0].Schema()
	} else if p.JoinType == base.RightOuterJoin {
		outerSchema = p.Children()[1].Schema()
	}
	if len(colGroups) == 0 || outerSchema == nil {
		return extracted
	}
	_, offsets := outerSchema.ExtractColGroups(colGroups)
	if len(offsets) == 0 {
		return extracted
	}
	for _, offset := range offsets {
		extracted = append(extracted, colGroups[offset])
	}
	return extracted
}

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalJoin) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	leftProperties := childrenProperties[0]
	rightProperties := childrenProperties[1]
	// TODO: We should consider properties propagation.
	p.LeftProperties = leftProperties
	p.RightProperties = rightProperties
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin {
		rightProperties = nil
	} else if p.JoinType == base.RightOuterJoin {
		leftProperties = nil
	}
	resultProperties := make([][]*expression.Column, len(leftProperties)+len(rightProperties))
	for i, cols := range leftProperties {
		resultProperties[i] = make([]*expression.Column, len(cols))
		copy(resultProperties[i], cols)
	}
	leftLen := len(leftProperties)
	for i, cols := range rightProperties {
		resultProperties[leftLen+i] = make([]*expression.Column, len(cols))
		copy(resultProperties[leftLen+i], cols)
	}
	return resultProperties
}

// ExtractCorrelatedCols implements the base.LogicalPlan.<15th> interface.
func (p *LogicalJoin) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	for _, fun := range p.EqualConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.LeftConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.RightConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	for _, fun := range p.OtherConditions {
		corCols = append(corCols, expression.ExtractCorColumns(fun)...)
	}
	return corCols
}

// MaxOneRow inherits the BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits the BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren inherits the BaseLogicalPlan.LogicalPlan.<18th> implementation.

// SetChild inherits the BaseLogicalPlan.LogicalPlan.<19th> implementation.

// RollBackTaskMap inherits the BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits the BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD implements the base.LogicalPlan.<22th> interface.
func (p *LogicalJoin) ExtractFD() *funcdep.FDSet {
	switch p.JoinType {
	case base.InnerJoin:
		return p.ExtractFDForInnerJoin(nil)
	case base.LeftOuterJoin, base.RightOuterJoin:
		return p.ExtractFDForOuterJoin(nil)
	case base.SemiJoin:
		return p.ExtractFDForSemiJoin(nil)
	default:
		return &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}
	}
}

// GetBaseLogicalPlan inherits the BaseLogicalPlan.LogicalPlan.<23th> implementation.

// ConvertOuterToInnerJoin implements base.LogicalPlan.<24th> interface.
func (p *LogicalJoin) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	innerTable := p.Children()[0]
	outerTable := p.Children()[1]
	switchChild := false

	if p.JoinType == base.LeftOuterJoin {
		innerTable, outerTable = outerTable, innerTable
		switchChild = true
	}

	// First, simplify this join
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		canBeSimplified := false
		for _, expr := range predicates {
			isOk := util.IsNullRejected(p.SCtx(), innerTable.Schema(), expr, true)
			if isOk {
				canBeSimplified = true
				break
			}
		}
		if canBeSimplified {
			p.JoinType = base.InnerJoin
		}
	}

	// Next simplify join children

	combinedCond := mergeOnClausePredicates(p, predicates)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.RightOuterJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	} else if p.JoinType == base.InnerJoin || p.JoinType == base.SemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(combinedCond)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else if p.JoinType == base.AntiSemiJoin {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(combinedCond)
	} else {
		innerTable = innerTable.ConvertOuterToInnerJoin(predicates)
		outerTable = outerTable.ConvertOuterToInnerJoin(predicates)
	}

	if switchChild {
		p.SetChild(0, outerTable)
		p.SetChild(1, innerTable)
	} else {
		p.SetChild(0, innerTable)
		p.SetChild(1, outerTable)
	}

	return p
}

// GetJoinChildStatsAndSchema gets the stats and schema of join children.
func (p *LogicalJoin) GetJoinChildStatsAndSchema() (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	stats1, schema1 = p.Children()[1].StatsInfo(), p.Children()[1].Schema()
	stats0, schema0 = p.Children()[0].StatsInfo(), p.Children()[0].Schema()
	return
}

// *************************** end implementation of logicalPlan interface ***************************

// IsNAAJ checks if the join is a non-adjacent-join.
func (p *LogicalJoin) IsNAAJ() bool {
	return len(p.NAEQConditions) > 0
}

// Shallow copies a LogicalJoin struct.
func (p *LogicalJoin) Shallow() *LogicalJoin {
	join := *p
	return join.Init(p.SCtx(), p.QueryBlockOffset())
}

// ExtractFDForSemiJoin extracts FD for semi join.
func (p *LogicalJoin) ExtractFDForSemiJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	// 1: since semi join will keep the part or all rows of the outer table, it's outer FD can be saved.
	// 2: the un-projected column will be left for the upper layer projection or already be pruned from bottom up.
	outerFD, _ := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	fds := outerFD

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(p.LeftConditions, p.SCtx(), fds)

	for _, equiv := range equivFromApply {
		fds.AddEquivalence(equiv[0], equiv[1])
	}

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	p.SetFDs(fds)
	return fds
}

// ExtractFDForInnerJoin extracts FD for inner join.
func (p *LogicalJoin) ExtractFDForInnerJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	child := p.Children()
	rightFD := child[1].ExtractFD()
	leftFD := child[0].ExtractFD()
	fds := leftFD
	fds.MakeCartesianProduct(rightFD)

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	// some join eq conditions are stored in the OtherConditions.
	allConds := append(eqCondSlice, p.OtherConditions...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), fds)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), fds)

	fds.MakeNotNull(notNullColsFromFilters)
	fds.AddConstants(constUniqueIDs)
	for _, equiv := range equivUniqueIDs {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	for _, equiv := range equivFromApply {
		fds.AddEquivalence(equiv[0], equiv[1])
	}
	// merge the not-null-cols/registered-map from both side together.
	fds.NotNullCols.UnionWith(rightFD.NotNullCols)
	if fds.HashCodeToUniqueID == nil {
		fds.HashCodeToUniqueID = rightFD.HashCodeToUniqueID
	} else {
		for k, v := range rightFD.HashCodeToUniqueID {
			// If there's same constant in the different subquery, we might go into this IF branch.
			if _, ok := fds.HashCodeToUniqueID[k]; ok {
				continue
			}
			fds.HashCodeToUniqueID[k] = v
		}
	}
	for i, ok := rightFD.GroupByCols.Next(0); ok; i, ok = rightFD.GroupByCols.Next(i + 1) {
		fds.GroupByCols.Insert(i)
	}
	fds.HasAggBuilt = fds.HasAggBuilt || rightFD.HasAggBuilt
	p.SetFDs(fds)
	return fds
}

// ExtractFDForOuterJoin extracts FD for outer join.
func (p *LogicalJoin) ExtractFDForOuterJoin(equivFromApply [][]intset.FastIntSet) *funcdep.FDSet {
	outerFD, innerFD := p.Children()[0].ExtractFD(), p.Children()[1].ExtractFD()
	innerCondition := p.RightConditions
	outerCondition := p.LeftConditions
	outerCols, innerCols := intset.NewFastIntSet(), intset.NewFastIntSet()
	for _, col := range p.Children()[0].Schema().Columns {
		outerCols.Insert(int(col.UniqueID))
	}
	for _, col := range p.Children()[1].Schema().Columns {
		innerCols.Insert(int(col.UniqueID))
	}
	if p.JoinType == base.RightOuterJoin {
		innerFD, outerFD = outerFD, innerFD
		innerCondition = p.LeftConditions
		outerCondition = p.RightConditions
		innerCols, outerCols = outerCols, innerCols
	}

	eqCondSlice := expression.ScalarFuncs2Exprs(p.EqualConditions)
	allConds := append(eqCondSlice, p.OtherConditions...)
	allConds = append(allConds, innerCondition...)
	allConds = append(allConds, outerCondition...)
	notNullColsFromFilters := util.ExtractNotNullFromConds(allConds, p)

	filterFD := &funcdep.FDSet{HashCodeToUniqueID: make(map[string]int)}

	constUniqueIDs := util.ExtractConstantCols(allConds, p.SCtx(), filterFD)

	equivUniqueIDs := util.ExtractEquivalenceCols(allConds, p.SCtx(), filterFD)

	filterFD.AddConstants(constUniqueIDs)
	equivOuterUniqueIDs := intset.NewFastIntSet()
	equivAcrossNum := 0
	// apply (left join)
	//   +--- child0 [t1.a]
	//   +--- project [correlated col{t1.a} --> col#9]
	// since project correlated col{t1.a} is equivalent to t1.a, we should maintain t1.a == col#9. since apply is
	// a left join, the probe side will append null value for the non-matched row, so we should maintain the equivalence
	// before the fds.MakeOuterJoin(). Even correlated col{t1.a} is not from outer side here, we could also maintain
	// the equivalence before the fds.MakeOuterJoin().
	equivUniqueIDs = append(equivUniqueIDs, equivFromApply...)
	for _, equiv := range equivUniqueIDs {
		filterFD.AddEquivalence(equiv[0], equiv[1])
		if equiv[0].SubsetOf(outerCols) && equiv[1].SubsetOf(innerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[0])
			equivAcrossNum++
			continue
		}
		if equiv[0].SubsetOf(innerCols) && equiv[1].SubsetOf(outerCols) {
			equivOuterUniqueIDs.UnionWith(equiv[1])
			equivAcrossNum++
		}
	}
	filterFD.MakeNotNull(notNullColsFromFilters)

	// pre-perceive the filters for the convenience judgement of 3.3.1.
	var opt funcdep.ArgOpts
	if equivAcrossNum > 0 {
		// find the equivalence FD across left and right cols.
		outerConditionUniqueIDs := intset.NewFastIntSet()
		if len(outerCondition) != 0 {
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, outerCondition...)
		}
		if len(p.OtherConditions) != 0 {
			// other condition may contain right side cols, it doesn't affect the judgement of intersection of non-left-equiv cols.
			expression.ExtractColumnsSetFromExpressions(&outerConditionUniqueIDs, nil, p.OtherConditions...)
		}
		// judge whether left filters is on non-left-equiv cols.
		if outerConditionUniqueIDs.Intersects(outerCols.Difference(equivOuterUniqueIDs)) {
			opt.SkipFDRule331 = true
		}
	} else {
		// if there is none across equivalence condition, skip rule 3.3.1.
		opt.SkipFDRule331 = true
	}

	opt.OnlyInnerFilter = len(eqCondSlice) == 0 && len(outerCondition) == 0 && len(p.OtherConditions) == 0
	if opt.OnlyInnerFilter {
		// if one of the inner condition is constant false, the inner side are all null, left make constant all of that.
		for _, one := range innerCondition {
			if c, ok := one.(*expression.Constant); ok && c.DeferredExpr == nil && c.ParamMarker == nil {
				if isTrue, err := c.Value.ToBool(p.SCtx().GetSessionVars().StmtCtx.TypeCtx()); err == nil {
					if isTrue == 0 {
						// c is false
						opt.InnerIsFalse = true
					}
				}
			}
		}
	}

	fds := outerFD
	fds.MakeOuterJoin(innerFD, filterFD, outerCols, innerCols, &opt)
	p.SetFDs(fds)
	return fds
}
