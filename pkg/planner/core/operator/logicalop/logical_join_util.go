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
	"maps"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// GetJoinKeys extracts join keys(columns) from EqualConditions. It returns left join keys, right
// join keys and an `isNullEQ` array which means the `joinKey[i]` is a `NullEQ` function. The `hasNullEQ`
// means whether there is a `NullEQ` of a join key.
func (p *LogicalJoin) GetJoinKeys() (leftKeys, rightKeys []*expression.Column, isNullEQ []bool, hasNullEQ bool) {
	for _, expr := range p.EqualConditions {
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
		isNullEQ = append(isNullEQ, expr.FuncName.L == ast.NullEQ)
		hasNullEQ = hasNullEQ || expr.FuncName.L == ast.NullEQ
	}
	return
}

// GetNAJoinKeys extracts join keys(columns) from NAEqualCondition.
func (p *LogicalJoin) GetNAJoinKeys() (leftKeys, rightKeys []*expression.Column) {
	for _, expr := range p.NAEQConditions {
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
	}
	return
}

// GetPotentialPartitionKeys return potential partition keys for join, the potential partition keys are
// the join keys of EqualConditions
func (p *LogicalJoin) GetPotentialPartitionKeys() (leftKeys, rightKeys []*property.MPPPartitionColumn) {
	for _, expr := range p.EqualConditions {
		_, coll := expr.CharsetAndCollation()
		collateID := property.GetCollateIDByNameForPartition(coll)
		l, r := expression.ExtractColumnsFromColOpCol(expr)
		leftKeys = append(leftKeys, &property.MPPPartitionColumn{Col: l, CollateID: collateID})
		rightKeys = append(rightKeys, &property.MPPPartitionColumn{Col: r, CollateID: collateID})
	}
	return
}

// Decorrelate eliminate the correlated column with if the col is in schema.
func (p *LogicalJoin) Decorrelate(schema *expression.Schema) {
	for i, cond := range p.LeftConditions {
		p.LeftConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.RightConditions {
		p.RightConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.OtherConditions {
		p.OtherConditions[i] = cond.Decorrelate(schema)
	}
	for i, cond := range p.EqualConditions {
		p.EqualConditions[i] = cond.Decorrelate(schema).(*expression.ScalarFunction)
	}
}

// ColumnSubstituteAll is used in projection elimination in apply de-correlation.
// Substitutions for all conditions should be successful, otherwise, we should keep all conditions unchanged.
func (p *LogicalJoin) ColumnSubstituteAll(schema *expression.Schema, exprs []expression.Expression) (hasFail bool) {
	// make a copy of exprs for convenience of substitution (may change/partially change the expr tree)
	cpLeftConditions := make(expression.CNFExprs, len(p.LeftConditions))
	cpRightConditions := make(expression.CNFExprs, len(p.RightConditions))
	cpOtherConditions := make(expression.CNFExprs, len(p.OtherConditions))
	cpEqualConditions := make([]*expression.ScalarFunction, len(p.EqualConditions))
	copy(cpLeftConditions, p.LeftConditions)
	copy(cpRightConditions, p.RightConditions)
	copy(cpOtherConditions, p.OtherConditions)
	copy(cpEqualConditions, p.EqualConditions)

	exprCtx := p.SCtx().GetExprCtx()
	// try to substitute columns in these condition.
	for i, cond := range cpLeftConditions {
		if hasFail, cpLeftConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpRightConditions {
		if hasFail, cpRightConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpOtherConditions {
		if hasFail, cpOtherConditions[i] = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
	}

	for i, cond := range cpEqualConditions {
		var tmp expression.Expression
		if hasFail, tmp = expression.ColumnSubstituteAll(exprCtx, cond, schema, exprs); hasFail {
			return
		}
		cpEqualConditions[i] = tmp.(*expression.ScalarFunction)
	}

	// if all substituted, change them atomically here.
	p.LeftConditions = cpLeftConditions
	p.RightConditions = cpRightConditions
	p.OtherConditions = cpOtherConditions
	p.EqualConditions = cpEqualConditions

	for i := len(p.EqualConditions) - 1; i >= 0; i-- {
		newCond := p.EqualConditions[i]

		// If the columns used in the new filter all come from the left child,
		// we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[0].Schema()) {
			p.LeftConditions = append(p.LeftConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}

		// If the columns used in the new filter all come from the right
		// child, we can push this filter to it.
		if expression.ExprFromSchema(newCond, p.Children()[1].Schema()) {
			p.RightConditions = append(p.RightConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}
		_, _, ok := expression.IsColOpCol(newCond)
		// If the columns used in the new filter are not all expression.Column,
		// we can not use it as join's equal condition.
		if !ok {
			p.OtherConditions = append(p.OtherConditions, newCond)
			p.EqualConditions = slices.Delete(p.EqualConditions, i, i+1)
			continue
		}

		p.EqualConditions[i] = newCond
	}
	return false
}

// AttachOnConds extracts on conditions for join and set the `EqualConditions`, `LeftConditions`, `RightConditions` and
// `OtherConditions` by the result of extract.
func (p *LogicalJoin) AttachOnConds(onConds []expression.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.AppendJoinConds(eq, left, right, other)
}

// AppendJoinConds appends new join conditions.
func (p *LogicalJoin) AppendJoinConds(eq []*expression.ScalarFunction, left, right, other []expression.Expression) {
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

func (p *LogicalJoin) isAllUniqueIDInTheSameLeaf(cond expression.Expression) bool {
	colset := slices.Collect(
		maps.Keys(expression.ExtractColumnsMapFromExpressions(nil, cond)),
	)
	if len(colset) == 1 {
		return true
	}

	for _, schema := range p.allJoinLeaf {
		inTheSameSchema := true
		// if we find a column in the table, the other is not in the same table.
		// They are impossible in the same table. we can directly return.
		findedSchema := false
		for _, unique := range colset {
			if !slices.ContainsFunc(schema.Columns, func(c *expression.Column) bool {
				return c.UniqueID == unique
			}) {
				inTheSameSchema = false
				break
			}
			findedSchema = true
		}
		if inTheSameSchema {
			return true
		} else if findedSchema {
			return false
		}
	}
	return false
}

// getAllJoinLeaf is to get all datasource's schema
func getAllJoinLeaf(plan base.LogicalPlan) []*expression.Schema {
	switch p := plan.(type) {
	case *DataSource, *LogicalAggregation, *LogicalProjection:
		// Because sometimes we put the output of the aggregation into the schema,
		// we can consider it as a new table.
		return []*expression.Schema{p.Schema()}
	default:
		result := make([]*expression.Schema, 0, len(p.Children()))
		for _, child := range p.Children() {
			result = append(result, getAllJoinLeaf(child)...)
		}
		return result
	}
}

// ExtractJoinKeys extract join keys as a schema for child with childIdx.
func (p *LogicalJoin) ExtractJoinKeys(childIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(p.EqualConditions))
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[childIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// ExtractUsedCols extracts all the needed columns.
func (p *LogicalJoin) ExtractUsedCols(parentUsedCols []*expression.Column) (leftCols []*expression.Column, rightCols []*expression.Column) {
	for _, eqCond := range p.EqualConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(eqCond)...)
	}
	for _, leftCond := range p.LeftConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(leftCond)...)
	}
	for _, rightCond := range p.RightConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(rightCond)...)
	}
	for _, otherCond := range p.OtherConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(otherCond)...)
	}
	for _, naeqCond := range p.NAEQConditions {
		parentUsedCols = append(parentUsedCols, expression.ExtractColumns(naeqCond)...)
	}
	lChild := p.Children()[0]
	rChild := p.Children()[1]
	lSchema := lChild.Schema()
	rSchema := rChild.Schema()
	var lFullSchema, rFullSchema *expression.Schema
	// parentused col = t2.a
	// leftChild schema = t1.a(t2.a) + and others
	// rightChild schema = t3 related + and others
	if join, ok := lChild.(*LogicalJoin); ok {
		lFullSchema = join.FullSchema
	}
	if join, ok := rChild.(*LogicalJoin); ok {
		rFullSchema = join.FullSchema
	}
	for _, col := range parentUsedCols {
		if (lSchema != nil && lSchema.Contains(col)) ||
			(lFullSchema != nil && lFullSchema.Contains(col)) {
			leftCols = append(leftCols, col)
		} else if (rSchema != nil && rSchema.Contains(col)) ||
			(rFullSchema != nil && rFullSchema.Contains(col)) {
			rightCols = append(rightCols, col)
		}
	}
	return leftCols, rightCols
}

// MergeSchema merge the schema of left and right child of join.
func (p *LogicalJoin) MergeSchema() {
	p.SetSchema(BuildLogicalJoinSchema(p.JoinType, p))
}

// pushDownTopNToChild will push a topN to one child of join. The idx stands for join child index. 0 is for left child.
// When it's outer join and there's unique key information. The TopN can be totally pushed down to the join.
// We just need reserve the ORDER informaion
func (p *LogicalJoin) pushDownTopNToChild(topN *LogicalTopN, idx int) (base.LogicalPlan, bool) {
	if topN == nil {
		return p.Children()[idx].PushDownTopN(nil), false
	}

	for _, by := range topN.ByItems {
		cols := expression.ExtractColumns(by.Expr)
		for _, col := range cols {
			if !p.Children()[idx].Schema().Contains(col) {
				return p.Children()[idx].PushDownTopN(nil), false
			}
		}
	}
	count, offset := topN.Count+topN.Offset, uint64(0)
	selfEliminated := false
	if p.JoinType == base.LeftOuterJoin {
		innerChild := p.Children()[1]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[1].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		// If it's unique key(unique with not null), we can push the offset down safely whatever the join key is normal eq or nulleq.
		// If the join key is nulleq, then we can only push the offset down when the inner side is unique key.
		// Only when the join key is normal eq, we can push the offset down when the inner side is unique(could be null).
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	} else if p.JoinType == base.RightOuterJoin {
		innerChild := p.Children()[0]
		innerJoinKey := make([]*expression.Column, 0, len(p.EqualConditions))
		isNullEQ := false
		for _, eqCond := range p.EqualConditions {
			innerJoinKey = append(innerJoinKey, eqCond.GetArgs()[0].(*expression.Column))
			if eqCond.FuncName.L == ast.NullEQ {
				isNullEQ = true
			}
		}
		if innerChild.Schema().IsUnique(true, innerJoinKey...) ||
			(!isNullEQ && innerChild.Schema().IsUnique(false, innerJoinKey...)) {
			count, offset = topN.Count, topN.Offset
			selfEliminated = true
		}
	}

	newTopN := LogicalTopN{
		Count:            count,
		Offset:           offset,
		ByItems:          make([]*util.ByItems, len(topN.ByItems)),
		PreferLimitToCop: topN.PreferLimitToCop,
	}.Init(topN.SCtx(), topN.QueryBlockOffset())
	for i := range topN.ByItems {
		newTopN.ByItems[i] = topN.ByItems[i].Clone()
	}
	return p.Children()[idx].PushDownTopN(newTopN), selfEliminated
}

// Add a new selection between parent plan and current plan with candidate predicates
/*
+-------------+                                    +-------------+
| parentPlan  |                                    | parentPlan  |
+-----^-------+                                    +-----^-------+
      |           --addCandidateSelection--->            |
+-----+-------+                              +-----------+--------------+
| currentPlan |                              |        selection         |
+-------------+                              |   candidate predicate    |
                                             +-----------^--------------+
                                                         |
                                                         |
                                                    +----+--------+
                                                    | currentPlan |
                                                    +-------------+
*/
// If the currentPlan at the top of query plan, return new root plan (selection)
// Else return nil
func addCandidateSelection(currentPlan base.LogicalPlan, currentChildIdx int, parentPlan base.LogicalPlan,
	candidatePredicates []expression.Expression) (newRoot base.LogicalPlan) {
	// generate a new selection for candidatePredicates
	selection := LogicalSelection{Conditions: candidatePredicates}.Init(currentPlan.SCtx(), currentPlan.QueryBlockOffset())
	// add selection above of p
	if parentPlan == nil {
		newRoot = selection
	} else {
		parentPlan.SetChild(currentChildIdx, selection)
	}
	selection.SetChildren(currentPlan)
	if parentPlan == nil {
		return newRoot
	}
	return nil
}

// logical join group ndv is just to output the corresponding child's groupNDV is asked previously and only outer side is cared.
func (p *LogicalJoin) getGroupNDVs(childStats []*property.StatsInfo) []property.GroupNDV {
	outerIdx := int(-1)
	if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin || p.JoinType == base.AntiLeftOuterSemiJoin {
		outerIdx = 0
	} else if p.JoinType == base.RightOuterJoin {
		outerIdx = 1
	}
	if outerIdx >= 0 {
		return childStats[outerIdx].GroupNDVs
	}
	return nil
}

// PreferAny checks whether the join type is in the joinFlags.
func (p *LogicalJoin) PreferAny(joinFlags ...uint) bool {
	for _, flag := range joinFlags {
		if p.PreferJoinType&flag > 0 {
			return true
		}
	}
	return false
}

// This function is only used with inner join and semi join.
func (p *LogicalJoin) isVaildConstantPropagationExpressionWithInnerJoinOrSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, true, true, true, true)
}

// This function is only used in LeftOuterJoin, LeftOuterSemiJoin, AntiLeftOuterSemiJoin, AntiSemiJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForLeftOuterJoinAndAntiSemiJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, false, true)
}

// This function is only used in RightOuterJoin
func (p *LogicalJoin) isVaildConstantPropagationExpressionForRightOuterJoin(expr expression.Expression) bool {
	return p.isVaildConstantPropagationExpression(expr, false, false, true, false)
}

// isVaildConstantPropagationExpression is to judge whether the expression is created by PropagationContant is vaild.
//
// Some expressions are not suitable for constant propagation. After constant propagation,
// these expressions will only become a projection, increasing the computational load without
// being able to filter data directly from the data source.
//
// `deriveLeft` and `driveRight` are used in conjunction with `extractOnCondition`.
//
// `canLeftPushDown` and `canRightPushDown` are used to mark that for some joins,
// the left or right condition will not be pushed down. For these conditions that cannot be pushed down,
// we can reject the new expressions from constant propagation.
func (p *LogicalJoin) isVaildConstantPropagationExpression(cond expression.Expression, deriveLeft, deriveRight, canLeftPushDown, canRightPushDown bool) bool {
	_, leftCond, rightCond, otherCond := p.extractOnCondition([]expression.Expression{cond}, deriveLeft, deriveRight)
	if len(otherCond) > 0 {
		// a new expression which is created by constant propagation, is a other condtion, we don't put it
		// into our final result.
		return false
	}
	intest.Assert(len(leftCond) == 0 || len(rightCond) == 0, "An expression cannot be both a left and a right condition at the same time.")
	// When the expression is a left/right condition, we want it to filter more of the underlying data.
	if len(leftCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canLeftPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	if len(rightCond) > 0 {
		// If this expression's columns is in the same table. We will push it down.
		if canRightPushDown && p.isAllUniqueIDInTheSameLeaf(cond) {
			return true
		}
		return false
	}
	return true
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	ctx := p.SCtx()
	for _, expr := range conditions {
		// For queries like `select a in (select a from s where s.b = t.b) from t`,
		// if subquery is empty caused by `s.b = t.b`, the result should always be
		// false even if t.a is null or s.a is null. To make this join "empty aware",
		// we should differentiate `t.a = s.a` from other column equal conditions, so
		// we put it into OtherConditions instead of EqualConditions of join.
		if expression.IsEQCondFromIn(expr) {
			otherCond = append(otherCond, expr)
			continue
		}
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			arg0, arg1, ok := expression.IsColOpCol(binop)
			if ok {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if util.IsNullRejected(ctx, leftSchema, expr, true) && !mysql.HasNotNullFlag(leftCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if util.IsNullRejected(ctx, rightSchema, expr, true) && !mysql.HasNotNullFlag(rightCol.RetType.GetFlag()) {
							notNullExpr := expression.BuildNotNullExpr(ctx.GetExprCtx(), rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					switch binop.FuncName.L {
					case ast.EQ, ast.NullEQ:
						cond := expression.NewFunctionInternal(ctx.GetExprCtx(), binop.FuncName.L, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			// The IsMutableEffectsExpr check is primarily designed to prevent mutable expressions
			// like rand() > 0.5 from being pushed down; instead, such expressions should remain
			// in other conditions.
			// Checking len(columns) == 0 first is to let filter like rand() > tbl.col
			// to be able pushdown as left or right condition
			if expression.IsMutableEffectsExpr(expr) {
				otherCond = append(otherCond, expr)
				continue
			}
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(ctx.GetExprCtx(), expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) (_, _ []expression.Expression) {
	switch p.JoinType {
	case base.LeftOuterJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case base.RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case base.SemiJoin, base.InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	case base.AntiSemiJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
		}
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	child := p.Children()
	rightSchema := child[1].Schema()
	leftSchema := child[0].Schema()
	return p.ExtractOnCondition(conditions, leftSchema, rightSchema, deriveLeft, deriveRight)
}
