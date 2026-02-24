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

package statistics

import (
	"cmp"
	"math"
	"slices"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)

// calculateLeftOverlapPercent calculates the percentage for an out-of-range overlap
// on the left side of the histogram. The predicate range [l, r] overlaps with
// the region (boundL, histL), and we calculate the percentage of the shaded area.
func calculateLeftOverlapPercent(l, r, boundL, histL, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "left triangle" of the histogram.
	l = max(l, boundL)
	r = min(r, histL)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Right side of the predicate as distance from the left edge of the histogram
	rightRange := math.Pow(r-boundL, 2)
	// Left side of the predicate as distance from the left edge of the histogram
	leftRange := math.Pow(l-boundL, 2)
	return (rightRange - leftRange) / histWidthSq
}

// calculateRightOverlapPercent calculates the percentage for an out-of-range overlap
// on the right side of the histogram. The predicate range [l, r] overlaps with
// the region (histR, boundR), and we calculate the percentage of the shaded area.
func calculateRightOverlapPercent(l, r, histR, boundR, histWidth float64) float64 {
	if histWidth <= 0 {
		return 0
	}
	// bound the left/right ranges of the predicates to the "right triangle" of the histogram.
	l = max(l, histR)
	r = min(r, boundR)
	// If there's no overlap after bounding, return 0.
	if l >= r {
		return 0
	}
	// NOTE: Ranges are squared to determine a triangular (linear) distribution rather than uniform.
	// Width of the histogram - squared to normalize to a percentage
	histWidthSq := math.Pow(histWidth, 2)
	// Left side of the predicate as distance from the right edge of the histogram.
	leftRange := math.Pow(boundR-l, 2)
	// Right side of the predicate as distance from the right edge of the histogram.
	rightRange := math.Pow(boundR-r, 2)
	return (leftRange - rightRange) / histWidthSq
}

// OutOfRangeRowCount estimate the row count of part of [lDatum, rDatum] which is out of range of the histogram.
// Here we assume the density of data is decreasing from the lower/upper bound of the histogram toward outside.
// The maximum row count it can get is the modifyCount. It reaches the maximum when out-of-range width reaches histogram range width.
// As it shows below. To calculate the out-of-range row count, we need to calculate the percentage of the shaded area.
// Note that we assume histL-boundL == histR-histL == boundR-histR here.
/*
               /│             │\
             /  │             │  \
           /x│  │◄─histogram─►│    \
         / xx│  │    range    │      \
       / │xxx│  │             │        \
     /   │xxx│  │             │          \
────┴────┴───┴──┴─────────────┴───────────┴─────
    ▲    ▲   ▲  ▲             ▲           ▲
    │    │   │  │             │           │
 boundL  │   │histL         histR       boundR
         │   │
    lDatum  rDatum
*/
// The percentage of shaded area on the left side calculation formula is:
// leftPercent = (math.Pow(actualR-boundL, 2) - math.Pow(actualL-boundL, 2)) / math.Pow(histWidth, 2)
// You can find more details at https://github.com/pingcap/tidb/pull/47966#issuecomment-1778866876
func (hg *Histogram) OutOfRangeRowCount(
	sctx planctx.PlanContext,
	lDatum, rDatum *types.Datum,
	realtimeRowCount, modifyCount, histNDV int64,
) (result RowEstimate) {
	if hg.Len() == 0 {
		return DefaultRowEst(0)
	}

	// Step 1: Calculate a default of "one value"
	// oneValue assumes "one value qualifies", and is used as a lower bound.
	histNDV = max(histNDV, 1)
	oneValue := hg.NotNullCount() / float64(histNDV)

	// Step 2: If modifications are not allowed, return the one value.
	// In OptObjectiveDeterminate mode, we can't rely on real time statistics, so default to assuming
	// one value qualifies.
	allowUseModifyCount := sctx.GetSessionVars().GetOptObjective() != vardef.OptObjectiveDeterminate
	if !allowUseModifyCount {
		return RowEstimate{Est: oneValue, MinEst: oneValue, MaxEst: oneValue}
	}

	// Step 3: Adjust oneValue if the NDV is low
	// If NDV is low, it may no longer be representative of the data since ANALYZE
	// was last run. Use a default value against realtimeRowCount.
	// If NDV is not representitative, then hg.NotNullCount may not be either.
	if float64(histNDV) < outOfRangeBetweenRate {
		oneValue = max(min(oneValue, float64(realtimeRowCount)/outOfRangeBetweenRate), 1.0)
	}
	// Step 4: Calculate how much of the statistics share a common prefix.
	// For bytes and string type, we need to cut the common prefix when converting them to scalar value.
	// Here we calculate the length of common prefix.
	// TODO: If the common prefix is large, we may underestimate the out-of-range
	// portion because we can't distinguish the values with the same prefix.
	commonPrefix := 0
	if hg.GetLower(0).Kind() == types.KindBytes || hg.GetLower(0).Kind() == types.KindString {
		// Calculate the common prefix length among the lower and upper bound of histogram and the range we want to estimate.
		commonPrefix = commonPrefixLength(hg.GetLower(0).GetBytes(),
			hg.GetUpper(hg.Len()-1).GetBytes(),
			lDatum.GetBytes(),
			rDatum.GetBytes())
	}

	// Step 5:Convert the range we want to estimate to scalar value(float64)
	l := convertDatumToScalar(lDatum, commonPrefix)
	r := convertDatumToScalar(rDatum, commonPrefix)
	unsigned := mysql.HasUnsignedFlag(hg.Tp.GetFlag())
	// If this is an unsigned column, we need to make sure values are not negative.
	// Normal negative value should have become 0. But this still might happen when met MinNotNull here.
	// Maybe it's better to do this transformation in the ranger like the normal negative value.
	// Track whether negative values were clamped to 0, to detect impossible ranges.
	var leftClamped, rightClamped bool
	if unsigned {
		if l < 0 {
			l = 0
			leftClamped = true
		}
		if r < 0 {
			r = 0
			rightClamped = true
		}
		// If both bounds collapsed to 0 due to clamping negative values, this is
		// an impossible range (e.g., unsigned_col < 0). Return 0 estimate.
		if l == 0 && r == 0 && (leftClamped || rightClamped) {
			return DefaultRowEst(0)
		}
	}

	// Step 6: Convert the lower and upper bound of the histogram to scalar value(float64)
	histL := convertDatumToScalar(hg.GetLower(0), commonPrefix)
	histR := convertDatumToScalar(hg.GetUpper(hg.Len()-1), commonPrefix)
	histWidth := histR - histL
	// If we find that the histogram width is too small or too large - we still may need to consider
	// the impact of modifications to the table. Reset the histogram width to 0.
	if histWidth < 0 {
		histWidth = 0
	}
	if math.IsInf(histWidth, 1) {
		histWidth = 0
	}
	boundL := histL - histWidth
	boundR := histR + histWidth

	// Step 7: Calculate the width of the predicate
	// TODO: If predWidth == 0, it may be because the common prefix is too large.
	// In future - out of range for equal predicates should also use this logic
	// for consistency. We need to handle both "equal" and "large common prefix".
	predWidth := r - l
	if predWidth < 0 {
		// This should never happen.
		intest.Assert(false, "Right bound should not be less than left bound")

		return DefaultRowEst(0)
	} else if predWidth == 0 {
		// Set histWidth to 0 so that we can still return a minimum of oneValue,
		// and return the max as worst case.
		histWidth = 0
	}

	// Step 8: Calculate the out of range percentages
	// Calculate left overlap percentage if the range overlaps with (boundL, histL)
	leftPercent := calculateLeftOverlapPercent(l, r, boundL, histL, histWidth)
	// Calculate right overlap percentage if the range overlaps with (histR, boundR)
	rightPercent := calculateRightOverlapPercent(l, r, histR, boundR, histWidth)

	totalPercent := min(leftPercent*0.5+rightPercent*0.5, 1.0)
	maxTotalPercent := min(leftPercent+rightPercent, 1.0)

	// Step 9: Calculate the added rows
	// Use absolute value to account for the case where rows may have been added on one side,
	// but deleted from the other, resulting in qualifying out of range rows even though
	// realtimeRowCount is less than histogram count
	addedRows := hg.AbsRowCountDifference(realtimeRowCount)
	maxAddedRows := addedRows

	// Step 10: Calculate the estimated rows
	estRows := oneValue
	skewRatio := sctx.GetSessionVars().RiskRangeSkewRatio
	sctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptRiskRangeSkewRatio)
	if totalPercent > 0 {
		// Multiplying addedRows by 0.5 provides the assumption that 50% "addedRows" are inside
		// the histogram range, and 50% (0.5) are out-of-range. Users can adjust this
		// magic number by setting the session variable `tidb_opt_risk_range_skew_ratio`.
		// When skewRatio > 0, estRows sets the starting point for the skew ratio calculation.
		addedRowMultiplier := 0.5
		// NOTE: Skew ratio is used twice in this function.
		// This first usage allows a user to specify a ratio that is smaller or
		// larger than the default 0.5.
		if skewRatio > 0 {
			addedRowMultiplier = skewRatio
		}
		estRows = (addedRows * addedRowMultiplier) * totalPercent
	}

	// Step 11: Calculate a potential worst case for use in final MaxEst
	// We may have missed the true lowest/highest values due to sampling OR there could be a delay in
	// updates to modifyCount (meaning modifyCount is incorrectly set to 0). So ensure we always
	// account for at least 1% of the total row count as a worst case for "addedRows".
	// We inflate this here so ONLY to impact the MaxEst value.
	if modifyCount == 0 || addedRows == 0 {
		if realtimeRowCount <= 0 {
			realtimeRowCount = int64(hg.TotalRowCount())
		}
		// Use outOfRangeBetweenRate as a divisor to get a small percentage of the approximate
		// modifyCount (since outOfRangeBetweenRate has a default value of 100).
		maxAddedRows = max(maxAddedRows, float64(realtimeRowCount)/outOfRangeBetweenRate)
	}
	if maxTotalPercent > 0 {
		// Always apply maxTotalPercent to maxAddedRows to limit scaling when the predicate has an upper bound
		maxAddedRows *= maxTotalPercent
	}

	// Step 12: Calculate the final min/max/est rows including the skew ratio adjustment
	result.MinEst = min(estRows, oneValue)
	// NOTE: Skew ratio is used twice in this function.
	// This second usage scales the estimate from the base estimate to the max estimate.
	if skewRatio > 0 {
		result = CalculateSkewRatioCounts(estRows, maxAddedRows, skewRatio)
	} else {
		// Do not scale the estimate if skew ratio is not set.
		result.Est = estRows
	}
	result.Est = max(result.Est, oneValue)
	result.MaxEst = max(result.Est, maxAddedRows)

	return result
}

// Copy deep copies the histogram.
func (hg *Histogram) Copy() *Histogram {
	if hg == nil {
		return nil
	}
	newHist := *hg
	if hg.Bounds != nil {
		newHist.Bounds = hg.Bounds.CopyConstruct()
	}
	newHist.Buckets = make([]Bucket, 0, len(hg.Buckets))
	newHist.Buckets = append(newHist.Buckets, hg.Buckets...)
	return &newHist
}

// TruncateHistogram truncates the histogram to `numBkt` buckets.
func (hg *Histogram) TruncateHistogram(numBkt int) *Histogram {
	hist := hg.Copy()
	hist.Buckets = hist.Buckets[:numBkt]
	hist.Bounds.TruncateTo(numBkt * 2)
	return hist
}

type dataCnt struct {
	data []byte
	cnt  uint64
}

// GetIndexPrefixLens returns an array representing
func GetIndexPrefixLens(data []byte, numCols int) (prefixLens []int, err error) {
	prefixLens = make([]int, 0, numCols)
	var colData []byte
	prefixLen := 0
	for len(data) > 0 {
		colData, data, err = codec.CutOne(data)
		if err != nil {
			return nil, err
		}
		prefixLen += len(colData)
		prefixLens = append(prefixLens, prefixLen)
	}
	return prefixLens, nil
}

// ExtractTopN extracts topn from histogram.
func (hg *Histogram) ExtractTopN(cms *CMSketch, topN *TopN, numCols int, numTopN uint32) error {
	if hg.Len() == 0 || cms == nil || numTopN == 0 {
		return nil
	}
	dataSet := make(map[string]struct{}, hg.Bounds.NumRows())
	dataCnts := make([]dataCnt, 0, hg.Bounds.NumRows())
	hg.PreCalculateScalar()
	// Set a limit on the frequency of boundary values to avoid extract values with low frequency.
	limit := hg.NotNullCount() / float64(hg.Len())
	// Since our histogram are equal depth, they must occurs on the boundaries of buckets.
	for i := range hg.Bounds.NumRows() {
		data := hg.Bounds.GetRow(i).GetBytes(0)
		prefixLens, err := GetIndexPrefixLens(data, numCols)
		if err != nil {
			return err
		}
		for _, prefixLen := range prefixLens {
			prefixColData := data[:prefixLen]
			_, ok := dataSet[string(prefixColData)]
			if ok {
				continue
			}
			dataSet[string(prefixColData)] = struct{}{}
			res := hg.BetweenRowCount(nil, types.NewBytesDatum(prefixColData), types.NewBytesDatum(kv.Key(prefixColData).PrefixNext())).Est
			if res >= limit {
				dataCnts = append(dataCnts, dataCnt{prefixColData, uint64(res)})
			}
		}
	}
	slices.SortStableFunc(dataCnts, func(a, b dataCnt) int { return -cmp.Compare(a.cnt, b.cnt) })
	if len(dataCnts) > int(numTopN) {
		dataCnts = dataCnts[:numTopN]
	}
	topN.TopN = make([]TopNMeta, 0, len(dataCnts))
	for _, dataCnt := range dataCnts {
		h1, h2 := murmur3.Sum128(dataCnt.data)
		realCnt := cms.queryHashValue(nil, h1, h2)
		cms.SubValue(h1, h2, realCnt)
		topN.AppendTopN(dataCnt.data, realCnt)
	}
	topN.Sort()
	return nil
}

var bucket4MergingPool = sync.Pool{
	New: func() any {
		return newBucket4Meging()
	},
}

func newbucket4MergingForRecycle() *bucket4Merging {
	return bucket4MergingPool.Get().(*bucket4Merging)
}

func releasebucket4MergingForRecycle(b *bucket4Merging) {
	b.disjointNDV = 0
	b.Repeat = 0
	b.NDV = 0
	b.Count = 0
	bucket4MergingPool.Put(b)
}

// bucket4Merging is only used for merging partition hists to global hist.
type bucket4Merging struct {
	lower *types.Datum
	upper *types.Datum
	Bucket
	// disjointNDV is used for merging bucket NDV, see mergeBucketNDV for more details.
	disjointNDV int64
}

// newBucket4Meging creates a new bucket4Merging.
// but we create it from bucket4MergingPool as soon as possible to reduce the cost of GC.
func newBucket4Meging() *bucket4Merging {
	return &bucket4Merging{
		lower: new(types.Datum),
		upper: new(types.Datum),
		Bucket: Bucket{
			Repeat: 0,
			NDV:    0,
			Count:  0,
		},
		disjointNDV: 0,
	}
}

// buildBucket4Merging builds bucket4Merging from Histogram
// Notice: Count in Histogram.Buckets is prefix sum but in bucket4Merging is not.
func (hg *Histogram) buildBucket4Merging() []*bucket4Merging {
	buckets := make([]*bucket4Merging, 0, hg.Len())
	for i := range hg.Len() {
		b := newbucket4MergingForRecycle()
		hg.LowerToDatum(i, b.lower)
		hg.UpperToDatum(i, b.upper)
		b.Repeat = hg.Buckets[i].Repeat
		b.NDV = hg.Buckets[i].NDV
		b.Count = hg.Buckets[i].Count
		if i != 0 {
			b.Count -= hg.Buckets[i-1].Count
		}
		buckets = append(buckets, b)
	}
	return buckets
}

func (b *bucket4Merging) Clone() bucket4Merging {
	result := newbucket4MergingForRecycle()
	result.Repeat = b.Repeat
	result.NDV = b.NDV
	b.upper.Copy(result.upper)
	b.lower.Copy(result.lower)
	result.Count = b.Count
	result.disjointNDV = b.disjointNDV
	return *result
}

// mergeBucketNDV merges bucket NDV from tow bucket `right` & `left`.
// Before merging, you need to make sure that when using (upper, lower) as the comparison key, `right` is greater than `left`
func mergeBucketNDV(sc *stmtctx.StatementContext, left *bucket4Merging, right *bucket4Merging) (*bucket4Merging, error) {
	res := right.Clone()
	if left.Count == 0 {
		return &res, nil
	}
	if right.Count == 0 {
		left.lower.Copy(res.lower)
		left.upper.Copy(res.upper)
		res.NDV = left.NDV
		return &res, nil
	}
	upperCompare, err := right.upper.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	// __right__|
	// _______left____|
	// illegal order.
	if upperCompare < 0 {
		err := errors.Errorf("illegal bucket order")
		statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
		return nil, err
	}
	//  ___right_|
	//  ___left__|
	// They have the same upper.
	if upperCompare == 0 {
		lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		//      |____right____|
		//         |__left____|
		// illegal order.
		if lowerCompare < 0 {
			err := errors.Errorf("illegal bucket order")
			statslogutil.StatsLogger().Warn("fail to mergeBucketNDV", zap.Error(err))
			return nil, err
		}
		// |___right___|
		// |____left___|
		// ndv = max(right.ndv, left.ndv)
		if lowerCompare == 0 {
			if left.NDV > right.NDV {
				res.NDV = left.NDV
			}
			return &res, nil
		}
		//         |_right_|
		// |_____left______|
		// |-ratio-|
		// ndv = ratio * left.ndv + max((1-ratio) * left.ndv, right.ndv)
		ratio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(ratio*float64(left.NDV) + math.Max((1-ratio)*float64(left.NDV), float64(right.NDV)))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// ____right___|
	// ____left__|
	// right.upper > left.upper
	lowerCompareUpper, err := right.lower.Compare(sc.TypeCtx(), left.upper, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//                  |_right_|
	//  |___left____|
	// `left` and `right` do not intersect
	// We add right.ndv in `disjointNDV`, and let `right.ndv = left.ndv` be used for subsequent merge.
	// This is because, for the merging of many buckets, we merge them from back to front.
	if lowerCompareUpper >= 0 {
		left.upper.Copy(res.upper)
		left.lower.Copy(res.lower)
		res.disjointNDV += right.NDV
		res.NDV = left.NDV
		return &res, nil
	}
	upperRatio := calcFraction4Datums(right.lower, right.upper, left.upper)
	lowerCompare, err := right.lower.Compare(sc.TypeCtx(), left.lower, collate.GetBinaryCollator())
	if err != nil {
		return nil, err
	}
	//              |-upperRatio-|
	//              |_______right_____|
	// |_______left______________|
	// |-lowerRatio-|
	// ndv = lowerRatio * left.ndv
	//		+ max((1-lowerRatio) * left.ndv, upperRatio * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	if lowerCompare >= 0 {
		lowerRatio := calcFraction4Datums(left.lower, left.upper, right.lower)
		res.NDV = int64(lowerRatio*float64(left.NDV) +
			math.Max((1-lowerRatio)*float64(left.NDV), upperRatio*float64(right.NDV)) +
			(1-upperRatio)*float64(right.NDV))
		res.lower = left.lower.Clone()
		return &res, nil
	}
	// |------upperRatio--------|
	// |-lowerRatio-|
	// |____________right______________|
	//              |___left____|
	// ndv = lowerRatio * right.ndv
	//		+ max(left.ndv + (upperRatio - lowerRatio) * right.ndv)
	//		+ (1-upperRatio) * right.ndv
	lowerRatio := calcFraction4Datums(right.lower, right.upper, left.lower)
	res.NDV = int64(lowerRatio*float64(right.NDV) +
		math.Max(float64(left.NDV), (upperRatio-lowerRatio)*float64(right.NDV)) +
		(1-upperRatio)*float64(right.NDV))
	return &res, nil
}

// mergeParitionBuckets merges buckets[l...r) to one global bucket.
// global bucket:
//
//	upper = buckets[r-1].upper
//	count = sum of buckets[l...r).count
//	repeat = sum of buckets[i] (buckets[i].upper == global bucket.upper && i in [l...r))
//	ndv = merge bucket ndv from r-1 to l by mergeBucketNDV
//
// Notice: lower is not calculated here.
func mergePartitionBuckets(sc *stmtctx.StatementContext, buckets []*bucket4Merging) (*bucket4Merging, error) {
	if len(buckets) == 0 {
		return nil, errors.Errorf("not enough buckets to merge")
	}
	res := newbucket4MergingForRecycle()
	buckets[len(buckets)-1].upper.Copy(res.upper)
	right := buckets[len(buckets)-1].Clone()

	totNDV := int64(0)
	intest.Assert(res.Count == 0, "Count in the new bucket4Merging should be 0")
	intest.Assert(res.Repeat == 0, "Repeat in the new bucket4Merging should be 0")
	intest.Assert(res.NDV == 0, "NDV in the new bucket4Merging bucket4Merging should be 0")
	for i := len(buckets) - 1; i >= 0; i-- {
		totNDV += buckets[i].NDV
		res.Count += buckets[i].Count
		compare, err := buckets[i].upper.Compare(sc.TypeCtx(), res.upper, collate.GetBinaryCollator())
		if err != nil {
			return nil, err
		}
		if compare == 0 {
			res.Repeat += buckets[i].Repeat
		}
		if i != len(buckets)-1 {
			tmp, err := mergeBucketNDV(sc, buckets[i], &right)
			if err != nil {
				return nil, err
			}
			right = *tmp
		}
	}
	res.NDV = right.NDV + right.disjointNDV

	// since `mergeBucketNDV` is based on uniform and inclusion assumptions, it has the trend to under-estimate,
	// and as the number of buckets increases, these assumptions become weak,
	// so to mitigate this problem, a damping factor based on the number of buckets is introduced.
	res.NDV = min(int64(float64(res.NDV)*math.Pow(1.15, float64(len(buckets)-1))), totNDV)
	return res, nil
}

func (t *TopNMeta) buildBucket4Merging(d *types.Datum, analyzeVer int) *bucket4Merging {
	res := newbucket4MergingForRecycle()
	d.Copy(res.lower)
	d.Copy(res.upper)
	res.Count = int64(t.Count)
	res.Repeat = int64(t.Count)
	if analyzeVer <= Version2 {
		res.NDV = 0
	}
	failpoint.Inject("github.com/pingcap/pkg/statistics/enableTopNNDV", func(_ failpoint.Value) {
		res.NDV = 1
	})
	intest.Assert(analyzeVer <= Version2)
	return res
}

// MergePartitionHist2GlobalHist merges hists (partition-level Histogram) to a global-level Histogram
func MergePartitionHist2GlobalHist(sc *stmtctx.StatementContext, hists []*Histogram, popedTopN []TopNMeta, expBucketNumber int64, isIndex bool, analyzeVer int) (*Histogram, error) {
	var totCount, totNull, totColSize int64
	var bucketNumber int
	if expBucketNumber == 0 {
		return nil, errors.Errorf("expBucketNumber can not be zero")
	}
	// This only occurs when there are no histogram records in the histogram system table.
	// It happens only to tables whose DDL events haven't been processed yet and that have no indexes or keys,
	// with the predicate column feature enabled.
	if len(hists) == 0 {
		return nil, nil
	}
	for _, hist := range hists {
		totColSize += hist.TotColSize
		totNull += hist.NullCount
		histLen := hist.Len()
		if histLen > 0 {
			bucketNumber += histLen
			totCount += hist.Buckets[hist.Len()-1].Count
		}
	}
	// If all the hist and the topn is empty, return a empty hist.
	if bucketNumber+len(popedTopN) == 0 {
		return NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, 0, totColSize), nil
	}

	bucketNumber += len(popedTopN)
	buckets := make([]*bucket4Merging, 0, bucketNumber)
	globalBuckets := make([]*bucket4Merging, 0, expBucketNumber)

	// init `buckets`.
	for _, hist := range hists {
		buckets = append(buckets, hist.buildBucket4Merging()...)
	}

	for _, meta := range popedTopN {
		totCount += int64(meta.Count)
		d, err := topNMetaToDatum(meta, hists[0].Tp.GetType(), isIndex, sc.TimeZone())
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, meta.buildBucket4Merging(&d, analyzeVer))
	}

	// Remove empty buckets
	tail := 0
	for i := range buckets {
		if buckets[i].Count != 0 {
			// Because we will reuse the tail of the slice in `releasebucket4MergingForRecycle`,
			// we need to shift the non-empty buckets to the front.
			buckets[tail], buckets[i] = buckets[i], buckets[tail]
			tail++
		}
	}
	for n := tail; n < len(buckets); n++ {
		releasebucket4MergingForRecycle(buckets[n])
	}
	buckets = buckets[:tail]

	err := sortBucketsByUpperBound(sc.TypeCtx(), buckets)
	if err != nil {
		return nil, err
	}

	var sum, prevSum int64
	r := len(buckets)
	bucketCount := int64(1)
	gBucketCountThreshold := (totCount / expBucketNumber) * 80 / 100 // expectedBucketSize * 0.8
	mergeBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber)-1)/int(expBucketNumber))
	cutAndFixBuffer := make([]*bucket4Merging, 0, (len(buckets)+int(expBucketNumber))/int(expBucketNumber))
	var currentLeftMost *types.Datum
	for i := len(buckets) - 1; i >= 0; i-- {
		if currentLeftMost == nil {
			currentLeftMost = buckets[i].lower
		} else {
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}
		}
		sum += buckets[i].Count
		if sum >= totCount*bucketCount/expBucketNumber && sum-prevSum >= gBucketCountThreshold {
			// If the buckets have the same upper, we merge them into the same new buckets.
			// We don't need to update the currentLeftMost in the for loop because the leftmost bucket's lower
			// will be the smallest when their upper is the same.
			// We just need to update it after the for loop.
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), buckets[i].upper, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				if res != 0 {
					break
				}
				sum += buckets[i-1].Count
			}
			res, err := currentLeftMost.Compare(sc.TypeCtx(), buckets[i].lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				currentLeftMost = buckets[i].lower
			}

			// Iterate possible overlapped ones.
			// We need to re-sort this part.
			mergeBuffer = mergeBuffer[:0]
			cutAndFixBuffer = cutAndFixBuffer[:0]
			leftMostValidPosForNonOverlapping := i
			for ; i > 0; i-- {
				res, err := buckets[i-1].upper.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].upper < currentLeftMost, this bucket has no overlap with current merging one. Break it.
				if res < 0 {
					break
				}
				// Now the bucket[i-1].upper >= currentLeftMost, they are overlapped.
				res, err = buckets[i-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
				if err != nil {
					return nil, err
				}
				// If buckets[i-1].lower >= currentLeftMost, this bucket is totally inside. So it can be totally merged.
				if res >= 0 {
					sum += buckets[i-1].Count
					mergeBuffer = append(mergeBuffer, buckets[i-1])
					continue
				}
				// Now buckets[i-1].lower < currentLeftMost < buckets[i-1].upper
				// calcFraction4Datums calc the value: (currentLeftMost - lower_bound) / (upper_bound - lower_bound)
				overlapping := 1 - calcFraction4Datums(buckets[i-1].lower, buckets[i-1].upper, currentLeftMost)
				overlappedCount := int64(float64(buckets[i-1].Count) * overlapping)
				overlappedNDV := int64(float64(buckets[i-1].NDV) * overlapping)
				sum += overlappedCount
				buckets[i-1].Count -= overlappedCount
				buckets[i-1].NDV -= overlappedNDV
				buckets[i-1].Repeat = 0
				if buckets[i-1].NDV < 0 {
					buckets[i-1].NDV = 0
				}
				if buckets[i-1].Count < 0 {
					buckets[i-1].Count = 0
				}

				// Cut it.
				cutBkt := newbucket4MergingForRecycle()
				buckets[i-1].upper.Copy(cutBkt.upper)
				currentLeftMost.Copy(cutBkt.lower)
				currentLeftMost.Copy(buckets[i-1].upper)
				cutBkt.Count = overlappedCount
				cutBkt.NDV = overlappedNDV
				mergeBuffer = append(mergeBuffer, cutBkt)
				cutAndFixBuffer = append(cutAndFixBuffer, cutBkt)
			}
			var merged *bucket4Merging
			if len(cutAndFixBuffer) == 0 {
				merged, err = mergePartitionBuckets(sc, buckets[i:r])
				if err != nil {
					return nil, err
				}
			} else {
				// mergedBuffer is in reverse order, we need to reverse it.
				slices.Reverse(mergeBuffer)
				// The content in the merge buffer don't need a re-sort since we just fix some lower bound for them.
				mergeBuffer = append(mergeBuffer, buckets[leftMostValidPosForNonOverlapping:r]...)
				checkBucket4MergingIsSorted(sc.TypeCtx(), mergeBuffer)
				merged, err = mergePartitionBuckets(sc, mergeBuffer)
				if err != nil {
					return nil, err
				}
				for _, bkt := range cutAndFixBuffer {
					releasebucket4MergingForRecycle(bkt)
				}
				// The buckets in buckets[i:origI] needs a re-sort.
				err = sortBucketsByUpperBound(sc.TypeCtx(), buckets[i:leftMostValidPosForNonOverlapping])
				if err != nil {
					return nil, err
				}
				// After the operation, the buckets in buckets[i:origI] contains two kinds of buckets:
				// 1. The buckets that are totally inside the merged bucket. => lower_bound >= currentLeftMost
				//    It's not changed. [lower_bound_i, upper_bound_i] with lower_bound_i >= currentLeftMost
				// 2. The buckets that are overlapped with the merged bucket. lower_bound < currentLeftMost < upper_bound
				//    After cutting, the remained part is [lower_bound_i, currentLeftMost]
				// To do the next round of merging, we need to kick out the 1st kind of buckets.
				// And after the re-sort, the 2nd kind of buckets will be in the front.
				leftMostInvalidPosForNextRound := leftMostValidPosForNonOverlapping
				for ; leftMostInvalidPosForNextRound > i; leftMostInvalidPosForNextRound-- {
					res, err := buckets[leftMostInvalidPosForNextRound-1].lower.Compare(sc.TypeCtx(), currentLeftMost, collate.GetBinaryCollator())
					if err != nil {
						return nil, err
					}
					// Once the lower bound < currentLeftMost, we've skipped all the 1st kind of bucket.
					// We can break here.
					if res < 0 {
						break
					}
				}
				checkBucket4MergingIsSorted(sc.TypeCtx(), buckets[i:leftMostInvalidPosForNextRound])
				i = leftMostInvalidPosForNextRound
			}
			currentLeftMost.Copy(merged.lower)
			currentLeftMost = nil
			globalBuckets = append(globalBuckets, merged)
			r = i
			bucketCount++
			prevSum = sum
		}
	}
	if r > 0 {
		leftMost := buckets[0].lower
		for i, b := range buckets[:r] {
			if i == 0 {
				continue
			}
			res, err := leftMost.Compare(sc.TypeCtx(), b.lower, collate.GetBinaryCollator())
			if err != nil {
				return nil, err
			}
			if res > 0 {
				leftMost = b.lower
			}
		}

		merged, err := mergePartitionBuckets(sc, buckets[:r])
		if err != nil {
			return nil, err
		}
		leftMost.Copy(merged.lower)
		globalBuckets = append(globalBuckets, merged)
	}
	for i := range buckets {
		releasebucket4MergingForRecycle(buckets[i])
	}
	// Because we merge backwards, we need to flip the slices.
	slices.Reverse(globalBuckets)

	for i := 1; i < len(globalBuckets); i++ {
		globalBuckets[i].Count = globalBuckets[i].Count + globalBuckets[i-1].Count
	}

	// Recalculate repeats
	// TODO: optimize it later since it's a simple but not the fastest implementation whose complexity is O(nBkt * nHist * log(nBkt))
	for _, bucket := range globalBuckets {
		var repeat float64
		for _, hist := range hists {
			histRowCount, _ := hist.EqualRowCount(nil, *bucket.upper, isIndex)
			repeat += histRowCount // only hists of indexes have bucket.NDV
		}
		if int64(repeat) > bucket.Repeat {
			bucket.Repeat = int64(repeat)
		}
	}

	globalHist := NewHistogram(hists[0].ID, 0, totNull, hists[0].LastUpdateVersion, hists[0].Tp, len(globalBuckets), totColSize)
	for _, bucket := range globalBuckets {
		if !isIndex {
			bucket.NDV = 0 // bucket.NDV is not maintained for column histograms
		}
		globalHist.AppendBucketWithNDV(bucket.lower, bucket.upper, bucket.Count, bucket.Repeat, bucket.NDV)
	}
	return globalHist, nil
}

// sortBucketsByUpperBound the bucket by upper bound first, then by lower bound.
// If bkt[i].upper = bkt[i+1].upper, then we'll get bkt[i].lower < bkt[i+1].lower.
func sortBucketsByUpperBound(ctx types.Context, buckets []*bucket4Merging) error {
	var sortError error
	slices.SortFunc(buckets, func(i, j *bucket4Merging) int {
		res, err := i.upper.Compare(ctx, j.upper, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		if res != 0 {
			return res
		}
		res, err = i.lower.Compare(ctx, j.lower, collate.GetBinaryCollator())
		if err != nil {
			sortError = err
		}
		return res
	})
	return sortError
}

// checkBucket4MergingIsSorted checks whether the buckets are sorted by upper bound first, then by lower bound.
// using intest.AssertFunc to avoid the check in production.
func checkBucket4MergingIsSorted(ctx types.Context, buckets []*bucket4Merging) {
	intest.AssertFunc(func() bool {
		var sortErr error
		isOrdered := slices.IsSortedFunc(buckets, func(i, j *bucket4Merging) int {
			res, err := i.upper.Compare(ctx, j.upper, collate.GetBinaryCollator())
			if err != nil {
				sortErr = err
			}
			if res != 0 {
				return res
			}
			res, err = i.lower.Compare(ctx, j.lower, collate.GetBinaryCollator())
			if err != nil {
				sortErr = err
			}
			return res
		})
		return isOrdered && sortErr == nil
	}, "the buckets are not sorted actually")
}

const (
	// AllLoaded indicates all statistics are loaded
	AllLoaded = iota
	// AllEvicted indicates all statistics are evicted
	AllEvicted
)

// StatsLoadedStatus indicates the status of statistics
type StatsLoadedStatus struct {
	statsInitialized bool
	evictedStatus    int
}

// NewStatsFullLoadStatus returns the status that the column/index fully loaded
func NewStatsFullLoadStatus() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: true,
		evictedStatus:    AllLoaded,
	}
}

// NewStatsAllEvictedStatus returns the status that only loads count/nullCount/NDV and doesn't load CMSketch/TopN/Histogram.
// When we load table stats, column stats is in AllEvicted status by default. CMSketch/TopN/Histogram of column is only
// loaded when we really need column stats.
func NewStatsAllEvictedStatus() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: true,
		evictedStatus:    AllEvicted,
	}
}

// Copy copies the status
func (s *StatsLoadedStatus) Copy() StatsLoadedStatus {
	return StatsLoadedStatus{
		statsInitialized: s.statsInitialized,
		evictedStatus:    s.evictedStatus,
	}
}

// IsStatsInitialized indicates whether the column/index's statistics was loaded from storage before.
// Note that `IsStatsInitialized` only can be set in initializing
func (s StatsLoadedStatus) IsStatsInitialized() bool {
	return s.statsInitialized
}

// IsLoadNeeded indicates whether it needs load statistics during LoadNeededHistograms or sync stats
// If the column/index was loaded and any statistics of it is evicting, it also needs re-load statistics.
func (s StatsLoadedStatus) IsLoadNeeded() bool {
	if s.statsInitialized {
		return s.evictedStatus > AllLoaded
	}
	// If statsInitialized is false, it means there is no stats for the column/index in the storage.
	// Hence, we don't need to trigger the task of loading the column/index stats.
	return false
}

// IsEssentialStatsLoaded indicates whether the essential statistics is loaded.
// If the column/index was loaded, and at least histogram and topN still exists, the necessary statistics is still loaded.
func (s StatsLoadedStatus) IsEssentialStatsLoaded() bool {
	return s.statsInitialized && (s.evictedStatus < AllEvicted)
}

// IsAllEvicted indicates whether all the stats got evicted or not.
func (s StatsLoadedStatus) IsAllEvicted() bool {
	return s.statsInitialized && s.evictedStatus >= AllEvicted
}

// IsFullLoad indicates whether the stats are full loaded
func (s StatsLoadedStatus) IsFullLoad() bool {
	return s.statsInitialized && s.evictedStatus == AllLoaded
}
