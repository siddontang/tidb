// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"fmt"
	"hash/crc32"
	"math"
	"strconv"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type crc32FunctionClass struct {
	baseFunctionClass
}

func (c *crc32FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinCRC32Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CRC32)
	return sig, nil
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCRC32Sig) Clone() builtinFunc {
	newSig := &builtinCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSignSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sign)
	return sig, nil
}

type builtinSignSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSignSig) Clone() builtinFunc {
	newSig := &builtinSignSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		return 1, false, nil
	} else if val == 0 {
		return 0, false, nil
	}
	return -1, false, nil
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSqrtSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sqrt)
	return sig, nil
}

type builtinSqrtSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSqrtSig) Clone() builtinFunc {
	newSig := &builtinSqrtSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		return 0, true, nil
	}
	return math.Sqrt(val), false, nil
}

type acosFunctionClass struct {
	baseFunctionClass
}

func (c *acosFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinAcosSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Acos)
	return sig, nil
}

type builtinAcosSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAcosSig) Clone() builtinFunc {
	newSig := &builtinAcosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAcosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Acos(val), false, nil
}

type asinFunctionClass struct {
	baseFunctionClass
}

func (c *asinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinAsinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Asin)
	return sig, nil
}

type builtinAsinSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAsinSig) Clone() builtinFunc {
	newSig := &builtinAsinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAsinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Asin(val), false, nil
}

type atanFunctionClass struct {
	baseFunctionClass
}

func (c *atanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	var err error
	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	}

	if argsLen == 1 {
		sig = &builtinAtan1ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Atan1Arg)
	} else {
		sig = &builtinAtan2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Atan2Args)
	}

	return sig, nil
}

type builtinAtan1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAtan1ArgSig) Clone() builtinFunc {
	newSig := &builtinAtan1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan1ArgSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan(val), false, nil
}

type builtinAtan2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAtan2ArgsSig) Clone() builtinFunc {
	newSig := &builtinAtan2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(y, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan2ArgsSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Atan2(val1, val2), false, nil
}

type cosFunctionClass struct {
	baseFunctionClass
}

func (c *cosFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinCosSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Cos)
	return sig, nil
}

type builtinCosSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCosSig) Clone() builtinFunc {
	newSig := &builtinCosSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Cos(val), false, nil
}

type cotFunctionClass struct {
	baseFunctionClass
}

func (c *cotFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinCotSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Cot)
	return sig, nil
}

type builtinCotSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCotSig) Clone() builtinFunc {
	newSig := &builtinCotSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	tan := math.Tan(val)
	if tan != 0 {
		cot := 1 / tan
		if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
			return cot, false, nil
		}
	}
	return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(val, 'f', -1, 64)))
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinDegreesSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Degrees)
	return sig, nil
}

type builtinDegreesSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDegreesSig) Clone() builtinFunc {
	newSig := &builtinDegreesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinDegreesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := val * 180 / math.Pi
	return res, false, nil
}

type expFunctionClass struct {
	baseFunctionClass
}

func (c *expFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinExpSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Exp)
	return sig, nil
}

type builtinExpSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinExpSig) Clone() builtinFunc {
	newSig := &builtinExpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	exp := math.Exp(val)
	if math.IsInf(exp, 0) || math.IsNaN(exp) {
		s := fmt.Sprintf("exp(%s)", b.args[0].StringWithCtx(ctx, perrors.RedactLogDisable))
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", s)
	}
	return exp, false, nil
}

type piFunctionClass struct {
	baseFunctionClass
}

func (c *piFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		bf  baseBuiltinFunc
		sig builtinFunc
	)

	var err error
	bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.SetDecimal(6)
	bf.tp.SetFlen(8)
	sig = &builtinPISig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_PI)
	return sig, nil
}

type builtinPISig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPISig) Clone() builtinFunc {
	newSig := &builtinPISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	return float64(math.Pi), false, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinRadiansSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Radians)
	return sig, nil
}

type builtinRadiansSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRadiansSig) Clone() builtinFunc {
	newSig := &builtinRadiansSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RADIANS(X).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return x * (math.Pi / 180), false, nil
}

type sinFunctionClass struct {
	baseFunctionClass
}

func (c *sinFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sin)
	return sig, nil
}

type builtinSinSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSinSig) Clone() builtinFunc {
	newSig := &builtinSinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinSinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Sin(val), false, nil
}

type tanFunctionClass struct {
	baseFunctionClass
}

func (c *tanFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinTanSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Tan)
	return sig, nil
}

type builtinTanSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTanSig) Clone() builtinFunc {
	newSig := &builtinTanSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinTanSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Tan(val), false, nil
}
