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

package expression

import (
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Length)
	return sig, nil
}

type builtinLengthSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLengthSig) Clone() builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(3)
	sig := &builtinASCIISig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ASCII)
	return sig, nil
}

type builtinASCIISig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinASCIISig) Clone() builtinFunc {
	newSig := &builtinASCIISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	addBinFlag(bf.tp)
	bf.tp.SetFlen(0)
	for i := range args {
		argType := args[i].GetType(ctx.GetEvalCtx())

		if argType.GetFlen() < 0 {
			bf.tp.SetFlen(mysql.MaxBlobWidth)
			logutil.BgLogger().Debug("unexpected `Flen` value(-1) in CONCAT's args", zap.Int("arg's index", i))
		}
		bf.tp.SetFlen(bf.tp.GetFlen() + argType.GetFlen())
	}
	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	sig := &builtinConcatSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Concat)
	return sig, nil
}

type builtinConcatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatSig) Clone() builtinFunc {
	newSig := &builtinConcatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinConcatSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	//nolint: prealloc
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		if uint64(len(s)+len(d)) > b.maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "concat", b.maxAllowedPacket)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(0)

	addBinFlag(bf.tp)
	for i := range args {
		argType := args[i].GetType(ctx.GetEvalCtx())

		// skip separator param
		if i != 0 {
			if argType.GetFlen() < 0 {
				bf.tp.SetFlen(mysql.MaxBlobWidth)
				logutil.BgLogger().Debug("unexpected `Flen` value(-1) in CONCAT_WS's args", zap.Int("arg's index", i))
			}
			bf.tp.SetFlen(bf.tp.GetFlen() + argType.GetFlen())
		}
	}

	// add separator
	sepsLen := len(args) - 2
	bf.tp.SetFlen(bf.tp.GetFlen() + sepsLen*args[0].GetType(ctx.GetEvalCtx()).GetFlen())

	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	sig := &builtinConcatWSSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_ConcatWS)
	return sig, nil
}

type builtinConcatWSSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatWSSig) Clone() builtinFunc {
	newSig := &builtinConcatWSSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a CONCAT_WS(separator,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	var targetLength int

	N := len(args)
	if N > 0 {
		val, isNull, err := args[0].EvalString(ctx, row)
		if err != nil || isNull {
			// If the separator is NULL, the result is NULL.
			return val, isNull, err
		}
		sep = val
	}
	for i := 1; i < N; i++ {
		val, isNull, err := args[i].EvalString(ctx, row)
		if err != nil {
			return val, isNull, err
		}
		if isNull {
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			continue
		}

		targetLength += len(val)
		if i > 1 {
			targetLength += len(sep)
		}
		if uint64(targetLength) > b.maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "concat_ws", b.maxAllowedPacket)
		}
		strs = append(strs, val)
	}

	str := strings.Join(strs, sep)
	// todo check whether the length of result is larger than flen
	// if b.tp.flen != types.UnspecifiedLength && len(str) > b.tp.flen {
	//	return "", true, nil
	// }
	return str, false, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinLeftSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Left)
		return sig, nil
	}
	sig := &builtinLeftUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LeftUTF8)
	return sig, nil
}

type builtinLeftSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeftSig) Clone() builtinFunc {
	newSig := &builtinLeftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	leftLength := int(left)
	if strLength := len(str); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return str[:leftLength], false, nil
}

type builtinLeftUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeftUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLeftUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes, leftLength := []rune(str), int(left)
	if runeLength := len(runes); leftLength > runeLength {
		leftLength = runeLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return string(runes[:leftLength]), false, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinRightSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Right)
		return sig, nil
	}
	sig := &builtinRightUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_RightUTF8)
	return sig, nil
}

type builtinRightSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRightSig) Clone() builtinFunc {
	newSig := &builtinRightSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	strLength, rightLength := len(str), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return str[strLength-rightLength:], false, nil
}

type builtinRightUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRightUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRightUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	strLength, rightLength := len(runes), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return string(runes[strLength-rightLength:]), false, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	SetBinFlagOrBinStr(args[0].GetType(ctx.GetEvalCtx()), bf.tp)
	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	sig := &builtinRepeatSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Repeat)
	return sig, nil
}

type builtinRepeatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRepeatSig) Clone() builtinFunc {
	newSig := &builtinRepeatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	byteLength := len(str)

	num, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "repeat", b.maxAllowedPacket)
	}

	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argTp.GetFlen())
	SetBinFlagOrBinStr(argTp, bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinLowerSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Lower)
	} else {
		sig = &builtinLowerUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LowerUTF8)
	}
	return sig, nil
}

type builtinLowerUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLowerUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLowerUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	return enc.ToLower(d), false, nil
}

type builtinLowerSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLowerSig) Clone() builtinFunc {
	newSig := &builtinLowerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	addBinFlag(bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) || types.IsTypeBit(argTp) {
		sig = &builtinReverseSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Reverse)
	} else {
		sig = &builtinReverseUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ReverseUTF8)
	}
	return sig, nil
}

type builtinReverseSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReverseSig) Clone() builtinFunc {
	newSig := &builtinReverseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReverseUTF8Sig) Clone() builtinFunc {
	newSig := &builtinReverseUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	sig := &builtinSpaceSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Space)
	return sig, nil
}

type builtinSpaceSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinSpaceSig) Clone() builtinFunc {
	newSig := &builtinSpaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if x < 0 {
		x = 0
	}
	if uint64(x) > b.maxAllowedPacket {
		return d, true, handleAllowedPacketOverflowed(ctx, "space", b.maxAllowedPacket)
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argTp.GetFlen())
	SetBinFlagOrBinStr(argTp, bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinUpperSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Upper)
	} else {
		sig = &builtinUpperUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UpperUTF8)
	}
	return sig, nil
}

type builtinUpperUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUpperUTF8Sig) Clone() builtinFunc {
	newSig := &builtinUpperUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	return enc.ToUpper(d), false, nil
}

type builtinUpperSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUpperSig) Clone() builtinFunc {
	newSig := &builtinUpperSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Strcmp)
	return sig, nil
}

type builtinStrcmpSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinStrcmpSig) Clone() builtinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	right, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := types.CompareString(left, right, b.collation)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.fixLength(ctx.GetEvalCtx(), args))
	for _, a := range args {
		SetBinFlagOrBinStr(a.GetType(ctx.GetEvalCtx()), bf.tp)
	}
	sig := &builtinReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Replace)
	return sig, nil
}

// fixLength calculate the flen of the return type.
func (c *replaceFunctionClass) fixLength(ctx EvalContext, args []Expression) int {
	charLen := args[0].GetType(ctx).GetFlen()
	oldStrLen := args[1].GetType(ctx).GetFlen()
	diff := args[2].GetType(ctx).GetFlen() - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReplaceSig) Clone() builtinFunc {
	newSig := &builtinReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	oldStr, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	newStr, isNull, err = b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.ReplaceAll(str, oldStr, newStr), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	charsetArg, ok := args[1].(*Constant)
	if !ok {
		// `args[1]` is limited by parser to be a constant string,
		// should never go into here.
		return nil, errIncorrectArgs.GenWithStackByArgs("charset")
	}
	transcodingName := charsetArg.Value.GetString()
	bf.tp.SetCharset(strings.ToLower(transcodingName))
	// Quoted about the behavior of syntax CONVERT(expr, type) to CHAR():
	// In all cases, the string has the default collation for the character set.
	// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
	// Here in syntax CONVERT(expr USING transcoding_name), behavior is kept the same,
	// picking the default collation of target charset.
	str1, err1 := charset.GetDefaultCollation(bf.tp.GetCharset())
	bf.tp.SetCollate(str1)
	if err1 != nil {
		return nil, errUnknownCharacterSet.GenWithStackByArgs(transcodingName)
	}
	// convert function should always derive to CoercibilityImplicit
	bf.SetCoercibility(CoercibilityImplicit)
	if bf.tp.GetCharset() == charset.CharsetASCII {
		bf.SetRepertoire(ASCII)
	} else {
		bf.SetRepertoire(UNICODE)
	}
	// Result will be a binary string if converts charset to BINARY.
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-binary-set.html
	if types.IsBinaryStr(bf.tp) {
		types.SetBinChsClnFlag(bf.tp)
	} else {
		bf.tp.DelFlag(mysql.BinaryFlag)
	}

	bf.tp.SetFlen(mysql.MaxBlobWidth)
	sig := &builtinConvertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Convert)
	return sig, nil
}

type builtinConvertSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinConvertSig) Clone() builtinFunc {
	newSig := &builtinConvertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONVERT(expr USING transcoding_name).
// Syntax CONVERT(expr, type) is parsed as cast expr so not handled here.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	expr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	argTp, resultTp := b.args[0].GetType(ctx), b.tp
	if !charset.IsSupportedEncoding(resultTp.GetCharset()) {
		return "", false, errUnknownCharacterSet.GenWithStackByArgs(resultTp.GetCharset())
	}
	if types.IsBinaryStr(argTp) {
		// Convert charset binary -> utf8. If it meets error, NULL is returned.
		enc := charset.FindEncoding(resultTp.GetCharset())
		ret, err := enc.Transform(nil, hack.Slice(expr), charset.OpDecodeReplace)
		return string(ret), err != nil, nil
	} else if types.IsBinaryStr(resultTp) {
		// Convert charset utf8 -> binary.
		enc := charset.FindEncoding(argTp.GetCharset())
		ret, err := enc.Transform(nil, hack.Slice(expr), charset.OpEncode)
		return string(ret), false, err
	}
	enc := charset.FindEncoding(resultTp.GetCharset())
	if !enc.IsValid(hack.Slice(expr)) {
		replace, _ := enc.Transform(nil, hack.Slice(expr), charset.OpReplaceNoErr)
		return string(replace), false, nil
	}
	return expr, false, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString, types.ETInt}
	if len(args) == 3 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}

	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)

	var sig builtinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		sig = &builtinSubstring3ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring3Args)
	case len(args) == 3:
		sig = &builtinSubstring3ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring3ArgsUTF8)
	case len(args) == 2 && types.IsBinaryStr(argType):
		sig = &builtinSubstring2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring2Args)
	case len(args) == 2:
		sig = &builtinSubstring2ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring2ArgsUTF8)
	default:
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	return sig, nil
}

type builtinSubstring2ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring2ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length := int64(len(str))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return str[pos:], false, nil
}

type builtinSubstring2ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	length := int64(len(runes))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return string(runes[pos:]), false, nil
}

type builtinSubstring3ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring3ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLen := int64(len(str))
	if pos < 0 {
		pos += byteLen
	} else {
		pos--
	}
	if pos > byteLen || pos < 0 {
		pos = byteLen
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < byteLen {
		return str[pos:end], false, nil
	}
	return str[pos:], false, nil
}

type builtinSubstring3ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runes := []rune(str)
	numRunes := int64(len(runes))
	if pos < 0 {
		pos += numRunes
	} else {
		pos--
	}
	if pos > numRunes || pos < 0 {
		pos = numRunes
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < numRunes {
		return string(runes[pos:end]), false, nil
	}
	return string(runes[pos:]), false, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinSubstringIndexSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SubstringIndex)
	return sig, nil
}

type builtinSubstringIndexSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstringIndexSig) Clone() builtinFunc {
	newSig := &builtinSubstringIndexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	delim, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	count, isNull, err = b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if len(delim) == 0 {
		return "", false, nil
	}
	// when count > MaxInt64, returns whole string.
	if count < 0 && mysql.HasUnsignedFlag(b.args[2].GetType(ctx).GetFlag()) {
		return str, false, nil
	}

	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < 0 {
			// -count overflows max int64, returns whole string.
			return str, false, nil
		}

		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	return strings.Join(substrs, delim), false, nil
}

type locateFunctionClass struct {
	baseFunctionClass
}

func (c *locateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	hasStartPos, argTps := len(args) == 3, []types.EvalType{types.ETString, types.ETString}
	if hasStartPos {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	// Locate is multibyte safe.
	useBinary := bf.collation == charset.CollationBin
	switch {
	case hasStartPos && useBinary:
		sig = &builtinLocate3ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate3Args)
	case hasStartPos:
		sig = &builtinLocate3ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate3ArgsUTF8)
	case useBinary:
		sig = &builtinLocate2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate2Args)
	default:
		sig = &builtinLocate2ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate2ArgsUTF8)
	}
	return sig, nil
}

type builtinLocate2ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate2ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if subStrLen == 0 {
		return 1, false, nil
	}
	ret, idx := 0, strings.Index(str, subStr)
	if idx != -1 {
		ret = idx + 1
	}
	return int64(ret), false, nil
}

type builtinLocate2ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if int64(len([]rune(subStr))) == 0 {
		return 1, false, nil
	}

	return locateStringWithCollation(str, subStr, b.collation), false, nil
}

type builtinLocate3ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate3ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pos, isNull, err := b.args[2].EvalInt(ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if pos < 0 || pos > int64(len(str)-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := str[pos:]
	idx := strings.Index(slice, subStr)
	if idx != -1 {
		return pos + int64(idx) + 1, false, nil
	}
	return 0, false, nil
}

type builtinLocate3ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if collate.IsCICollation(b.collation) {
		subStr = strings.ToLower(subStr)
		str = strings.ToLower(str)
	}
	pos, isNull, err := b.args[2].EvalInt(ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len([]rune(subStr))
	if pos < 0 || pos > int64(len([]rune(str))-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := string([]rune(str)[pos:])

	idx := locateStringWithCollation(slice, subStr, b.collation)
	if idx != 0 {
		return pos + idx, false, nil
	}
	return 0, false, nil
}
