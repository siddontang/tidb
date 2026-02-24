// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

//revive:disable:defer
func (b *builtinLowerSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	// if error is not nil return error, or builtinLowerSig is for binary strings (do nothing)
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinLowerSig) vectorized() bool {
	return true
}

func (b *builtinLowerUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(enc.ToLower(buf.GetString(i)))
		}
	}
	return nil
}

func (b *builtinLowerUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRepeatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		num := nums[i]
		if num < 1 {
			result.AppendString("")
			continue
		}
		if num > math.MaxInt32 {
			// to avoid overflow when calculating uint64(byteLength)*uint64(num) later
			num = math.MaxInt32
		}

		str := buf.GetString(i)
		byteLength := len(str)
		if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "repeat", b.maxAllowedPacket); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		if int64(byteLength) > int64(b.tp.GetFlen())/num {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.Repeat(str, int(num)))
	}
	return nil
}

func (b *builtinRepeatSig) vectorized() bool {
	return true
}

func (b *builtinStringIsNullSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinStringIsNullSig) vectorized() bool {
	return true
}

func (b *builtinUpperUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(enc.ToUpper(buf.GetString(i)))
		}
	}
	return nil
}

func (b *builtinUpperUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinUpperSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalString(ctx, input, result)
}

func (b *builtinUpperSig) vectorized() bool {
	return true
}

func (b *builtinLeftUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		runes, leftLength := []rune(str), int(nums[i])
		if runeLength := len(runes); leftLength > runeLength {
			leftLength = runeLength
		} else if leftLength < 0 {
			leftLength = 0
		}

		result.AppendString(string(runes[:leftLength]))
	}
	return nil
}

func (b *builtinLeftUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinRightUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		runes := []rune(str)
		strLength, rightLength := len(runes), int(nums[i])
		if rightLength > strLength {
			rightLength = strLength
		} else if rightLength < 0 {
			rightLength = 0
		}

		result.AppendString(string(runes[strLength-rightLength:]))
	}
	return nil
}

func (b *builtinRightUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		num := max(nums[i], 0)
		if uint64(num) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "space", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		if num > mysql.MaxBlobWidth {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.Repeat(" ", int(num)))
	}
	return nil
}

func (b *builtinSpaceSig) vectorized() bool {
	return true
}

// vecEvalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		str := result.GetString(i)
		reversed := reverseRunes([]rune(str))
		result.SetRaw(i, []byte(string(reversed)))
	}
	return nil
}

func (b *builtinReverseUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinConcatSig) vectorized() bool {
	return true
}

// vecEvalString evals a CONCAT(str1,str2,...)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	strs := make([][]byte, n)
	isNulls := make([]bool, n)
	result.ReserveString(n)
	var byteBuf []byte
	for j := range b.args {
		if err := b.args[j].VecEvalString(ctx, input, buf); err != nil {
			return err
		}
		for i := range n {
			if isNulls[i] {
				continue
			}
			if buf.IsNull(i) {
				isNulls[i] = true
				continue
			}
			byteBuf = buf.GetBytes(i)
			if uint64(len(strs[i])+len(byteBuf)) > b.maxAllowedPacket {
				if err := handleAllowedPacketOverflowed(ctx, "concat", b.maxAllowedPacket); err != nil {
					return err
				}

				isNulls[i] = true
				continue
			}
			strs[i] = append(strs[i], byteBuf...)
		}
	}
	for i := range n {
		if isNulls[i] {
			result.AppendNull()
		} else {
			result.AppendBytes(strs[i])
		}
	}
	return nil
}

func (b *builtinLocate3ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str,pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	// store positions in result
	if err := b.args[2].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	ci := collate.IsCICollation(b.collation)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		pos := i64s[i]

		// Transfer the argument which starts from 1 to real index which starts from 0.
		pos--
		strLen := int64(len([]rune(str)))
		subStrLen := int64(len([]rune(subStr)))
		if pos < 0 || pos > strLen-subStrLen {
			i64s[i] = 0
			continue
		} else if subStrLen == 0 {
			i64s[i] = pos + 1
			continue
		}
		slice := string([]rune(str)[pos:])
		if ci {
			subStr = strings.ToLower(subStr)
			slice = strings.ToLower(slice)
		}
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = pos + int64(utf8.RuneCountInString(slice[:idx])) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinHexStrArgSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.ToUpper(hex.EncodeToString(buf0.GetBytes(i))))
	}
	return nil
}

func (b *builtinLTrimSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		result.AppendString(strings.TrimLeft(str, spaceChars))
	}

	return nil
}

func (b *builtinQuoteSig) vectorized() bool {
	return true
}

func (b *builtinQuoteSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendString("NULL")
			continue
		}
		str := buf.GetString(i)
		result.AppendString(Quote(str))
	}
	return nil
}

func (b *builtinInsertSig) vectorized() bool {
	return true
}

func (b *builtinInsertSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	pos, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(pos)
	if err := b.args[1].VecEvalInt(ctx, input, pos); err != nil {
		return err
	}
	length, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(length)
	if err := b.args[2].VecEvalInt(ctx, input, length); err != nil {
		return err
	}
	newstr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(newstr)
	if err := b.args[3].VecEvalString(ctx, input, newstr); err != nil {
		return err
	}
	posIs := pos.Int64s()
	lengthIs := length.Int64s()
	result.ReserveString(n)
	for i := range n {
		if str.IsNull(i) || pos.IsNull(i) || length.IsNull(i) || newstr.IsNull(i) {
			result.AppendNull()
			continue
		}
		strI := str.GetString(i)
		strLength := int64(len(strI))
		posI := posIs[i]
		if posI < 1 || posI > strLength {
			result.AppendString(strI)
			continue
		}
		lengthI := lengthIs[i]
		if lengthI > strLength-posI+1 || lengthI < 0 {
			lengthI = strLength - posI + 1
		}
		newstrI := newstr.GetString(i)
		if uint64(strLength-lengthI+int64(len(newstrI))) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		result.AppendString(strI[0:posI-1] + newstrI + strI[posI+lengthI-1:])
	}
	return nil
}

func (b *builtinConcatWSSig) vectorized() bool {
	return true
}

// vecEvalString evals a CONCAT_WS(separator,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	argsLen := len(b.args)

	bufs := make([]*chunk.Column, argsLen)
	var err error
	for i := range argsLen {
		bufs[i], err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(bufs[i])
		if err := b.args[i].VecEvalString(ctx, input, bufs[i]); err != nil {
			return err
		}
	}

	isNulls := make([]bool, n)
	seps := make([]string, n)
	strs := make([][]string, n)
	for i := range n {
		if bufs[0].IsNull(i) {
			// If the separator is NULL, the result is NULL.
			isNulls[i] = true
			continue
		}
		isNulls[i] = false
		seps[i] = bufs[0].GetString(i)
		strs[i] = make([]string, 0, argsLen-1)
	}

	var strBuf string
	targetLengths := make([]int, n)
	for j := 1; j < argsLen; j++ {
		for i := range n {
			if isNulls[i] || bufs[j].IsNull(i) {
				// CONCAT_WS() does not skip empty strings. However,
				// it does skip any NULL values after the separator argument.
				continue
			}
			strBuf = bufs[j].GetString(i)
			targetLengths[i] += len(strBuf)
			if i > 1 {
				targetLengths[i] += len(seps[i])
			}
			if uint64(targetLengths[i]) > b.maxAllowedPacket {
				if err := handleAllowedPacketOverflowed(ctx, "concat_ws", b.maxAllowedPacket); err != nil {
					return err
				}

				isNulls[i] = true
				continue
			}
			strs[i] = append(strs[i], strBuf)
		}
	}
	result.ReserveString(n)
	for i := range n {
		if isNulls[i] {
			result.AppendNull()
			continue
		}
		str := strings.Join(strs[i], seps[i])
		// todo check whether the length of result is larger than flen
		// if b.tp.flen != types.UnspecifiedLength && len(str) > b.tp.flen {
		//	result.AppendNull()
		//	continue
		// }
		result.AppendString(str)
	}
	return nil
}

func (b *builtinConvertSig) vectorized() bool {
	return true
}

func (b *builtinConvertSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	expr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(expr)
	if err := b.args[0].VecEvalString(ctx, input, expr); err != nil {
		return err
	}
	argTp, resultTp := b.args[0].GetType(ctx), b.tp
	result.ReserveString(n)
	done := vecEvalStringConvertBinary(result, n, expr, argTp, resultTp)
	if done {
		return nil
	}
	enc := charset.FindEncoding(resultTp.GetCharset())
	encBuf := &bytes.Buffer{}
	for i := range n {
		if expr.IsNull(i) {
			result.AppendNull()
			continue
		}
		exprI := expr.GetBytes(i)
		if !enc.IsValid(exprI) {
			val, _ := enc.Transform(encBuf, exprI, charset.OpReplaceNoErr)
			result.AppendBytes(val)
		} else {
			result.AppendBytes(exprI)
		}
	}
	return nil
}

func vecEvalStringConvertBinary(result *chunk.Column, n int, expr *chunk.Column,
	argTp, resultTp *types.FieldType) (done bool) {
	var chs string
	var op charset.Op
	if types.IsBinaryStr(argTp) {
		chs = resultTp.GetCharset()
		op = charset.OpDecode
	} else if types.IsBinaryStr(resultTp) {
		chs = argTp.GetCharset()
		op = charset.OpEncode
	} else {
		return false
	}
	enc := charset.FindEncoding(chs)
	encBuf := &bytes.Buffer{}
	for i := range n {
		if expr.IsNull(i) {
			result.AppendNull()
			continue
		}
		val, err := enc.Transform(encBuf, expr.GetBytes(i), op)
		if err != nil {
			result.AppendNull()
		} else {
			result.AppendBytes(val)
		}
		continue
	}
	return true
}

func (b *builtinSubstringIndexSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	counts := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		delim := buf1.GetString(i)
		count := counts[i]

		if len(delim) == 0 {
			result.AppendString("")
			continue
		}

		// when count > MaxInt64, returns whole string.
		if count < 0 && mysql.HasUnsignedFlag(b.args[2].GetType(ctx).GetFlag()) {
			result.AppendString(str)
			continue
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
				result.AppendString(str)
				continue
			}

			if count < end {
				start = end - count
			}
		}
		substrs := strs[start:end]
		result.AppendString(strings.Join(substrs, delim))
	}

	return nil
}

func (b *builtinUnHexSig) vectorized() bool {
	return true
}

func (b *builtinUnHexSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		if len(str)%2 != 0 {
			str = "0" + str
		}
		bs, e := hex.DecodeString(str)
		if e != nil {
			result.AppendNull()
			continue
		}
		result.AppendString(string(bs))
	}
	return nil
}

func (b *builtinExportSet3ArgSig) vectorized() bool {
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bits)
	if err := b.args[0].VecEvalInt(ctx, input, bits); err != nil {
		return err
	}
	on, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(on)
	if err := b.args[1].VecEvalString(ctx, input, on); err != nil {
		return err
	}
	off, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(off)
	if err := b.args[2].VecEvalString(ctx, input, off); err != nil {
		return err
	}
	result.ReserveString(n)
	i64s := bits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			",", 64))
	}
	return nil
}

func (b *builtinASCIISig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		if len(str) == 0 {
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(str[0])
	}
	return nil
}

func (b *builtinLpadSig) vectorized() bool {
	return true
}

// vecEvalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(ctx, input, strBuf); err != nil {
		return err
	}
	lenBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lenBuf)
	if err := b.args[1].VecEvalInt(ctx, input, lenBuf); err != nil {
		return err
	}
	padBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(padBuf)
	if err := b.args[2].VecEvalString(ctx, input, padBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := lenBuf.Int64s()
	lenBuf.MergeNulls(strBuf)
	for i := range n {
		if lenBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}

		if padBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := strBuf.GetString(i)
		strLength := len(str)
		padStr := padBuf.GetString(i)
		padLength := len(padStr)
		if targetLength < 0 || targetLength > b.tp.GetFlen() {
			result.AppendNull()
			continue
		}
		if strLength < targetLength && padLength == 0 {
			result.AppendString("")
			continue
		}
		if tailLen := targetLength - strLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
		}
		result.AppendString(str[:targetLength])
	}
	return nil
}

func (b *builtinLpadUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf1.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		targetLength := int(i64s[i])
		if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		if buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		padStr := buf2.GetString(i)
		runeLength := len([]rune(str))
		padLength := len([]rune(padStr))

		if targetLength < 0 || targetLength*4 > b.tp.GetFlen() {
			result.AppendNull()
			continue
		}
		if runeLength < targetLength && padLength == 0 {
			result.AppendString("")
			continue
		}
		if tailLen := targetLength - runeLength; tailLen > 0 {
			repeatCount := tailLen/padLength + 1
			str = string([]rune(strings.Repeat(padStr, repeatCount))[:tailLen]) + str
		}
		result.AppendString(string([]rune(str)[:targetLength]))
	}
	return nil
}

func (b *builtinFindInSetSig) vectorized() bool {
	return true
}

func (b *builtinFindInSetSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	strlist, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strlist)
	if err := b.args[1].VecEvalString(ctx, input, strlist); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(str, strlist)
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strlistI := strlist.GetString(i)
		if len(strlistI) == 0 {
			res[i] = 0
			continue
		}
		for j, strInSet := range strings.Split(strlistI, ",") {
			if b.ctor.Compare(str.GetString(i), strInSet) == 0 {
				res[i] = int64(j + 1)
			}
		}
	}
	return nil
}

func (b *builtinLeftSig) vectorized() bool {
	return true
}

func (b *builtinLeftSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	left := buf2.Int64s()
	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		leftLength, str := int(left[i]), buf.GetString(i)
		if strLength := len(str); leftLength > strLength {
			leftLength = strLength
		} else if leftLength < 0 {
			leftLength = 0
		}
		result.AppendString(str[:leftLength])
	}
	return nil
}

func (b *builtinEltSig) vectorized() bool {
	return true
}

// vecEvalString evals a ELT(N,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf0.Int64s()
	argLen := len(b.args)
	bufs := make([]*chunk.Column, argLen)
	for i := range n {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		j := i64s[i]
		if j < 1 || j >= int64(argLen) {
			result.AppendNull()
			continue
		}
		if bufs[j] == nil {
			bufs[j], err = b.bufAllocator.get()
			if err != nil {
				return err
			}
			defer b.bufAllocator.put(bufs[j])
			if err := b.args[j].VecEvalString(ctx, input, bufs[j]); err != nil {
				return err
			}
		}
		if bufs[j].IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(bufs[j].GetString(i))
	}
	return nil
}

func (b *builtinInsertUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_insert
func (b *builtinInsertUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	buf3, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf3)
	if err := b.args[3].VecEvalString(ctx, input, buf3); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s1 := buf1.Int64s()
	i64s2 := buf2.Int64s()
	buf1.MergeNulls(buf2)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf3.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		pos := i64s1[i]
		length := i64s2[i]
		newstr := buf3.GetString(i)

		runes := []rune(str)
		runeLength := int64(len(runes))
		if pos < 1 || pos > runeLength {
			result.AppendString(str)
			continue
		}
		if length > runeLength-pos+1 || length < 0 {
			length = runeLength - pos + 1
		}

		strHead := string(runes[0 : pos-1])
		strTail := string(runes[pos+length-1:])
		if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
			if err := handleAllowedPacketOverflowed(ctx, "insert", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}
		result.AppendString(strHead + newstr + strTail)
	}
	return nil
}

func (b *builtinExportSet5ArgSig) vectorized() bool {
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bits)
	if err := b.args[0].VecEvalInt(ctx, input, bits); err != nil {
		return err
	}
	on, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(on)
	if err := b.args[1].VecEvalString(ctx, input, on); err != nil {
		return err
	}
	off, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(off)
	if err := b.args[2].VecEvalString(ctx, input, off); err != nil {
		return err
	}
	separator, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(separator)
	if err := b.args[3].VecEvalString(ctx, input, separator); err != nil {
		return err
	}
	numberOfBits, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(numberOfBits)
	if err := b.args[4].VecEvalInt(ctx, input, numberOfBits); err != nil {
		return err
	}
	result.ReserveString(n)
	bits.MergeNulls(numberOfBits)
	i64s := bits.Int64s()
	i64s2 := numberOfBits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) || separator.IsNull(i) {
			result.AppendNull()
			continue
		}
		if i64s2[i] < 0 || i64s2[i] > 64 {
			i64s2[i] = 64
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			separator.GetString(i), i64s2[i]))
	}
	return nil
}

func (b *builtinSubstring3ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	positions := buf1.Int64s()
	lengths := buf2.Int64s()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		pos := positions[i]
		length := lengths[i]
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
			result.AppendString("")
			continue
		} else if end < numRunes {
			result.AppendString(string(runes[pos:end]))
			continue
		}
		result.AppendString(string(runes[pos:]))
	}

	return nil
}

func (b *builtinTrim3ArgsSig) vectorized() bool {
	return true
}

func (b *builtinTrim3ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[2].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		direction := ast.TrimDirectionType(buf2.GetInt64(i))
		baseStr := buf0.GetString(i)
		remStr := buf1.GetString(i)
		switch direction {
		case ast.TrimLeading:
			result.AppendString(trimLeft(baseStr, remStr))
		case ast.TrimTrailing:
			result.AppendString(trimRight(baseStr, remStr))
		default:
			tmpStr := trimLeft(baseStr, remStr)
			result.AppendString(trimRight(tmpStr, remStr))
		}
	}
	return nil
}

func (b *builtinOrdSig) vectorized() bool {
	return true
}

func (b *builtinOrdSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	var x [4]byte
	encBuf := bytes.NewBuffer(x[:])
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strBytes := buf.GetBytes(i)
		w := len(charset.EncodingUTF8Impl.Peek(strBytes))
		val, err := enc.Transform(encBuf, strBytes[:w], charset.OpEncode)
		if err != nil {
			i64s[i] = calcOrd(strBytes[:1])
			continue
		}
		// Only the first character is considered.
		i64s[i] = calcOrd(val[:len(enc.Peek(val))])
	}
	return nil
}

func (b *builtinInstrSig) vectorized() bool {
	return true
}

func (b *builtinInstrSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	str, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(str)
	if err := b.args[0].VecEvalString(ctx, input, str); err != nil {
		return err
	}
	substr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(substr)
	if err := b.args[1].VecEvalString(ctx, input, substr); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(str, substr)
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		strI := str.GetString(i)
		substrI := substr.GetString(i)
		idx := strings.Index(strI, substrI)
		res[i] = int64(idx + 1)
	}
	return nil
}

func (b *builtinLengthSig) vectorized() bool {
	return true
}

// vecEvalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str))
	}
	return nil
}

func (b *builtinLocate2ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf, buf1)
	i64s := result.Int64s()
	ci := collate.IsCICollation(b.collation)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf.GetString(i)
		str := buf1.GetString(i)
		subStrLen := int64(len([]rune(subStr)))
		if subStrLen == 0 {
			i64s[i] = 1
			continue
		}
		slice := str
		if ci {
			slice = strings.ToLower(slice)
			subStr = strings.ToLower(subStr)
		}
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = int64(utf8.RuneCountInString(slice[:idx])) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinBitLengthSig) vectorized() bool {
	return true
}

func (b *builtinBitLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetBytes(i)
		i64s[i] = int64(len(str) * 8)
	}
	return nil
}

func (b *builtinCharSig) vectorized() bool {
	return true
}

func (b *builtinCharSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	l := len(b.args)
	buf := make([]*chunk.Column, l-1)
	for i := range len(b.args) - 1 {
		te, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		buf[i] = te
		defer b.bufAllocator.put(buf[i])
		if err := b.args[i].VecEvalInt(ctx, input, buf[i]); err != nil {
			return err
		}
	}
	bufstr, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufstr)
	bigints := make([]int64, 0, l-1)
	result.ReserveString(n)
	bufint := make([]([]int64), l-1)
	for i := range l - 1 {
		bufint[i] = buf[i].Int64s()
	}
	encBuf := &bytes.Buffer{}
	enc := charset.FindEncoding(b.tp.GetCharset())
	hasStrictMode := sqlMode(ctx).HasStrictMode()
	for i := range n {
		bigints = bigints[0:0]
		for j := range l - 1 {
			if buf[j].IsNull(i) {
				continue
			}
			bigints = append(bigints, bufint[j][i])
		}
		dBytes := b.convertToBytes(bigints)
		resultBytes, err := enc.Transform(encBuf, dBytes, charset.OpDecode)
		if err != nil {
			tc := typeCtx(ctx)
			tc.AppendWarning(err)
			if hasStrictMode {
				result.AppendNull()
				continue
			}
		}
		result.AppendString(string(resultBytes))
	}
	return nil
}

func (b *builtinReplaceSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		oldStr := buf1.GetString(i)
		newStr := buf2.GetString(i)
		if oldStr == "" {
			result.AppendString(str)
			continue
		}
		str = strings.ReplaceAll(str, oldStr, newStr)
		result.AppendString(str)
	}
	return nil
}

func (b *builtinMakeSetSig) vectorized() bool {
	return true
}

// vecEvalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nr := input.NumRows()
	bitsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bitsBuf)
	if err := b.args[0].VecEvalInt(ctx, input, bitsBuf); err != nil {
		return err
	}

	strBuf := make([]*chunk.Column, len(b.args)-1)
	for i := 1; i < len(b.args); i++ {
		strBuf[i-1], err = b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(strBuf[i-1])
		if err := b.args[i].VecEvalString(ctx, input, strBuf[i-1]); err != nil {
			return err
		}
	}

	bits := bitsBuf.Int64s()
	result.ReserveString(nr)
	sets := make([]string, 0, len(b.args)-1)
	for i := range nr {
		if bitsBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		sets = sets[:0]
		for j := range len(b.args) - 1 {
			if strBuf[j].IsNull(i) || (bits[i]&(1<<uint(j))) == 0 {
				continue
			}
			sets = append(sets, strBuf[j].GetString(i))
		}
		result.AppendString(strings.Join(sets, ","))
	}

	return nil
}

func (b *builtinOctIntSig) vectorized() bool {
	return true
}

func (b *builtinOctIntSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strconv.FormatUint(uint64(nums[i]), 8))
	}
	return nil
}
