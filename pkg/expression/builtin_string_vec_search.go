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
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

func (b *builtinReverseSig) vectorized() bool {
	return true
}

func (b *builtinReverseSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		reversed := reverseBytes(result.GetBytes(i))
		result.SetRaw(i, reversed)
	}
	return nil
}

func (b *builtinRTrimSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(strings.TrimRight(str, spaceChars))
	}

	return nil
}

func (b *builtinStrcmpSig) vectorized() bool {
	return true
}

func (b *builtinStrcmpSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	leftBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(leftBuf)
	if err := b.args[0].VecEvalString(ctx, input, leftBuf); err != nil {
		return err
	}
	rightBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rightBuf)
	if err := b.args[1].VecEvalString(ctx, input, rightBuf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(leftBuf, rightBuf)
	i64s := result.Int64s()
	for i := range n {
		// if left or right is null, then set to null and return 0(which is the default value)
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(types.CompareString(leftBuf.GetString(i), rightBuf.GetString(i), b.collation))
	}
	return nil
}

func (b *builtinLocate2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinLocate2ArgsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf0, buf1)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		subStr := buf0.GetString(i)
		if len(subStr) == 0 {
			i64s[i] = 1
			continue
		}
		i64s[i] = int64(strings.Index(buf1.GetString(i), subStr) + 1)
	}
	return nil
}

func (b *builtinLocate3ArgsSig) vectorized() bool {
	return true
}

// vecEvalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
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

	result.MergeNulls(buf0, buf1)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		pos := i64s[i]
		// Transfer the argument which starts from 1 to real index which starts from 0.
		pos--
		subStr := buf0.GetString(i)
		str := buf1.GetString(i)
		subStrLen := len(subStr)
		if pos < 0 || pos > int64(len(str)-subStrLen) {
			i64s[i] = 0
			continue
		} else if subStrLen == 0 {
			i64s[i] = pos + 1
			continue
		}
		slice := str[pos:]
		idx := strings.Index(slice, subStr)
		if idx != -1 {
			i64s[i] = pos + int64(idx) + 1
			continue
		}
		i64s[i] = 0
	}
	return nil
}

func (b *builtinExportSet4ArgSig) vectorized() bool {
	return true
}

// vecEvalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	result.ReserveString(n)
	i64s := bits.Int64s()
	for i := range n {
		if bits.IsNull(i) || on.IsNull(i) || off.IsNull(i) || separator.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(exportSet(i64s[i], on.GetString(i), off.GetString(i),
			separator.GetString(i), 64))
	}
	return nil
}

func (b *builtinRpadSig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			if err := handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket); err != nil {
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
			str = str + strings.Repeat(padStr, repeatCount)
		}
		result.AppendString(str[:targetLength])
	}
	return nil
}

func (b *builtinFormatWithLocaleSig) vectorized() bool {
	return true
}

func (b *builtinFormatWithLocaleSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dBuf)
	if err := b.args[1].VecEvalInt(ctx, input, dBuf); err != nil {
		return err
	}
	dInt64s := dBuf.Int64s()

	localeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(localeBuf)
	if err := b.args[2].VecEvalString(ctx, input, localeBuf); err != nil {
		return err
	}

	// decimal x
	if b.args[0].GetType(ctx).EvalType() == types.ETDecimal {
		xBuf, err := b.bufAllocator.get()
		if err != nil {
			return err
		}
		defer b.bufAllocator.put(xBuf)
		if err := b.args[0].VecEvalDecimal(ctx, input, xBuf); err != nil {
			return err
		}

		result.ReserveString(n)
		xBuf.MergeNulls(dBuf)
		return formatDecimal(ctx, xBuf, dInt64s, result, localeBuf)
	}

	// real x
	xBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(xBuf)
	if err := b.args[0].VecEvalReal(ctx, input, xBuf); err != nil {
		return err
	}

	result.ReserveString(n)
	xBuf.MergeNulls(dBuf)
	return formatReal(ctx, xBuf, dInt64s, result, localeBuf)
}

func (b *builtinSubstring2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinSubstring2ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		pos := nums[i]
		length := int64(len(str))
		if pos < 0 {
			pos += length
		} else {
			pos--
		}
		if pos > length || pos < 0 {
			pos = length
		}
		result.AppendString(str[pos:])
	}
	return nil
}

func (b *builtinSubstring2ArgsUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		pos := nums[i]

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
		result.AppendString(string(runes[pos:]))
	}

	return nil
}

func (b *builtinTrim2ArgsSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)
		remstr := buf1.GetString(i)
		result.AppendString(trimRight(trimLeft(str, remstr), remstr))
	}

	return nil
}

func (b *builtinInstrUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinInstrUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	ci := collate.IsCICollation(b.collation)
	var strI string
	var substrI string
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ci {
			strI = strings.ToLower(str.GetString(i))
			substrI = strings.ToLower(substr.GetString(i))
		} else {
			strI = str.GetString(i)
			substrI = substr.GetString(i)
		}
		idx := strings.Index(strI, substrI)
		if idx == -1 {
			res[i] = 0
			continue
		}
		res[i] = int64(utf8.RuneCountInString(strI[:idx]) + 1)
	}
	return nil
}

func (b *builtinOctStringSig) vectorized() bool {
	return true
}

func (b *builtinOctStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

		// for issue #59446 should return NULL for empty string
		str := buf.GetString(i)
		if len(str) == 0 {
			result.AppendNull()
			continue
		}

		negative, overflow := false, false
		str = getValidPrefix(strings.TrimSpace(str), 10)
		if len(str) == 0 {
			result.AppendString("0")
			continue
		}
		if str[0] == '-' {
			negative, str = true, str[1:]
		}
		numVal, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			numError, ok := err.(*strconv.NumError)
			if !ok || numError.Err != strconv.ErrRange {
				return err
			}
			overflow = true
		}
		if negative && !overflow {
			numVal = -numVal
		}
		result.AppendString(strconv.FormatUint(numVal, 8))
	}
	return nil
}
