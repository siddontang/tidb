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
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinToBase64Sig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		needEncodeLen := base64NeededEncodedLength(len(str))
		if needEncodeLen == -1 {
			result.AppendNull()
			continue
		} else if needEncodeLen > int(b.maxAllowedPacket) {
			if err := handleAllowedPacketOverflowed(ctx, "to_base64", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		} else if b.tp.GetFlen() == -1 || b.tp.GetFlen() > mysql.MaxBlobWidth {
			b.tp.SetFlen(mysql.MaxBlobWidth)
		}

		newStr := base64.StdEncoding.EncodeToString([]byte(str))
		// A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
		count := len(newStr)
		if count > 76 {
			newStr = strings.Join(splitToSubN(newStr, 76), "\n")
		}
		result.AppendString(newStr)
	}
	return nil
}

func (b *builtinTrim1ArgSig) vectorized() bool {
	return true
}

func (b *builtinTrim1ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(strings.Trim(str, spaceChars))
	}

	return nil
}

func (b *builtinRpadUTF8Sig) vectorized() bool {
	return true
}

// vecEvalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			if err := handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket); err != nil {
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
			str = str + strings.Repeat(padStr, repeatCount)
		}
		result.AppendString(string([]rune(str)[:targetLength]))
	}
	return nil
}

func (b *builtinCharLengthBinarySig) vectorized() bool {
	return true
}

func (b *builtinCharLengthBinarySig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	res := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		str := buf.GetString(i)
		res[i] = int64(len(str))
	}
	return nil
}

func (b *builtinBinSig) vectorized() bool {
	return true
}

func (b *builtinBinSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		result.AppendString(fmt.Sprintf("%b", uint64(nums[i])))
	}
	return nil
}

func (b *builtinFormatSig) vectorized() bool {
	return true
}

// vecEvalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		return formatDecimal(ctx, xBuf, dInt64s, result, nil)
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
	return formatReal(ctx, xBuf, dInt64s, result, nil)
}

func (b *builtinRightSig) vectorized() bool {
	return true
}

func (b *builtinRightSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	right := buf2.Int64s()
	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		str, rightLength := buf.GetString(i), int(right[i])
		strLength := len(str)
		if rightLength > strLength {
			rightLength = strLength
		} else if rightLength < 0 {
			rightLength = 0
		}
		result.AppendString(str[strLength-rightLength:])
	}
	return nil
}

func (b *builtinSubstring3ArgsSig) vectorized() bool {
	return true
}

// vecEvalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
			result.AppendString("")
			continue
		} else if end < byteLen {
			result.AppendString(str[pos:end])
			continue
		}
		result.AppendString(str[pos:])
	}

	return nil
}

func (b *builtinHexIntArgSig) vectorized() bool {
	return true
}

func (b *builtinHexIntArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	i64s := buf.Int64s()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(strings.ToUpper(fmt.Sprintf("%x", uint64(i64s[i]))))
	}
	return nil
}

func (b *builtinFromBase64Sig) vectorized() bool {
	return true
}

// vecEvalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		needDecodeLen := base64NeededDecodedLength(len(str))
		if needDecodeLen == -1 {
			result.AppendNull()
			continue
		} else if needDecodeLen > int(b.maxAllowedPacket) {
			if err := handleAllowedPacketOverflowed(ctx, "from_base64", b.maxAllowedPacket); err != nil {
				return err
			}

			result.AppendNull()
			continue
		}

		str = strings.ReplaceAll(str, "\t", "")
		str = strings.ReplaceAll(str, " ", "")
		newStr, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			// When error happens, take `from_base64("asc")` as an example, we should return NULL.
			result.AppendNull()
			continue
		}
		result.AppendString(string(newStr))
	}
	return nil
}

func (b *builtinCharLengthUTF8Sig) vectorized() bool {
	return true
}

func (b *builtinCharLengthUTF8Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		str := buf.GetString(i)
		i64s[i] = int64(len([]rune(str)))
	}
	return nil
}

func formatDecimal(ctx EvalContext, xBuf *chunk.Column, dInt64s []int64, result *chunk.Column, localeBuf *chunk.Column) error {
	xDecimals := xBuf.Decimals()
	for i := range xDecimals {
		if xBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		x, d := xDecimals[i], dInt64s[i]

		if d < 0 {
			d = 0
		} else if d > formatMaxDecimals {
			d = formatMaxDecimals
		}

		locale := "en_US"
		isNull := false
		if localeBuf == nil {
			// FORMAT(x, d)
		} else if localeBuf.IsNull(i) {
			// FORMAT(x, d, NULL)
			isNull = true
			tc := typeCtx(ctx)
			tc.AppendWarning(errUnknownLocale.FastGenByArgs("NULL"))
		} else {
			// force copy of the string
			// https://github.com/pingcap/tidb/issues/56193
			locale = strings.Clone(localeBuf.GetString(i))
		}

		xStr := roundFormatArgs(x.String(), int(d))
		dStr := strconv.FormatInt(d, 10)
		formatString, found, err := mysql.FormatByLocale(xStr, dStr, locale)
		if err != nil {
			return err
		}
		// Check 'found' flag, only warn for unknown locales
		if !isNull && !found {
			tc := typeCtx(ctx)
			if localeBuf != nil {
				locale = strings.Clone(localeBuf.GetString(i))
			}
			tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale))
		}
		result.AppendString(formatString)
	}
	return nil
}

func formatReal(ctx EvalContext, xBuf *chunk.Column, dInt64s []int64, result *chunk.Column, localeBuf *chunk.Column) error {
	xFloat64s := xBuf.Float64s()
	for i := range xFloat64s {
		if xBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		x, d := xFloat64s[i], dInt64s[i]

		if d < 0 {
			d = 0
		} else if d > formatMaxDecimals {
			d = formatMaxDecimals
		}

		locale := "en_US"
		isNull := false
		if localeBuf == nil {
			// FORMAT(x, d)
		} else if localeBuf.IsNull(i) {
			// FORMAT(x, d, NULL)
			isNull = true
			tc := typeCtx(ctx)
			tc.AppendWarning(errUnknownLocale.FastGenByArgs("NULL"))
		} else {
			// force copy of the string
			// https://github.com/pingcap/tidb/issues/56193
			locale = strings.Clone(localeBuf.GetString(i))
		}

		xStr := roundFormatArgs(strconv.FormatFloat(x, 'f', -1, 64), int(d))
		dStr := strconv.FormatInt(d, 10)

		formatString, found, err := mysql.FormatByLocale(xStr, dStr, locale)
		if err != nil {
			return err
		}
		if !isNull && !found {
			tc := typeCtx(ctx)
			if localeBuf != nil {
				locale = strings.Clone(localeBuf.GetString(i))
			}
			tc.AppendWarning(errUnknownLocale.FastGenByArgs(locale))
		}
		result.AppendString(formatString)
	}
	return nil
}

func (b *builtinTranslateBinarySig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	var (
		mp           map[byte]uint16
		useCommonMap = false
	)
	_, isFromConst := b.args[1].(*Constant)
	_, isToConst := b.args[2].(*Constant)
	if isFromConst && isToConst {
		if !(ExprNotNull(ctx, b.args[1]) && ExprNotNull(ctx, b.args[2])) {
			for range n {
				result.AppendNull()
			}
			return nil
		}
		useCommonMap = true
		fromBytes, toBytes := []byte(buf1.GetString(0)), []byte(buf2.GetString(0))
		mp = buildTranslateMap4Binary(fromBytes, toBytes)
	}
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		srcStr := buf0.GetString(i)
		var tgt []byte
		if !useCommonMap {
			fromBytes, toBytes := []byte(buf1.GetString(i)), []byte(buf2.GetString(i))
			mp = buildTranslateMap4Binary(fromBytes, toBytes)
		}
		for _, charSrc := range []byte(srcStr) {
			if charTo, ok := mp[charSrc]; ok {
				if charTo != invalidByte {
					tgt = append(tgt, byte(charTo))
				}
			} else {
				tgt = append(tgt, charSrc)
			}
		}
		result.AppendString(string(tgt))
	}
	return nil
}

func (b *builtinTranslateBinarySig) vectorized() bool {
	return true
}

func (b *builtinTranslateUTF8Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[2].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}
	result.ReserveString(n)
	var (
		mp           map[rune]rune
		useCommonMap = false
	)
	_, isFromConst := b.args[1].(*Constant)
	_, isToConst := b.args[2].(*Constant)
	if isFromConst && isToConst {
		if !(ExprNotNull(ctx, b.args[1]) && ExprNotNull(ctx, b.args[2])) {
			for range n {
				result.AppendNull()
			}
			return nil
		}
		useCommonMap = true
		fromRunes, toRunes := []rune(buf1.GetString(0)), []rune(buf2.GetString(0))
		mp = buildTranslateMap4UTF8(fromRunes, toRunes)
	}
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		srcStr := buf0.GetString(i)
		var tgt strings.Builder
		if !useCommonMap {
			fromRunes, toRunes := []rune(buf1.GetString(i)), []rune(buf2.GetString(i))
			mp = buildTranslateMap4UTF8(fromRunes, toRunes)
		}
		for _, charSrc := range srcStr {
			if charTo, ok := mp[charSrc]; ok {
				if charTo != invalidRune {
					tgt.WriteRune(charTo)
				}
			} else {
				tgt.WriteRune(charSrc)
			}
		}
		result.AppendString(tgt.String())
	}
	return nil
}

func (b *builtinTranslateUTF8Sig) vectorized() bool {
	return true
}
