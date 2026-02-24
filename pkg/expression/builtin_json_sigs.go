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

package expression

import (
	"bytes"
	"context"
	goJSON "encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/qri-io/jsonschema"
)

type jsonOverlapsFunctionClass struct {
	baseFunctionClass
}

type builtinJSONOverlapsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONOverlapsSig) Clone() builtinFunc {
	newSig := &builtinJSONOverlapsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonOverlapsFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1)
}

func (c *jsonOverlapsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETJson, types.ETJson}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONOverlapsSig{bf}
	return sig, nil
}

func (b *builtinJSONOverlapsSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	target, isNull, err := b.args[1].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if types.OverlapsBinaryJSON(obj, target) {
		return 1, false, nil
	}
	return 0, false, nil
}

type jsonValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	var sig builtinFunc
	argType := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argType {
	case types.ETJson:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidJSONSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidJsonSig)
	case types.ETString:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidStringSig)
	default:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argType)
		if err != nil {
			return nil, err
		}
		sig = &builtinJSONValidOthersSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonValidOthersSig)
	}
	return sig, nil
}

type builtinJSONValidJSONSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidJSONSig) Clone() builtinFunc {
	newSig := &builtinJSONValidJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidJSONSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidJSONSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(ctx, row)
	return 1, isNull, err
}

type builtinJSONValidStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidStringSig) Clone() builtinFunc {
	newSig := &builtinJSONValidStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidStringSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, isNull, err
	}

	data := hack.Slice(val)
	if goJSON.Valid(data) {
		res = 1
	}
	return res, false, nil
}

type builtinJSONValidOthersSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONValidOthersSig) Clone() builtinFunc {
	newSig := &builtinJSONValidOthersSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinJSONValidOthersSig.
// See https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid
func (b *builtinJSONValidOthersSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	datum, err := b.args[0].Eval(ctx, row)
	return 0, datum.IsNull(), err
}

type jsonArrayAppendFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayAppendSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayAppendFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if len(args) < 3 || (len(args)&1 != 1) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	return nil
}

func (c *jsonArrayAppendFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayAppendSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayAppendSig)
	return sig, nil
}

func (b *builtinJSONArrayAppendSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayAppendSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayAppendSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, sNull, err := b.args[i].EvalString(ctx, row)
		if sNull || err != nil {
			return res, true, err
		}
		value, vNull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}
		if vNull {
			value = types.CreateBinaryJSON(nil)
		}
		res, isNull, err = b.appendJSONArray(res, s, value)
		if isNull || err != nil {
			return res, isNull, err
		}
	}
	return res, false, nil
}

func (b *builtinJSONArrayAppendSig) appendJSONArray(res types.BinaryJSON, p string, v types.BinaryJSON) (types.BinaryJSON, bool, error) {
	// We should do the following checks to get correct values in res.Extract
	pathExpr, err := types.ParseJSONPathExpr(p)
	if err != nil {
		return res, true, err
	}
	if pathExpr.CouldMatchMultipleValues() {
		return res, true, types.ErrInvalidJSONPathMultipleSelection
	}

	obj, exists := res.Extract([]types.JSONPathExpression{pathExpr})
	if !exists {
		// If path not exists, just do nothing and no errors.
		return res, false, nil
	}

	if obj.TypeCode != types.JSONTypeCodeArray {
		// res.Extract will return a json object instead of an array if there is an object at path pathExpr.
		// JSON_ARRAY_APPEND({"a": "b"}, "$", {"b": "c"}) => [{"a": "b"}, {"b", "c"}]
		// We should wrap them to a single array first.
		obj, err = types.CreateBinaryJSONWithCheck([]any{obj})
		if err != nil {
			return res, true, err
		}
	}

	// wrap the new value `v` into an array explicitly, in case that the `v` is an array itself.
	// For example, `JSON_ARRAY_APPEND('[1]', '$', JSON_ARRAY(2, 3))` should return `[1, [2, 3]]`
	v = types.CreateBinaryJSON([]any{v})

	obj = types.MergeBinaryJSON([]types.BinaryJSON{obj, v})
	res, err = res.Modify([]types.JSONPathExpression{pathExpr}, []types.BinaryJSON{obj}, types.JSONModifySet)
	return res, false, err
}

type jsonArrayInsertFunctionClass struct {
	baseFunctionClass
}

type builtinJSONArrayInsertSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (c *jsonArrayInsertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if len(args)&1 != 1 {
		return nil, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for i := 1; i < len(args)-1; i += 2 {
		argTps = append(argTps, types.ETString, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	for i := 2; i < len(args); i += 2 {
		DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[i])
	}
	sig := &builtinJSONArrayInsertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonArrayInsertSig)
	return sig, nil
}

func (b *builtinJSONArrayInsertSig) Clone() builtinFunc {
	newSig := &builtinJSONArrayInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONArrayInsertSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if err != nil || isNull {
		return res, true, err
	}

	for i := 1; i < len(b.args)-1; i += 2 {
		// If JSON path is NULL, MySQL breaks and returns NULL.
		s, isNull, err := b.args[i].EvalString(ctx, row)
		if err != nil || isNull {
			return res, true, err
		}

		pathExpr, err := types.ParseJSONPathExpr(s)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}

		value, isnull, err := b.args[i+1].EvalJSON(ctx, row)
		if err != nil {
			return res, true, err
		}

		if isnull {
			value = types.CreateBinaryJSON(nil)
		}

		res, err = res.ArrayInsert(pathExpr, value)
		if err != nil {
			return res, true, err
		}
	}
	return res, false, nil
}

type jsonMergePatchFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePatchFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePatchFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergePatchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePatchSig)
	return sig, nil
}

type builtinJSONMergePatchSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONMergePatchSig) Clone() builtinFunc {
	newSig := &builtinJSONMergePatchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONMergePatchSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	values := make([]*types.BinaryJSON, 0, len(b.args))
	for _, arg := range b.args {
		var value types.BinaryJSON
		value, isNull, err = arg.EvalJSON(ctx, row)
		if err != nil {
			return
		}
		if isNull {
			values = append(values, nil)
		} else {
			values = append(values, &value)
		}
	}
	tmpRes, err := types.MergePatchBinaryJSON(values)
	if err != nil {
		return
	}
	if tmpRes != nil {
		res = *tmpRes
	} else {
		isNull = true
	}
	return res, isNull, nil
}

type jsonMergePreserveFunctionClass struct {
	baseFunctionClass
}

func (c *jsonMergePreserveFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args)
}

func (c *jsonMergePreserveFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETJson)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONMergeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonMergePreserveSig)
	return sig, nil
}

type jsonPrettyFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSPrettySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSPrettySig) Clone() builtinFunc {
	newSig := &builtinJSONSPrettySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonPrettyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETJson)
	if err != nil {
		return nil, err
	}
	bf.tp.AddFlag(mysql.BinaryFlag)
	bf.tp.SetFlen(mysql.MaxBlobWidth * 4)
	sig := &builtinJSONSPrettySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonPrettySig)
	return sig, nil
}

func (b *builtinJSONSPrettySig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	buf, err := obj.MarshalJSON()
	if err != nil {
		return res, isNull, err
	}
	var resBuf bytes.Buffer
	if err = goJSON.Indent(&resBuf, buf, "", "  "); err != nil {
		return res, isNull, err
	}
	return resBuf.String(), false, nil
}

type jsonQuoteFunctionClass struct {
	baseFunctionClass
}

type builtinJSONQuoteSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONQuoteSig) Clone() builtinFunc {
	newSig := &builtinJSONQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonQuoteFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	if evalType := args[0].GetType(ctx).EvalType(); evalType != types.ETString {
		return ErrIncorrectType.GenWithStackByArgs("1", "json_quote")
	}
	return nil
}

func (c *jsonQuoteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	DisableParseJSONFlag4Expr(ctx.GetEvalCtx(), args[0])
	bf.tp.AddFlag(mysql.BinaryFlag)
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen()*6 + 2)
	sig := &builtinJSONQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonQuoteSig)
	return sig, nil
}

func (b *builtinJSONQuoteSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	buffer := &bytes.Buffer{}
	encoder := goJSON.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err = encoder.Encode(str)
	if err != nil {
		return "", isNull, err
	}
	return string(bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))), false, nil
}

type jsonSearchFunctionClass struct {
	baseFunctionClass
}

type builtinJSONSearchSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONSearchSig) Clone() builtinFunc {
	newSig := &builtinJSONSearchSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonSearchFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonSearchFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	// json_doc, one_or_all, search_str[, escape_char[, path] ...])
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	for range args[1:] {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONSearchSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonSearchSig)
	return sig, nil
}

func (b *builtinJSONSearchSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	// json_doc
	var obj types.BinaryJSON
	obj, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// one_or_all
	var containType string
	containType, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	containType = strings.ToLower(containType)
	if containType != types.JSONContainsPathAll && containType != types.JSONContainsPathOne {
		return res, true, errors.AddStack(types.ErrInvalidJSONContainsPathType)
	}

	// search_str & escape_char
	var searchStr string
	searchStr, isNull, err = b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	escape := byte('\\')
	if len(b.args) >= 4 {
		var escapeStr string
		escapeStr, isNull, err = b.args[3].EvalString(ctx, row)
		if err != nil {
			return res, isNull, err
		}
		if isNull || len(escapeStr) == 0 {
			escape = byte('\\')
		} else if len(escapeStr) == 1 {
			escape = escapeStr[0]
		} else {
			return res, true, errIncorrectArgs.GenWithStackByArgs("ESCAPE")
		}
	}
	if len(b.args) >= 5 { // path...
		pathExprs := make([]types.JSONPathExpression, 0, len(b.args)-4)
		for i := 4; i < len(b.args); i++ {
			var s string
			s, isNull, err = b.args[i].EvalString(ctx, row)
			if isNull || err != nil {
				return res, isNull, err
			}
			var pathExpr types.JSONPathExpression
			pathExpr, err = types.ParseJSONPathExpr(s)
			if err != nil {
				return res, true, err
			}
			pathExprs = append(pathExprs, pathExpr)
		}
		return obj.Search(containType, searchStr, escape, pathExprs)
	}
	return obj.Search(containType, searchStr, escape, nil)
}

type jsonStorageFreeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONStorageFreeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONStorageFreeSig) Clone() builtinFunc {
	newSig := &builtinJSONStorageFreeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonStorageFreeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONStorageFreeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonStorageFreeSig)
	return sig, nil
}

func (b *builtinJSONStorageFreeSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	_, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return 0, false, nil
}

type jsonStorageSizeFunctionClass struct {
	baseFunctionClass
}

type builtinJSONStorageSizeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONStorageSizeSig) Clone() builtinFunc {
	newSig := &builtinJSONStorageSizeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonStorageSizeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONStorageSizeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonStorageSizeSig)
	return sig, nil
}

func (b *builtinJSONStorageSizeSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	// returns the length of obj value plus 1 (the TypeCode)
	return int64(len(obj.Value)) + 1, false, nil
}

type jsonDepthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONDepthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONDepthSig) Clone() builtinFunc {
	newSig := &builtinJSONDepthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonDepthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONDepthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonDepthSig)
	return sig, nil
}

func (b *builtinJSONDepthSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	// as TiDB doesn't support partial update json value, so only check the
	// json format and whether it's NULL. For NULL return NULL, for invalid json, return
	// an error, otherwise return 0

	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	return int64(obj.GetElemDepth()), false, nil
}

type jsonKeysFunctionClass struct {
	baseFunctionClass
}

func (c *jsonKeysFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}
	return verifyJSONArgsType(ctx, c.funcName, true, args, 0)
}

func (c *jsonKeysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETJson}
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETJson, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	switch len(args) {
	case 1:
		sig = &builtinJSONKeysSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeysSig)
	case 2:
		sig = &builtinJSONKeys2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_JsonKeys2ArgsSig)
	}
	return sig, nil
}

type builtinJSONKeysSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONKeysSig) Clone() builtinFunc {
	newSig := &builtinJSONKeysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeysSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if res.TypeCode != types.JSONTypeCodeObject {
		return res, true, nil
	}
	return res.GetKeys(), false, nil
}

type builtinJSONKeys2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONKeys2ArgsSig) Clone() builtinFunc {
	newSig := &builtinJSONKeys2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONKeys2ArgsSig) evalJSON(ctx EvalContext, row chunk.Row) (res types.BinaryJSON, isNull bool, err error) {
	res, isNull, err = b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	path, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	pathExpr, err := types.ParseJSONPathExpr(path)
	if err != nil {
		return res, true, err
	}
	if pathExpr.CouldMatchMultipleValues() {
		return res, true, types.ErrInvalidJSONPathMultipleSelection
	}

	res, exists := res.Extract([]types.JSONPathExpression{pathExpr})
	if !exists || res.TypeCode != types.JSONTypeCodeObject {
		return res, true, nil
	}

	return res.GetKeys(), false, nil
}

type jsonLengthFunctionClass struct {
	baseFunctionClass
}

type builtinJSONLengthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinJSONLengthSig) Clone() builtinFunc {
	newSig := &builtinJSONLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *jsonLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETJson)
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &builtinJSONLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_JsonLengthSig)
	return sig, nil
}

func (b *builtinJSONLengthSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	obj, isNull, err := b.args[0].EvalJSON(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	if len(b.args) == 2 {
		path, isNull, err := b.args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}

		pathExpr, err := types.ParseJSONPathExpr(path)
		if err != nil {
			return res, true, err
		}
		if pathExpr.CouldMatchMultipleValues() {
			return res, true, types.ErrInvalidJSONPathMultipleSelection
		}

		var exists bool
		obj, exists = obj.Extract([]types.JSONPathExpression{pathExpr})
		if !exists {
			return res, true, nil
		}
	}

	if obj.TypeCode != types.JSONTypeCodeObject && obj.TypeCode != types.JSONTypeCodeArray {
		return 1, false, nil
	}
	return int64(obj.GetElemCount()), false, nil
}

type jsonSchemaValidFunctionClass struct {
	baseFunctionClass
}

func (c *jsonSchemaValidFunctionClass) verifyArgs(ctx EvalContext, args []Expression) error {
	if err := c.baseFunctionClass.verifyArgs(args); err != nil {
		return err
	}

	if err := verifyJSONArgsType(ctx, c.funcName, true, args, 0, 1); err != nil {
		return err
	}
	if c, ok := args[0].(*Constant); ok {
		// If args[0] is NULL, then don't check the length of *both* arguments.
		// JSON_SCHEMA_VALID(NULL,NULL) -> NULL
		// JSON_SCHEMA_VALID(NULL,'') -> NULL
		// JSON_SCHEMA_VALID('',NULL) -> ErrInvalidJSONTextInParam
		if !c.Value.IsNull() {
			if len(c.Value.GetBytes()) == 0 {
				return types.ErrInvalidJSONTextInParam.GenWithStackByArgs(
					1, "json_schema_valid", "The document is empty.", 0)
			}
			if c1, ok := args[1].(*Constant); ok {
				if !c1.Value.IsNull() && len(c1.Value.GetBytes()) == 0 {
					return types.ErrInvalidJSONTextInParam.GenWithStackByArgs(
						2, "json_schema_valid", "The document is empty.", 0)
				}
			}
		}
	}
	return nil
}

func (c *jsonSchemaValidFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(ctx.GetEvalCtx(), args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETJson, types.ETJson)
	if err != nil {
		return nil, err
	}

	sig := &builtinJSONSchemaValidSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinJSONSchemaValidSig struct {
	baseBuiltinFunc

	schemaCache builtinFuncCache[jsonschema.Schema]
}

func (b *builtinJSONSchemaValidSig) Clone() builtinFunc {
	newSig := &builtinJSONSchemaValidSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinJSONSchemaValidSig) evalInt(ctx EvalContext, row chunk.Row) (res int64, isNull bool, err error) {
	var schema jsonschema.Schema

	// First argument is the schema
	schemaData, schemaIsNull, err := b.args[0].EvalJSON(ctx, row)
	if err != nil {
		return res, false, err
	}
	if schemaIsNull {
		return res, true, err
	}

	if b.args[0].ConstLevel() >= ConstOnlyInContext {
		schema, err = b.schemaCache.getOrInitCache(ctx, func() (jsonschema.Schema, error) {
			failpoint.Inject("jsonSchemaValidDisableCacheRefresh", func() {
				failpoint.Return(jsonschema.Schema{}, errors.New("Cache refresh disabled by failpoint"))
			})
			dataBin, err := schemaData.MarshalJSON()
			if err != nil {
				return jsonschema.Schema{}, err
			}
			if err := goJSON.Unmarshal(dataBin, &schema); err != nil {
				if _, ok := err.(*goJSON.UnmarshalTypeError); ok {
					return jsonschema.Schema{},
						types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", "object")
				}
				return jsonschema.Schema{},
					types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", err)
			}
			return schema, nil
		})
		if err != nil {
			return res, false, err
		}
	} else {
		dataBin, err := schemaData.MarshalJSON()
		if err != nil {
			return res, false, err
		}
		if err := goJSON.Unmarshal(dataBin, &schema); err != nil {
			if _, ok := err.(*goJSON.UnmarshalTypeError); ok {
				return res, false,
					types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", "object")
			}
			return res, false,
				types.ErrInvalidJSONType.GenWithStackByArgs(1, "json_schema_valid", err)
		}
	}

	// Second argument is the JSON document
	docData, docIsNull, err := b.args[1].EvalJSON(ctx, row)
	if err != nil {
		return res, false, err
	}
	if docIsNull {
		return res, true, err
	}
	docDataBin, err := docData.MarshalJSON()
	if err != nil {
		return res, false, err
	}
	errs, err := schema.ValidateBytes(context.Background(), docDataBin)
	if err != nil {
		return res, false, err
	}
	if len(errs) > 0 {
		return res, false, nil
	}
	res = 1
	return res, false, nil
}
