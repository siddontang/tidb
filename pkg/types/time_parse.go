// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	gotime "time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

var maxDaysInMonth = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func getTime(ctx Context, num, originNum int64, tp byte) (Time, error) {
	s1 := num / 1000000
	s2 := num - s1*1000000

	year := int(s1 / 10000)
	s1 %= 10000
	month := int(s1 / 100)
	day := int(s1 % 100)

	hour := int(s2 / 10000)
	s2 %= 10000
	minute := int(s2 / 100)
	second := int(s2 % 100)

	ct, ok := FromDateChecked(year, month, day, hour, minute, second, 0)
	if !ok {
		numStr := strconv.FormatInt(originNum, 10)
		return ZeroDatetime, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, numStr))
	}
	t := NewTime(ct, tp, DefaultFsp)
	err := t.Check(ctx)
	return t, errors.Trace(err)
}

// parseDateTimeFromNum parses date time from num.
// See number_to_datetime function.
// https://github.com/mysql/mysql-server/blob/5.7/sql-common/my_time.c
func parseDateTimeFromNum(ctx Context, num int64) (Time, error) {
	t := ZeroDate
	// Check zero.
	if num == 0 {
		return t, nil
	}
	originNum := num

	// Check datetime type.
	if num >= 10000101000000 {
		t.SetType(mysql.TypeDatetime)
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check MMDD.
	if num < 101 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDD, year: 2000-2069
	if num <= (70-1)*10000+1231 {
		num = (num + 20000000) * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check YYMMDD.
	if num < 70*10000+101 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDD, year: 1970-1999
	if num <= 991231 {
		num = (num + 19000000) * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Adjust hour/min/second.
	if num <= 99991231 {
		num = num * 1000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check MMDDHHMMSS.
	if num < 101000000 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Set TypeDatetime type.
	t.SetType(mysql.TypeDatetime)

	// Adjust year
	// YYMMDDHHMMSS, 2000-2069
	if num <= 69*10000000000+1231235959 {
		num = num + 20000000000000
		return getTime(ctx, num, originNum, t.Type())
	}

	// Check YYYYMMDDHHMMSS.
	if num < 70*10000000000+101000000 {
		return t, errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.FormatInt(num, 10)))
	}

	// Adjust year
	// YYMMDDHHMMSS, 1970-1999
	if num <= 991231235959 {
		num = num + 19000000000000
		return getTime(ctx, num, originNum, t.Type())
	}

	return getTime(ctx, num, originNum, t.Type())
}

// ParseTime parses a formatted string with type tp and specific fsp.
// Type is TypeDatetime, TypeTimestamp and TypeDate.
// Fsp is in range [0, 6].
// MySQL supports many valid datetime format, but still has some limitation.
// If delimiter exists, the date part and time part is separated by a space or T,
// other punctuation character can be used as the delimiter between date parts or time parts.
// If no delimiter, the format must be YYYYMMDDHHMMSS or YYMMDDHHMMSS
// If we have fractional seconds part, we must use decimal points as the delimiter.
// The valid datetime range is from '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
// The valid timestamp range is from '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
// The valid date range is from '1000-01-01' to '9999-12-31'
// explicitTz is used to handle a data race of timeZone, refer to https://github.com/pingcap/tidb/issues/40710. It only works for timestamp now, be careful to use it!
func ParseTime(ctx Context, str string, tp byte, fsp int) (Time, error) {
	return parseTime(ctx, str, tp, fsp, false)
}

// ParseTimeFromFloatString is similar to ParseTime, except that it's used to parse a float converted string.
func ParseTimeFromFloatString(ctx Context, str string, tp byte, fsp int) (Time, error) {
	// MySQL compatibility: 0.0 should not be converted to null, see #11203
	if len(str) >= 3 && str[:3] == "0.0" {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), nil
	}
	return parseTime(ctx, str, tp, fsp, true)
}

func parseTime(ctx Context, str string, tp byte, fsp int, isFloat bool) (Time, error) {
	fsp, err := CheckFsp(fsp)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t, err := parseDatetime(ctx, str, fsp, isFloat)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t.SetType(tp)
	if err = t.Check(ctx); err != nil {
		if tp == mysql.TypeTimestamp && !t.IsZero() {
			tAdjusted, errAdjusted := adjustTimestampErrForDST(ctx.Location(), str, tp, t, err)
			if ErrTimestampInDSTTransition.Equal(errAdjusted) {
				return tAdjusted, errors.Trace(errAdjusted)
			}
		}
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}
	return t, nil
}

func adjustTimestampErrForDST(loc *gotime.Location, str string, tp byte, t Time, err error) (Time, error) {
	if tp != mysql.TypeTimestamp || t.IsZero() {
		return t, err
	}
	minTS, maxTS := MinTimestamp, MaxTimestamp
	minErr := minTS.ConvertTimeZone(gotime.UTC, loc)
	maxErr := maxTS.ConvertTimeZone(gotime.UTC, loc)
	if minErr == nil && maxErr == nil &&
		t.Compare(minTS) > 0 && t.Compare(maxTS) < 0 {
		// Handle the case when the timestamp given is in the DST transition
		if tAdjusted, err2 := t.AdjustedGoTime(loc); err2 == nil {
			t.SetCoreTime(FromGoTime(tAdjusted))
			return t, errors.Trace(ErrTimestampInDSTTransition.GenWithStackByArgs(str, loc.String()))
		}
	}
	return t, err
}

// ParseDatetime is a helper function wrapping ParseTime with datetime type and default fsp.
func ParseDatetime(ctx Context, str string) (Time, error) {
	return ParseTime(ctx, str, mysql.TypeDatetime, GetFsp(str))
}

// ParseTimestamp is a helper function wrapping ParseTime with timestamp type and default fsp.
func ParseTimestamp(ctx Context, str string) (Time, error) {
	return ParseTime(ctx, str, mysql.TypeTimestamp, GetFsp(str))
}

// ParseDate is a helper function wrapping ParseTime with date type.
func ParseDate(ctx Context, str string) (Time, error) {
	// date has no fractional seconds precision
	return ParseTime(ctx, str, mysql.TypeDate, MinFsp)
}

// ParseTimeFromYear parse a `YYYY` formed year to corresponded Datetime type.
// Note: the invoker must promise the `year` is in the range [MinYear, MaxYear].
func ParseTimeFromYear(year int64) (Time, error) {
	if year == 0 {
		return NewTime(ZeroCoreTime, mysql.TypeDate, DefaultFsp), nil
	}

	dt := FromDate(int(year), 0, 0, 0, 0, 0, 0)
	return NewTime(dt, mysql.TypeDatetime, DefaultFsp), nil
}

// ParseTimeFromNum parses a formatted int64,
// returns the value which type is tp.
func ParseTimeFromNum(ctx Context, num int64, tp byte, fsp int) (Time, error) {
	// MySQL compatibility: 0 should not be converted to null, see #11203
	if num == 0 {
		zt := NewTime(ZeroCoreTime, tp, DefaultFsp)
		if !ctx.Flags().IgnoreZeroDateErr() {
			switch tp {
			case mysql.TypeTimestamp:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(TimestampStr, "0")
			case mysql.TypeDate:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(DateStr, "0")
			case mysql.TypeDatetime:
				return zt, ErrTruncatedWrongVal.GenWithStackByArgs(DateTimeStr, "0")
			}
		}
		return zt, nil
	}
	fsp, err := CheckFsp(fsp)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t, err := parseDateTimeFromNum(ctx, num)
	if err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}

	t.SetType(tp)
	t.SetFsp(fsp)
	if err := t.Check(ctx); err != nil {
		return NewTime(ZeroCoreTime, tp, DefaultFsp), errors.Trace(err)
	}
	return t, nil
}

// ParseDatetimeFromNum is a helper function wrapping ParseTimeFromNum with datetime type and default fsp.
func ParseDatetimeFromNum(ctx Context, num int64) (Time, error) {
	return ParseTimeFromNum(ctx, num, mysql.TypeDatetime, DefaultFsp)
}

// ParseTimestampFromNum is a helper function wrapping ParseTimeFromNum with timestamp type and default fsp.
func ParseTimestampFromNum(ctx Context, num int64) (Time, error) {
	return ParseTimeFromNum(ctx, num, mysql.TypeTimestamp, DefaultFsp)
}

// ParseDateFromNum is a helper function wrapping ParseTimeFromNum with date type.
func ParseDateFromNum(ctx Context, num int64) (Time, error) {
	// date has no fractional seconds precision
	return ParseTimeFromNum(ctx, num, mysql.TypeDate, MinFsp)
}

// TimeFromDays Converts a day number to a date.
func TimeFromDays(num int64) Time {
	if num < 0 {
		return NewTime(FromDate(0, 0, 0, 0, 0, 0, 0), mysql.TypeDate, 0)
	}
	year, month, day := getDateFromDaynr(uint(num))
	ct, ok := FromDateChecked(int(year), int(month), int(day), 0, 0, 0, 0)
	if !ok {
		return NewTime(FromDate(0, 0, 0, 0, 0, 0, 0), mysql.TypeDate, 0)
	}
	return NewTime(ct, mysql.TypeDate, 0)
}

func checkDateType(t CoreTime, allowZeroInDate, allowInvalidDate bool) error {
	year, month, day := t.Year(), t.Month(), t.Day()
	if year == 0 && month == 0 && day == 0 {
		return nil
	}

	if !allowZeroInDate && (month == 0 || day == 0) {
		return ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%04d-%02d-%02d", year, month, day))
	}

	if err := checkDateRange(t); err != nil {
		return errors.Trace(err)
	}

	if err := checkMonthDay(year, month, day, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDateRange(t CoreTime) error {
	// Oddly enough, MySQL document says date range should larger than '1000-01-01',
	// but we can insert '0001-01-01' actually.
	if t.Year() < 0 || t.Month() < 0 || t.Day() < 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}
	if compareTime(t, MaxDatetime) > 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}
	return nil
}

func checkMonthDay(year, month, day int, allowInvalidDate bool) error {
	if month < 0 || month > 12 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%d-%d-%d", year, month, day)))
	}

	maxDay := 31
	if !allowInvalidDate {
		if month > 0 {
			maxDay = maxDaysInMonth[month-1]
		}
		if month == 2 && !isLeapYear(uint16(year)) {
			maxDay = 28
		}
	}

	if day < 0 || day > maxDay {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(DateTimeStr, fmt.Sprintf("%d-%d-%d", year, month, day)))
	}
	return nil
}

func checkTimestampType(t CoreTime, tz *gotime.Location) error {
	if compareTime(t, ZeroCoreTime) == 0 {
		return nil
	}

	var checkTime CoreTime
	if tz != BoundTimezone {
		convertTime := NewTime(t, mysql.TypeTimestamp, DefaultFsp)
		err := convertTime.ConvertTimeZone(tz, BoundTimezone)
		if err != nil {
			_, err2 := adjustTimestampErrForDST(tz, t.String(), mysql.TypeTimestamp, Time{t}, err)
			return err2
		}
		checkTime = convertTime.coreTime
	} else {
		checkTime = t
	}
	if compareTime(checkTime, MaxTimestamp.coreTime) > 0 || compareTime(checkTime, MinTimestamp.coreTime) < 0 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, t))
	}

	if _, err := t.GoTime(tz); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDatetimeType(t CoreTime, allowZeroInDate, allowInvalidDate bool) error {
	if err := checkDateType(t, allowZeroInDate, allowInvalidDate); err != nil {
		return errors.Trace(err)
	}

	hour, minute, second := t.Hour(), t.Minute(), t.Second()
	if hour < 0 || hour >= 24 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(hour)))
	}
	if minute < 0 || minute >= 60 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(minute)))
	}
	if second < 0 || second >= 60 {
		return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(second)))
	}

	return nil
}

// ExtractDatetimeNum extracts time value number from datetime unit and format.
func ExtractDatetimeNum(t *Time, unit string) (int64, error) {
	// TODO: Consider time_zone variable.
	switch strings.ToUpper(unit) {
	case "DAY":
		return int64(t.Day()), nil
	case "WEEK":
		week := t.Week(0)
		return int64(week), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "QUARTER":
		m := int64(t.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return int64(t.Year()), nil
	case "DAY_MICROSECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d*1000000+h*10000+m*100+s)*1000000 + int64(t.Microsecond()), nil
	case "DAY_SECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d)*1000000 + int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "DAY_MINUTE":
		h, m, _ := t.Clock()
		d := t.Day()
		return int64(d)*10000 + int64(h)*100 + int64(m), nil
	case "DAY_HOUR":
		h, _, _ := t.Clock()
		d := t.Day()
		return int64(d)*100 + int64(h), nil
	case "YEAR_MONTH":
		y, m := t.Year(), t.Month()
		return int64(y)*100 + int64(m), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

// ExtractDurationNum extracts duration value number from duration unit and format.
func ExtractDurationNum(d *Duration, unit string) (res int64, err error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		res = int64(d.MicroSecond())
	case "SECOND":
		res = int64(d.Second())
	case "MINUTE":
		res = int64(d.Minute())
	case "HOUR":
		res = int64(d.Hour())
	case "SECOND_MICROSECOND":
		res = int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "MINUTE_MICROSECOND":
		res = int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "MINUTE_SECOND":
		res = int64(d.Minute()*100 + d.Second())
	case "HOUR_MICROSECOND":
		res = int64(d.Hour())*10000000000 + int64(d.Minute())*100000000 + int64(d.Second())*1000000 + int64(d.MicroSecond())
	case "HOUR_SECOND":
		res = int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second())
	case "HOUR_MINUTE":
		res = int64(d.Hour())*100 + int64(d.Minute())
	case "DAY_MICROSECOND":
		res = int64(d.Hour()*10000+d.Minute()*100+d.Second())*1000000 + int64(d.MicroSecond())
	case "DAY_SECOND":
		res = int64(d.Hour())*10000 + int64(d.Minute())*100 + int64(d.Second())
	case "DAY_MINUTE":
		res = int64(d.Hour())*100 + int64(d.Minute())
	case "DAY_HOUR":
		res = int64(d.Hour())
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
	if d.Duration < 0 {
		res = -res
	}
	return res, nil
}

// parseSingleTimeValue parse the format according the given unit. If we set strictCheck true, we'll check whether
// the converted value not exceed the range of MySQL's TIME type.
// The returned values are year, month, day, nanosecond and fsp.
func parseSingleTimeValue(unit string, format string, strictCheck bool) (year int64, month int64, day int64, nanosecond int64, fsp int, err error) {
	// Format is a preformatted number, it format should be A[.[B]].
	decimalPointPos := strings.IndexRune(format, '.')
	if decimalPointPos == -1 {
		decimalPointPos = len(format)
	}
	sign := int64(1)
	if len(format) > 0 && format[0] == '-' {
		sign = int64(-1)
	}

	// We should also continue even if an error occurs here
	// because the called may ignore the error and use the return value.
	iv, err := strconv.ParseInt(format[0:decimalPointPos], 10, 64)
	if err != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
	}
	riv := iv // Rounded integer value

	decimalLen := 0
	dv := int64(0)
	lf := len(format) - 1
	// Has fraction part
	if decimalPointPos < lf {
		var tmpErr error
		dvPre := oneToSixDigitRegex.FindString(format[decimalPointPos+1:]) // the numberical prefix of the fraction part
		decimalLen = len(dvPre)
		if decimalLen >= 6 {
			// MySQL rounds down to 1e-6.
			if dv, tmpErr = strconv.ParseInt(dvPre[0:6], 10, 64); tmpErr != nil && err == nil {
				err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
			}
		} else {
			if dv, tmpErr = strconv.ParseInt(dvPre+"000000"[:6-decimalLen], 10, 64); tmpErr != nil && err == nil {
				err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, format)
			}
		}
		if dv >= 500000 { // Round up, and we should keep 6 digits for microsecond, so dv should in [000000, 999999].
			riv += sign
		}
		if unit != "SECOND" && err == nil {
			err = ErrTruncatedWrongVal.GenWithStackByArgs(format)
		}
		dv *= sign
	}
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		if strictCheck && mathutil.Abs(riv) > TimeMaxValueSeconds*1000 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Microsecond)
		riv %= int64(GoDurationDay / gotime.Microsecond)
		return 0, 0, dayCount, riv * int64(gotime.Microsecond), MaxFsp, err
	case "SECOND":
		if strictCheck && mathutil.Abs(iv) > TimeMaxValueSeconds {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := iv / int64(GoDurationDay/gotime.Second)
		iv %= int64(GoDurationDay / gotime.Second)
		return 0, 0, dayCount, iv*int64(gotime.Second) + dv*int64(gotime.Microsecond), decimalLen, err
	case "MINUTE":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour*60+TimeMaxMinute {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / int64(GoDurationDay/gotime.Minute)
		riv %= int64(GoDurationDay / gotime.Minute)
		return 0, 0, dayCount, riv * int64(gotime.Minute), 0, err
	case "HOUR":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		dayCount := riv / 24
		riv %= 24
		return 0, 0, dayCount, riv * int64(gotime.Hour), 0, err
	case "DAY":
		if strictCheck && mathutil.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, riv, 0, 0, err
	case "WEEK":
		if strictCheck && 7*mathutil.Abs(riv) > TimeMaxHour/24 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 0, 7 * riv, 0, 0, err
	case "MONTH":
		if strictCheck && mathutil.Abs(riv) > 1 {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, riv, 0, 0, 0, err
	case "QUARTER":
		if strictCheck {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return 0, 3 * riv, 0, 0, 0, err
	case "YEAR":
		if strictCheck {
			return 0, 0, 0, 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
		}
		return riv, 0, 0, 0, 0, err
	}

	return 0, 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// parseTimeValue gets years, months, days, nanoseconds and fsp from a string
// nanosecond will not exceed length of single day
// MySQL permits any punctuation delimiter in the expr format.
// See https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
func parseTimeValue(format string, index, cnt int) (years int64, months int64, days int64, nanoseconds int64, fsp int, err error) {
	neg := false
	originalFmt := format
	fsp = map[bool]int{true: MaxFsp, false: MinFsp}[index == MicrosecondIndex]
	format = strings.TrimSpace(format)
	if len(format) > 0 && format[0] == '-' {
		neg = true
		format = format[1:]
	}
	fields := make([]string, TimeValueCnt)
	for i := range fields {
		fields[i] = "0"
	}
	matches := numericRegex.FindAllString(format, -1)
	if len(matches) > cnt {
		return 0, 0, 0, 0, 0, ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	for i := range matches {
		if neg {
			fields[index] = "-" + matches[len(matches)-1-i]
		} else {
			fields[index] = matches[len(matches)-1-i]
		}
		index--
	}

	// ParseInt may return an error when overflowed, but we should continue to parse the rest of the string because
	// the caller may ignore the error and use the return value.
	// In this case, we should return a big value to make sure the result date after adding this interval
	// is also overflowed and NULL is returned to the user.
	years, err = strconv.ParseInt(fields[YearIndex], 10, 64)
	if err != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	var tmpErr error
	months, tmpErr = strconv.ParseInt(fields[MonthIndex], 10, 64)
	if err == nil && tmpErr != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	days, tmpErr = strconv.ParseInt(fields[DayIndex], 10, 64)
	if err == nil && tmpErr != nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}

	hours, tmpErr := strconv.ParseInt(fields[HourIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	minutes, tmpErr := strconv.ParseInt(fields[MinuteIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	seconds, tmpErr := strconv.ParseInt(fields[SecondIndex], 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	microseconds, tmpErr := strconv.ParseInt(alignFrac(fields[MicrosecondIndex], MaxFsp), 10, 64)
	if tmpErr != nil && err == nil {
		err = ErrWrongValue.GenWithStackByArgs(DateTimeStr, originalFmt)
	}
	seconds = hours*3600 + minutes*60 + seconds
	days += seconds / (3600 * 24)
	seconds %= 3600 * 24
	return years, months, days, seconds*int64(gotime.Second) + microseconds*int64(gotime.Microsecond), fsp, err
}

func parseAndValidateDurationValue(format string, index, cnt int) (int64, int, error) {
	year, month, day, nano, fsp, err := parseTimeValue(format, index, cnt)
	if err != nil {
		return 0, 0, err
	}
	if year != 0 || month != 0 || mathutil.Abs(day) > TimeMaxHour/24 {
		return 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	dur := day*int64(GoDurationDay) + nano
	if mathutil.Abs(dur) > int64(MaxTime) {
		return 0, 0, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	}
	return dur, fsp, nil
}

// ParseDurationValue parses time value from time unit and format.
// Returns y years m months d days + n nanoseconds
// Nanoseconds will no longer than one day.
func ParseDurationValue(unit string, format string) (y int64, m int64, d int64, n int64, fsp int, _ error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		return parseSingleTimeValue(unit, format, false)
	case "SECOND_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
	case "MINUTE_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
	case "MINUTE_SECOND":
		return parseTimeValue(format, SecondIndex, MinuteSecondMaxCnt)
	case "HOUR_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
	case "HOUR_SECOND":
		return parseTimeValue(format, SecondIndex, HourSecondMaxCnt)
	case "HOUR_MINUTE":
		return parseTimeValue(format, MinuteIndex, HourMinuteMaxCnt)
	case "DAY_MICROSECOND":
		return parseTimeValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
	case "DAY_SECOND":
		return parseTimeValue(format, SecondIndex, DaySecondMaxCnt)
	case "DAY_MINUTE":
		return parseTimeValue(format, MinuteIndex, DayMinuteMaxCnt)
	case "DAY_HOUR":
		return parseTimeValue(format, HourIndex, DayHourMaxCnt)
	case "YEAR_MONTH":
		return parseTimeValue(format, MonthIndex, YearMonthMaxCnt)
	default:
		return 0, 0, 0, 0, 0, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// ExtractDurationValue extract the value from format to Duration.
func ExtractDurationValue(unit string, format string) (Duration, error) {
	unit = strings.ToUpper(unit)
	switch unit {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		_, month, day, nano, fsp, err := parseSingleTimeValue(unit, format, true)
		if err != nil {
			return ZeroDuration, err
		}
		dur := Duration{Duration: gotime.Duration((month*30+day)*int64(GoDurationDay) + nano), Fsp: fsp}
		return dur, err
	case "SECOND_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, SecondMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "MINUTE_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, MinuteMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "MINUTE_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, MinuteSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, HourMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, HourSecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "HOUR_MINUTE":
		d, fsp, err := parseAndValidateDurationValue(format, MinuteIndex, HourMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_MICROSECOND":
		d, fsp, err := parseAndValidateDurationValue(format, MicrosecondIndex, DayMicrosecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_SECOND":
		d, fsp, err := parseAndValidateDurationValue(format, SecondIndex, DaySecondMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_MINUTE":
		d, fsp, err := parseAndValidateDurationValue(format, MinuteIndex, DayMinuteMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "DAY_HOUR":
		d, fsp, err := parseAndValidateDurationValue(format, HourIndex, DayHourMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		return Duration{Duration: gotime.Duration(d), Fsp: fsp}, nil
	case "YEAR_MONTH":
		_, _, err := parseAndValidateDurationValue(format, MonthIndex, YearMonthMaxCnt)
		if err != nil {
			return ZeroDuration, err
		}
		// MONTH must exceed the limit of mysql's duration. So just returns overflow error.
		return ZeroDuration, ErrDatetimeFunctionOverflow.GenWithStackByArgs("time")
	default:
		return ZeroDuration, errors.Errorf("invalid single timeunit - %s", unit)
	}
}

// IsClockUnit returns true when unit is interval unit with hour, minute, second or microsecond.
func IsClockUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR",
		"SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND",
		"MINUTE_SECOND", "HOUR_SECOND", "DAY_SECOND",
		"HOUR_MINUTE", "DAY_MINUTE",
		"DAY_HOUR":
		return true
	default:
		return false
	}
}

// IsDateUnit returns true when unit is interval unit with year, quarter, month, week or day.
func IsDateUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
		"DAY_MICROSECOND", "DAY_SECOND", "DAY_MINUTE", "DAY_HOUR",
		"YEAR_MONTH":
		return true
	default:
		return false
	}
}

// IsMicrosecondUnit returns true when unit is interval unit with microsecond.
func IsMicrosecondUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
		return true
	default:
		return false
	}
}

// IsDateFormat returns true when the specified time format could contain only date.
func IsDateFormat(format string) bool {
	format = strings.TrimSpace(format)
	seps := ParseDateFormat(format)
	length := len(format)
	switch len(seps) {
	case 1:
		// "20129" will be parsed to 2020-12-09, which is date format.
		if (length == 8) || (length == 6) || (length == 5) {
			return true
		}
	case 3:
		return true
	}
	return false
}

// ParseTimeFromInt64 parses mysql time value from int64.
func ParseTimeFromInt64(ctx Context, num int64) (Time, error) {
	return parseDateTimeFromNum(ctx, num)
}

// ParseTimeFromFloat64 parses mysql time value from float64.
// It is used in scenarios that distinguish date and datetime, e.g., date_add/sub() with first argument being real.
// For example, 20010203 parses to date (no HMS) and 20010203040506 parses to datetime (with HMS).
func ParseTimeFromFloat64(ctx Context, f float64) (Time, error) {
	intPart := int64(f)
	t, err := parseDateTimeFromNum(ctx, intPart)
	if err != nil {
		return ZeroTime, err
	}
	if t.Type() == mysql.TypeDatetime {
		// US part is only kept when the integral part is recognized as datetime.
		fracPart := uint32(math.Round((f - float64(intPart)) * 1000000.0))
		ct := t.CoreTime()
		ct.setMicrosecond(fracPart)
		t.SetCoreTime(ct)
	}
	return t, err
}

// ParseTimeFromDecimal parses mysql time value from decimal.
// It is used in scenarios that distinguish date and datetime, e.g., date_add/sub() with first argument being decimal.
// For example, 20010203 parses to date (no HMS) and 20010203040506 parses to datetime (with HMS).
func ParseTimeFromDecimal(ctx Context, dec *MyDecimal) (t Time, err error) {
	intPart, err := dec.ToInt()
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, err
	}
	fsp := min(MaxFsp, int(dec.GetDigitsFrac()))
	t, err = parseDateTimeFromNum(ctx, intPart)
	if err != nil {
		return ZeroTime, err
	}
	t.SetFsp(fsp)
	if fsp == 0 || t.Type() == mysql.TypeDate {
		// Shortcut for integer value or date value (fractional part omitted).
		return t, err
	}

	intPartDec := new(MyDecimal).FromInt(intPart)
	fracPartDec := new(MyDecimal)
	err = DecimalSub(dec, intPartDec, fracPartDec)
	if err != nil {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}
	million := new(MyDecimal).FromInt(1000000)
	msPartDec := new(MyDecimal)
	err = DecimalMul(fracPartDec, million, msPartDec)
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}
	msPart, err := msPartDec.ToInt()
	if err != nil && !terror.ErrorEqual(err, ErrTruncated) {
		return ZeroTime, errors.Trace(dbterror.ClassTypes.NewStd(errno.ErrIncorrectDatetimeValue).GenWithStackByArgs(dec.ToString()))
	}

	ct := t.CoreTime()
	ct.setMicrosecond(uint32(msPart))
	t.SetCoreTime(ct)

	return t, nil
}

// DateFormat returns a textual representation of the time value formatted
// according to layout.
// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t Time) DateFormat(layout string) (string, error) {
	var buf bytes.Buffer
	inPatternMatch := false
	for _, b := range layout {
		if inPatternMatch {
			if err := t.convertDateFormat(b, &buf); err != nil {
				return "", errors.Trace(err)
			}
			inPatternMatch = false
			continue
		}

		// It's not in pattern match now.
		if b == '%' {
			inPatternMatch = true
		} else {
			buf.WriteRune(b)
		}
	}
	return buf.String(), nil
}

var abbrevWeekdayName = []string{
	"Sun", "Mon", "Tue",
	"Wed", "Thu", "Fri", "Sat",
}

func (t Time) convertDateFormat(b rune, buf *bytes.Buffer) error {
	switch b {
	case 'b':
		m := t.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(m)))
		}
		buf.WriteString(MonthNames[m-1][:3])
	case 'M':
		m := t.Month()
		if m == 0 || m > 12 {
			return errors.Trace(ErrWrongValue.GenWithStackByArgs(TimeStr, strconv.Itoa(m)))
		}
		buf.WriteString(MonthNames[m-1])
	case 'm':
		buf.WriteString(FormatIntWidthN(t.Month(), 2))
	case 'c':
		buf.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	case 'D':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
		buf.WriteString(abbrDayOfMonth(t.Day()))
	case 'd':
		buf.WriteString(FormatIntWidthN(t.Day(), 2))
	case 'e':
		buf.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	case 'j':
		fmt.Fprintf(buf, "%03d", t.YearDay())
	case 'H':
		buf.WriteString(FormatIntWidthN(t.Hour(), 2))
	case 'k':
		buf.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	case 'h', 'I':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(FormatIntWidthN(tt%12, 2))
		}
	case 'l':
		tt := t.Hour()
		if tt%12 == 0 {
			buf.WriteString("12")
		} else {
			buf.WriteString(strconv.FormatInt(int64(tt%12), 10))
		}
	case 'i':
		buf.WriteString(FormatIntWidthN(t.Minute(), 2))
	case 'p':
		hour := t.Hour()
		if hour/12%2 == 0 {
			buf.WriteString("AM")
		} else {
			buf.WriteString("PM")
		}
	case 'r':
		h := t.Hour()
		h %= 24
		switch {
		case h == 0:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", 12, t.Minute(), t.Second())
		case h == 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", 12, t.Minute(), t.Second())
		case h < 12:
			fmt.Fprintf(buf, "%02d:%02d:%02d AM", h, t.Minute(), t.Second())
		default:
			fmt.Fprintf(buf, "%02d:%02d:%02d PM", h-12, t.Minute(), t.Second())
		}
	case 'T':
		fmt.Fprintf(buf, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
	case 'S', 's':
		buf.WriteString(FormatIntWidthN(t.Second(), 2))
	case 'f':
		fmt.Fprintf(buf, "%06d", t.Microsecond())
	case 'U':
		w := t.Week(0)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'u':
		w := t.Week(1)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'V':
		w := t.Week(2)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'v':
		_, w := t.YearWeek(3)
		buf.WriteString(FormatIntWidthN(w, 2))
	case 'a':
		weekday := t.Weekday()
		buf.WriteString(abbrevWeekdayName[weekday])
	case 'W':
		buf.WriteString(t.Weekday().String())
	case 'w':
		buf.WriteString(strconv.FormatInt(int64(t.Weekday()), 10))
	case 'X':
		year, _ := t.YearWeek(2)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'x':
		year, _ := t.YearWeek(3)
		if year < 0 {
			buf.WriteString(strconv.FormatUint(uint64(math.MaxUint32), 10))
		} else {
			buf.WriteString(FormatIntWidthN(year, 4))
		}
	case 'Y':
		buf.WriteString(FormatIntWidthN(t.Year(), 4))
	case 'y':
		str := FormatIntWidthN(t.Year(), 4)
		buf.WriteString(str[2:])
	default:
		buf.WriteRune(b)
	}

	return nil
}

// FormatIntWidthN uses to format int with width. Insufficient digits are filled by 0.
func FormatIntWidthN(num, n int) string {
	numString := strconv.FormatInt(int64(num), 10)
	if len(numString) >= n {
		return numString
	}
	padBytes := make([]byte, n-len(numString))
	for i := range padBytes {
		padBytes[i] = '0'
	}
	return string(padBytes) + numString
}

func abbrDayOfMonth(day int) string {
	var str string
	switch day {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}
	return str
}

// StrToDate converts date string according to format.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (t *Time) StrToDate(typeCtx Context, date, format string) bool {
	ctx := make(map[string]int)
	var tm CoreTime
	success, warning := strToDate(&tm, date, format, ctx)
	if !success {
		t.SetCoreTime(ZeroCoreTime)
		t.SetType(mysql.TypeDatetime)
		t.SetFsp(0)
		return false
	}
	if err := mysqlTimeFix(&tm, ctx); err != nil {
		return false
	}

	t.SetCoreTime(tm)
	t.SetType(mysql.TypeDatetime)
	if t.Check(typeCtx) != nil {
		return false
	}
	if warning {
		// Only append this warning when success but still need warning.
		// Currently this only happens when `date` has extra characters at the end.
		typeCtx.AppendWarning(ErrTruncatedWrongVal.FastGenByArgs(DateTimeStr, date))
	}
	return true
}

// mysqlTimeFix fixes the Time use the values in the context.
func mysqlTimeFix(t *CoreTime, ctx map[string]int) error {
	// Key of the ctx is the format char, such as `%j` `%p` and so on.
	if yearOfDay, ok := ctx["%j"]; ok {
		// TODO: Implement the function that converts day of year to yy:mm:dd.
		_ = yearOfDay
	}
	if valueAMorPm, ok := ctx["%p"]; ok {
		if _, ok := ctx["%H"]; ok {
			return ErrWrongValue.GenWithStackByArgs(TimeStr, t)
		}
		if t.Hour() == 0 {
			return ErrWrongValue.GenWithStackByArgs(TimeStr, t)
		}
		if t.Hour() == 12 {
			// 12 is a special hour.
			switch valueAMorPm {
			case constForAM:
				t.setHour(0)
			case constForPM:
				t.setHour(12)
			}
			return nil
		}
		if valueAMorPm == constForPM {
			t.setHour(t.getHour() + 12)
		}
	} else {
		if _, ok := ctx["%h"]; ok && t.Hour() == 12 {
			t.setHour(0)
		}
	}
	return nil
}

// strToDate converts date string according to format,
// the value will be stored in argument t or ctx.
// The second return value is true when success but still need to append a warning.
func strToDate(t *CoreTime, date string, format string, ctx map[string]int) (success bool, warning bool) {
	date = skipWhiteSpace(date)
	format = skipWhiteSpace(format)

	token, formatRemain, succ := getFormatToken(format)
	if !succ {
		return false, false
	}

	if token == "" {
		if len(date) != 0 {
			// Extra characters at the end of date are ignored, but a warning should be reported at this case.
			return true, true
		}
		// Normal case. Both token and date are empty now.
		return true, false
	}

	if len(date) == 0 {
		ctx[token] = 0
		return true, false
	}

	dateRemain, succ := matchDateWithToken(t, date, token, ctx)
	if !succ {
		return false, false
	}

	return strToDate(t, dateRemain, formatRemain, ctx)
}

// getFormatToken takes one format control token from the string.
// format "%d %H %m" will get token "%d" and the remain is " %H %m".
func getFormatToken(format string) (token string, remain string, succ bool) {
	if len(format) == 0 {
		return "", "", true
	}

	// Just one character.
	if len(format) == 1 {
		if format[0] == '%' {
			return "", "", false
		}
		return format, "", true
	}

	// More than one character.
	if format[0] == '%' {
		return format[:2], format[2:], true
	}

	return format[:1], format[1:], true
}

func skipWhiteSpace(input string) string {
	for i, c := range input {
		if !unicode.IsSpace(c) {
			return input[i:]
		}
	}
	return ""
}

var monthAbbrev = map[string]gotime.Month{
	"jan": gotime.January,
	"feb": gotime.February,
	"mar": gotime.March,
	"apr": gotime.April,
	"may": gotime.May,
	"jun": gotime.June,
	"jul": gotime.July,
	"aug": gotime.August,
	"sep": gotime.September,
	"oct": gotime.October,
	"nov": gotime.November,
	"dec": gotime.December,
}

type dateFormatParser func(t *CoreTime, date string, ctx map[string]int) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%b": abbreviatedMonth,      // Abbreviated month name (Jan..Dec)
	"%c": monthNumeric,          // Month, numeric (0..12)
	"%d": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%e": dayOfMonthNumeric,     // Day of the month, numeric (0..31)
	"%f": microSeconds,          // Microseconds (000000..999999)
	"%h": hour12Numeric,         // Hour (01..12)
	"%H": hour24Numeric,         // Hour (00..23)
	"%I": hour12Numeric,         // Hour (01..12)
	"%i": minutesNumeric,        // Minutes, numeric (00..59)
	"%j": dayOfYearNumeric,      // Day of year (001..366)
	"%k": hour24Numeric,         // Hour (0..23)
	"%l": hour12Numeric,         // Hour (1..12)
	"%M": fullNameMonth,         // Month name (January..December)
	"%m": monthNumeric,          // Month, numeric (00..12)
	"%p": isAMOrPM,              // AM or PM
	"%r": time12Hour,            // Time, 12-hour (hh:mm:ss followed by AM or PM)
	"%s": secondsNumeric,        // Seconds (00..59)
	"%S": secondsNumeric,        // Seconds (00..59)
	"%T": time24Hour,            // Time, 24-hour (hh:mm:ss)
	"%Y": yearNumericFourDigits, // Year, numeric, four digits
	"%#": skipAllNums,           // Skip all numbers
	"%.": skipAllPunct,          // Skip all punctation characters
	"%@": skipAllAlpha,          // Skip all alpha characters
	// Deprecated since MySQL 5.7.5
	"%y": yearNumericTwoDigits, // Year, numeric (two digits)
	// TODO: Add the following...
	// "%a": abbreviatedWeekday,         // Abbreviated weekday name (Sun..Sat)
	// "%D": dayOfMonthWithSuffix,       // Day of the month with English suffix (0th, 1st, 2nd, 3rd)
	// "%U": weekMode0,                  // Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
	// "%u": weekMode1,                  // Week (00..53), where Monday is the first day of the week; WEEK() mode 1
	// "%V": weekMode2,                  // Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
	// "%v": weekMode3,                  // Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
	// "%W": weekdayName,                // Weekday name (Sunday..Saturday)
	// "%w": dayOfWeek,                  // Day of the week (0=Sunday..6=Saturday)
	// "%X": yearOfWeek,                 // Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
	// "%x": yearOfWeek,                 // Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
}

// GetFormatType checks the type(Duration, Date or Datetime) of a format string.
func GetFormatType(format string) (isDuration, isDate bool) {
	format = skipWhiteSpace(format)
	var token string
	var succ bool
	for {
		token, format, succ = getFormatToken(format)
		if len(token) == 0 {
			break
		}
		if !succ {
			isDuration, isDate = false, false
			break
		}
		if len(token) >= 2 && token[0] == '%' {
			switch token[1] {
			case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'f', 'r', 'T':
				isDuration = true
			case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
				isDate = true
			}
		}
		if isDuration && isDate {
			break
		}
	}
	return
}

func matchDateWithToken(t *CoreTime, date string, token string, ctx map[string]int) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date, ctx)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

// Try to parse digits with number of `limit` starting from `input`
// Return <number, n chars to step forward> if success.
// Return <_, 0> if fail.
func parseNDigits(input string, limit int) (number int, step int) {
	if limit <= 0 {
		return 0, 0
	}

	var num uint64 = 0
	step = 0
	for ; step < len(input) && step < limit && '0' <= input[step] && input[step] <= '9'; step++ {
		num = num*10 + uint64(input[step]-'0')
	}
	return int(num), step
}

func secondsNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setSecond(uint8(v))
	return input[step:], true
}

func minutesNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2)
	if step <= 0 || v >= 60 {
		return input, false
	}
	t.setMinute(uint8(v))
	return input[step:], true
}

type parseState int32

const (
	parseStateNormal    parseState = 1
	parseStateFail      parseState = 2
	parseStateEndOfLine parseState = 3
)

func parseSep(input string) (string, parseState) {
	input = skipWhiteSpace(input)
	if len(input) == 0 {
		return input, parseStateEndOfLine
	}
	if input[0] != ':' {
		return input, parseStateFail
	}
	if input = skipWhiteSpace(input[1:]); len(input) == 0 {
		return input, parseStateEndOfLine
	}
	return input, parseStateNormal
}

func time12Hour(t *CoreTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss AM
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 1..12
		if step <= 0 || hour > 12 || hour == 0 {
			return input, parseStateFail
		}
		// Handle special case: 12:34:56 AM -> 00:34:56
		// For PM, we will add 12 it later
		if hour == 12 {
			hour = 0
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))

		input = skipWhiteSpace(input[step:])
		if len(input) == 0 {
			// No "AM"/"PM" suffix, it is ok
			return input, parseStateEndOfLine
		} else if len(input) < 2 {
			// some broken char, fail
			return input, parseStateFail
		}

		switch {
		case hasCaseInsensitivePrefix(input, "AM"):
			t.setHour(uint8(hour))
		case hasCaseInsensitivePrefix(input, "PM"):
			t.setHour(uint8(hour + 12))
		default:
			return input, parseStateFail
		}

		return input[2:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

func time24Hour(t *CoreTime, input string, _ map[string]int) (string, bool) {
	tryParse := func(input string) (string, parseState) {
		// hh:mm:ss
		/// Note that we should update `t` as soon as possible, or we
		/// can not get correct result for incomplete input like "12:13"
		/// that is shorter than "hh:mm:ss"
		hour, step := parseNDigits(input, 2) // 0..23
		if step <= 0 || hour > 23 {
			return input, parseStateFail
		}
		t.setHour(uint8(hour))

		// ':'
		var state parseState
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		minute, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || minute > 59 {
			return input, parseStateFail
		}
		t.setMinute(uint8(minute))

		// ':'
		if input, state = parseSep(input[step:]); state != parseStateNormal {
			return input, state
		}

		second, step := parseNDigits(input, 2) // 0..59
		if step <= 0 || second > 59 {
			return input, parseStateFail
		}
		t.setSecond(uint8(second))
		return input[step:], parseStateNormal
	}

	remain, state := tryParse(input)
	if state == parseStateFail {
		return input, false
	}
	return remain, true
}

const (
	constForAM = 1 + iota
	constForPM
)

func isAMOrPM(_ *CoreTime, input string, ctx map[string]int) (string, bool) {
	if len(input) < 2 {
		return input, false
	}

	s := strings.ToLower(input[:2])
	switch s {
	case "am":
		ctx["%p"] = constForAM
	case "pm":
		ctx["%p"] = constForPM
	default:
		return input, false
	}
	return input[2:], true
}

// oneToSixDigitRegex: it was just for [0, 999999]
var oneToSixDigitRegex = regexp.MustCompile("^[0-9]{0,6}")

// numericRegex: it was for any numeric characters
var numericRegex = regexp.MustCompile("[0-9]+")

func dayOfMonthNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..31
	if step <= 0 || v > 31 {
		return input, false
	}
	t.setDay(uint8(v))
	return input[step:], true
}

func hour24Numeric(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 0..23
	if step <= 0 || v > 23 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%H"] = v
	return input[step:], true
}

func hour12Numeric(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 || v == 0 {
		return input, false
	}
	t.setHour(uint8(v))
	ctx["%h"] = v
	return input[step:], true
}

func microSeconds(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 6)
	if step <= 0 {
		t.setMicrosecond(0)
		return input, true
	}
	for i := step; i < 6; i++ {
		v *= 10
	}
	t.setMicrosecond(uint32(v))
	return input[step:], true
}

func yearNumericFourDigits(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 4)
}

func yearNumericTwoDigits(t *CoreTime, input string, ctx map[string]int) (string, bool) {
	return yearNumericNDigits(t, input, ctx, 2)
}

func yearNumericNDigits(t *CoreTime, input string, _ map[string]int, n int) (string, bool) {
	year, step := parseNDigits(input, n)
	if step <= 0 {
		return input, false
	} else if step <= 2 {
		year = adjustYear(year)
	}
	t.setYear(uint16(year))
	return input[step:], true
}

func dayOfYearNumeric(_ *CoreTime, input string, ctx map[string]int) (string, bool) {
	// MySQL declares that "%j" should be "Day of year (001..366)". But actually,
	// it accepts a number that is up to three digits, which range is [1, 999].
	v, step := parseNDigits(input, 3)
	if step <= 0 || v == 0 {
		return input, false
	}
	ctx["%j"] = v
	return input[step:], true
}

func abbreviatedMonth(t *CoreTime, input string, _ map[string]int) (string, bool) {
	if len(input) >= 3 {
		monthName := strings.ToLower(input[:3])
		if month, ok := monthAbbrev[monthName]; ok {
			t.setMonth(uint8(month))
			return input[len(monthName):], true
		}
	}
	return input, false
}

func hasCaseInsensitivePrefix(input, prefix string) bool {
	if len(input) < len(prefix) {
		return false
	}
	return strings.EqualFold(input[:len(prefix)], prefix)
}

func fullNameMonth(t *CoreTime, input string, _ map[string]int) (string, bool) {
	for i, month := range MonthNames {
		if hasCaseInsensitivePrefix(input, month) {
			t.setMonth(uint8(i + 1))
			return input[len(month):], true
		}
	}
	return input, false
}

func monthNumeric(t *CoreTime, input string, _ map[string]int) (string, bool) {
	v, step := parseNDigits(input, 2) // 1..12
	if step <= 0 || v > 12 {
		return input, false
	}
	t.setMonth(uint8(v))
	return input[step:], true
}

// DateFSP gets fsp from date string.
func DateFSP(date string) (fsp int) {
	i := strings.LastIndex(date, ".")
	if i != -1 {
		fsp = len(date) - i - 1
	}
	return
}

// DateTimeIsOverflow returns if this date is overflow.
// See: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DateTimeIsOverflow(ctx Context, date Time) (bool, error) {
	tz := ctx.Location()
	if tz == nil {
		logutil.BgLogger().Warn("use gotime.local because sc.timezone is nil")
		tz = gotime.Local
	}

	var err error
	var b, e, t gotime.Time
	switch date.Type() {
	case mysql.TypeDate, mysql.TypeDatetime:
		if b, err = MinDatetime.GoTime(tz); err != nil {
			return false, err
		}
		if e, err = MaxDatetime.GoTime(tz); err != nil {
			return false, err
		}
	case mysql.TypeTimestamp:
		minTS, maxTS := MinTimestamp, MaxTimestamp
		if tz != gotime.UTC {
			if err = minTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				return false, err
			}
			if err = maxTS.ConvertTimeZone(gotime.UTC, tz); err != nil {
				return false, err
			}
		}
		if b, err = minTS.GoTime(tz); err != nil {
			return false, err
		}
		if e, err = maxTS.GoTime(tz); err != nil {
			return false, err
		}
	default:
		return false, nil
	}

	if t, err = date.AdjustedGoTime(tz); err != nil {
		return false, err
	}

	inRange := (t.After(b) || t.Equal(b)) && (t.Before(e) || t.Equal(e))
	return !inRange, nil
}

func skipAllNums(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsNumber(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}

func skipAllPunct(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsPunct(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}

func skipAllAlpha(_ *CoreTime, input string, _ map[string]int) (string, bool) {
	retIdx := 0
	for i, ch := range input {
		if !unicode.IsLetter(ch) {
			break
		}
		retIdx = i + 1
	}
	return input[retIdx:], true
}
