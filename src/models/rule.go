package models

import (
	"encoding/json"
	"math"
	"strconv"
)

type Rule struct {
	Predicate
	column []string
}

type Predicate map[string][]Atom

type Atom struct {
	op  string
	val interface{}
	RealValue
}

type RealValue struct {
	boolValue   bool
	numberValue json.Number
	stringValue string
	realType    int
}

func (n *Atom) Check(value interface{}) bool {
	if value == nil {
		return (n.val == nil && (n.op == "==" || n.op == "=" || n.op == ">=" || n.op == "<=")) || (n.val != nil && (n.op == "!=" || n.op == "<>"))
	}
	var b RealValue
	if n.val == nil {
		return checkType(value, n.realType, &b) && (n.op == "!=" || n.op == "<>")
	}

	if checkType(value, n.realType, &b) {
		return false
	}
	if n.op == "==" || n.op == "=" {
		return n.val == value
	}
	if n.op == "!=" || n.op == "<>" {
		return n.val != value
	}
	switch n.realType {
	case TypeInt32, TypeInt64:
		if a, err1 := b.numberValue.Int64(); err1 == nil {
			if v1, err2 := n.numberValue.Float64(); err2 == nil {
				t := float64(a)
				switch n.op {
				case "<":
					return t < v1
				case "<=":
					return t <= v1
				case ">":
					return t > v1
				case ">=":
					return t >= v1
				}
			} else if v2, err3 := n.numberValue.Int64(); err3 == nil {
				switch n.op {
				case "<":
					return a < v2
				case "<=":
					return a <= v2
				case ">":
					return a > v2
				case ">=":
					return a >= v2
				}
			}
		}
	case TypeFloat, TypeDouble:
		if a, err1 := b.numberValue.Float64(); err1 == nil {
			if v1, err2 := n.numberValue.Float64(); err2 == nil {
				switch n.op {
				case "<":
					return a < v1
				case "<=":
					return a <= v1
				case ">":
					return a > v1
				case ">=":
					return a >= v1
				}
			} else if v2, err3 := n.numberValue.Int64(); err3 == nil {
				t := float64(v2)
				switch n.op {
				case "<":
					return a < t
				case "<=":
					return a <= t
				case ">":
					return a > t
				case ">=":
					return a >= t
				}
			}
		}
	case TypeBoolean:
		if n.op == "<=" || n.op == ">=" {
			return b.boolValue == n.boolValue
		}
	case TypeString:
		switch n.op {
		case "<":
			return b.stringValue < n.stringValue
		case "<=":
			return b.stringValue <= n.stringValue
		case ">":
			return b.stringValue > n.stringValue
		case ">=":
			return b.stringValue >= n.stringValue
		}
	}
	return false
}

func checkType(value interface{}, typeName int, t *RealValue) bool {
	var ans bool
	switch value.(type) {
	case json.Number:
		t.numberValue = value.(json.Number)
		switch typeName {
		case TypeInt32:
			v, err := t.numberValue.Int64()
			ans = err == nil && v >= math.MinInt32 && v <= math.MaxInt32
		case TypeInt64:
			_, err := t.numberValue.Int64()
			ans = err == nil
		case TypeFloat:
			v, err := t.numberValue.Float64()
			ans = err == nil && math.Abs(v) <= math.MaxFloat32
		case TypeDouble:
			_, err := t.numberValue.Float64()
			ans = err == nil
		}
	case int:
		v := value.(int)
		t.numberValue = json.Number(strconv.Itoa(v))
		switch typeName {
		case TypeInt32:
			ans = v >= math.MinInt32 && v <= math.MaxInt32
		case TypeInt64:
			ans = v >= math.MinInt64 && v <= math.MaxInt64
		case TypeFloat:
			ans = int(float32(v)) == v
		case TypeDouble:
			ans = int(float64(v)) == v
		}
	case int32:
		v := value.(int32)
		t.numberValue = json.Number(strconv.Itoa(int(v)))
		if typeName == TypeFloat {
			ans = int32(float32(v)) == v
		}
		ans = typeName == TypeInt32 || typeName == TypeInt64 || typeName == TypeDouble
	case int64:
		v := value.(int64)
		t.numberValue = json.Number(strconv.Itoa(int(v)))
		switch typeName {
		case TypeInt32:
			ans = v >= math.MinInt32 && v <= math.MaxInt32
		case TypeInt64:
			ans = true
		case TypeFloat:
			ans = int64(float32(v)) == v
		case TypeDouble:
			ans = int64(float64(v)) == v
		}
	case float32:
		v := value.(float32)
		switch typeName {
		case TypeInt32:
			ans = v <= math.MaxInt32 && v >= math.MinInt32 && float32(int32(v)) == v
		case TypeInt64:
			ans = v <= math.MaxInt64 && v >= math.MinInt64 && float32(int64(v)) == v
		case TypeFloat, TypeDouble:
			t.numberValue = json.Number(strconv.FormatFloat(float64(v), 'f', -1, 32))
			ans = true
		}
		if ans && (typeName == TypeInt32 || typeName == TypeInt64) {
			t.numberValue = json.Number(strconv.Itoa(int(v)))
		}
	case float64:
		v := value.(float64)
		t.numberValue = json.Number(strconv.FormatFloat(v, 'f', -1, 64))
		switch typeName {
		case TypeInt32:
			ans = v <= math.MaxInt32 && v >= math.MinInt32 && float64(int32(v)) == v
		case TypeInt64:
			ans = v <= math.MaxInt64 && v >= math.MinInt64 && float64(int64(v)) == v
		case TypeFloat:
			ans = math.Abs(v) <= math.MaxFloat32 && float64(float32(v)) == v
		case TypeDouble:
			ans = true
		}
		if ans && (typeName == TypeInt32 || typeName == TypeInt64) {
			t.numberValue = json.Number(strconv.Itoa(int(v)))
		}
	case bool:
		t.boolValue = value.(bool)
		ans = typeName == TypeBoolean
	case string:
		t.stringValue = value.(string)
		ans = typeName == TypeString
	}
	return ans
}
