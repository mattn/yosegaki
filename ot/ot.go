// Package ot implements plain text operational transformation compatible
// with the wire format of ot.js: an operation is a JSON array whose items are
// a positive number (retain), a negative number (delete) or a string (insert).
// Lengths are counted in Unicode code points.
package ot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

type component struct {
	n int
	s string
}

func (c component) isRetain() bool { return c.s == "" && c.n > 0 }
func (c component) isDelete() bool { return c.s == "" && c.n < 0 }
func (c component) isInsert() bool { return c.s != "" }

type Operation struct {
	ops       []component
	BaseLen   int
	TargetLen int
}

func (o *Operation) Retain(n int) *Operation {
	if n <= 0 {
		return o
	}
	o.BaseLen += n
	o.TargetLen += n
	if l := len(o.ops); l > 0 && o.ops[l-1].isRetain() {
		o.ops[l-1].n += n
	} else {
		o.ops = append(o.ops, component{n: n})
	}
	return o
}

func (o *Operation) Insert(s string) *Operation {
	if s == "" {
		return o
	}
	o.TargetLen += utf8.RuneCountInString(s)
	l := len(o.ops)
	switch {
	case l > 0 && o.ops[l-1].isInsert():
		o.ops[l-1].s += s
	case l > 0 && o.ops[l-1].isDelete():
		// Keep inserts before deletes so equal operations look the same.
		if l > 1 && o.ops[l-2].isInsert() {
			o.ops[l-2].s += s
		} else {
			o.ops = append(o.ops, o.ops[l-1])
			o.ops[l-1] = component{s: s}
		}
	default:
		o.ops = append(o.ops, component{s: s})
	}
	return o
}

func (o *Operation) Delete(n int) *Operation {
	if n <= 0 {
		return o
	}
	o.BaseLen += n
	if l := len(o.ops); l > 0 && o.ops[l-1].isDelete() {
		o.ops[l-1].n -= n
	} else {
		o.ops = append(o.ops, component{n: -n})
	}
	return o
}

// ContainsRune reports whether any inserted text contains r.
func (o *Operation) ContainsRune(r rune) bool {
	for _, c := range o.ops {
		if strings.ContainsRune(c.s, r) {
			return true
		}
	}
	return false
}

// Size estimates the memory an operation holds, in bytes.
func (o *Operation) Size() int {
	n := 0
	for _, c := range o.ops {
		n += 16 + len(c.s)
	}
	return n
}

func (o *Operation) IsNoop() bool {
	return len(o.ops) == 0 || (len(o.ops) == 1 && o.ops[0].isRetain())
}

func (o *Operation) Apply(s string) (string, error) {
	r := []rune(s)
	if len(r) != o.BaseLen {
		return "", fmt.Errorf("ot: base length %d does not match document length %d", o.BaseLen, len(r))
	}
	out := make([]rune, 0, o.TargetLen)
	i := 0
	for _, c := range o.ops {
		switch {
		case c.isRetain():
			out = append(out, r[i:i+c.n]...)
			i += c.n
		case c.isInsert():
			out = append(out, []rune(c.s)...)
		default:
			i -= c.n
		}
	}
	return string(out), nil
}

// TransformIndex moves a cursor position in the base document to the
// matching position in the target document.
func (o *Operation) TransformIndex(idx int) int {
	pos, shift := 0, 0
	for _, c := range o.ops {
		if pos > idx {
			break
		}
		switch {
		case c.isRetain():
			pos += c.n
		case c.isInsert():
			shift += utf8.RuneCountInString(c.s)
		default:
			d := -c.n
			if idx >= pos+d {
				shift -= d
			} else {
				shift -= idx - pos
			}
			pos += d
		}
	}
	return idx + shift
}

var errLength = errors.New("ot: operations have incompatible lengths")

func cut(s string, n int) (string, string) {
	i := 0
	for n > 0 {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
		n--
	}
	return s[:i], s[i:]
}

// Transform returns a' and b' such that apply(apply(S, a), b') equals
// apply(apply(S, b), a'). Inserts of a win ties.
func Transform(a, b *Operation) (*Operation, *Operation, error) {
	if a.BaseLen != b.BaseLen {
		return nil, nil, errLength
	}
	a1, b1 := &Operation{}, &Operation{}
	ops1, ops2 := a.ops, b.ops
	var op1, op2 *component
	next := func(ops *[]component) *component {
		if len(*ops) == 0 {
			return nil
		}
		c := (*ops)[0]
		*ops = (*ops)[1:]
		return &c
	}
	op1, op2 = next(&ops1), next(&ops2)
	for op1 != nil || op2 != nil {
		if op1 != nil && op1.isInsert() {
			a1.Insert(op1.s)
			b1.Retain(utf8.RuneCountInString(op1.s))
			op1 = next(&ops1)
			continue
		}
		if op2 != nil && op2.isInsert() {
			a1.Retain(utf8.RuneCountInString(op2.s))
			b1.Insert(op2.s)
			op2 = next(&ops2)
			continue
		}
		if op1 == nil || op2 == nil {
			return nil, nil, errLength
		}
		switch {
		case op1.isRetain() && op2.isRetain():
			var m int
			switch {
			case op1.n > op2.n:
				m = op2.n
				op1.n -= op2.n
				op2 = next(&ops2)
			case op1.n == op2.n:
				m = op2.n
				op1, op2 = next(&ops1), next(&ops2)
			default:
				m = op1.n
				op2.n -= op1.n
				op1 = next(&ops1)
			}
			a1.Retain(m)
			b1.Retain(m)
		case op1.isDelete() && op2.isDelete():
			switch {
			case -op1.n > -op2.n:
				op1.n -= op2.n
				op2 = next(&ops2)
			case op1.n == op2.n:
				op1, op2 = next(&ops1), next(&ops2)
			default:
				op2.n -= op1.n
				op1 = next(&ops1)
			}
		case op1.isDelete() && op2.isRetain():
			var m int
			switch {
			case -op1.n > op2.n:
				m = op2.n
				op1.n += op2.n
				op2 = next(&ops2)
			case -op1.n == op2.n:
				m = op2.n
				op1, op2 = next(&ops1), next(&ops2)
			default:
				m = -op1.n
				op2.n += op1.n
				op1 = next(&ops1)
			}
			a1.Delete(m)
		case op1.isRetain() && op2.isDelete():
			var m int
			switch {
			case op1.n > -op2.n:
				m = -op2.n
				op1.n += op2.n
				op2 = next(&ops2)
			case op1.n == -op2.n:
				m = op1.n
				op1, op2 = next(&ops1), next(&ops2)
			default:
				m = op1.n
				op2.n += op1.n
				op1 = next(&ops1)
			}
			b1.Delete(m)
		}
	}
	return a1, b1, nil
}

// Compose returns an operation equivalent to applying a then b.
func Compose(a, b *Operation) (*Operation, error) {
	if a.TargetLen != b.BaseLen {
		return nil, errLength
	}
	c := &Operation{}
	ops1, ops2 := a.ops, b.ops
	next := func(ops *[]component) *component {
		if len(*ops) == 0 {
			return nil
		}
		c := (*ops)[0]
		*ops = (*ops)[1:]
		return &c
	}
	op1, op2 := next(&ops1), next(&ops2)
	for op1 != nil || op2 != nil {
		if op1 != nil && op1.isDelete() {
			c.Delete(-op1.n)
			op1 = next(&ops1)
			continue
		}
		if op2 != nil && op2.isInsert() {
			c.Insert(op2.s)
			op2 = next(&ops2)
			continue
		}
		if op1 == nil || op2 == nil {
			return nil, errLength
		}
		switch {
		case op1.isRetain() && op2.isRetain():
			switch {
			case op1.n > op2.n:
				c.Retain(op2.n)
				op1.n -= op2.n
				op2 = next(&ops2)
			case op1.n == op2.n:
				c.Retain(op1.n)
				op1, op2 = next(&ops1), next(&ops2)
			default:
				c.Retain(op1.n)
				op2.n -= op1.n
				op1 = next(&ops1)
			}
		case op1.isInsert() && op2.isDelete():
			l := utf8.RuneCountInString(op1.s)
			switch {
			case l > -op2.n:
				_, op1.s = cut(op1.s, -op2.n)
				op2 = next(&ops2)
			case l == -op2.n:
				op1, op2 = next(&ops1), next(&ops2)
			default:
				op2.n += l
				op1 = next(&ops1)
			}
		case op1.isInsert() && op2.isRetain():
			l := utf8.RuneCountInString(op1.s)
			switch {
			case l > op2.n:
				var head string
				head, op1.s = cut(op1.s, op2.n)
				c.Insert(head)
				op2 = next(&ops2)
			case l == op2.n:
				c.Insert(op1.s)
				op1, op2 = next(&ops1), next(&ops2)
			default:
				c.Insert(op1.s)
				op2.n -= l
				op1 = next(&ops1)
			}
		case op1.isRetain() && op2.isDelete():
			switch {
			case op1.n > -op2.n:
				c.Delete(-op2.n)
				op1.n += op2.n
				op2 = next(&ops2)
			case op1.n == -op2.n:
				c.Delete(-op2.n)
				op1, op2 = next(&ops1), next(&ops2)
			default:
				c.Delete(op1.n)
				op2.n += op1.n
				op1 = next(&ops1)
			}
		}
	}
	return c, nil
}

func (o *Operation) MarshalJSON() ([]byte, error) {
	a := make([]any, len(o.ops))
	for i, c := range o.ops {
		if c.isInsert() {
			a[i] = c.s
		} else {
			a[i] = c.n
		}
	}
	return json.Marshal(a)
}

func (o *Operation) UnmarshalJSON(b []byte) error {
	var a []json.RawMessage
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*o = Operation{}
	for _, raw := range a {
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			o.Insert(s)
			continue
		}
		var n int
		if err := json.Unmarshal(raw, &n); err != nil {
			return fmt.Errorf("ot: invalid component %s", raw)
		}
		if n > 0 {
			o.Retain(n)
		} else if n < 0 {
			o.Delete(-n)
		} else {
			return errors.New("ot: zero length component")
		}
	}
	return nil
}
