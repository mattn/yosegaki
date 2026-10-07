package ot

import (
	"encoding/json"
	"math/rand"
	"testing"
	"unicode/utf8"
)

var alphabet = []rune("abc\nあい😀")

func randomString(r *rand.Rand, n int) string {
	s := make([]rune, n)
	for i := range s {
		s[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(s)
}

func randomOperation(r *rand.Rand, doc string) *Operation {
	o := &Operation{}
	left := utf8.RuneCountInString(doc)
	for left > 0 {
		n := 1 + r.Intn(left)
		switch r.Intn(3) {
		case 0:
			o.Retain(n)
			left -= n
		case 1:
			o.Delete(n)
			left -= n
		default:
			o.Insert(randomString(r, 1+r.Intn(4)))
		}
	}
	if r.Intn(2) == 0 {
		o.Insert(randomString(r, 1+r.Intn(4)))
	}
	return o
}

func TestTransform(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for range 2000 {
		doc := randomString(r, r.Intn(20))
		a, b := randomOperation(r, doc), randomOperation(r, doc)
		a1, b1, err := Transform(a, b)
		if err != nil {
			t.Fatal(err)
		}
		sa, _ := a.Apply(doc)
		sb, _ := b.Apply(doc)
		x, err := b1.Apply(sa)
		if err != nil {
			t.Fatal(err)
		}
		y, err := a1.Apply(sb)
		if err != nil {
			t.Fatal(err)
		}
		if x != y {
			t.Fatalf("diverged: %q vs %q", x, y)
		}
	}
}

func TestCompose(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for range 2000 {
		doc := randomString(r, r.Intn(20))
		a := randomOperation(r, doc)
		mid, _ := a.Apply(doc)
		b := randomOperation(r, mid)
		c, err := Compose(a, b)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := b.Apply(mid)
		got, err := c.Apply(doc)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestJSON(t *testing.T) {
	var o Operation
	if err := json.Unmarshal([]byte(`[3,"あ",-2,1]`), &o); err != nil {
		t.Fatal(err)
	}
	got, err := o.Apply("abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if got != "abcあf" {
		t.Fatalf("got %q", got)
	}
	b, _ := json.Marshal(&o)
	if string(b) != `[3,"あ",-2,1]` {
		t.Fatalf("got %s", b)
	}
}

func TestTransformIndex(t *testing.T) {
	o := (&Operation{}).Retain(2).Insert("xy").Delete(3).Retain(5)
	for _, tt := range []struct{ in, want int }{{0, 0}, {2, 4}, {3, 4}, {5, 4}, {7, 6}} {
		if got := o.TransformIndex(tt.in); got != tt.want {
			t.Errorf("TransformIndex(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
