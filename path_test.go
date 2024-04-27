package aggd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathString(t *testing.T) {
	p := Path{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}

	assert.Equal(t, "foo.bar.baz", p.String())
}

func TestParsePath(t *testing.T) {
	tt := []struct {
		input    string
		expected Path
		error    bool
	}{
		{
			input: "foo.bar.baz",
			expected: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			error: false,
		},
		{
			input: "UpperCase.and12345.and-some_break-characters",
			expected: Path{
				[]byte("UpperCase"),
				[]byte("and12345"),
				[]byte("and-some_break-characters"),
			},
			error: false,
		},
		{
			input:    "paths.have no.whitespace",
			expected: nil,
			error:    true,
		},
		{
			input:    "paths.with..empty.components",
			expected: nil,
			error:    true,
		},
		{
			input:    "paths.with.*.wildcards",
			expected: nil,
			error:    true,
		},
		{
			input:    "paths.with.$pecial.characters",
			expected: nil,
			error:    true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParsePath(tc.input)

			assert.Equal(t, tc.expected, got)

			if tc.error {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestParsePathPattern(t *testing.T) {
	tt := []struct {
		input    string
		expected Path
		error    bool
	}{
		{
			input: "foo.bar.baz",
			expected: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			error: false,
		},
		{
			input: "UpperCase.and12345.and-some_break-characters",
			expected: Path{
				[]byte("UpperCase"),
				[]byte("and12345"),
				[]byte("and-some_break-characters"),
			},
			error: false,
		},
		{
			input:    "paths.have no.whitespace",
			expected: nil,
			error:    true,
		},
		{
			input:    "paths.with..empty.components",
			expected: nil,
			error:    true,
		},
		{
			input: "paths.with.*.wildcards",
			expected: Path{
				[]byte("paths"),
				[]byte("with"),
				[]byte("*"),
				[]byte("wildcards"),
			},
			error: false,
		},
		{
			input:    "paths.with.$pecial.characters",
			expected: nil,
			error:    true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParsePathPattern(tc.input)

			assert.Equal(t, tc.expected, got)

			if tc.error {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPathEqual(t *testing.T) {
	tt := []struct {
		name    string
		lhs     Path
		rhs     Path
		equal   bool
		matches bool
	}{
		{
			name: "basic equality",
			lhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			rhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			equal:   true,
			matches: true,
		},
		{
			name: "wildcard equality",
			lhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			rhs: Path{
				[]byte("foo"),
				[]byte("*"),
				[]byte("baz"),
			},
			equal:   false,
			matches: true,
		},
		{
			name: "lhs longer than rhs",
			lhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
				[]byte("qux"),
			},
			rhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			equal:   false,
			matches: false,
		},
		{
			name: "lhs shorter than rhs",
			lhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
			},
			rhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
				[]byte("qux"),
			},
			equal:   false,
			matches: false,
		},
		{
			// todo: support this?
			name: "rhs trailing glob",
			lhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("baz"),
				[]byte("qux"),
			},
			rhs: Path{
				[]byte("foo"),
				[]byte("bar"),
				[]byte("*"),
			},
			equal:   false,
			matches: false,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.equal, tc.lhs.Equal(tc.rhs))
			assert.Equal(t, tc.matches, tc.lhs.MatchesPattern(tc.rhs))
		})
	}
}
