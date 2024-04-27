package aggd

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var wildcardComponent = []byte{'*'}

type Path [][]byte

var InvalidPathError = errors.New("invalid path")

func isInvalidComponentCharacter(r rune) bool {
	return !unicode.IsLetter(r) &&
		!unicode.IsNumber(r) &&
		r != '-' &&
		r != '_'
}

func ParsePath(s string) (Path, error) {
	return parsePath(s, false)
}

func ParsePathPattern(s string) (Path, error) {
	return parsePath(s, true)
}

func parsePath(s string, allowWildcard bool) (Path, error) {
	toks := strings.Split(s, ".")

	p := make([][]byte, 0, len(toks))

	for _, t := range toks {
		c, err := parseComponent(t, allowWildcard)
		if err != nil {
			return nil, fmt.Errorf("could not parse component `%s`: %w", c, err)
		}

		p = append(p, c)
	}

	return p, nil
}

func parseComponent(c string, allowWildcard bool) ([]byte, error) {
	if len(c) == 0 {
		return []byte{}, fmt.Errorf("%w: %s", InvalidPathError, "empty component")
	}

	if len(c) > 32 {
		return []byte{}, fmt.Errorf("%w: %s", InvalidPathError, "component too long")
	}

	if c == "*" {
		if !allowWildcard {
			return []byte{}, fmt.Errorf("%w: %s", InvalidPathError, "wildcard component not allowed here")
		} else {
			return []byte{'*'}, nil
		}
	}

	if strings.ContainsFunc(c, isInvalidComponentCharacter) {
		return []byte{}, fmt.Errorf("%w: %s", InvalidPathError, "component contains invalid characters")
	}

	return []byte(c), nil
}

func (p Path) String() string {
	return string(bytes.Join([][]byte(p), []byte{'.'}))
}

func (p Path) Equal(other Path) bool {
	if len(p) != len(other) {
		return false
	}

	for i := range p {
		if !bytes.Equal(p[i], other[i]) {
			return false
		}
	}

	return true
}

func (p Path) MatchesPattern(pat Path) bool {
	if len(p) != len(pat) {
		return false
	}

	for i := range p {
		if !bytes.Equal(p[i], pat[i]) && !bytes.Equal(pat[i], wildcardComponent) {
			return false
		}
	}

	return true
}
