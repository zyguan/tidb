package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	errBadQuote = errors.New("bad quote")
	errBadUTF8  = errors.New("bad UTF-8")
)

func quote(s string) string {
	var buf strings.Builder
	buf.Grow(3 * len(s) / 2)
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case '"':
			buf.WriteString(`\"`)
		case '\\':
			buf.WriteString(`\\`)
		default:
			if isPrint := c >= 0x20 && c < 0x7f; isPrint {
				buf.WriteByte(c)
			} else {
				buf.WriteString(fmt.Sprintf(`\%03o`, c))
			}
		}
	}
	buf.WriteByte('"')
	return buf.String()
}

func unquote(s string) (string, error) {
	if len(s) == 0 {
		return "", nil
	}

	// Remove quotes.
	if s[0] == '"' || s[0] == '\'' {
		if len(s) == 1 || s[len(s)-1] != s[0] {
			return "", errBadQuote
		}
		s = s[1 : len(s)-1]
	}

	// Avoid allocation in trivial cases.
	simple := true
	for _, r := range s {
		if r == '\\' || r == '"' || r == '\'' {
			simple = false
			break
		}
	}
	if simple {
		return s, nil
	}

	// This is based on C++'s tokenizer.cc.
	// Despite its name, this is *not* parsing C syntax.
	// For instance, "\0" is an invalid quoted string.
	var buf strings.Builder
	buf.Grow(3 * len(s) / 2)
	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && n == 1 {
			return "", errBadUTF8
		}
		s = s[n:]
		if r != '\\' {
			if r < utf8.RuneSelf {
				buf.WriteByte(byte(r))
			} else {
				buf.WriteString(string(r))
			}
			continue
		}

		ch, tail, err := unescape(s)
		if err != nil {
			return "", err
		}
		buf.WriteString(ch)
		s = tail
	}
	return buf.String(), nil
}

func unescape(s string) (ch string, tail string, err error) {
	r, n := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError && n == 1 {
		return "", "", errBadUTF8
	}
	s = s[n:]
	switch r {
	case 'a':
		return "\a", s, nil
	case 'b':
		return "\b", s, nil
	case 'f':
		return "\f", s, nil
	case 'n':
		return "\n", s, nil
	case 'r':
		return "\r", s, nil
	case 't':
		return "\t", s, nil
	case 'v':
		return "\v", s, nil
	case '?':
		return "?", s, nil // trigraph workaround
	case '\'', '"', '\\':
		return string(r), s, nil
	case '0', '1', '2', '3', '4', '5', '6', '7':
		if len(s) < 2 {
			return "", "", fmt.Errorf(`\%c requires 2 following digits`, r)
		}
		ss := string(r) + s[:2]
		s = s[2:]
		i, err := strconv.ParseUint(ss, 8, 8)
		if err != nil {
			return "", "", fmt.Errorf(`\%s contains non-octal digits`, ss)
		}
		return string([]byte{byte(i)}), s, nil
	case 'x', 'X', 'u', 'U':
		var n int
		switch r {
		case 'x', 'X':
			n = 2
		case 'u':
			n = 4
		case 'U':
			n = 8
		}
		if len(s) < n {
			return "", "", fmt.Errorf(`\%c requires %d following digits`, r, n)
		}
		ss := s[:n]
		s = s[n:]
		i, err := strconv.ParseUint(ss, 16, 64)
		if err != nil {
			return "", "", fmt.Errorf(`\%c%s contains non-hexadecimal digits`, r, ss)
		}
		if r == 'x' || r == 'X' {
			return string([]byte{byte(i)}), s, nil
		}
		if i > utf8.MaxRune {
			return "", "", fmt.Errorf(`\%c%s is not a valid Unicode code point`, r, ss)
		}
		return string(rune(i)), s, nil
	}
	return "", "", fmt.Errorf(`unknown escape \%c`, r)
}
