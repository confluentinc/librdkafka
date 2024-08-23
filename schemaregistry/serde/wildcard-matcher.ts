/**
 * Matches fully-qualified names that use dot (.) as the name boundary.
 *
 * <p>A '?' matches a single character.
 * A '*' matches one or more characters within a name boundary.
 * A '**' matches one or more characters across name boundaries.
 *
 * <p>Examples:
 * <pre>
 * wildcardMatch("eve", "eve*")                  --&gt; true
 * wildcardMatch("alice.bob.eve", "a*.bob.eve")  --&gt; true
 * wildcardMatch("alice.bob.eve", "a*.bob.e*")   --&gt; true
 * wildcardMatch("alice.bob.eve", "a*")          --&gt; false
 * wildcardMatch("alice.bob.eve", "a**")         --&gt; true
 * wildcardMatch("alice.bob.eve", "alice.bob*")  --&gt; false
 * wildcardMatch("alice.bob.eve", "alice.bob**") --&gt; true
 * </pre>
 *
 * @param str             the string to match on
 * @param wildcardMatcher the wildcard string to match against
 * @return true if the string matches the wildcard string
 */
export function match(str: string, wildcardMatcher: string): boolean {
  let re = wildcardToRegexp(wildcardMatcher, '.')
  let pattern: RegExp
  try {
    pattern = new RegExp(re)
  } catch (error) {
    return false
  }
  let match = str.match(pattern)
  return match != null && match[0] === str
}

function wildcardToRegexp(globExp: string, separator: string): string {
  let dst = ''
  let src = globExp.replaceAll('**'+separator+'*', '**')
  let i = 0;
  let size = src.length;
  while (i < size) {
    let c = src[i]
    i++
    switch (c) {
    case '*':
      // One char lookahead for **
      if (i < src.length && src[i] == '*') {
        dst += '.*'
        i++
      } else {
        dst += '[^' + separator + ']*'
      }
      break
    case '?':
      dst += '[^' + separator + ']'
      break
    case '.':
    case '+':
    case '{':
    case '}':
    case '(':
    case ')':
    case '|':
    case '^':
    case '$':
      // These need to be escaped in regular expressions
      dst += '\\' + c
      break
    case '\\':
      [dst, i] = doubleSlashes(dst, src, i)
      break
    default:
      dst += c
      break
    }
  }
  return dst
}

function doubleSlashes(dst: string, src: string, i: number): [string, number] {
  // Emit the next character without special interpretation
  dst += '\\'
  if (i+1 < src.length) {
    dst += '\\' + src[i]
    i++
  } else {
    // A backslash at the very end is treated like an escaped backslash
    dst += '\\'
  }
  return [dst, i]
}
