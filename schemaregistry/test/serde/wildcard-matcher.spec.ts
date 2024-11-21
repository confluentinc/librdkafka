import { match } from '../../serde/wildcard-matcher';
import { describe, expect, it } from '@jest/globals';

describe('WildcardMatcher', () => {
  it('when match', () => {
    expect(match('', 'Foo')).toBe(false)
  })
  it('when match', () => {
    expect(match('Foo', '')).toBe(false)
  })
  it('when match', () => {
    expect(match('', '')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo', 'Foo')).toBe(true)
  })
  it('when match', () => {
    expect(match('', '*')).toBe(true)
  })
  it('when match', () => {
    expect(match('', '?')).toBe(false)
  })
  it('when match', () => {
    expect(match('Foo', 'Fo*')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo', 'Fo?')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo Bar and Catflag', 'Fo*')).toBe(true)
  })
  it('when match', () => {
    expect(match('New Bookmarks', 'N?w ?o?k??r?s')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo', 'Bar')).toBe(false)
  })
  it('when match', () => {
    expect(match('Foo Bar Foo', 'F*o Bar*')).toBe(true)
  })
  it('when match', () => {
    expect(match('Adobe Acrobat Installer', 'Ad*er')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo', '*Foo')).toBe(true)
  })
  it('when match', () => {
    expect(match('BarFoo', '*Foo')).toBe(true)
  })
  it('when match', () => {
    expect(match('Foo', 'Foo*')).toBe(true)
  })
  it('when match', () => {
    expect(match('FOO', '*Foo')).toBe(false)
  })
  it('when match', () => {
    expect(match('BARFOO', '*Foo')).toBe(false)
  })
  it('when match', () => {
    expect(match('FOO', 'Foo*')).toBe(false)
  })
  it('when match', () => {
    expect(match('FOOBAR', 'Foo*')).toBe(false)
  })
  it('when match', () => {
    expect(match('eve', 'eve*')).toBe(true)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'a*.bob.eve')).toBe(true)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'a*.bob.e*')).toBe(true)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'a*')).toBe(false)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'a**')).toBe(true)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'alice.bob*')).toBe(false)
  })
  it('when match', () => {
    expect(match('alice.bob.eve', 'alice.bob**')).toBe(true)
  })
})
