import type { OrderByItemBuilder } from 'kysely'
import type { SortProperty } from '../declarations.js'
import { getSortDirection } from './get-sort-direction.js'

export function getOrderByModifier(
  order: SortProperty,
): (ob: OrderByItemBuilder) => OrderByItemBuilder {
  const dir = getSortDirection(order)
  if (dir === 1 || dir === '-1' || dir === 'asc') {
    return (ob: OrderByItemBuilder) => ob.asc()
  }
  if (dir === -1 || dir === '1' || dir === 'desc') {
    return (ob: OrderByItemBuilder) => ob.desc()
  }
  if (dir === 'asc nulls first') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsFirst()
  }
  if (dir === 'asc nulls last') {
    return (ob: OrderByItemBuilder) => ob.asc().nullsLast()
  }
  if (dir === 'desc nulls first') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsFirst()
  }
  if (dir === 'desc nulls last') {
    return (ob: OrderByItemBuilder) => ob.desc().nullsLast()
  }
  return (ob: OrderByItemBuilder) => ob.asc()
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest

  // A chainable recorder standing in for kysely's OrderByItemBuilder.
  const record = (order: SortProperty) => {
    const calls: string[] = []
    const ob: any = {
      asc: () => (calls.push('asc'), ob),
      desc: () => (calls.push('desc'), ob),
      nullsFirst: () => (calls.push('nullsFirst'), ob),
      nullsLast: () => (calls.push('nullsLast'), ob),
    }
    getOrderByModifier(order)(ob as OrderByItemBuilder)
    return calls
  }

  describe('getOrderByModifier', () => {
    it('maps numeric and "asc" directions to ascending', () => {
      expect(record(1)).toEqual(['asc'])
      expect(record('asc')).toEqual(['asc'])
    })

    it('maps numeric -1 and "desc" to descending', () => {
      expect(record(-1)).toEqual(['desc'])
      expect(record('desc')).toEqual(['desc'])
    })

    it('treats the stringified directions inversely ("1"=desc, "-1"=asc)', () => {
      expect(record('1')).toEqual(['desc'])
      expect(record('-1')).toEqual(['asc'])
    })

    it('applies nulls ordering modifiers', () => {
      expect(record('asc nulls first')).toEqual(['asc', 'nullsFirst'])
      expect(record('asc nulls last')).toEqual(['asc', 'nullsLast'])
      expect(record('desc nulls first')).toEqual(['desc', 'nullsFirst'])
      expect(record('desc nulls last')).toEqual(['desc', 'nullsLast'])
    })

    it('reads the direction out of the object form', () => {
      expect(record({ direction: 'desc' })).toEqual(['desc'])
    })

    it('feeds the object form through the full direction mapping', () => {
      expect(record({ direction: 1 })).toEqual(['asc'])
      expect(record({ direction: 'asc nulls last' })).toEqual([
        'asc',
        'nullsLast',
      ])
    })

    it('defaults to ascending for an unrecognized direction', () => {
      expect(record('bogus' as unknown as SortProperty)).toEqual(['asc'])
    })
  })
}
