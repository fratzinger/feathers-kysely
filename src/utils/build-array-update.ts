import { sql } from 'kysely'
import { BadRequest } from '@feathersjs/errors'
import type { RawBuilder } from 'kysely'
import type { DialectType } from '../declarations.js'
import { isPostgresArrayType } from './is-postgres-array-type.js'

export type ArrayUpdateOperator = '$push' | '$pull' | '$addToSet'

/**
 * Build the SET expression for the array update operators `$push` / `$pull` /
 * `$addToSet`, choosing the SQL per **detected column storage**:
 *
 * - a native Postgres array column (`text[]`, `varchar(255)[]`, … — recognized
 *   via `isPostgresArrayType`) uses `array_append` / `array_remove` / `||`;
 * - a `json` / `jsonb` column uses the dialect's JSON functions.
 *
 * A scalar `value` appends/removes one element; an array `value` appends each
 * (`$push`), removes every listed element (`$pull`) or appends each one not
 * already present, once (`$addToSet`).
 *
 * `NULL` follows MongoDB's missing-field rule: `$push` / `$addToSet` start a
 * new array, `$pull` leaves the column `NULL`.
 *
 * Throws `BadRequest` when the storage can't be determined (no `x-db-type` /
 * `getPropertyType`), when a native-array op is used off Postgres, when `$pull`
 * targets a JSON column on MySQL/SQLite (not expressible in one statement
 * there), or when `$addToSet` targets a JSON column on MySQL.
 */
export function buildArrayUpdate(opts: {
  key: string
  operator: ArrayUpdateOperator
  value: unknown
  dialectType: DialectType | undefined
  columnType: string | undefined
}): RawBuilder<unknown> {
  const { key, operator, value, dialectType, columnType } = opts
  const ref = sql.ref(key)
  const items = Array.isArray(value) ? value : [value]

  // --- Native Postgres array column (text[], varchar(255)[], bigint[], …) ---
  if (isPostgresArrayType(columnType)) {
    if (dialectType !== 'postgres') {
      throw new BadRequest(
        `Native array operators require the postgres dialect (column '${key}' is '${columnType}')`,
      )
    }
    const arrType = columnType.trim()

    if (operator === '$push') {
      if (!Array.isArray(value)) {
        return sql`array_append(${ref}, ${value})`
      }
      // push each: concat a typed array literal
      return sql`${ref} || ARRAY[${sql.join(value)}]::${sql.raw(arrType)}`
    }

    if (operator === '$addToSet') {
      // append the input values not yet in the column — each once, in input
      // order; `is not distinct from` so a NULL element compares as equal
      return sql`${ref} || array(
        select u.x
        from unnest(ARRAY[${sql.join(items)}]::${sql.raw(arrType)}) with ordinality as u(x, i)
        where not exists (select 1 from unnest(${ref}) as e(x) where e.x is not distinct from u.x)
        group by u.x
        order by min(u.i)
      )`
    }

    // $pull: fold array_remove so every listed element is removed
    let expr: RawBuilder<unknown> = ref
    for (const item of items) {
      expr = sql`array_remove(${expr}, ${item})`
    }
    return expr
  }

  // --- JSON / JSONB column ---
  if (columnType === 'json' || columnType === 'jsonb') {
    switch (operator) {
      case '$push':
        return buildJsonPush(ref, items, columnType, dialectType)
      case '$pull':
        return buildJsonPull(ref, items, columnType, dialectType, key)
      case '$addToSet':
        return buildJsonAddToSet(ref, items, columnType, dialectType, key)
    }
  }

  throw new BadRequest(
    `Cannot determine array storage for column '${key}'. Annotate it with an 'x-db-type' (e.g. 'jsonb' or 'text[]') or provide a getPropertyType option to use '${operator}'.`,
  )
}

function buildJsonPush(
  ref: RawBuilder<unknown>,
  items: unknown[],
  columnType: 'json' | 'jsonb',
  dialectType: DialectType | undefined,
): RawBuilder<unknown> {
  // The JSON append functions of every dialect return NULL for a NULL input,
  // so start from an empty array instead.
  if (dialectType === 'postgres') {
    // Concat a JSON array literal (one element per item). Going through
    // JSON.stringify preserves each element's JS type, and computing in jsonb +
    // casting back keeps the assignment valid for a `json` column too.
    return sql`(coalesce(${ref}::jsonb, '[]'::jsonb) || ${JSON.stringify(items)}::jsonb)::${sql.raw(columnType)}`
  }

  if (dialectType === 'mysql') {
    let expr: RawBuilder<unknown> = sql`coalesce(${ref}, json_array())`
    for (const item of items) {
      expr = sql`json_array_append(${expr}, ${'$'}, ${item})`
    }
    return expr
  }

  // sqlite: json_insert appends with the '$[#]' end-of-array path; multiple
  // path/value pairs append several at once.
  const pairs = items.flatMap((item) => [sql`${'$[#]'}`, sql`${item}`])
  return sql`json_insert(coalesce(${ref}, '[]'), ${sql.join(pairs)})`
}

function buildJsonAddToSet(
  ref: RawBuilder<unknown>,
  items: unknown[],
  columnType: 'json' | 'jsonb',
  dialectType: DialectType | undefined,
  key: string,
): RawBuilder<unknown> {
  // Both variants append the input elements not yet in the column, each once,
  // in input order. JSON.stringify preserves each element's JS type.
  const input = JSON.stringify(items)

  if (dialectType === 'postgres') {
    return sql`(coalesce(${ref}::jsonb, '[]'::jsonb) || coalesce((
      select jsonb_agg(d.v order by d.i)
      from (
        select u.v, min(u.i) as i
        from jsonb_array_elements(${input}::jsonb) with ordinality as u(v, i)
        where not exists (select 1 from jsonb_array_elements(${ref}::jsonb) as e(v) where e.v = u.v)
        group by u.v
      ) as d
    ), '[]'::jsonb))::${sql.raw(columnType)}`
  }

  if (dialectType === 'mysql') {
    // JSON_ARRAYAGG has no ORDER BY, so the input order can't be kept
    throw new BadRequest(
      `'$addToSet' on a JSON column is not supported on the mysql dialect (column '${key}')`,
    )
  }

  // sqlite has no JSON array concat, so rebuild the array: the current
  // elements, then the new ones. Elements are compared by their minified JSON
  // text (`->`), which keeps `true` and `1` apart. Needs SQLite >= 3.44
  // (ORDER BY inside an aggregate).
  const current = sql`coalesce(${ref}, '[]')`
  return sql`(
    select json_group_array(json(a.j) order by a.src, a.k)
    from (
      select 0 as src, e.key as k, ${current} -> e.key as j
      from json_each(${current}) as e
      union all
      select 1, min(u.k), u.j
      from (select key as k, ${input} -> key as j from json_each(${input})) as u
      where u.j not in (select ${current} -> e.key from json_each(${current}) as e)
      group by u.j
    ) as a
  )`
}

function buildJsonPull(
  ref: RawBuilder<unknown>,
  items: unknown[],
  columnType: 'json' | 'jsonb',
  dialectType: DialectType | undefined,
  key: string,
): RawBuilder<unknown> {
  if (dialectType !== 'postgres') {
    throw new BadRequest(
      `'$pull' on a JSON column is only supported on the postgres dialect (column '${key}')`,
    )
  }

  const remove = sql`${JSON.stringify(items)}::jsonb`
  // rebuild the array keeping only elements not present in the remove-set;
  // the CASE keeps a NULL column NULL instead of turning it into `[]`
  return sql`case when ${ref} is null then null else (
    select coalesce(jsonb_agg(t.v), '[]'::jsonb)
    from jsonb_array_elements(${ref}::jsonb) as t(v)
    where t.v <> all(array(select r.v from jsonb_array_elements(${remove}) as r(v)))
  )::${sql.raw(columnType)} end`
}

if (import.meta.vitest) {
  const { describe, it, expect } = import.meta.vitest
  const {
    Kysely,
    DummyDriver,
    PostgresAdapter,
    PostgresIntrospector,
    PostgresQueryCompiler,
    MysqlAdapter,
    MysqlIntrospector,
    MysqlQueryCompiler,
    SqliteAdapter,
    SqliteIntrospector,
    SqliteQueryCompiler,
  } = await import('kysely')

  const mk = (Adapter: any, Introspector: any, Compiler: any) =>
    new Kysely<any>({
      dialect: {
        createAdapter: () => new Adapter(),
        createDriver: () => new DummyDriver(),
        createIntrospector: (db: any) => new Introspector(db),
        createQueryCompiler: () => new Compiler(),
      },
    })
  const pg = mk(PostgresAdapter, PostgresIntrospector, PostgresQueryCompiler)
  const mysql = mk(MysqlAdapter, MysqlIntrospector, MysqlQueryCompiler)
  const sqlite = mk(SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler)

  const compile = (db: any, opts: any) => buildArrayUpdate(opts).compile(db)

  describe('buildArrayUpdate', () => {
    describe('native Postgres array', () => {
      it('$push scalar -> array_append', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$push',
          value: 'x',
          dialectType: 'postgres',
          columnType: 'text[]',
        })
        expect(text).toBe('array_append("tags", $1)')
        expect(parameters).toEqual(['x'])
      })

      it('$push array -> typed concat (push each)', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$push',
          value: ['a', 'b'],
          dialectType: 'postgres',
          columnType: 'varchar(255)[]',
        })
        expect(text).toBe('"tags" || ARRAY[$1, $2]::varchar(255)[]')
        expect(parameters).toEqual(['a', 'b'])
      })

      it('$pull scalar -> array_remove', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$pull',
          value: 'x',
          dialectType: 'postgres',
          columnType: 'text[]',
        })
        expect(text).toBe('array_remove("tags", $1)')
        expect(parameters).toEqual(['x'])
      })

      it('$pull array -> folded array_remove', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$pull',
          value: ['a', 'b'],
          dialectType: 'postgres',
          columnType: 'text[]',
        })
        expect(text).toBe('array_remove(array_remove("tags", $1), $2)')
        expect(parameters).toEqual(['a', 'b'])
      })

      it('$addToSet -> concat of the missing input values, in input order', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$addToSet',
          value: ['a', 'b'],
          dialectType: 'postgres',
          columnType: 'varchar(255)[]',
        })
        expect(text.startsWith('"tags" || array(')).toBe(true)
        expect(text).toContain('unnest(ARRAY[$1, $2]::varchar(255)[])')
        expect(text).toContain('e.x is not distinct from u.x')
        expect(parameters).toEqual(['a', 'b'])
      })

      it('rejects a native-array op off Postgres', () => {
        expect(() =>
          compile(sqlite, {
            key: 'tags',
            operator: '$push',
            value: 'x',
            dialectType: 'sqlite',
            columnType: 'text[]',
          }),
        ).toThrow('require the postgres dialect')
      })
    })

    describe('jsonb (Postgres)', () => {
      it('$push scalar -> jsonb concat', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$push',
          value: 'x',
          dialectType: 'postgres',
          columnType: 'jsonb',
        })
        expect(text).toBe(
          '(coalesce("tags"::jsonb, \'[]\'::jsonb) || $1::jsonb)::jsonb',
        )
        expect(parameters).toEqual(['["x"]'])
      })

      it('$push array -> jsonb array concat', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$push',
          value: ['a', 'b'],
          dialectType: 'postgres',
          columnType: 'json',
        })
        expect(text).toBe(
          '(coalesce("tags"::jsonb, \'[]\'::jsonb) || $1::jsonb)::json',
        )
        expect(parameters).toEqual(['["a","b"]'])
      })

      it('$pull -> jsonb_agg filter, cast back to the column type', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$pull',
          value: 'x',
          dialectType: 'postgres',
          columnType: 'jsonb',
        })
        expect(
          text.startsWith('case when "tags" is null then null else ('),
        ).toBe(true)
        expect(text).toContain('jsonb_agg(t.v)')
        expect(text).toContain('jsonb_array_elements("tags"::jsonb) as t(v)')
        expect(text).toContain(
          '<> all(array(select r.v from jsonb_array_elements($1::jsonb) as r(v)))',
        )
        expect(text.trimEnd().endsWith(')::jsonb end')).toBe(true)
        expect(parameters).toEqual(['["x"]'])
      })
    })

    describe('$addToSet on JSON', () => {
      it('Postgres -> jsonb concat of the missing input values', () => {
        const { sql: text, parameters } = compile(pg, {
          key: 'tags',
          operator: '$addToSet',
          value: 'x',
          dialectType: 'postgres',
          columnType: 'json',
        })
        expect(
          text.startsWith('(coalesce("tags"::jsonb, \'[]\'::jsonb) || '),
        ).toBe(true)
        expect(text).toContain('jsonb_array_elements($1::jsonb)')
        expect(text.trimEnd().endsWith(')::json')).toBe(true)
        expect(parameters).toEqual(['["x"]'])
      })

      it('SQLite -> rebuilt array via json_each / json_group_array', () => {
        const { sql: text, parameters } = compile(sqlite, {
          key: 'tags',
          operator: '$addToSet',
          value: ['a', { k: 1 }],
          dialectType: 'sqlite',
          columnType: 'json',
        })
        expect(text).toContain('json_group_array(')
        expect(text).toContain(`json_each(coalesce("tags", '[]'))`)
        expect(parameters).toEqual(['["a",{"k":1}]', '["a",{"k":1}]'])
      })

      it('rejects $addToSet on a JSON column on MySQL', () => {
        expect(() =>
          compile(mysql, {
            key: 'tags',
            operator: '$addToSet',
            value: 'x',
            dialectType: 'mysql',
            columnType: 'json',
          }),
        ).toThrow('not supported on the mysql dialect')
      })
    })

    describe('JSON on MySQL / SQLite', () => {
      it('MySQL $push scalar -> json_array_append', () => {
        const { sql: text, parameters } = compile(mysql, {
          key: 'tags',
          operator: '$push',
          value: 'x',
          dialectType: 'mysql',
          columnType: 'json',
        })
        expect(text).toBe(
          'json_array_append(coalesce(`tags`, json_array()), ?, ?)',
        )
        expect(parameters).toEqual(['$', 'x'])
      })

      it('MySQL $push array -> chained json_array_append', () => {
        const { sql: text, parameters } = compile(mysql, {
          key: 'tags',
          operator: '$push',
          value: ['a', 'b'],
          dialectType: 'mysql',
          columnType: 'json',
        })
        expect(text).toBe(
          'json_array_append(json_array_append(coalesce(`tags`, json_array()), ?, ?), ?, ?)',
        )
        expect(parameters).toEqual(['$', 'a', '$', 'b'])
      })

      it('SQLite $push scalar -> json_insert', () => {
        const { sql: text, parameters } = compile(sqlite, {
          key: 'tags',
          operator: '$push',
          value: 'x',
          dialectType: 'sqlite',
          columnType: 'json',
        })
        expect(text).toBe('json_insert(coalesce("tags", \'[]\'), ?, ?)')
        expect(parameters).toEqual(['$[#]', 'x'])
      })

      it('SQLite $push array -> json_insert with multiple pairs', () => {
        const { sql: text, parameters } = compile(sqlite, {
          key: 'tags',
          operator: '$push',
          value: ['a', 'b'],
          dialectType: 'sqlite',
          columnType: 'json',
        })
        expect(text).toBe('json_insert(coalesce("tags", \'[]\'), ?, ?, ?, ?)')
        expect(parameters).toEqual(['$[#]', 'a', '$[#]', 'b'])
      })

      it('rejects $pull on a JSON column off Postgres', () => {
        for (const [db, dialectType] of [
          [mysql, 'mysql'],
          [sqlite, 'sqlite'],
        ] as const) {
          expect(() =>
            compile(db, {
              key: 'tags',
              operator: '$pull',
              value: 'x',
              dialectType,
              columnType: 'json',
            }),
          ).toThrow('only supported on the postgres dialect')
        }
      })
    })

    it('throws when the column storage cannot be determined', () => {
      expect(() =>
        compile(pg, {
          key: 'tags',
          operator: '$push',
          value: 'x',
          dialectType: 'postgres',
          columnType: undefined,
        }),
      ).toThrow("Cannot determine array storage for column 'tags'")
    })
  })
}
