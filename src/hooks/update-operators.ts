import { _ } from '@feathersjs/commons'
import { BadRequest } from '@feathersjs/errors'
import type { HookContext, NextFunction } from '@feathersjs/feathers'
import type { DialectType } from '../declarations.js'
import { applyUpdateOperators, containsUpdateOperator } from '../utils/index.js'

// The slice of the Kysely service the hook reads to drive $push/$pull.
interface KyselyUpdateService {
  options?: { dialectType?: DialectType }
  getPropertyType?: (column: string) => string | undefined
}

/**
 * Hook that rewrites MongoDB-style update operators in `context.data` into
 * atomic Kysely SET expressions:
 *
 * - `$inc` / `$mul` — add / multiply, e.g. `{ $inc: { views: 1 } }`;
 * - `$min` / `$max` — clamp to the smaller / larger value;
 * - `$push` / `$pull` — append to / remove from an array column. The SQL is
 *   chosen per column from its detected storage (native Postgres array vs.
 *   `json`/`jsonb`) and the dialect, via the service's `getPropertyType`.
 *
 * Usable as either a `before` hook or an `around` hook — `next` is optional, and
 * when present (around) it is awaited after the data has been transformed:
 *
 *   service.hooks({ before: { patch: [updateOperators()] } })
 *   // or
 *   service.hooks({ around: { patch: [updateOperators()] } })
 *
 * Operators are only meaningful for `patch` (a partial update); `update`
 * replaces the whole record, so operators there are rejected with a
 * `BadRequest`. Register on both methods to surface that error clearly.
 *
 * Invalid payloads (non-object operator, non-finite number, undefined array
 * value, undetectable array column) throw `BadRequest`.
 */
export const updateOperators =
  () =>
  async (context: HookContext, next?: NextFunction): Promise<HookContext> => {
    const { method, data } = context

    if (data && !Array.isArray(data) && _.isObject(data)) {
      if (method === 'patch') {
        const service = context.service as unknown as KyselyUpdateService
        context.data = applyUpdateOperators(data as Record<string, any>, {
          dialectType: service.options?.dialectType,
          getColumnType: service.getPropertyType?.bind(service),
        })
      } else if (
        method === 'update' &&
        containsUpdateOperator(data as Record<string, any>)
      ) {
        throw new BadRequest(
          'Update operators ($inc/$mul/$min/$max/$push/$pull) are only supported on patch, not update',
        )
      }
    }

    await next?.()

    return context
  }
