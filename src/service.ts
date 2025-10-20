import type { PaginationOptions } from '@feathersjs/adapter-commons'
import { MethodNotAllowed } from '@feathersjs/errors'
import type {
  Paginated,
  ServiceMethods,
  Id,
  NullableId,
  Params,
} from '@feathersjs/feathers'
import { KyselyAdapter } from './adapter.js'
import type { KyselyAdapterParams, UpsertOptions } from './declarations.js'

export class KyselyService<
    Result extends Record<string, any> = Record<string, any>,
    Data = Partial<Result>,
    ServiceParams extends Params<any> = KyselyAdapterParams,
    PatchData = Partial<Data>,
  >
  extends KyselyAdapter<Result, Data, ServiceParams, PatchData>
  implements
    ServiceMethods<Result | Paginated<Result>, Data, ServiceParams, PatchData>
{
  async find(
    params?: ServiceParams & { paginate?: PaginationOptions },
  ): Promise<Paginated<Result>>
  async find(params?: ServiceParams & { paginate: false }): Promise<Result[]>
  async find(params?: ServiceParams): Promise<Paginated<Result> | Result[]>
  async find(params?: ServiceParams): Promise<Paginated<Result> | Result[]> {
    return this._find({
      ...params,
      query: await this.sanitizeQuery(params),
    } as any)
  }

  async get(id: Id, params?: ServiceParams): Promise<Result> {
    return this._get(id, {
      ...params,
      query: await this.sanitizeQuery(params),
    } as any)
  }

  async create(data: Data, params?: ServiceParams): Promise<Result>
  async create(data: Data[], params?: ServiceParams): Promise<Result[]>
  async create(
    data: Data | Data[],
    params?: ServiceParams,
  ): Promise<Result | Result[]>
  async create(
    data: Data | Data[],
    params?: ServiceParams,
  ): Promise<Result | Result[]> {
    if (Array.isArray(data) && !this.allowsMulti('create', params)) {
      throw new MethodNotAllowed('Can not create multiple entries')
    }

    return this._create(data, params)
  }

  async upsert(
    data: Data,
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result>
  async upsert(
    data: Data[],
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result[]>
  async upsert(
    data: Data | Data[],
    params: ServiceParams & UpsertOptions<Result>,
  ): Promise<Result | Result[]> {
    if (Array.isArray(data) && !this.allowsMulti('create', params)) {
      throw new MethodNotAllowed('Can not upsert multiple entries')
    }

    return this._upsert(data, params)
  }

  async update(id: Id, data: Data, params?: ServiceParams): Promise<Result> {
    return this._update(id, data, {
      ...params,
      query: await this.sanitizeQuery(params),
    } as any)
  }

  async patch(id: Id, data: PatchData, params?: ServiceParams): Promise<Result>
  async patch(
    id: null,
    data: PatchData,
    params?: ServiceParams,
  ): Promise<Result[]>
  async patch(
    id: NullableId,
    data: PatchData,
    params?: ServiceParams,
  ): Promise<Result | Result[]>
  async patch(
    id: NullableId,
    data: PatchData,
    params?: ServiceParams,
  ): Promise<Result | Result[]> {
    const { $limit, ...query } = await this.sanitizeQuery(params)

    return this._patch(id, data, {
      ...params,
      query,
    } as any)
  }

  async remove(id: Id, params?: ServiceParams): Promise<Result>
  async remove(id: null, params?: ServiceParams): Promise<Result[]>
  async remove(
    id: NullableId,
    params?: ServiceParams,
  ): Promise<Result | Result[]>
  async remove(
    id: NullableId,
    params?: ServiceParams,
  ): Promise<Result | Result[]> {
    const { $limit, ...query } = await this.sanitizeQuery(params)

    return this._remove(id, {
      ...params,
      query,
    } as any)
  }
}
