import {
  Constants,
  ServiceResponse,
  TableBatch,
  TableQuery,
  TableService,
  TableUtilities,
} from "azure-storage";
import * as e from "fp-ts/lib/Either";
import * as o from "fp-ts/lib/Option";

import { TaskEither, taskify } from "fp-ts/lib/TaskEither";
import { NonEmptyString } from "@pagopa/ts-commons/lib/strings";
import { Either } from "fp-ts/lib/Either";

const Operations = Constants.TableConstants.Operations;

/**
 */
export const getOperationBulkFiscalCodes =
  (tableService: TableService, tableName: NonEmptyString, op: string) =>
  (
    fiscalCodes: ReadonlyArray<string>
  ): TaskEither<Error, ReadonlyArray<TableService.BatchResult>> => {
    const eg = TableUtilities.entityGenerator;

    const tableBatch = new TableBatch();

    fiscalCodes
      .map((fc) => ({
        PartitionKey: eg.String("1"),
        RowKey: eg.String(fc),
      }))
      .forEach((entity) => tableBatch.addOperation(op, entity));

    return taskify<Error, ReadonlyArray<TableService.BatchResult>>((cb) =>
      tableService.executeBatch(tableName, tableBatch, cb)
    )();
  };

export const getInsertBulkFiscalCodes = (
  tableService: TableService,
  tableName: NonEmptyString
): ReturnType<typeof getOperationBulkFiscalCodes> =>
  getOperationBulkFiscalCodes(tableService, tableName, Operations.INSERT);

export const getDeleteBulkFiscalCodes = (
  tableService: TableService,
  tableName: NonEmptyString
): ReturnType<typeof getOperationBulkFiscalCodes> =>
  getOperationBulkFiscalCodes(tableService, tableName, Operations.DELETE);

/**
 * A minimal Youth Card storage table Entry
 */
export type TableEntry = Readonly<{
  readonly RowKey: Readonly<{
    readonly _: string;
  }>;
}>;

/**
 * A function that returns a page of query results given a pagination token
 *
 * @see https://docs.microsoft.com/en-us/rest/api/storageservices/query-timeout-and-pagination
 */
export type PagedQuery = (
  currentToken: TableService.TableContinuationToken
) => Promise<Either<Error, TableService.QueryEntitiesResult<TableEntry>>>;

/**
 * Returns a paged query function for a certain query on a storage table
 */
export const getPagedQuery =
  (tableService: TableService, table: string) =>
  (tableQuery: TableQuery): PagedQuery =>
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
  (currentToken) =>
    new Promise((resolve) =>
      tableService.queryEntities(
        table,
        tableQuery,
        currentToken,
        (
          error: Error,
          result: TableService.QueryEntitiesResult<TableEntry>,
          response: ServiceResponse
        ) => resolve(response.isSuccessful ? e.right(result) : e.left(error))
      )
    );

/**
 * Iterates over all pages of entries returned by the provided paged query
 * function.
 *
 * @throws Exception on query failure
 */
export async function* iterateOnPages(
  pagedQuery: PagedQuery
): AsyncIterableIterator<ReadonlyArray<TableEntry>> {
  // eslint-disable-next-line functional/no-let
  let token = undefined as unknown as TableService.TableContinuationToken;
  do {
    // query for a page of entries
    const errorOrResults = await pagedQuery(token);
    if (e.isLeft(errorOrResults)) {
      // throw an exception in case of error
      throw errorOrResults.value;
    }
    // call the async callback with the current page of entries
    const results = errorOrResults.value;
    yield results.entries;
    // update the continuation token, the loop will continue until
    // the token is defined
    token = o
      .fromNullable(results.continuationToken)
      .getOrElse(undefined as unknown as TableService.TableContinuationToken);
  } while (token !== undefined && token !== null);
}
