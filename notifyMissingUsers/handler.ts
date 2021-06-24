/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { createWriteStream } from "fs";
import {
  ErrorOrResult,
  QueueService,
  ServiceResponse,
  TableQuery,
  TableService,
} from "azure-storage";
import { Container } from "@azure/cosmos";

import { toError } from "fp-ts/lib/Either";
import * as te from "fp-ts/lib/TaskEither";
import {
  getInsertBulkFiscalCodes,
  getPagedQuery,
  iterateOnPages,
  PagedQuery,
  TableEntry,
} from "../utils/table_storage";
import { IConfig } from "../utils/config";
import { getFiscalCodes, getFiscalCodesWithAMessage } from "../utils/cosmosdb";
import { createQueryForAllProfiles } from ".";

const fiscaCodeLowerBound = "A".repeat(16);
const fiscaCodeUpperBound = "Z".repeat(16);

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const createLogger = () => {
  const logFileName = `${process.cwd()}/log.${Date.now()}.csv`;
  const logStream = createWriteStream(logFileName, {
    flags: "a",
  });
  // header
  logStream.write(`fiscal_code,result,note\n`);

  return {
    finalize: (): void => logStream.close(),
    logFailure: (fiscalCode: string, error: unknown): boolean =>
      logStream.write(`${fiscalCode},failure,${error}\n`),
    logSuccess: (fiscalCode: string): boolean =>
      logStream.write(`${fiscalCode},success,\n`),
  };
};

/**
 */
const populateAllFiscalCodeTable = async (
  profileContainer: Container,
  fromId: string,
  toId: string,
  insertBulkAllFiscalCodes: ReturnType<typeof getInsertBulkFiscalCodes>,
  logger: {
    readonly finalize: () => void;
    readonly logFailure: (fiscalCode: string, error: unknown) => boolean;
    readonly logSuccess: (fiscalCode: string) => boolean;
  }
): Promise<void> => {
  const iterator = getFiscalCodes(profileContainer, fromId, toId);

  for await (const results of iterator.getAsyncIterator()) {
    await insertBulkAllFiscalCodes(
      results.resources.map((obj) => obj.fiscalCode)
    )
      .run()
      .then((_) => logger.logSuccess(""))
      .catch((error) => logger.logFailure("", error));
  }
};

/**
 *
 */
const populateFiscalCodeWithMessageTable = async (
  messageContainer: Container,
  serviceId: string,
  fromId: string,
  toId: string,
  insertBulkFiscalCodesWithMessage: ReturnType<typeof getInsertBulkFiscalCodes>,
  logger: {
    readonly finalize: () => void;
    readonly logFailure: (fiscalCode: string, error: unknown) => boolean;
    readonly logSuccess: (fiscalCode: string) => boolean;
  }
): Promise<void> => {
  const iterator = getFiscalCodesWithAMessage(
    messageContainer,
    serviceId,
    fromId,
    toId
  );

  for await (const results of iterator.getAsyncIterator()) {
    await insertBulkFiscalCodesWithMessage(
      results.resources.map((obj) => obj.fiscalCode)
    )
      .run()
      .then((_) => logger.logSuccess(""))
      .catch((error) => logger.logFailure("", error));
  }
};

/**
 * Fetches all user hashed returned by the provided paged query
 */
export const queryUsers = async (
  pagedQuery: PagedQuery
): Promise<ReadonlySet<TableEntry>> => {
  const entries = new Set<TableEntry>();
  for await (const page of iterateOnPages(pagedQuery)) {
    page.forEach((e) => entries.add(e));
  }
  return entries;
};

/**
 *
 */
export const getHandler =
  (
    profileContainer: Container,
    messageContainer: Container,
    getProfilesWithoutMessages: ReturnType<typeof getPagedQuery>,
    queueService: QueueService,
    insertBulkAllFiscalCodes: ReturnType<typeof getInsertBulkFiscalCodes>,
    insertBulkFiscalCodesWithMessage: ReturnType<
      typeof getInsertBulkFiscalCodes
    >,
    { NOTIFY_USER_QUEUE_NAME, DGC_SERVICE_ID, PROFILE_TABLE_NAME }: IConfig,
    queryForAllProfiles: (
      table: string,
      tableQuery: TableQuery,
      currentToken: TableService.TableContinuationToken,
      callback: ErrorOrResult<TableService.QueryEntitiesResult<TableEntry>>
    ) => void
  ) =>
  async (
    fromId: string = fiscaCodeLowerBound,
    toId: string = fiscaCodeUpperBound
  ): Promise<void> => {
    const logger = createLogger();

    await populateAllFiscalCodeTable(
      profileContainer,
      fromId,
      toId,
      insertBulkAllFiscalCodes,
      logger
    );
    await populateFiscalCodeWithMessageTable(
      messageContainer,
      DGC_SERVICE_ID,
      fromId,
      toId,
      insertBulkFiscalCodesWithMessage,
      logger
    );

    const query = new TableQuery().select();

    await te.taskEither
      .of<Error, ReturnType<typeof getProfilesWithoutMessages>>(
        getProfilesWithoutMessages(query)
      )
      .chain((allUsersQuery) =>
        te
          .tryCatch(() => queryUsers(allUsersQuery), toError)
          // eslint-disable-next-line no-console
          .map((s) => s.forEach((v) => console.log(`------> ${v}`)))
      )
      .run();


    logger.finalize();
  };

