/* eslint-disable no-console */
/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { createWriteStream } from "fs";
import {
  ErrorOrResult,
  QueueService,
  ServiceResponse,
  TableQuery,
  TableService,
  TableUtilities,
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

const TO_SEND = "TO_SEND";
const ERROR = "ERROR";
const SENDED = "SENDED";

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const getStatusCodeItem = (status: string) => ({
  Status: TableUtilities.entityGenerator.String(status),
});

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
      results.resources.map((obj) => obj.fiscalCode),
      getStatusCodeItem(TO_SEND)
    )
      .run()
      .then((_) => logger.logSuccess(""))
      .catch((error) => logger.logFailure("", error));
  }
};

/**
 *
 */
const cleanupFiscalCodeWithMessageTable = async (
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
    updateBulkFiscalCodesWithMessage: ReturnType<
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
    await cleanupFiscalCodeWithMessageTable(
      messageContainer,
      DGC_SERVICE_ID,
      fromId,
      toId,
      insertBulkFiscalCodesWithMessage,
      logger
    );


    await te.taskEither
      .of<Error, ReturnType<typeof getProfilesWithoutMessages>>(
        getProfilesWithoutMessages(new TableQuery().select())
      )
      .chain((allUsersQuery) =>
        te.tryCatch(async () => {
          for await (const page of iterateOnPages(allUsersQuery)) {
            // eslint-disable-next-line no-console
            for await (const fiscalCode of page.map((e) => e.RowKey._)) {
              await new Promise<string>((resolve, reject) => {
                queueService.createMessage(
                  NOTIFY_USER_QUEUE_NAME,
                  fiscalCode,
                  (_) => (_ ? reject(_) : resolve(fiscalCode))
                );
              })
                .then(async (fc) => {
                  console.log(`---> SUCCESS ${fiscalCode} <---`);
                  await updateBulkFiscalCodesWithMessage(
                    [fc],
                    getStatusCodeItem(SENDED)
                  ).run();
                  logger.logSuccess(fc);
                })
                .catch(async (error) => {
                  await updateBulkFiscalCodesWithMessage(
                    [fiscalCode],
                    getStatusCodeItem(ERROR)
                  ).run();
                  console.log(`---> ERROR ${fiscalCode} <---`);
                  logger.logFailure(fiscalCode, error);
                });
            }
          }
        }, toError)
      )
      .run();

    logger.finalize();
  };
