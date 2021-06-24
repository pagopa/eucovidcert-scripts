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
import * as a from "fp-ts/lib/Array";
import * as t from "fp-ts/lib/Task";
import {
  getInsertBulkFiscalCodes,
  getPagedQuery,
  iterateOnPages,
  TableEntry,
} from "../utils/table_storage";
import { IConfig } from "../utils/config";
import { getFiscalCodes, getFiscalCodesWithAMessage } from "../utils/cosmosdb";

const fiscaCodeLowerBound = "A".repeat(16);
const fiscaCodeUpperBound = "Z".repeat(16);

const TO_SEND = "TO_SEND";
const ERROR = "ERROR";
const SENDED = "SENDED";

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const getStatusCodeItem = (status: string) => ({
  Status: TableUtilities.entityGenerator.String(status),
});

export type Logger = ReturnType<typeof createLogger>;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const createLogger = () => {
  const logFileName = `${process.cwd()}/log.${Date.now()}.csv`;
  const logStream = createWriteStream(logFileName, {
    flags: "a",
  });
  // header
  logStream.write(`log level,\ttext\n`);

  return {
    finalize: (): void => logStream.close(),
    logFailure: (info: string, error: unknown): boolean =>
      logStream.write(`failure,\t${info}: ${error}\n`),
    logInfo: (info: string): boolean => logStream.write(`info,${info}\t\n`),
  };
};

/**
 */
const populateAllFiscalCodeTable = async (
  profileContainer: Container,
  fromId: string,
  toId: string,
  insertBulkAllFiscalCodes: ReturnType<typeof getInsertBulkFiscalCodes>,
  logger: Logger
): Promise<void> => {
  const iterator = getFiscalCodes(profileContainer, fromId, toId);

  for await (const results of iterator.getAsyncIterator()) {
    await insertBulkAllFiscalCodes(
      results.resources.map((obj) => obj.fiscalCode),
      getStatusCodeItem(TO_SEND)
    )
      .run()
      .catch((error) => logger.logFailure("insertBulkAllFiscalCodes", error));
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
  updateBulkFiscalCodesWithMessage: ReturnType<typeof getInsertBulkFiscalCodes>,
  logger: Logger
): Promise<void> => {
  const iterator = getFiscalCodesWithAMessage(
    messageContainer,
    serviceId,
    fromId,
    toId
  );

  for await (const results of iterator.getAsyncIterator()) {
    await updateBulkFiscalCodesWithMessage(
      results.resources.map((obj) => obj.fiscalCode)
    )
      .run()
      .catch((error) =>
        logger.logFailure("updateBulkFiscalCodesWithMessage", error)
      );
  }
};

const sendPage = (
  queueService: QueueService,
  NOTIFY_USER_QUEUE_NAME: string,
  updateBulkFiscalCodesWithMessage: ReturnType<typeof getInsertBulkFiscalCodes>,
  page: ReadonlyArray<
    Readonly<{
      readonly RowKey: Readonly<{
        readonly _: string;
      }>;
    }>
  >
): t.Task<ReadonlyArray<string>> =>
  a.array.sequence(t.task)(
    page
      .map((e) => e.RowKey._)
      .map((fiscalCode) =>
        te
          .taskify<Error, QueueService.QueueMessageResult>((cb) =>
            queueService.createMessage(
              NOTIFY_USER_QUEUE_NAME,
              Buffer.from(JSON.stringify({ fiscal_code: fiscalCode })).toString(
                "base64"
              ),
              cb
            )
          )()
          .fold(
            (_) => ({ fiscalCode, status: ERROR }),
            (_) => ({ fiscalCode, status: SENDED })
          )
          .chain((fc) =>
            updateBulkFiscalCodesWithMessage(
              [fc.fiscalCode],
              getStatusCodeItem(fc.status)
            ).fold(
              (l) => fiscalCode,
              (r) => fiscalCode
            )
          )
      )
  );

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
            await sendPage(
              queueService,
              NOTIFY_USER_QUEUE_NAME,
              updateBulkFiscalCodesWithMessage,
              page
            ).run();
          }
        }, toError)
      )
      .run();

    logger.finalize();
  };
interface IOpStatus {
  readonly fiscalCode: string;
  readonly status: string;
}
