/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { createWriteStream } from "fs";
import { TableUtilities } from "azure-storage";
import { Container } from "@azure/cosmos";

import { getInsertBulkFiscalCodes } from "../utils/table_storage";
import { IConfig } from "../utils/config";
import { getFiscalCodes, getFiscalCodesWithAMessage } from "../utils/cosmosdb";

const TO_SEND = "TO_SEND";

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
  logStream.write(`time,\tlog level,\ttext\n`);

  return {
    finalize: (): void => logStream.close(),
    logFailure: (info: string, error: unknown): boolean =>
      logStream.write(
        `${new Date().toISOString()},failure,\t${info}: ${error}\n`
      ),
    logInfo: (info: string): boolean =>
      logStream.write(`${new Date().toISOString()},info,${info}\n`),
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
    logger.logInfo(
      `populateAllFiscalCodeTable fetched ${results.resources.length}`
    );
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
    logger.logInfo(
      `cleanupFiscalCodeWithMessageTable fetched ${results.resources.length}`
    );
    await updateBulkFiscalCodesWithMessage(
      results.resources.map((obj) => obj.fiscalCode)
    )
      .run()
      .catch((error) =>
        logger.logFailure("updateBulkFiscalCodesWithMessage", error)
      );
  }
};

/**
 *
 */
export const getHandler =
  (
    profileContainer: Container,
    messageContainer: Container,
    insertBulkAllFiscalCodes: ReturnType<typeof getInsertBulkFiscalCodes>,
    insertBulkFiscalCodesWithMessage: ReturnType<
      typeof getInsertBulkFiscalCodes
    >,
    {
      DGC_SERVICE_ID,
      FISCAL_CODE_LOWER_BOUND,
      FISCAL_CODE_UPPER_BOUND,
    }: IConfig
  ) =>
  async (
    fromId: string = FISCAL_CODE_LOWER_BOUND,
    toId: string = FISCAL_CODE_UPPER_BOUND
  ): Promise<void> => {
    const logger = createLogger();
    logger.logInfo("starting populateAllFiscalCodeTable()");
    await populateAllFiscalCodeTable(
      profileContainer,
      fromId,
      toId,
      insertBulkAllFiscalCodes,
      logger
    );
    logger.logInfo("ended populateAllFiscalCodeTable()");
    logger.logInfo("starting cleanupFiscalCodeWithMessageTable()");
    await cleanupFiscalCodeWithMessageTable(
      messageContainer,
      DGC_SERVICE_ID,
      fromId,
      toId,
      insertBulkFiscalCodesWithMessage,
      logger
    );
    logger.logInfo("ended cleanupFiscalCodeWithMessageTable()");

    logger.finalize();
  };
