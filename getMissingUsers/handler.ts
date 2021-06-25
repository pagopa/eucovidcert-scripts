/* eslint-disable max-params */
import { TableUtilities } from "azure-storage";
import { Container } from "@azure/cosmos";

import { getInsertBulkFiscalCodes } from "../utils/table_storage";
import { IConfig } from "../utils/config";
import { getFiscalCodes, getFiscalCodesWithAMessage } from "../utils/cosmosdb";
import { Logger } from "../utils/logger";
import { getUdateBulkFiscalCodes } from "../utils/table_storage";
import { isRight } from "fp-ts/lib/Either";

const TO_SEND = "TO_SEND";
const DELETED = "DELETED";

const MAX_RETRY = 100;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const getStatusCodeItem = (status: string) => ({
  Status: TableUtilities.entityGenerator.String(status),
});

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
    const executeBulkInsert = () =>
      insertBulkAllFiscalCodes(
        results.resources.map((obj) => obj.fiscalCode),
        getStatusCodeItem(TO_SEND)
      ).mapLeft((azureError) =>
        logger.logInfo(
          `populateAllFiscalCodeTable: error received from table storage (batch will be retired) ${JSON.stringify(
            azureError
          )}`
        )
      );
    let batchResult = false;
    let count = 0;
    do {
      batchResult = isRight(await executeBulkInsert().run());
      count++;
    } while (!batchResult && count < MAX_RETRY);
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
  updateBulkFiscalCodesWithMessage: ReturnType<typeof getUdateBulkFiscalCodes>,
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
    const executeBulkUpdate = () =>
      updateBulkFiscalCodesWithMessage(
        results.resources
          .map((obj) => obj.fiscalCode)
          .filter((elem, index, self) => index === self.indexOf(elem)),
        getStatusCodeItem(DELETED)
      ).mapLeft((azureError) =>
        logger.logInfo(
          `cleanupFiscalCodeWithMessageTable: error received ftom table storage (batch will be retired) ${JSON.stringify(
            azureError
          )}`
        )
      );
    let batchResult = false;
    let count = 0;
    do {
      batchResult = isRight(await executeBulkUpdate().run());
      count++;
    } while (!batchResult && count < MAX_RETRY);
  }
};

/**
 *
 */
export const getHandler =
  (
    logger: Logger,
    profileContainer: Container,
    messageContainer: Container,
    insertBulkAllFiscalCodes: ReturnType<typeof getInsertBulkFiscalCodes>,
    updateBulkFiscalCodesWithMessage: ReturnType<
      typeof getUdateBulkFiscalCodes
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
      updateBulkFiscalCodesWithMessage,
      logger
    );
    logger.logInfo("ended cleanupFiscalCodeWithMessageTable()");

    logger.finalize();
  };
