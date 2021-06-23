/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { createWriteStream } from "fs";
import { QueueService, TableQuery, TableService } from "azure-storage";
import { Container } from "@azure/cosmos";

import * as te from "fp-ts/lib/TaskEither";
import {
  getInsertBulkFiscalCodes,
  getPagedQuery,
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
    queryForAllProfiles: ReturnType<typeof createQueryForAllProfiles>
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

    // te.taskEither
    //   .of<Error, ReturnType<typeof getProfilesWithoutMessages>>(
    //     getProfilesWithoutMessages(new TableQuery().select())
    //   )
    //   .chain((query) =>
    //     te
    //       .tryCatch(() => queryUsers(cgnExpirationQuery), toError)
    //       .map((readSet) => Array.from(readSet.values()))
    //   );

    const query = new TableQuery().select();

    // eslint-disable-next-line no-var
    var nextContinuationToken =
      null as unknown as TableService.TableContinuationToken;
    queryForAllProfiles(
      PROFILE_TABLE_NAME,
      query,
      nextContinuationToken,
      (error, results) => {
        if (error) {
          throw error;
        }
        // eslint-disable-next-line no-console
        results.entries.forEach(console.log);
        if (results.continuationToken) {
          nextContinuationToken = results.continuationToken;
        }
      }
    );

    logger.finalize();
  };

// new Promise<void>((resolve, reject) =>
//         queueService.createMessage(queueName, e.fiscalCode, (_) =>
//           _ ? reject(_) : resolve()
//         )
//       )
//         .then(() => logger.logSuccess(e.fiscalCode))
//         .catch((error) => logger.logFailure(e.fiscalCode, error))
