import {
  createTableService,
  ErrorOrResult,
  QueueService,
  TableQuery,
  TableService,
} from "azure-storage";
import { getConfigOrThrow } from "../utils/config";
import { createLogger } from "../utils/logger";
import { getUdateBulkFiscalCodes } from "../utils/table_storage";
import { getPagedQuery } from "../utils/table_storage";
import { getHandler } from "./handler";

export const MESSAGES_COLLECTION_NAME = "messages";
export const PROFILES_COLLECTION_NAME = "profiles";

const config = getConfigOrThrow();

const queueService = new QueueService(config.NOTIFY_USER_QUEUE_CONNECTION);

const tableService = createTableService(
  config.SCRIPT_STORAGE_CONNECTION_STRING
);

export const createQueryForAllProfiles =
  <T>(ts: TableService) =>
  (
    table: string,
    tableQuery: TableQuery,
    currentToken: TableService.TableContinuationToken,
    callback: ErrorOrResult<TableService.QueryEntitiesResult<T>>
  ): void =>
    ts.queryEntities(table, tableQuery, currentToken, callback);

tableService.createTableIfNotExists(
  config.PROFILE_WITH_MESSAGE_TABLE_NAME,
  (err1, _res1) => {
    if (err1) {
      // eslint-disable-next-line no-console
      throw Error("Table PROFILE_WITH_MESSAGE_TABLE_NAME not crated");
    }

    tableService.createTableIfNotExists(
      config.PROFILE_TABLE_NAME,
      (err, _res) => {
        if (err) {
          // eslint-disable-next-line no-console
          throw Error("Table PROFILE_TABLE_NAME not crated");
        }

        getHandler(
          getPagedQuery(tableService, config.PROFILE_TABLE_NAME),
          queueService,
          getUdateBulkFiscalCodes(tableService, config.PROFILE_TABLE_NAME),
          config,
          createLogger()
        )()
          .then((c) => {
            // eslint-disable-next-line no-console
            console.log(`Processed ${c} profiles`);
            process.exit(0);
          })
          .catch((c) => {
            // eslint-disable-next-line no-console
            console.error(`Failed to process profiles`, c);
            process.exit(1);
          });
      }
    );
  }
);
