import { createTableService, QueueService, Tab } from "azure-storage";
import { getConfigOrThrow } from "../utils/config";
import { cosmosdbClient } from "../utils/cosmosdb";
import {
  getDeleteBulkFiscalCodes,
  getInsertBulkFiscalCodes,
} from "../utils/table_storage";
import { getPagedQuery } from "../utils/table_storage";
import { getHandler } from "./handler";

export const MESSAGES_COLLECTION_NAME = "messages";
export const PROFILES_COLLECTION_NAME = "profiles";

const [, , fromId, toId = fromId] = process.argv;

const config = getConfigOrThrow();

const queueService = new QueueService(config.NOTIFY_USER_QUEUE_CONNECTION);

const tableService = createTableService(
  config.SCRIPT_STORAGE_CONNECTION_STRING
);

const profileContainer = cosmosdbClient
  .database(config.COSMOSDB_NAME)
  .container(PROFILES_COLLECTION_NAME);
const messageContainer = cosmosdbClient
  .database(config.COSMOSDB_NAME)
  .container(MESSAGES_COLLECTION_NAME);

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export const createQueryForAllProfiles =
  (ts: ReturnType<typeof createTableService>) =>
  (
    table: string,
    tableQuery: TableQuery,
    currentToken: TableService.TableContinuationToken,
    options: TableService.TableEntityRequestOptions,
    callback: ErrorOrResult<TableService.QueryEntitiesResult<T>>
  ) =>
    ts.queryEntities(table, tableQuery, currentToken, options, callback);

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
          profileContainer,
          messageContainer,
          getPagedQuery(tableService, config.PROFILE_TABLE_NAME),
          queueService,
          getInsertBulkFiscalCodes(tableService, config.PROFILE_TABLE_NAME),
          getDeleteBulkFiscalCodes(tableService, config.PROFILE_TABLE_NAME),
          config,
          createQueryForAllProfiles(tableService)
        )(fromId, toId)
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
