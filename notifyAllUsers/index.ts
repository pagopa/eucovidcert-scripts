import { QueueService } from "azure-storage";
import { getConfigOrThrow } from "../utils/config";
import { cosmosdbClient } from "../utils/cosmosdb";
import { getHandler } from "./handler";

const [, , fromId, toId = fromId] = process.argv;

const config = getConfigOrThrow();

const profileContainer = cosmosdbClient
  .database(config.COSMOSDB_NAME)
  .container("profiles");
const queueService = new QueueService(config.NOTIFY_USER_QUEUE_CONNECTION);

getHandler(
  profileContainer,
  queueService,
  config.NOTIFY_USER_QUEUE_NAME
)(fromId, toId)
  .then((c) => {
    console.log(`Processed ${c} profiles`);
    process.exit(0);
  })
  .catch((c) => {
    console.error(`Failed to process profiles`, c);
    process.exit(1);
  });
