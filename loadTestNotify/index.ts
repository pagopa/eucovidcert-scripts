import { QueueService } from "azure-storage";
import { getConfigOrThrow } from "../utils/config";
import { getHandler } from "./handler";

const config = getConfigOrThrow();

const queueService = new QueueService(config.NOTIFY_USER_QUEUE_CONNECTION);

getHandler(queueService, config.NOTIFY_USER_QUEUE_NAME)()
  .then((c) => {
    console.log(`Processed ${c} profiles`);
    process.exit(0);
  })
  .catch((c) => {
    console.error(`Failed to process profiles`, c);
    process.exit(1);
  });
