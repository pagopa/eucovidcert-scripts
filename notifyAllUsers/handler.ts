import { createWriteStream } from "fs";
import { QueueService } from "azure-storage";
import { Container, SqlQuerySpec } from "@azure/cosmos";

const fiscaCodeLowerBound = "A".repeat(16);
const fiscaCodeUpperBound = "Z".repeat(16);

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

export const getHandler =
  (
    profileContainer: Container,
    queueService: QueueService,
    queueName: string
  ) =>
  async (
    fromId: string = fiscaCodeLowerBound,
    toId: string = fiscaCodeUpperBound
  ): Promise<number> => {
    // eslint-disable-next-line functional/no-let
    let totProcessedItems = 0;

    const logger = createLogger();

    const q: SqlQuerySpec = {
      parameters: [
        {
          name: "@fromId",
          value: fromId,
        },
        {
          name: "@toId",
          value: toId,
        },
      ],
      // Note: do not use ${collectionName} here as it may contain special characters
      query: `
        SELECT DISTINCT c.fiscalCode 
        FROM c 
        WHERE 
          c.fiscalCode >= @fromId 
          AND c.fiscalCode <= @toId
      `,
    };
    const iterator =
      profileContainer.items.query<{ readonly fiscalCode: string }>(q);

    for await (const results of iterator.getAsyncIterator()) {
      const proc = results.resources.map((e) =>
        new Promise<void>((resolve, reject) =>
          queueService.createMessage(queueName, e.fiscalCode, (_) =>
            _ ? reject(_) : resolve()
          )
        )
          .then(() => logger.logSuccess(e.fiscalCode))
          .catch((error) => logger.logFailure(e.fiscalCode, error))
      );
      totProcessedItems += proc.length;
      await Promise.all(proc);
    }

    logger.finalize();
    return totProcessedItems;
  };
