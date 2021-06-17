import { createWriteStream } from "fs";
import { QueueService } from "azure-storage";
import { toSHA256 } from "../utils/conversions";
import { FiscalCode } from "@pagopa/ts-commons/lib/strings";

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
  (queueService: QueueService, queueName: string) =>
  async (): Promise<number> => {
    const logger = createLogger();

    const cf = "ZZZZZP25A01H501J" as FiscalCode;
    const tot = 1e6;
    const parallel = 1e2;

    for (let i = 0; i < tot / parallel; i++) {
      const p = Array.from({ length: parallel }).map((_) =>
        new Promise<void>((resolve, reject) =>
          queueService.createMessage(
            queueName,
            Buffer.from(toSHA256(cf)).toString("base64"),
            (_) => (_ ? reject(_) : resolve())
          )
        )
          .then(() => logger.logSuccess(cf))
          .catch((error) => logger.logFailure(cf, error))
      );

      await Promise.all(p);
    }

    logger.finalize();
    return tot;
  };
