import { createWriteStream } from "fs";

export type Logger = ReturnType<typeof createLogger>;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export const createLogger = () => {
  const logFileName = `${process.cwd()}/log.${Date.now()}.csv`;
  const logStream = createWriteStream(logFileName, {
    flags: "a",
  });
  // header
  logStream.write(`time, log level, text\n`);

  return {
    finalize: (): void => logStream.close(),
    logFailure: (info: string, error: unknown): boolean =>
      logStream.write(`${new Date().toISOString()}, fail, ${info}: ${error}\n`),
    logInfo: (info: string): boolean =>
      logStream.write(`${new Date().toISOString()}, info, ${info}\n`),
  };
};
