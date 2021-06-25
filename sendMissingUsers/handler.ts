/* eslint-disable functional/no-let */
/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { QueueService, TableQuery, TableUtilities } from "azure-storage";

import { toError } from "fp-ts/lib/Either";
import * as te from "fp-ts/lib/TaskEither";
import * as a from "fp-ts/lib/Array";
import * as t from "fp-ts/lib/Task";
import * as e from "fp-ts/lib/Either";
import {
  getInsertBulkFiscalCodes,
  getPagedQuery,
  iterateOnPages,
  TableEntry,
} from "../utils/table_storage";
import { IConfig } from "../utils/config";
import { Logger } from "../utils/logger";

const TO_SEND = "TO_SEND";
const ERROR = "ERROR";
const SENT = "SENT";

const MAX_RETRY = 100;
const QUEUE_THROTTLE = 80;

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const getStatusCodeItem = (status: string) => ({
  Status: TableUtilities.entityGenerator.String(status),
});

interface ITableEntity {
  fiscalCode: string;
  status: string;
}

const sendPage = (
  queueService: QueueService,
  NOTIFY_USER_QUEUE_NAME: string,
  updateBulkFiscalCodesWithMessage: ReturnType<typeof getInsertBulkFiscalCodes>,
  page: ReadonlyArray<
    Readonly<{
      readonly RowKey: Readonly<{
        readonly _: string;
      }>;
    }>
  >
): t.Task<ReadonlyArray<e.Either<ITableEntity, ITableEntity>>> =>
  a.array.sequence(t.taskSeq)(
    page
      .map((entity) => entity.RowKey._)
      .map((fiscalCode) =>
        te
          .taskify<Error, QueueService.QueueMessageResult>((cb) =>
            queueService.createMessage(
              NOTIFY_USER_QUEUE_NAME,
              Buffer.from(JSON.stringify({ fiscal_code: fiscalCode })).toString(
                "base64"
              ),
              cb
            )
          )()
          .mapLeft((l) => {
            console.log(
              `sendPage failed to createMessage for ${fiscalCode} ${JSON.stringify(
                l
              )}`
            );
            return l;
          })
          .fold(
            (_) => ({ fiscalCode, status: ERROR }),
            (_) => ({ fiscalCode, status: SENT })
          )
          .chain((fc) =>
            updateBulkFiscalCodesWithMessage(
              [fc.fiscalCode],
              getStatusCodeItem(fc.status)
            ).fold(
              (l) => e.left(fc),
              (r) => e.right(fc)
            )
          )
      )
  );

type TPage = ReadonlyArray<TableEntry>;

/**
 *
 */
export const getHandler =
  (
    getProfilesWithoutMessages: ReturnType<typeof getPagedQuery>,
    queueService: QueueService,
    updateBulkFiscalCodesWithMessage: ReturnType<
      typeof getInsertBulkFiscalCodes
    >,
    { NOTIFY_USER_QUEUE_NAME }: IConfig,
    logger: Logger
  ) =>
  async (): Promise<void> => {
    logger.logInfo("starting sendMissingUsers");
    await te.taskEither
      .of<Error, ReturnType<typeof getProfilesWithoutMessages>>(
        getProfilesWithoutMessages(
          new TableQuery().select().where("Status eq ?", ERROR)
        )
      )
      .chain((allUsersQuery) =>
        te.tryCatch(async () => {
          for await (const page of iterateOnPages(allUsersQuery)) {
            // eslint-disable-next-line no-console
            logger.logInfo(`sendMissingUsers fetched ${page.length}`);
            const sendCurrentPage = (currentPage: TPage) =>
              sendPage(
                queueService,
                NOTIFY_USER_QUEUE_NAME,
                updateBulkFiscalCodesWithMessage,
                currentPage
              );

            const sendCurrentPageWithRetry = async (currentPage: TPage) => {
              let currentFailedUpdateFc = (
                await sendCurrentPage(currentPage).run()
              )
                .filter(e.isLeft)
                .map((l) => l.value);

              let batchResult = false;
              let count = 0;
              do {
                let nextFailedUpdateFC = [];
                for (const fc of currentFailedUpdateFc) {
                  if (
                    (
                      await updateBulkFiscalCodesWithMessage(
                        [fc.fiscalCode],
                        getStatusCodeItem(fc.status)
                      ).run()
                    ).isLeft()
                  ) {
                    nextFailedUpdateFC.push(fc);
                  }
                }
                currentFailedUpdateFc = nextFailedUpdateFC;
                batchResult = currentFailedUpdateFc.length === 0;
                count++;
              } while (!batchResult && count < MAX_RETRY);
              if (currentFailedUpdateFc.length > 0) {
                logger.logInfo(
                  `Can not update status for ${JSON.stringify(
                    currentFailedUpdateFc
                  )}`
                );
              }
            };

            let sendPageTasks = [];
            if (page.length >= QUEUE_THROTTLE) {
              const pageChunkSize = page.length / QUEUE_THROTTLE;
              for (let i = 0; i < QUEUE_THROTTLE; i++) {
                sendPageTasks.push(
                  sendCurrentPageWithRetry(
                    page.slice(i * pageChunkSize, (i + 1) * pageChunkSize)
                  )
                );
              }
              await Promise.all(sendPageTasks);
            } else {
              await sendCurrentPageWithRetry(page);
            }
          }
        }, toError)
      )
      .run();
    logger.logInfo("ended sendMissingUsers");
  };
