/* eslint-disable max-params */
/* eslint-disable @typescript-eslint/no-unused-vars */
import { QueueService, TableQuery, TableUtilities } from "azure-storage";

import { toError } from "fp-ts/lib/Either";
import * as te from "fp-ts/lib/TaskEither";
import * as a from "fp-ts/lib/Array";
import * as t from "fp-ts/lib/Task";
import {
  getInsertBulkFiscalCodes,
  getPagedQuery,
  iterateOnPages,
} from "../utils/table_storage";
import { IConfig } from "../utils/config";

const TO_SEND = "TO_SEND";
const ERROR = "ERROR";
const SENDED = "SENDED";

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const getStatusCodeItem = (status: string) => ({
  Status: TableUtilities.entityGenerator.String(status),
});

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
): t.Task<ReadonlyArray<string>> =>
  a.array.sequence(t.task)(
    page
      .map((e) => e.RowKey._)
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
          .fold(
            (_) => ({ fiscalCode, status: ERROR }),
            (_) => ({ fiscalCode, status: SENDED })
          )
          .chain((fc) =>
            updateBulkFiscalCodesWithMessage(
              [fc.fiscalCode],
              getStatusCodeItem(fc.status)
            ).fold(
              (l) => fiscalCode,
              (r) => fiscalCode
            )
          )
      )
  );

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
    { NOTIFY_USER_QUEUE_NAME }: IConfig
  ) =>
  async (): Promise<void> => {
    await te.taskEither
      .of<Error, ReturnType<typeof getProfilesWithoutMessages>>(
        getProfilesWithoutMessages(
          new TableQuery().select().where("Status eq ?", TO_SEND)
        )
      )
      .chain((allUsersQuery) =>
        te.tryCatch(async () => {
          for await (const page of iterateOnPages(allUsersQuery)) {
            // eslint-disable-next-line no-console
            await sendPage(
              queueService,
              NOTIFY_USER_QUEUE_NAME,
              updateBulkFiscalCodesWithMessage,
              page
            ).run();
          }
        }, toError)
      )
      .run();
  };
