/* eslint-disable sort-keys */
/**
 * Config module
 *
 * Single point of access for the application confguration. Handles validation on required environment variables.
 * The configuration is evaluate eagerly at the first access to the module. The module exposes convenient methods to access such value.
 */

import * as t from "io-ts";
import { ValidationError } from "io-ts";
import { readableReport } from "@pagopa/ts-commons/lib/reporters";
import { NonEmptyString } from "@pagopa/ts-commons/lib/strings";
import { NumberFromString } from "@pagopa/ts-commons/lib/numbers";

// global app configuration
export type IConfig = t.TypeOf<typeof IConfig>;
// eslint-disable-next-line @typescript-eslint/ban-types
export const IConfig = t.interface({
  COSMOSDB_KEY: NonEmptyString,
  COSMOSDB_NAME: NonEmptyString,
  COSMOSDB_URI: NonEmptyString,
  COSMOSDB_MAX_ITEM_COUNT: NumberFromString,

  NOTIFY_USER_QUEUE_CONNECTION: NonEmptyString,
  NOTIFY_USER_QUEUE_NAME: NonEmptyString,

  SCRIPT_STORAGE_CONNECTION_STRING: NonEmptyString,
  PROFILE_TABLE_NAME: NonEmptyString,
  PROFILE_WITH_MESSAGE_TABLE_NAME: NonEmptyString,
  BATCH_RESULT_TABLE_NAME: NonEmptyString,

  DGC_SERVICE_ID: NonEmptyString,

  FISCAL_CODE_LOWER_BOUND: NonEmptyString,
  FISCAL_CODE_UPPER_BOUND: NonEmptyString,

  isProduction: t.boolean,
});

// No need to re-evaluate this object for each call
const errorOrConfig: t.Validation<IConfig> = IConfig.decode({
  ...process.env,
  isProduction: process.env.NODE_ENV === "production",
});

/**
 * Read the application configuration and check for invalid values.
 * Configuration is eagerly evalued when the application starts.
 *
 * @returns either the configuration values or a list of validation errors
 */
export const getConfig = (): t.Validation<IConfig> => errorOrConfig;

/**
 * Read the application configuration and check for invalid values.
 * If the application is not valid, raises an exception.
 *
 * @returns the configuration values
 * @throws validation errors found while parsing the application configuration
 */
export const getConfigOrThrow = (): IConfig =>
  errorOrConfig.getOrElseL((errors: ReadonlyArray<ValidationError>) => {
    throw new Error(`Invalid configuration: ${readableReport(errors)}`);
  });
