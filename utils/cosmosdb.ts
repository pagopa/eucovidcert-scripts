/**
 * Use a singleton CosmosDB client across functions.
 */
import { Container, CosmosClient, QueryIterator } from "@azure/cosmos";
import { getConfigOrThrow } from "./config";

const config = getConfigOrThrow();
const cosmosDbUri = config.COSMOSDB_URI;
const masterKey = config.COSMOSDB_KEY;
const maxItemCount = config.COSMOSDB_MAX_ITEM_COUNT;

export const cosmosdbClient = new CosmosClient({
  endpoint: cosmosDbUri,
  key: masterKey,
});

/**
 * Get all fiscal codes iterator query
 *
 * @param profileContainer
 * @param fromId
 * @param toId
 * @returns
 */
export const getFiscalCodes = (
  profileContainer: Container,
  fromId: string,
  toId: string
): QueryIterator<{ readonly fiscalCode: string }> => {
  const q = {
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

  return profileContainer.items.query<{ readonly fiscalCode: string }>(q, {
    maxItemCount,
  });
};

/**
 *
 * @param profileContainer
 * @param fromId
 * @param toId
 * @returns
 */
export const getFiscalCodesWithAMessage = (
  messageContainer: Container,
  serviceId: string,
  fromId: string,
  toId: string
): QueryIterator<{ readonly fiscalCode: string }> => {
  const q = {
    parameters: [
      {
        name: "@fromId",
        value: fromId,
      },
      {
        name: "@toId",
        value: toId,
      },
      {
        name: "@serviceId",
        value: serviceId,
      },
    ],
    // Note: do not use ${collectionName} here as it may contain special characters
    query: `
        SELECT DISTINCT c.fiscalCode 
        FROM c 
        WHERE 
          c.fiscalCode >= @fromId 
          AND c.fiscalCode <= @toId
          AND c.senderServiceId = @serviceId
      `,
  };

  return messageContainer.items.query<{ readonly fiscalCode: string }>(q, {
    maxItemCount,
  });
};
