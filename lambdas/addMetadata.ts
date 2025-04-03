import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_METADATA_TYPES = ['description', 'location', 'tags', 'photographer'];

export const handler: SQSHandler = async (event) => {
  console.log("AddMetadata Lambda triggered");
  
  if (!TABLE_NAME) {
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      let message;
      try {
        message = JSON.parse(record.body);
      } catch (error) {
        console.error("Error parsing message body:", error);
        continue;
      }

      if (message.message_type !== 'metadata_update') {
        console.log(`Unexpected message type: ${message.message_type}, expected 'metadata_update'`);
        continue;
      }
      
      const { id, value, metadata_type = 'description' } = message;
      
      if (!id || !value) {
        console.log("Missing required fields (id or value), skipping message");
        continue;
      }

      if (!VALID_METADATA_TYPES.includes(metadata_type)) {
        console.log(`Invalid metadata type: ${metadata_type}, must be one of ${VALID_METADATA_TYPES.join(', ')}`);
        continue;
      }

      // Check if the image exists in the database
      const getParams = {
        TableName: TABLE_NAME,
        Key: { id }
      };
      
      const existingItem = await docClient.send(new GetCommand(getParams));
      
      if (!existingItem.Item) {
        console.log(`Image with id: ${id} not found in database, skipping update`);
        continue;
      }

      // Map metadata type to attribute name
      let attributeName = '';
      switch(metadata_type) {
        case 'description':
          attributeName = 'description';
          break;
        case 'location':
          attributeName = 'location';
          break;
        case 'tags':
          attributeName = 'tags';
          break;
        case 'photographer':
          attributeName = 'photographer';
          break;
        default:
          console.log(`Unexpected metadata type: ${metadata_type}`);
          continue;
      }

      // Update the item in DynamoDB with the new metadata
      const updateParams = {
        TableName: TABLE_NAME,
        Key: { id },
        UpdateExpression: `SET ${attributeName} = :value`,
        ExpressionAttributeValues: {
          ':value': value
        },
        ReturnValues: "ALL_NEW" as ReturnValue
      };

      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated metadata for image: ${id}, type: ${metadata_type}`);
    } catch (error) {
      console.error("Error processing metadata update message:", error);
    }
  }
};
