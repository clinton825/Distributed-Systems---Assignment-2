import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_METADATA_TYPES = ['description', 'location', 'tags', 'photographer'];

export const handler: SQSHandler = async (event) => {
  console.log("AddMetadata Lambda triggered with event: ", JSON.stringify(event, null, 2));
  console.log("Using DynamoDB table: ", TABLE_NAME);

  if (!TABLE_NAME) {
    console.error("ERROR: TABLE_NAME environment variable is not set!");
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      console.log("Processing record: ", JSON.stringify(record, null, 2));
      
      // With rawMessageDelivery: true, the message is directly the JSON payload
      let message;
      try {
        message = JSON.parse(record.body);
        console.log("Parsed message: ", JSON.stringify(message, null, 2));
      } catch (error) {
        console.error("Error parsing message body:", error);
        continue;
      }

      // Validate message type
      if (message.message_type !== 'metadata_update') {
        console.log(`Unexpected message type: ${message.message_type}, expected 'metadata_update'`);
        continue;
      }
      
      // Extract metadata values
      const { id, value, metadata_type = 'description' } = message;
      console.log(`Extracted id: ${id}, value: ${value}, metadata type: ${metadata_type}`);
      
      if (!id || !value) {
        console.log("Missing required fields (id or value), skipping message");
        continue;
      }

      // Validate metadata type
      if (!VALID_METADATA_TYPES.includes(metadata_type)) {
        console.log(`Invalid metadata type: ${metadata_type}, must be one of ${VALID_METADATA_TYPES.join(', ')}`);
        continue;
      }

      // Check if the image exists in the database
      const getParams = {
        TableName: TABLE_NAME,
        Key: { id }
      };
      
      console.log(`Checking if image exists with params: ${JSON.stringify(getParams)}`);
      const existingItem = await docClient.send(new GetCommand(getParams));
      console.log(`Existing item result: ${JSON.stringify(existingItem)}`);
      
      if (!existingItem.Item) {
        console.log(`Image with id: ${id} not found in database, skipping update`);
        continue;
      }
      
      console.log(`Found existing item in DynamoDB: ${JSON.stringify(existingItem.Item)}`);

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
          // This should never happen due to the validation above
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

      console.log(`Updating DynamoDB with params: ${JSON.stringify(updateParams)}`);
      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated metadata for image: ${id}, updated item: ${JSON.stringify(result.Attributes)}`);
    } catch (error) {
      console.error("Error processing metadata update message:", error);
      // Continue processing other records even if one fails
    }
  }
  
  console.log("AddMetadata Lambda execution completed successfully");
};
