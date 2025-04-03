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
      
      const recordBody = JSON.parse(record.body);
      console.log("Parsed record body: ", JSON.stringify(recordBody, null, 2));
      
      // Get the message attributes which include metadata_type
      const messageAttributes = recordBody.MessageAttributes || {};
      console.log("Message attributes: ", JSON.stringify(messageAttributes, null, 2));
      
      // Extract metadata type from message attributes
      const metadataType = messageAttributes.metadata_type?.Value;
      console.log("Metadata type: ", metadataType);
      
      if (!metadataType || !VALID_METADATA_TYPES.includes(metadataType)) {
        console.log(`Invalid or missing metadata type: ${metadataType}`);
        continue;
      }
      
      const snsMessage = JSON.parse(recordBody.Message);
      console.log("Parsed SNS message: ", JSON.stringify(snsMessage, null, 2));
      
      // Extract metadata values
      const { id, value } = snsMessage;
      console.log(`Extracted id: ${id}, value: ${value}, metadata type: ${metadataType}`);
      
      if (!id || !value) {
        console.log("Missing required fields (id or value), skipping message");
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
      switch(metadataType) {
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
          console.log(`Unexpected metadata type: ${metadataType}`);
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
