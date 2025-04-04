import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
// Per project requirements, only Caption, Date, and name (Photographer's name) are valid
const VALID_METADATA_TYPES = ['Caption', 'Date', 'name'];

export const handler: SQSHandler = async (event) => {
  console.log("AddMetadata Lambda triggered");
  
  if (!TABLE_NAME) {
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      console.log("Processing record:", JSON.stringify(record, null, 2));
      let message;
      let metadataType;
      
      try {
        // Parse the message body
        message = JSON.parse(record.body);
        console.log("Parsed message:", JSON.stringify(message, null, 2));
        
        // Check if this is an SNS message
        if (message.Type === 'Notification' && message.Message) {
          console.log("SNS message detected, parsing inner message");
          const innerMessage = JSON.parse(message.Message);
          console.log("Inner message:", JSON.stringify(innerMessage, null, 2));
          message = innerMessage;
          
          // Extract metadata_type from message attributes
          if (message.MessageAttributes && message.MessageAttributes.metadata_type) {
            metadataType = message.MessageAttributes.metadata_type.Value;
          }
        }
        
        // For direct SQS messages (not wrapped in SNS)
        if (!metadataType && record.messageAttributes && record.messageAttributes.metadata_type) {
          metadataType = record.messageAttributes.metadata_type.stringValue;
        }
        
        console.log(`Metadata type: ${metadataType}`);
      } catch (error) {
        console.error("Error parsing message:", error);
        continue;
      }

      if (!message.message_type || message.message_type !== 'metadata_update') {
        console.log(`Unexpected or missing message_type: ${message.message_type}, expected 'metadata_update'`);
        continue;
      }
      
      const { id, value } = message;
      
      if (!id || !value) {
        console.log("Missing required fields (id or value), skipping message");
        continue;
      }

      if (!metadataType || !VALID_METADATA_TYPES.includes(metadataType)) {
        console.log(`Invalid or missing metadata_type: ${metadataType}, must be one of ${VALID_METADATA_TYPES.join(', ')}`);
        continue;
      }

      // Check if the image exists in the database
      const getParams = {
        TableName: TABLE_NAME,
        Key: { id }
      };
      
      console.log("Checking if image exists:", getParams);
      const existingItem = await docClient.send(new GetCommand(getParams));
      
      if (!existingItem.Item) {
        console.log(`Image with id: ${id} not found in database, creating new entry`);
        // Create a new item if it doesn't exist
        const putParams = {
          TableName: TABLE_NAME,
          Item: {
            id,
            [metadataType.toLowerCase()]: value
          }
        };

        await docClient.send(new PutCommand(putParams));
        console.log(`Created new entry for image: ${id} with ${metadataType}: ${value}`);
        continue;
      }

      // Update the item in DynamoDB with the new metadata
      const updateParams = {
        TableName: TABLE_NAME,
        Key: { id },
        UpdateExpression: `SET ${metadataType.toLowerCase()} = :value`,
        ExpressionAttributeValues: {
          ':value': value
        },
        ReturnValues: "ALL_NEW" as ReturnValue
      };

      console.log("Updating item:", updateParams);
      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated metadata for image: ${id}, type: ${metadataType}, value: ${value}`);
    } catch (error) {
      console.error("Error processing metadata update message:", error);
    }
  }
  
  console.log("AddMetadata Lambda execution completed");
};
