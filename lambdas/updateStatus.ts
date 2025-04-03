import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_STATUSES = ['Pass', 'Reject'];

export const handler: SQSHandler = async (event) => {
  console.log("UpdateStatus Lambda triggered with event: ", JSON.stringify(event, null, 2));
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
      if (message.message_type !== 'status_update') {
        console.log(`Unexpected message type: ${message.message_type}, expected 'status_update'`);
        continue;
      }

      // Extract status update details
      const { id, date, update } = message;
      console.log(`Extracted details - id: ${id}, date: ${date}, update: ${JSON.stringify(update)}`);
      
      if (!id || !update || !update.status) {
        console.log("Missing required fields (id, update, or status), skipping message");
        continue;
      }

      // Validate status
      if (!VALID_STATUSES.includes(update.status)) {
        console.log(`Invalid status: ${update.status}, must be 'Pass' or 'Reject'`);
        continue;
      }

      console.log(`Updating status for image: ${id} to: ${update.status}`);

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

      // Update the item in DynamoDB with the new status
      const updateParams = {
        TableName: TABLE_NAME,
        Key: { id },
        UpdateExpression: 'SET #status = :status, reviewDate = :date, reason = :reason',
        ExpressionAttributeNames: {
          '#status': 'status'  // Using ExpressionAttributeNames for 'status' as it's a reserved word
        },
        ExpressionAttributeValues: {
          ':status': update.status,
          ':date': date || new Date().toISOString(),
          ':reason': update.reason || 'No reason provided'
        },
        ReturnValues: "ALL_NEW" as ReturnValue
      };

      console.log(`Updating DynamoDB with params: ${JSON.stringify(updateParams)}`);
      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated status for image: ${id}, updated item: ${JSON.stringify(result.Attributes)}`);
    } catch (error) {
      console.error("Error processing status update message:", error);
      // Continue processing other records even if one fails
    }
  }
  
  console.log("UpdateStatus Lambda execution completed successfully");
};
