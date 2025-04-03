import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_STATUSES = ['Pass', 'Reject'];

export const handler: SQSHandler = async (event) => {
  console.log("UpdateStatus Lambda triggered with event: ", JSON.stringify(event, null, 2));
  
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

      if (message.message_type !== 'status_update') {
        console.log(`Unexpected message type: ${message.message_type}, expected 'status_update'`);
        continue;
      }

      const { id, date, update } = message;
      
      if (!id || !update || !update.status) {
        console.log("Missing required fields (id, update, or status), skipping message");
        continue;
      }

      if (!VALID_STATUSES.includes(update.status)) {
        console.log(`Invalid status: ${update.status}, must be 'Pass' or 'Reject'`);
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

      // Update the item in DynamoDB with the new status
      const updateParams = {
        TableName: TABLE_NAME,
        Key: { id },
        UpdateExpression: 'SET #status = :status, reviewDate = :date, reason = :reason',
        ExpressionAttributeNames: {
          '#status': 'status'
        },
        ExpressionAttributeValues: {
          ':status': update.status,
          ':date': date || new Date().toISOString(),
          ':reason': update.reason || 'No reason provided'
        },
        ReturnValues: "ALL_NEW" as ReturnValue
      };

      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated status for image: ${id}, status: ${update.status}`);
    } catch (error) {
      console.error("Error processing status update message:", error);
    }
  }
};
