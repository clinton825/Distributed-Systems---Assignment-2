import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_STATUSES = ['Pass', 'Reject'];

export const handler: SQSHandler = async (event) => {
  console.log("Processing status update event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      const recordBody = JSON.parse(record.body);
      const snsMessage = JSON.parse(recordBody.Message);

      // Extract status update details
      const { id, date, update } = snsMessage;
      
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
          '#status': 'status'  // Using ExpressionAttributeNames for 'status' as it's a reserved word
        },
        ExpressionAttributeValues: {
          ':status': update.status,
          ':date': date || new Date().toISOString(),
          ':reason': update.reason || 'No reason provided'
        },
        ReturnValues: "ALL_NEW" as ReturnValue
      };

      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated status for image: ${id}`, result);
    } catch (error) {
      console.error("Error processing status update message:", error);
      // Continue processing other records even if one fails
    }
  }
};
