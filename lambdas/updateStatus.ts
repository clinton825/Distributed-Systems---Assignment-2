import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand, GetCommand, PutCommand } from "@aws-sdk/lib-dynamodb";

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
      console.log("Processing record:", JSON.stringify(record, null, 2));
      let message;
      
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
        }
      } catch (error) {
        console.error("Error parsing message:", error);
        continue;
      }

      // Extract message fields based on schema in requirements
      const { id, date, update } = message;
      
      if (!id || !update || !update.status) {
        console.log("Missing required fields (id, update, or status), skipping message");
        continue;
      }

      if (!VALID_STATUSES.includes(update.status)) {
        console.log(`Invalid status: ${update.status}, must be one of ${VALID_STATUSES.join(', ')}`);
        continue;
      }

      console.log(`Processing status update for image: ${id}, status: ${update.status}, reason: ${update.reason}`);

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
            status: update.status,
            reason: update.reason || 'No reason provided',
            reviewDate: date || new Date().toISOString()
          }
        };

        await docClient.send(new PutCommand(putParams));
        console.log(`Created new entry for image: ${id} with status: ${update.status}`);
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

      console.log("Updating item:", updateParams);
      const result = await docClient.send(new UpdateCommand(updateParams));
      console.log(`Successfully updated status for image: ${id}, status: ${update.status}, reason: ${update.reason}`);
    } catch (error) {
      console.error("Error processing status update message:", error);
    }
  }
  
  console.log("UpdateStatus Lambda execution completed");
};
