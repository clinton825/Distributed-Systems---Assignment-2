import { SQSHandler } from "aws-lambda";
import { DynamoDBClient, ReturnValue } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_METADATA_TYPES = ['Caption', 'Date', 'name'];

export const handler: SQSHandler = async (event) => {
  console.log("Processing metadata event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      const recordBody = JSON.parse(record.body);
      const messageAttributes = recordBody.MessageAttributes;
      const snsMessage = JSON.parse(recordBody.Message);

      // Validate metadata type
      if (!messageAttributes || !messageAttributes.metadata_type) {
        console.log("Missing metadata_type attribute, skipping message");
        continue;
      }

      const metadataType = messageAttributes.metadata_type.StringValue;
      
      if (!VALID_METADATA_TYPES.includes(metadataType)) {
        console.log(`Invalid metadata type: ${metadataType}, skipping message`);
        continue;
      }

      // Extract metadata from the message
      const { id, value } = snsMessage;
      
      if (!id || !value) {
        console.log("Missing required fields (id or value), skipping message");
        continue;
      }

      console.log(`Updating ${metadataType} metadata for image: ${id} with value: ${value}`);

      // Update the item in DynamoDB
      const attributeName = metadataType.toLowerCase(); // Convert to lowercase for DB attribute names
      
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
      console.log(`Successfully updated ${metadataType} for image: ${id}`, result);
    } catch (error) {
      console.error("Error processing metadata message:", error);
      // Continue processing other records even if one fails
    }
  }
};
