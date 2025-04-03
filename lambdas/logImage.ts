import { SQSHandler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const s3Client = new S3Client({});

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_EXTENSIONS = ['.jpeg', '.jpg', '.png'];

export const handler: SQSHandler = async (event) => {
  console.log("LogImage Lambda triggered with event: ", JSON.stringify(event, null, 2));
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
        // For S3 events, we may receive the message in different formats
        // Try to parse the body first
        message = JSON.parse(record.body);
        console.log("Parsed message: ", JSON.stringify(message, null, 2));
        
        // If this is an S3 event wrapped in an SNS message, extract the S3 event
        if (message.Records && message.Records[0]?.eventSource === 'aws:s3') {
          // This is a direct S3 event
          console.log("Processing direct S3 event");
        } else if (message.Message) {
          // This is an SNS-wrapped message
          const innerMessage = JSON.parse(message.Message);
          console.log("Parsed inner message: ", JSON.stringify(innerMessage, null, 2));
          
          if (innerMessage.Records && innerMessage.Records[0]?.eventSource === 'aws:s3') {
            message = innerMessage;
            console.log("Extracted S3 event from SNS message");
          }
        }
      } catch (error) {
        console.error("Error parsing message body:", error);
        continue;
      }

      // Check if this is an S3 event
      if (message.Records && message.Records[0]?.eventSource === 'aws:s3') {
        for (const messageRecord of message.Records) {
          if (messageRecord.eventSource === 'aws:s3' && messageRecord.eventName.startsWith('ObjectCreated:')) {
            const s3Info = messageRecord.s3;
            const bucketName = s3Info.bucket.name;
            // Handle URL encoding in the object key
            const objectKey = decodeURIComponent(s3Info.object.key.replace(/\+/g, " "));
            
            console.log(`Processing S3 object: ${bucketName}/${objectKey}`);
            
            try {
              // Validate file extension
              const fileExtension = objectKey.substring(objectKey.lastIndexOf('.')).toLowerCase();
              console.log(`File extension: ${fileExtension}`);
              
              if (!VALID_EXTENSIONS.includes(fileExtension)) {
                console.log(`Invalid file extension: ${fileExtension} for object ${objectKey}`);
                throw new Error(`Invalid file extension: ${fileExtension}`);
              }

              // Get the image metadata from S3
              const headObjectParams = {
                Bucket: bucketName,
                Key: objectKey,
              };
              console.log(`Getting S3 object metadata with params: ${JSON.stringify(headObjectParams)}`);
              
              const headObjectResponse = await s3Client.send(new HeadObjectCommand(headObjectParams));
              console.log(`S3 object metadata: ${JSON.stringify(headObjectResponse)}`);
              
              // Create a new entry in DynamoDB
              const item = {
                id: objectKey,
                uploadTime: new Date().toISOString(),
                size: headObjectResponse.ContentLength,
                type: headObjectResponse.ContentType,
              };

              console.log(`Creating DynamoDB item: ${JSON.stringify(item)}`);
              
              const putParams = {
                TableName: TABLE_NAME,
                Item: item,
              };

              const result = await docClient.send(new PutCommand(putParams));
              console.log(`Successfully logged image to DynamoDB: ${objectKey}, result: ${JSON.stringify(result)}`);
            } catch (error) {
              console.error(`Error processing image ${objectKey}:`, error);
              // Re-throw to allow SQS retry/DLQ mechanism to handle invalid files
              throw error;
            }
          } else {
            console.log(`Ignoring non-S3 creation event or non-S3 event: ${JSON.stringify(messageRecord)}`);
          }
        }
      } else {
        console.log("Message does not contain S3 Records, ignoring");
      }
    } catch (error) {
      console.error("Error processing SQS message:", error);
      // Re-throw to allow SQS retry mechanism
      throw error;
    }
  }
  
  console.log("LogImage Lambda execution completed successfully");
};
