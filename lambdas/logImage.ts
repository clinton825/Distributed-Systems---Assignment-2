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
  console.log("LogImage Lambda triggered");
  
  if (!TABLE_NAME) {
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      let message;
      try {
        message = JSON.parse(record.body);
        
        if (message.Records && message.Records[0]?.eventSource === 'aws:s3') {
          // Direct S3 event
        } else if (message.Message) {
          // SNS-wrapped message
          const innerMessage = JSON.parse(message.Message);
          
          if (innerMessage.Records && innerMessage.Records[0]?.eventSource === 'aws:s3') {
            message = innerMessage;
          }
        }
      } catch (error) {
        console.error("Error parsing message body:", error);
        continue;
      }

      if (message.Records && message.Records[0]?.eventSource === 'aws:s3') {
        for (const messageRecord of message.Records) {
          if (messageRecord.eventSource === 'aws:s3' && messageRecord.eventName.startsWith('ObjectCreated:')) {
            const s3Info = messageRecord.s3;
            const bucketName = s3Info.bucket.name;
            const objectKey = decodeURIComponent(s3Info.object.key.replace(/\+/g, " "));
            
            try {
              // Validate file extension
              const fileExtension = objectKey.substring(objectKey.lastIndexOf('.')).toLowerCase();
              
              if (!VALID_EXTENSIONS.includes(fileExtension)) {
                console.log(`Invalid file extension: ${fileExtension} for object ${objectKey}`);
                throw new Error(`Invalid file extension: ${fileExtension}`);
              }

              // Get the image metadata from S3
              const headObjectParams = {
                Bucket: bucketName,
                Key: objectKey,
              };
              
              const headObjectResponse = await s3Client.send(new HeadObjectCommand(headObjectParams));
              
              // Create a new entry in DynamoDB
              const item = {
                id: objectKey,
                uploadTime: new Date().toISOString(),
                size: headObjectResponse.ContentLength,
                type: headObjectResponse.ContentType,
              };
              
              const putParams = {
                TableName: TABLE_NAME,
                Item: item,
              };

              await docClient.send(new PutCommand(putParams));
              console.log(`Successfully logged image to DynamoDB: ${objectKey}`);
            } catch (error) {
              console.error(`Error processing image ${objectKey}:`, error);
              throw error;
            }
          }
        }
      } else {
        console.log("Message does not contain S3 Records, ignoring");
      }
    } catch (error) {
      console.error("Error processing SQS message:", error);
      throw error;
    }
  }
};
