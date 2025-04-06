import { S3Event, S3EventRecord, S3Handler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const s3Client = new S3Client({});

const TABLE_NAME = process.env.IMAGES_TABLE || '';
const VALID_EXTENSIONS = ['.jpeg', '.jpg', '.png'];

export const handler: S3Handler = async (event: S3Event) => {
  console.log("LogImage Lambda triggered");
  console.log("Using DynamoDB table: ", TABLE_NAME);
  
  if (!TABLE_NAME) {
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      console.log("Processing S3 record: ", JSON.stringify(record, null, 2));
      
      if (record.eventSource === 'aws:s3' && record.eventName.startsWith('ObjectCreated:')) {
        const bucketName = record.s3.bucket.name;
        const objectKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
        
        console.log(`Processing S3 object: ${objectKey} from bucket ${bucketName}`);
        
        try {
          // Validate file extension
          const fileExtension = objectKey.substring(objectKey.lastIndexOf('.')).toLowerCase();
          
          if (!VALID_EXTENSIONS.includes(fileExtension)) {
            console.log(`Invalid file extension: ${fileExtension} for object ${objectKey}`);
            throw new Error(`Invalid file extension: ${fileExtension}`);
          }

          const headObjectParams = {
            Bucket: bucketName,
            Key: objectKey,
          };
          
          const headObjectResponse = await s3Client.send(new HeadObjectCommand(headObjectParams));
          
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
          throw error; // Throw to trigger retry and eventually DLQ
        }
      } else {
        console.log(`Unexpected event source or event name: ${record.eventSource}, ${record.eventName}`);
      }
    } catch (error) {
      console.error("Error processing S3 event:", error);
      throw error; // Throw to trigger retry and eventually DLQ
    }
  }
  
  console.log("LogImage Lambda execution completed successfully");
};
