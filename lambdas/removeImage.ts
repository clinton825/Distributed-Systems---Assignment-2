import { SQSHandler } from "aws-lambda";
import { S3Client, DeleteObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

export const handler: SQSHandler = async (event) => {
  console.log("RemoveImage Lambda triggered");
  console.log("Processing DLQ event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      console.log("Processing DLQ record: ", JSON.stringify(record, null, 2));
      
      // Extract bucket name and object key using regex
      const bodyStr = typeof record.body === 'string' ? record.body : JSON.stringify(record.body);
      const bucketMatch = bodyStr.match(/"bucket":\s*{\s*"name":\s*"([^"]+)"/);
      const keyMatch = bodyStr.match(/"object":\s*{\s*"key":\s*"([^"]+)"/);
      
      if (bucketMatch && keyMatch) {
        const bucketName = bucketMatch[1];
        const objectKey = keyMatch[1];
        
        console.log(`Extracted bucket: ${bucketName} and key: ${objectKey} from message`);
        
        try {
          await deleteS3Object(bucketName, objectKey);
        } catch (deleteError) {
          console.error("Error deleting object:", deleteError);
        }
      } else {
        console.error("Could not extract S3 information from message");
        
        // Try parsing as JSON as a fallback
        try {
          let recordBody = JSON.parse(record.body);
          console.log("Parsed record body: ", JSON.stringify(recordBody, null, 2));
          
          let snsMessage;
          if (recordBody.Type === 'Notification' && recordBody.Message) {
            // This is an SNS notification
            console.log("Found SNS notification in DLQ");
            
            // Check if Message is already an object or needs to be parsed
            if (typeof recordBody.Message === 'string') {
              snsMessage = JSON.parse(recordBody.Message);
            } else {
              snsMessage = recordBody.Message;
            }
            
            console.log("Parsed SNS message: ", JSON.stringify(snsMessage, null, 2));
            
            if (snsMessage.Records) {
              for (const messageRecord of snsMessage.Records) {
                if (messageRecord.eventSource === 'aws:s3' && messageRecord.eventName.startsWith('ObjectCreated:')) {
                  const s3Info = messageRecord.s3;
                  const bucketName = s3Info.bucket.name;
                  const objectKey = decodeURIComponent(s3Info.object.key.replace(/\+/g, " "));
                  
                  console.log(`Removing invalid file: ${objectKey} from bucket: ${bucketName}`);
                  
                  try {
                    await deleteS3Object(bucketName, objectKey);
                  } catch (error) {
                    console.error(`Error removing file ${objectKey}:`, error);
                  }
                }
              }
            }
          }
        } catch (parseError) {
          console.error("Error parsing message as JSON:", parseError);
        }
      }
    } catch (error) {
      console.error("Error processing DLQ message:", error);
      // Continue processing other records even if one fails
    }
  }
  
  console.log("RemoveImage Lambda execution completed");
};

async function deleteS3Object(bucketName: string, objectKey: string): Promise<void> {
  // First check if the file exists
  const headParams = {
    Bucket: bucketName,
    Key: objectKey,
  };
  
  try {
    await s3Client.send(new HeadObjectCommand(headParams));
  } catch (error) {
    console.log(`File ${objectKey} does not exist in bucket ${bucketName}, skipping deletion`);
    return;
  }
  
  // Delete the invalid file from S3
  const deleteParams = {
    Bucket: bucketName,
    Key: objectKey,
  };
  
  await s3Client.send(new DeleteObjectCommand(deleteParams));
  console.log(`Successfully removed invalid file: ${objectKey}`);
}
