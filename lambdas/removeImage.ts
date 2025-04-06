import { SQSHandler } from "aws-lambda";
import { S3Client, DeleteObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

export const handler: SQSHandler = async (event) => {
  console.log("RemoveImage Lambda triggered");
  console.log("Processing DLQ event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      console.log("Processing DLQ record: ", JSON.stringify(record, null, 2));
      
      // Parse the message body
      let messageBody;
      try {
        if (typeof record.body === 'string') {
          messageBody = JSON.parse(record.body);
        } else {
          messageBody = record.body;
        }
        console.log("Parsed message body:", JSON.stringify(messageBody, null, 2));
      } catch (error) {
        console.error("Error parsing message body:", error);
        continue;
      }
      
      // CASE 1: Handle SNS notifications
      if (messageBody.Type === 'Notification' && messageBody.Message) {
        let s3Event;
        try {
          if (typeof messageBody.Message === 'string') {
            s3Event = JSON.parse(messageBody.Message);
          } else {
            s3Event = messageBody.Message;
          }
          console.log("Parsed S3 event from SNS:", JSON.stringify(s3Event, null, 2));
          
          await processS3Event(s3Event);
        } catch (error) {
          console.error("Error parsing SNS message:", error);
          continue;
        }
      } 
      // CASE 2: Handle direct S3 event in the DLQ message
      else if (messageBody.Records && messageBody.Records[0]?.s3) {
        console.log("Found direct S3 event in message body");
        await processS3Event(messageBody);
      }
      // CASE 3: Look for original S3 record in the body.messageAttributes
      else if (messageBody.messageAttributes) {
        console.log("Looking for S3 information in message attributes");
        try {
          const bucketName = messageBody.messageAttributes?.bucketName?.stringValue;
          const objectKey = messageBody.messageAttributes?.objectKey?.stringValue;
          
          if (bucketName && objectKey) {
            console.log(`Found S3 object info in attributes: ${objectKey} in bucket: ${bucketName}`);
            await deleteS3Object(bucketName, objectKey);
          }
        } catch (error) {
          console.error("Error processing message attributes:", error);
        }
      }
      // CASE 4: Try to extract from the error message itself
      else {
        console.log("Attempting to find S3 information in the error message");
        
        try {
          const errorMessage = messageBody.errorMessage || record.body;
          
          if (typeof errorMessage === 'string' && 
             (errorMessage.includes('S3') || errorMessage.includes('bucket'))) {
            
            const recordStr = JSON.stringify(record);
            
            const objectKeyMatch = recordStr.match(/"([^"]+\.txt)"/);
            if (objectKeyMatch && objectKeyMatch[1]) {
              const objectKey = objectKeyMatch[1];
              
              const bucketMatch = recordStr.match(/"bucket":\s*{\s*"name":\s*"([^"]+)"/);
              if (bucketMatch && bucketMatch[1]) {
                const bucketName = bucketMatch[1];
                console.log(`Extracted from error: ${objectKey} in bucket: ${bucketName}`);
                await deleteS3Object(bucketName, objectKey);
              }
            }
          }
        } catch (error) {
          console.error("Error trying to extract S3 info from error message:", error);
        }
      }
    } catch (error) {
      console.error("Error processing record:", error);
    }
  }
  
  console.log("RemoveImage Lambda execution completed");
};

// Helper function to process an S3 event
async function processS3Event(s3Event: any): Promise<void> {
  if (s3Event.Records && s3Event.Records.length > 0) {
    const s3Record = s3Event.Records[0];
    if (s3Record.s3 && s3Record.s3.bucket && s3Record.s3.object) {
      const bucketName = s3Record.s3.bucket.name;
      const objectKey = decodeURIComponent(s3Record.s3.object.key.replace(/\+/g, " "));
      
      console.log(`Found S3 object: ${objectKey} in bucket: ${bucketName}`);
      
      try {
        await deleteS3Object(bucketName, objectKey);
      } catch (error) {
        console.error(`Error deleting S3 object:`, error);
      }
    }
  }
}

async function deleteS3Object(bucketName: string, objectKey: string): Promise<void> {
  const deleteParams = {
    Bucket: bucketName,
    Key: objectKey,
  };
  
  try {
    // Check if the object exists first
    try {
      const headParams = {
        Bucket: bucketName,
        Key: objectKey,
      };
      await s3Client.send(new HeadObjectCommand(headParams));
      console.log(`Object ${objectKey} exists in bucket ${bucketName}.`);
    } catch (error) {
      console.log(`Object ${objectKey} does not exist in bucket ${bucketName}. Skipping deletion.`);
      return;
    }
    
    // Delete the object
    await s3Client.send(new DeleteObjectCommand(deleteParams));
    console.log(`Successfully removed invalid file: ${objectKey}`);
  } catch (error) {
    console.error(`Error deleting object ${objectKey} from bucket ${bucketName}:`, error);
    throw error;
  }
}
