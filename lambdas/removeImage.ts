import { SQSHandler } from "aws-lambda";
import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3";

const s3Client = new S3Client({});

export const handler: SQSHandler = async (event) => {
  console.log("Processing DLQ event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      // Parse the original message that failed
      const recordBody = JSON.parse(record.body);
      const snsMessage = JSON.parse(recordBody.Message);

      if (snsMessage.Records) {
        for (const messageRecord of snsMessage.Records) {
          if (messageRecord.eventSource === 'aws:s3' && messageRecord.eventName.startsWith('ObjectCreated:')) {
            const s3Info = messageRecord.s3;
            const bucketName = s3Info.bucket.name;
            const objectKey = decodeURIComponent(s3Info.object.key.replace(/\+/g, " "));
            
            console.log(`Removing invalid file: ${objectKey} from bucket: ${bucketName}`);
            
            // Delete the invalid file from S3
            const deleteParams = {
              Bucket: bucketName,
              Key: objectKey,
            };
            
            await s3Client.send(new DeleteObjectCommand(deleteParams));
            console.log(`Successfully removed invalid file: ${objectKey}`);
          }
        }
      }
    } catch (error) {
      console.error("Error processing DLQ message:", error);
      // Continue processing other records even if one fails
    }
  }
};
