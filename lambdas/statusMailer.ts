import { DynamoDBStreamEvent, DynamoDBRecord } from "aws-lambda";
import { SES_EMAIL_FROM, SES_REGION } from "../env";
import {
  SESClient,
  SendEmailCommand,
  SendEmailCommandInput,
} from "@aws-sdk/client-ses";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand } from "@aws-sdk/lib-dynamodb";

if (!SES_EMAIL_FROM || !SES_REGION) {
  throw new Error(
    "Please add the SES_EMAIL_FROM and SES_REGION environment variables in an env.ts file"
  );
}

const sesClient = new SESClient({ region: SES_REGION });
const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const TABLE_NAME = process.env.IMAGES_TABLE || '';

type ImageDetails = {
  id: string;
  name?: string; // Photographer's name
  status: string;
  reason: string;
  caption?: string;
  date?: string;
  email?: string;
};

export const handler = async (event: DynamoDBStreamEvent) => {
  console.log("Processing DynamoDB Stream event: ", JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    // Only process MODIFY events with status changes
    if (record.eventName === 'MODIFY') {
      try {
        const newImage = record.dynamodb?.NewImage;
        const imageId = record.dynamodb?.Keys?.id?.S;
        
        if (!imageId) {
          console.log("Missing image ID, skipping");
          continue;
        }

        // Check if status was changed
        if (newImage?.status?.S) {
          const status = newImage.status.S;
          
          // Get the full image details from DynamoDB
          const getParams = {
            TableName: TABLE_NAME,
            Key: { id: imageId }
          };

          const result = await docClient.send(new GetCommand(getParams));
          const imageDetails = result.Item as ImageDetails;
          
          if (!imageDetails) {
            console.log(`Image details not found for ${imageId}`);
            continue;
          }
          
          // Default email if photographer's email not available
          const photographerEmail = imageDetails.email || SES_EMAIL_FROM;
          const photographerName = imageDetails.name || "Photographer";
          
          await sendStatusUpdateEmail(imageDetails, photographerEmail, photographerName);
          console.log(`Status update email sent for image: ${imageId}`);
        }
      } catch (error) {
        console.error("Error processing DynamoDB stream record:", error);
      }
    }
  }
};

async function sendStatusUpdateEmail(
  imageDetails: ImageDetails,
  toEmail: string,
  photographerName: string
) {
  const emailParams = {
    Destination: {
      ToAddresses: [toEmail],
    },
    Message: {
      Body: {
        Html: {
          Charset: "UTF-8",
          Data: getHtmlContent(imageDetails, photographerName),
        },
        Text: {
          Charset: "UTF-8",
          Data: getTextContent(imageDetails, photographerName),
        },
      },
      Subject: {
        Charset: "UTF-8",
        Data: `Photo Status Update: ${imageDetails.status}`,
      },
    },
    Source: SES_EMAIL_FROM,
  };

  await sesClient.send(new SendEmailCommand(emailParams));
}

function getHtmlContent(details: ImageDetails, photographerName: string) {
  return `
    <html>
      <body>
        <h2>Photo Status Update</h2>
        <p>Hello ${photographerName},</p>
        <p>Your photo <strong>${details.id}</strong> ${details.caption ? `(${details.caption})` : ''} has been reviewed.</p>
        <div style="padding: 10px; margin: 10px 0; background-color: ${details.status === 'Pass' ? '#e7f7e7' : '#f7e7e7'}; border-radius: 5px;">
          <h3>Status: <span style="color: ${details.status === 'Pass' ? 'green' : 'red'}">${details.status}</span></h3>
          <p><strong>Reason:</strong> ${details.reason || 'No reason provided'}</p>
          ${details.date ? `<p><strong>Review Date:</strong> ${details.date}</p>` : ''}
        </div>
        <p>Thank you for using our Photo Gallery service.</p>
      </body>
    </html>
  `;
}

function getTextContent(details: ImageDetails, photographerName: string) {
  return `
    Photo Status Update
    
    Hello ${photographerName},
    
    Your photo ${details.id} ${details.caption ? `(${details.caption})` : ''} has been reviewed.
    
    Status: ${details.status}
    Reason: ${details.reason || 'No reason provided'}
    ${details.date ? `Review Date: ${details.date}` : ''}
    
    Thank you for using our Photo Gallery service.
  `;
}
