import { DynamoDBStreamHandler } from "aws-lambda";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand } from "@aws-sdk/lib-dynamodb";
import { SES_EMAIL_FROM, SES_REGION, SES_EMAIL_TO } from "../env";

const dynamoClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const sesClient = new SESClient({ region: SES_REGION });

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

export const handler: DynamoDBStreamHandler = async (event) => {
  console.log("StatusMailer Lambda triggered");

  if (!TABLE_NAME) {
    throw new Error("TABLE_NAME environment variable is not set");
  }

  for (const record of event.Records) {
    try {
      if (record.eventName !== 'MODIFY') {
        continue;
      }

      const newImage = record.dynamodb?.NewImage;
      const oldImage = record.dynamodb?.OldImage;
      
      if (!newImage || !oldImage) {
        console.log("Missing new or old image data, skipping record");
        continue;
      }

      const newStatus = newImage.status?.S;
      const oldStatus = oldImage.status?.S;
      
      if (newStatus === oldStatus) {
        console.log("Status unchanged, skipping notification");
        continue;
      }

      const imageId = newImage.id?.S;
      if (!imageId) {
        console.log("Missing image ID, skipping notification");
        continue;
      }

      console.log(`Processing status change for image ${imageId}: ${oldStatus} -> ${newStatus}`);

      // Get the full image details from DynamoDB
      const getParams = {
        TableName: TABLE_NAME,
        Key: { id: imageId }
      };
      
      const result = await docClient.send(new GetCommand(getParams));
      const imageDetails = result.Item as ImageDetails;
      
      if (!imageDetails) {
        console.log(`Image with id ${imageId} not found in database, skipping notification`);
        continue;
      }

      // Get photographer details (in a real app, this might come from a user database)
      const photographerName = imageDetails.name || "Photographer";
      const toEmail = SES_EMAIL_TO; // Using the verified email from env.ts

      // Send email notification
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

      try {
        await sesClient.send(new SendEmailCommand(emailParams));
        console.log(`Status update email sent to ${toEmail} for image ${imageId}`);
      } catch (error) {
        console.error("Error sending email:", error);
        console.log("Email content that would have been sent:");
        console.log(`To: ${toEmail}`);
        console.log(`Subject: Photo Status Update: ${imageDetails.status}`);
        console.log(`Text Content: ${getTextContent(imageDetails, photographerName)}`);
      }
    } catch (error) {
      console.error("Error processing status change:", error);
    }
  }
};

function getHtmlContent(imageDetails: ImageDetails, photographerName: string): string {
  const statusColor = imageDetails.status === 'Pass' ? 'green' : 'red';
  
  return `
    <!DOCTYPE html>
    <html>
      <head>
        <style>
          body { font-family: Arial, sans-serif; line-height: 1.6; }
          .container { max-width: 600px; margin: 0 auto; padding: 20px; }
          .header { background-color: #f0f0f0; padding: 10px; border-radius: 5px; }
          .status { font-weight: bold; color: ${statusColor}; }
          .details { margin: 20px 0; }
          .footer { font-size: 12px; color: #666; margin-top: 30px; }
        </style>
      </head>
      <body>
        <div class="container">
          <div class="header">
            <h2>Photo Status Update</h2>
          </div>
          <p>Hello ${photographerName},</p>
          <p>Your photo "${imageDetails.id}" has been reviewed and its status has been updated.</p>
          <div class="details">
            <p><strong>Status:</strong> <span class="status">${imageDetails.status}</span></p>
            <p><strong>Reason:</strong> ${imageDetails.reason || 'No reason provided'}</p>
            ${imageDetails.date ? `<p><strong>Review Date:</strong> ${imageDetails.date}</p>` : ''}
          </div>
          <p>Thank you for your submission.</p>
          <div class="footer">
            <p>This is an automated message, please do not reply to this email.</p>
          </div>
        </div>
      </body>
    </html>
  `;
}

function getTextContent(imageDetails: ImageDetails, photographerName: string): string {
  return `
    Photo Status Update
    
    Hello ${photographerName},
    
    Your photo "${imageDetails.id}" has been reviewed and its status has been updated.
    
    Status: ${imageDetails.status}
    Reason: ${imageDetails.reason || 'No reason provided'}
    ${imageDetails.date ? `Review Date: ${imageDetails.date}` : ''}
    
    Thank you for your submission.
  `;
}
