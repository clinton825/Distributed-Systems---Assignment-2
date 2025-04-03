# Event-Driven Architecture Photo Gallery Application

This project implements a photo gallery application using AWS services and an event-driven architecture. The application supports functionalities such as image logging, metadata updating, invalid image removal, status updating, and email notifications.

## Architecture Overview

The application uses the following AWS services:

- **S3**: For storing images
- **SNS**: For messaging and event notifications
- **SQS**: For processing events asynchronously
- **DynamoDB**: For storing image metadata
- **Lambda**: For serverless processing of events
- **SES**: For sending email notifications

## Key Components

### Lambda Functions

1. **logImage**: Processes S3 events and logs images to DynamoDB
2. **addMetadata**: Processes metadata update messages and updates corresponding DynamoDB records
3. **updateStatus**: Processes status update messages and updates image status in DynamoDB
4. **removeImage**: Removes invalid images from S3
5. **statusMailer**: Sends email notifications when image statuses change

### Event Flow

1. Images are uploaded to S3
2. S3 triggers an SNS notification
3. SNS routes messages to appropriate SQS queues based on message attributes
4. Lambda functions process messages from SQS queues
5. DynamoDB streams trigger additional Lambda functions for status changes

## Message Filtering

The application implements advanced message filtering using SNS subscription filter policies. Each SQS queue subscribes to the SNS topic with a specific filter policy based on the `message_type` attribute:

- `s3_event`: For S3 object creation events
- `metadata_update`: For metadata update messages
- `status_update`: For status update messages

## Deployment

The application is deployed using AWS CDK. To deploy the application:

```bash
npm run cdk -- deploy
```

## Testing

### Upload an Image

```bash
aws s3 cp images/sunflower.jpeg s3://BUCKET_NAME/
```

### Update Metadata

```bash
aws sns publish --topic-arn "TOPIC_ARN" --message-attributes '{"message_type":{"DataType":"String","StringValue":"metadata_update"}}' --message '{"id":"sunflower.jpeg","value":"Beautiful yellow sunflower in a green field","message_type":"metadata_update","metadata_type":"description"}'
```

### Update Status

```bash
aws sns publish --topic-arn "TOPIC_ARN" --message-attributes '{"message_type":{"DataType":"String","StringValue":"status_update"}}' --message '{"id":"sunflower.jpeg","date":"2025-04-04T00:55:00Z","update":{"status":"Reject","reason":"Changed my mind - composition could be improved"},"message_type":"status_update"}'
```

## Environment Variables

- `SES_REGION`: AWS region for SES (default: 'eu-west-1')
- `SES_EMAIL_FROM`: Email address used to send notifications
- `IMAGES_TABLE`: Name of the DynamoDB table for storing image metadata

## Development

This project uses TypeScript and AWS CDK. To set up the development environment:

```bash
npm install
```

To compile TypeScript to JavaScript:

```bash
npm run build
```

To run tests:

```bash
npm test
```
