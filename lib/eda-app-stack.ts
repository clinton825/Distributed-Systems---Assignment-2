import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as events from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sns from "aws-cdk-lib/aws-sns";
import * as subs from "aws-cdk-lib/aws-sns-subscriptions";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as iam from "aws-cdk-lib/aws-iam";

import { Construct } from "constructs";

/**
 * EDA Application Stack for Photo Gallery
 * Stage 6: Status Update Mailer
 */
export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Bucket for storing images
    const imagesBucket = new s3.Bucket(this, "images", {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      publicReadAccess: false,
    });

    // DynamoDB table for storing image metadata
    const imagesTable = new dynamodb.Table(this, "ImagesTable", {
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
    });

    // SNS Topic for messaging
    const newImageTopic = new sns.Topic(this, "NewImageTopic", {
      displayName: "New Image Topic",
    });

    // Dead Letter Queue for invalid image files
    const deadLetterQueue = new sqs.Queue(this, "DeadLetterQueue", {
      queueName: "InvalidImagesQueue",
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    // SQS Queue for valid image processing with DLQ
    const validImageQueue = new sqs.Queue(this, "ValidImageQueue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
      deadLetterQueue: {
        queue: deadLetterQueue,
        maxReceiveCount: 3,
      },
    });

    // Queue for metadata updates
    const metadataUpdateQueue = new sqs.Queue(this, "MetadataUpdateQueue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    // Queue for status updates
    const statusUpdateQueue = new sqs.Queue(this, "StatusUpdateQueue", {
      receiveMessageWaitTime: cdk.Duration.seconds(10),
    });

    // Lambda Function for logging images
    const logImageFn = new lambdanode.NodejsFunction(
      this,
      "LogImageFunction",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/logImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          IMAGES_TABLE: imagesTable.tableName,
        },
      }
    );

    // Grant permissions for the Lambda functions
    // LogImage function needs to read from S3 and write to DynamoDB
    imagesBucket.grantRead(logImageFn);
    imagesTable.grantWriteData(logImageFn);

    // Lambda Function for removing invalid images
    const removeImageFn = new lambdanode.NodejsFunction(
      this,
      "RemoveImageFunction",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/removeImage.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
      }
    );
    
    // RemoveImage function needs to delete from S3
    imagesBucket.grantDelete(removeImageFn);

    // Lambda Function for adding metadata to images
    const addMetadataFn = new lambdanode.NodejsFunction(
      this,
      "AddMetadataFunction",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/addMetadata.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          IMAGES_TABLE: imagesTable.tableName,
        },
      }
    );
    
    // AddMetadata function needs to read and write to DynamoDB
    imagesTable.grantReadWriteData(addMetadataFn);

    // Lambda Function for updating image status
    const updateStatusFn = new lambdanode.NodejsFunction(
      this,
      "UpdateStatusFunction",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/updateStatus.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          IMAGES_TABLE: imagesTable.tableName,
        },
      }
    );
    
    // UpdateStatus function needs to read and write to DynamoDB
    imagesTable.grantReadWriteData(updateStatusFn);

    // Lambda Function for sending status update emails
    const statusMailerFn = new lambdanode.NodejsFunction(
      this,
      "StatusMailerFunction",
      {
        runtime: lambda.Runtime.NODEJS_18_X,
        entry: `${__dirname}/../lambdas/statusMailer.ts`,
        timeout: cdk.Duration.seconds(15),
        memorySize: 128,
        environment: {
          IMAGES_TABLE: imagesTable.tableName,
        },
      }
    );
    
    // StatusMailer function needs to read from DynamoDB and send emails via SES
    imagesTable.grantReadData(statusMailerFn);
    // Grant SES permissions (requires a specific policy for sending emails)
    statusMailerFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['ses:SendEmail', 'ses:SendRawEmail'],
        resources: ['*'], // Scope this down to specific resources in production
      })
    );

    // S3 -> SNS: Send object creation events to SNS topic
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    // SNS -> SQS: Subscribe queue to SNS topic
    newImageTopic.addSubscription(
      new subs.SqsSubscription(validImageQueue)
    );

    // SNS -> SQS: Subscribe queue to SNS topic with filter for metadata events
    newImageTopic.addSubscription(
      new subs.SqsSubscription(metadataUpdateQueue, {
        // Requires a MessageAttribute with metadata_type
        filterPolicy: {
          metadata_type: sns.SubscriptionFilter.existsFilter(),
        },
      })
    );

    // SNS -> SQS: Subscribe queue to SNS topic with filter for status updates
    newImageTopic.addSubscription(
      new subs.SqsSubscription(statusUpdateQueue, {
        // Matches messages with update.status in the JSON structure
        filterPolicy: {
          "update.status": sns.SubscriptionFilter.stringFilter({
            allowlist: ["Pass", "Reject"],
          }),
        },
        rawMessageDelivery: true
      })
    );

    // SQS -> Lambda: Process valid image events
    const validImageEventSource = new events.SqsEventSource(validImageQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    logImageFn.addEventSource(validImageEventSource);

    // DLQ -> Lambda: Process invalid image events
    const dlqEventSource = new events.SqsEventSource(deadLetterQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    removeImageFn.addEventSource(dlqEventSource);

    // SQS -> Lambda: Process metadata update events
    const metadataEventSource = new events.SqsEventSource(metadataUpdateQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    addMetadataFn.addEventSource(metadataEventSource);

    // SQS -> Lambda: Process status update events
    const statusUpdateEventSource = new events.SqsEventSource(statusUpdateQueue, {
      batchSize: 5,
      maxBatchingWindow: cdk.Duration.seconds(5),
    });
    updateStatusFn.addEventSource(statusUpdateEventSource);

    // DynamoDB Stream -> Lambda: Trigger status mailer on status updates
    const streamEventSource = new events.DynamoEventSource(imagesTable, {
      startingPosition: lambda.StartingPosition.TRIM_HORIZON,
      batchSize: 5,
      bisectBatchOnError: true,
      retryAttempts: 10,
      filters: [
        {
          pattern: JSON.stringify({
            dynamodb: {
              NewImage: {
                status: {
                  S: ["Pass", "Reject"],
                },
              },
            },
          }),
        },
      ],
    });
    statusMailerFn.addEventSource(streamEventSource);

    // Outputs
    new cdk.CfnOutput(this, "bucketName", {
      value: imagesBucket.bucketName,
    });
    
    new cdk.CfnOutput(this, "tableName", {
      value: imagesTable.tableName,
    });
    
    new cdk.CfnOutput(this, "topicArn", {
      value: newImageTopic.topicArn,
    });
  }
}
