import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as path from 'path';
import * as lambdanode from 'aws-cdk-lib/aws-lambda-nodejs';

/**
 * EDA Application Stack for Photo Gallery
 * Stage 6: Status Update Mailer
 */
export class EDAAppStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // DynamoDB table for storing image metadata
    const imagesTable = new dynamodb.Table(this, 'ImagesTable', {
      partitionKey: { name: 'id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
    });

    // S3 bucket for storing images
    const imagesBucket = new s3.Bucket(this, 'Images', {
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      cors: [
        {
          allowedMethods: [
            s3.HttpMethods.GET,
            s3.HttpMethods.POST,
            s3.HttpMethods.PUT,
          ],
          allowedOrigins: ['*'],
          allowedHeaders: ['*'],
        },
      ],
    });

    // SNS topic for new image notifications
    const newImageTopic = new sns.Topic(this, 'NewImageTopic');

    // SQS queues for different types of processing
    const validImageQueue = new sqs.Queue(this, 'ValidImageQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(7),
    });

    const metadataUpdateQueue = new sqs.Queue(this, 'MetadataUpdateQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(7),
    });

    const statusUpdateQueue = new sqs.Queue(this, 'StatusUpdateQueue', {
      visibilityTimeout: cdk.Duration.seconds(30),
      retentionPeriod: cdk.Duration.days(7),
    });

    // Lambda functions
    const logImageFunction = new lambdanode.NodejsFunction(this, 'LogImageFunction', {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: path.join(__dirname, '../lambdas/logImage.ts'),
      timeout: cdk.Duration.seconds(15),
      environment: {
        IMAGES_TABLE: imagesTable.tableName,
      },
    });

    const removeImageFunction = new lambdanode.NodejsFunction(this, 'RemoveImageFunction', {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: path.join(__dirname, '../lambdas/removeImage.ts'),
      timeout: cdk.Duration.seconds(15),
      environment: {
        IMAGES_TABLE: imagesTable.tableName,
      },
    });

    const addMetadataFunction = new lambdanode.NodejsFunction(this, 'AddMetadataFunction', {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: path.join(__dirname, '../lambdas/addMetadata.ts'),
      timeout: cdk.Duration.seconds(15),
      environment: {
        IMAGES_TABLE: imagesTable.tableName,
      },
    });

    const updateStatusFunction = new lambdanode.NodejsFunction(this, 'UpdateStatusFunction', {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: path.join(__dirname, '../lambdas/updateStatus.ts'),
      timeout: cdk.Duration.seconds(15),
      environment: {
        IMAGES_TABLE: imagesTable.tableName,
      },
    });

    const statusMailerFunction = new lambdanode.NodejsFunction(this, 'StatusMailerFunction', {
      runtime: lambda.Runtime.NODEJS_18_X,
      entry: path.join(__dirname, '../lambdas/statusMailer.ts'),
      timeout: cdk.Duration.seconds(15),
      environment: {
        IMAGES_TABLE: imagesTable.tableName,
        SES_REGION: 'eu-west-1',
        SES_EMAIL_FROM: 'your-verified-email@example.com',
      },
    });

    // Grant permissions
    imagesTable.grantReadWriteData(logImageFunction);
    imagesTable.grantReadWriteData(removeImageFunction);
    imagesTable.grantReadWriteData(addMetadataFunction);
    imagesTable.grantReadWriteData(updateStatusFunction);
    imagesTable.grantReadData(statusMailerFunction);
    
    imagesBucket.grantRead(logImageFunction);
    imagesBucket.grantReadWrite(removeImageFunction);

    // Add SES permissions to the status mailer function
    statusMailerFunction.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ['ses:SendEmail', 'ses:SendRawEmail'],
        resources: ['*'],
      })
    );

    // Event sources
    logImageFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(validImageQueue)
    );

    addMetadataFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(metadataUpdateQueue)
    );

    updateStatusFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(statusUpdateQueue)
    );

    statusMailerFunction.addEventSource(
      new lambdaEventSources.DynamoEventSource(imagesTable, {
        startingPosition: lambda.StartingPosition.LATEST,
        filters: [
          {
            pattern: '{ "dynamodb": { "NewImage": { "status": { "S": [{ "anything-but": null }] } } } }',
          },
        ],
        retryAttempts: 3,
      })
    );

    // S3 -> SNS: Send notification when an object is created in the bucket
    imagesBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.SnsDestination(newImageTopic)
    );

    // SNS -> SQS: Subscribe queue to SNS topic with filter for image events
    newImageTopic.addSubscription(
      new subs.SqsSubscription(validImageQueue, {
        filterPolicy: {
          "message_type": sns.SubscriptionFilter.stringFilter({
            allowlist: ["s3_event"],
          }),
        },
        rawMessageDelivery: true
      })
    );

    // SNS -> SQS: Subscribe queue to SNS topic with filter for metadata events
    newImageTopic.addSubscription(
      new subs.SqsSubscription(metadataUpdateQueue, {
        filterPolicy: {
          "message_type": sns.SubscriptionFilter.stringFilter({
            allowlist: ["metadata_update"],
          }),
        },
        rawMessageDelivery: true
      })
    );

    // SNS -> SQS: Subscribe queue to SNS topic for status updates
    newImageTopic.addSubscription(
      new subs.SqsSubscription(statusUpdateQueue, {
        filterPolicy: {
          "message_type": sns.SubscriptionFilter.stringFilter({
            allowlist: ["status_update"],
          }),
        },
        rawMessageDelivery: true
      })
    );

    // Outputs
    new cdk.CfnOutput(this, 'bucketName', {
      value: imagesBucket.bucketName,
    });

    new cdk.CfnOutput(this, 'tableName', {
      value: imagesTable.tableName,
    });

    new cdk.CfnOutput(this, 'topicArn', {
      value: newImageTopic.topicArn,
    });
  }
}
