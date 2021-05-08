import { ApiKey, LambdaIntegration, MethodLoggingLevel, Period, RestApi } from '@aws-cdk/aws-apigateway';
import { Connections, InstanceClass, InstanceSize, InstanceType, IPeer, ISecurityGroup, Port, SecurityGroup, SubnetType, Vpc } from '@aws-cdk/aws-ec2';
import { AnyPrincipal, Effect, PolicyStatement } from '@aws-cdk/aws-iam';
import { Code, Function, Runtime } from '@aws-cdk/aws-lambda';
import { S3EventSource, SqsEventSource } from '@aws-cdk/aws-lambda-event-sources';
import { DatabaseInstance, DatabaseInstanceEngine, PostgresEngineVersion } from '@aws-cdk/aws-rds';
import { BlockPublicAccess, Bucket, EventType } from '@aws-cdk/aws-s3';
import { Queue } from '@aws-cdk/aws-sqs';
import * as cdk from '@aws-cdk/core';
import { Duration, RemovalPolicy } from '@aws-cdk/core';

export class ServerlessCdkAppStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const csvBucket = new Bucket(this, 'CSV-Bucket', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      bucketName: 'csv-files-for-processing',
      removalPolicy: RemovalPolicy.DESTROY
    });

    csvBucket.addToResourcePolicy(new PolicyStatement({
      actions: ['s3:PutObject'],
      effect: Effect.DENY,
      principals: [new AnyPrincipal()],
      notResources: [
        `${csvBucket.bucketArn}/*.csv`
      ]
    }));

    const csvBatchQueue = new Queue(this, 'CSVBatchesQueue', {
      queueName: 'csv-batches',
      visibilityTimeout: Duration.minutes(2),
      deadLetterQueue: {
        queue: new Queue(this, 'CSVBatchesDLQ', { queueName: 'csv-batches-dlq' }),
        maxReceiveCount: 3
      }
    });

    const csvBatchLambda = new Function(this, 'Create-CSV-Batches', {
      code: Code.fromAsset('functions/dist/batch'),
      handler: 'CreateBatch.handler',
      runtime: Runtime.NODEJS_14_X,
      functionName: 'create-csv-batch',
      timeout: Duration.minutes(1),
      memorySize: 256,
      environment: {
        SQS_URL: csvBatchQueue.queueUrl
      },
      reservedConcurrentExecutions: 10
    });

    csvBatchLambda.addEventSource(new S3EventSource(csvBucket, {
      events: [EventType.OBJECT_CREATED_PUT]
    }));

    csvBucket.grantRead(csvBatchLambda);
    csvBatchQueue.grantSendMessages(csvBatchLambda);


    const csvDataQueue = new Queue(this, 'CsvDataQueue', {
      queueName: 'csv-data',
      deadLetterQueue: {
        queue: new Queue(this, 'CsvDataDLQ', { queueName: 'csv-data-dlq' }),
        maxReceiveCount: 3
      }
    });

    const csvProcessor = new Function(this, 'ProcessCSVInBatches', {
      code: Code.fromAsset('functions/dist/enqueue'),
      handler: 'ProcessBatch.handler',
      runtime: Runtime.NODEJS_14_X,
      functionName: 'process-csv-batch',
      timeout: Duration.minutes(1),
      memorySize: 512,
      environment: {
        SQS_URL: csvDataQueue.queueUrl
      },
      reservedConcurrentExecutions: 100
    });

    csvDataQueue.grantSendMessages(csvProcessor);
    csvBucket.grantRead(csvProcessor);
    csvProcessor.addEventSource(new SqsEventSource(csvBatchQueue, { batchSize: 1 }));


    const vpc = new Vpc(this, 'CsvVpc');
    const lambdaSG = new SecurityGroup(this, 'LambdaSecurityGroup', {
      vpc,
      securityGroupName: 'LambdaSecurityGroup'
    });

    const rdsSG = new SecurityGroup(this, 'RDSSecurityGroup', {
      vpc,
      securityGroupName: 'RDSSecurityGroup'
    });

    rdsSG.node.addDependency(lambdaSG);
    rdsSG.addIngressRule(new SourceSecurityGroup(lambdaSG), Port.tcp(5432), 'Allow lambda to connect', true);

    const databaseName = 'user_db'
    const dbEngine = DatabaseInstanceEngine.postgres({ version: PostgresEngineVersion.VER_12_5 })
    const userDb = new DatabaseInstance(this, 'UserDataDB', {
      engine: dbEngine,
      instanceType: InstanceType.of(InstanceClass.T2, InstanceSize.MICRO),
      allocatedStorage: 20,
      databaseName,
      vpc,
      securityGroups: [rdsSG],
      vpcSubnets: {
        subnetType: SubnetType.PRIVATE
      }
    });


    const saveToDbLambda = new Function(this, 'SaveToDB', {
      code: Code.fromAsset('functions/dist'),
      handler: 'EventsToDB.handler',
      runtime: Runtime.NODEJS_14_X,
      functionName: 'save-csv-data',
      vpc: vpc,
      vpcSubnets: { subnets: vpc.privateSubnets },
      securityGroups: [lambdaSG],
      environment: {
        DB_PASSWORD: userDb.secret!.secretValue.toString(),
        DB_NAME: databaseName,
        DB_HOST: userDb.dbInstanceEndpointAddress,
        DB_USER: dbEngine.defaultUsername!
      },
      reservedConcurrentExecutions: 50
    });

    saveToDbLambda.addEventSource(new SqsEventSource(csvDataQueue, { batchSize: 1, enabled: true }));

    const getPresignedUrl = new Function(this, 'PresignedUrl', {
      code: Code.fromAsset('functions/dist/signed-url'),
      handler: 'GeneratePreSignedUrl.handler',
      runtime: Runtime.NODEJS_14_X,
      functionName: 'generate-signed-url',
      environment: {
        S3_BUCKET_NAME: csvBucket.bucketName
      },
      reservedConcurrentExecutions: 10
    });
    csvBucket.grantReadWrite(getPresignedUrl);

    const queryDataLambda = new Function(this, 'QueryDB', {
      code: Code.fromAsset('functions/dist'),
      handler: 'QueryDB.handler',
      runtime: Runtime.NODEJS_14_X,
      functionName: 'query-csv-data',
      vpc: vpc,
      vpcSubnets: { subnets: vpc.privateSubnets },
      securityGroups: [lambdaSG],
      environment: {
        DB_PASSWORD: userDb.secret!.secretValue.toString(),
        DB_NAME: databaseName,
        DB_HOST: userDb.dbInstanceEndpointAddress,
        DB_USER: dbEngine.defaultUsername!,
        PAGE_SIZE: '25'
      },
      reservedConcurrentExecutions: 100
    });

    const apiGw = new RestApi(this, 'CSV-Processing-Api', {
      restApiName: 'csv-api',
      binaryMediaTypes: ['application/json'],
      deployOptions: {
        loggingLevel: MethodLoggingLevel.INFO,
        dataTraceEnabled: true
      }
    });

    const usagePlan = apiGw.addUsagePlan('CSV-API-UsagePlan', {
      quota: {
        limit: 10,
        period: Period.DAY
      },
      apiStages: [{
        api: apiGw,
        stage: apiGw.deploymentStage
      }]
    });

    const apiKey = new ApiKey(this, 'CSV-API-Key');
    usagePlan.addApiKey(apiKey);

    apiGw.root
      .resourceForPath('/files/{filename}/signed-url')
      .addMethod('GET', new LambdaIntegration(getPresignedUrl), { apiKeyRequired: true });

    apiGw.root
      .resourceForPath('/queries')
      .addMethod('GET', new LambdaIntegration(queryDataLambda), { apiKeyRequired: true });

    apiGw.root
      .resourceForPath('/queries/{mobile}')
      .addMethod('GET', new LambdaIntegration(queryDataLambda), { apiKeyRequired: true });

    apiGw.root
      .resourceForPath('/queries/count')
      .addMethod('GET', new LambdaIntegration(queryDataLambda), { apiKeyRequired: true });
  }
}

class SourceSecurityGroup implements IPeer {
  canInlineRule: boolean;
  uniqueId: string;
  connections: Connections;
  sourceSecurityGroupId: string;
  toIngressRuleConfig() {
    return { sourceSecurityGroupId: this.sourceSecurityGroupId };
  }
  toEgressRuleConfig() {
    throw new Error('Method not implemented.');
  }

  constructor(sg: ISecurityGroup) {
    this.connections = new Connections({ peer: this }),
      this.canInlineRule = false;
    this.uniqueId = sg.securityGroupId;
    this.sourceSecurityGroupId = sg.securityGroupId;
  }
}


// apiGw.root
//   .resourceForPath('/files/{filename}')
//   .addMethod('POST', new S3Integration(csvBucket), {
//     requestParameters: {
//       'method.request.path.filename': true
//     },
//     methodResponses: [{
//       statusCode: '200'
//     }]
//   });
// class S3Integration extends AwsIntegration {
//   constructor(bucket: IBucket) {
//     const apiRole = new Role(bucket.stack, 'ApiGWRole', {
//       assumedBy: new ServicePrincipal('apigateway.amazonaws.com')
//     });
//     bucket.grantPut(apiRole);
//     super({
//       service: 's3',
//       integrationHttpMethod: 'PUT',
//       path: `${bucket.bucketName}/{filename}.csv`,
//       options: {
//         credentialsRole: apiRole,
//         requestParameters: {
//           'integration.request.path.filename': 'method.request.path.filename'
//         },
//         requestTemplates: {
//           'application/json': `$input.json('$').file`
//         },
//         integrationResponses: [{
//           statusCode: '200'
//         }]
//       }
//     })
//   }
// }
