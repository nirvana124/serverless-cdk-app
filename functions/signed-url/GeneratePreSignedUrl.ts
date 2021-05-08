import { APIGatewayProxyHandler } from "aws-lambda";
import { S3 } from 'aws-sdk';

const s3 = new S3();
export const handler: APIGatewayProxyHandler = async (event) => {
    const url = s3.getSignedUrl('putObject', {
        Bucket: process.env.S3_BUCKET_NAME,
        Key: `${event.pathParameters.filename}.csv`,
        Expires: 300
    });

    return {
        statusCode: 200,
        body: JSON.stringify({ url })
    }
}