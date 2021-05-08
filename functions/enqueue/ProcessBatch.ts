import { S3, SQS } from 'aws-sdk';

const s3 = new S3();
const sqs = new SQS();
export const handler = async (sqsEvent) => {

    const event = JSON.parse(sqsEvent.Records[0].body);
    console.log(JSON.stringify(event));

    
    var csvRows = await s3.getObject({
        Bucket: event.bucket,
        Key: event.key
    }).promise()
        .then(response => response.Body.toString().trim())
        .then(csvString => csvString.split('\n'))
        .then(rows => rows.slice(event.start, event.end));

    console.log(`End : ${csvRows.length}`);

    var batchMessagePromises = [];
    var messages = [];

    const end = csvRows.length

    for (let index = 0; index < end; index++) {
        const data = csvRows[index].trim().split(',');
        messages.push({ firstName: data[0], lastName: data[1], age: data[2], mobile: data[3] });

        if (messages.length == 10 || index == end - 1) {
            batchMessagePromises.push(sqs.sendMessageBatch({
                Entries: messages.map(obj => {
                    return {
                        Id: `${obj.mobile}`,
                        MessageBody: JSON.stringify(obj)
                    }
                }),
                QueueUrl: process.env.SQS_URL
            }).promise());
            messages = []; // Reinitializing objects array
        }
    }

    const resolved = await Promise.all(batchMessagePromises);

    console.log(`Messages sent : ${resolved.length}`);

    // const csvStream = s3.getObject({
    //     Bucket: event.Records[0].s3.bucket.name,
    //     Key: event.Records[0].s3.object.key
    // }).createReadStream();

    // await new Promise((resolve, reject) => {
    //     var entries = [];
    //     parseStream(csvStream, { headers: true })
    //         .on('data', async data => {
    //             entries.push(sqs.sendMessage({
    //                 MessageBody: JSON.stringify(data),
    //                 QueueUrl: process.env.SQS_URL
    //             }).promise());
    //         })
    //         .on('end', () => {
    //             console.log('Stream completed.');
    //             console.log(`Entries: ${entries.length}`);
    //             resolve(Promise.all(entries));
    //         })
    //         .on('error', () => {
    //             console.log('Error in stream');
    //             reject('Error in processing.')
    //         });
    // });

    // const csvString = await s3.getObject({
    //     Bucket: event.Records[0].s3.bucket.name,
    //     Key: event.Records[0].s3.object.key
    // }).promise().then(response => response.Body.toString().trim());

    // console.log(`File size: ${csvString.length}`);

    // const rows = csvString.split('\n')
    //     .slice(1)
    //     .map(row => {
    //         const data = row.split(",");
    //         const firstName = data[0];
    //         const lastName = data[1];
    //         const age = data[2];
    //         const mobile = data[3];
    //         return { firstName, lastName, age, mobile };
    //     });
    // console.log(`Rows : ${rows.length}`);

    // const resolved = await Promise.all(rows.map(row => sqs.sendMessage({
    //     MessageBody: JSON.stringify(row),
    //     QueueUrl: process.env.SQS_URL
    // }).promise()));

    // console.log(`Messages sent : ${resolved.length}`);

}
