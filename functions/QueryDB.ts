import { APIGatewayProxyEvent, APIGatewayProxyHandler, APIGatewayProxyResult } from 'aws-lambda';
import { Pool } from 'pg';

const pool = new Pool({
    port: 5432,
    database: process.env.DB_NAME,
    password: JSON.parse(process.env.DB_PASSWORD).password,
    user: process.env.DB_USER,
    host: process.env.DB_HOST
});

const pageSize = parseInt(process.env.PAGE_SIZE);

const SELECT_QUERY = 'SELECT * FROM csv_user';
const LIMIT = ` LIMIT ${pageSize}`;


export const handler: APIGatewayProxyHandler = async (event) => {
    if (event.path.includes('count')) {
        return await count(event);
    }

    const query = buildQuery(event);
    console.log(query);

    const resultArray = await pool.query(query)
        .then(response => response.rows)
        .then(rows => rows.map(row => {
            return {
                firstName: row.first_name,
                lastName: row.last_name,
                phone: row.mobile,
                age: row.age
            }
        }))
        .catch(err => {
            console.log(`Error occurred in saving data: ${err}`);
            throw err;
        });

    return response(resultArray);
}

const count = async (evet: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    const count = await pool.query(`SELECT count(*) from csv_user`)
        .then(response => {
            console.log(`Count response: ${JSON.stringify(response)}`);
            return response.rows[0];
        });
    return {
        statusCode: 200,
        body: JSON.stringify(count),
        headers: {
            ''
        }
    };
}


const response = (resultArray: any[]): APIGatewayProxyResult => {
    if (!resultArray || !Array.isArray(resultArray) || resultArray.length < 1) {
        return {
            statusCode: 404,
            body: 'Data Not Found for request.'
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify(resultArray)
    }
}


const buildQuery = (event: APIGatewayProxyEvent): string => {
    if (event.pathParameters?.mobile) {
        return `${SELECT_QUERY} WHERE mobile = '${event.pathParameters.mobile}'`;
    }
    var query = SELECT_QUERY;

    const queryParams = event.queryStringParameters;
    if (queryParams['firstName'] || queryParams['lastName'] || queryParams['age']) {
        query += ' WHERE';
    }

    var isAndRequired = false;
    if (queryParams['firstName']) {
        query += ` first_name = '${queryParams['firstName']}'`;
        isAndRequired = true;
    }

    if (queryParams['lastName']) {
        if (isAndRequired) {
            query += ' AND';
        }
        query += ` last_name = '${queryParams['lastName']}'`;
        isAndRequired = true;
    }

    if (queryParams['age']) {
        if (isAndRequired) {
            query += ' AND';
        }
        query += ` age = '${queryParams['age']}'`;
    }

    const pageNo = queryParams['page'] ? parseInt(queryParams['page']) : 1;

    return `${query}${LIMIT}${offset(pageNo)}`;
}

const offset = (pageNo: number): string => {
    return ` OFFSET ${(pageNo - 1) * pageSize}`;
}