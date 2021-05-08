import { Pool } from 'pg';

const pool = new Pool({
    port: 5432,
    database: process.env.DB_NAME,
    password: JSON.parse(process.env.DB_PASSWORD).password,
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
});

var tableCreated = false;

export const handler = async (sqsEvent: any) => {
    await createTableIfNotExists();

    console.log(JSON.stringify(sqsEvent))
    const event = JSON.parse(sqsEvent.Records[0].body);
    console.log(JSON.stringify(event))

    await pool.query(`INSERT INTO csv_user(mobile, first_name, last_name, age) values ('${event.mobile}', '${event.firstName}', '${event.lastName}', '${event.age}')`)
        .then(response => console.log(`Data inserted : ${JSON.stringify(response)}`))
        .catch(err => {
            console.log(`Error occurred in saving data: ${err}`);
            throw err;
        });
}


async function createTableIfNotExists() {
    if (!tableCreated) {
        await pool.query(`CREATE TABLE IF NOT EXISTS csv_user (
            mobile VARCHAR(15) PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50),
            age VARCHAR(3)
            );`)
            .then(res => console.log(`Table created:  ${JSON.stringify(res)}`))
            .catch(err => {
                console.log(`Error in table creation : ${err}`);
                throw err;
            });
        tableCreated = true;
    }
}