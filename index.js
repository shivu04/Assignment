const fs = require('fs');
const { Pool } = require('pg');
const Table = require('cli-table3'); 
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'postgres',
  port: 5432, 
});

fs.readFile('users.csv', 'utf8', async function (err, data) {
  if (err) {
    console.error(err);
    return;
  }

  const lines = data.split('\n');
  const headers = lines[0].split(',');
  
  const formattedData = [];
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split(',');
    const user = {
      name: {
        firstname: values[0],
        lastname: values[1]
      },
      age: parseInt(values[2]),
      address: {
        line1: values[3],
        line2: values[4],
        city: values[5],
        state: values[6]
      },
      gender: values[7]
    };
    formattedData.push(user);
    console.log(user)
  }

  const client = await pool.connect();
  try {
    for (const user of formattedData) {
      const query = `
        INSERT INTO public.users (name, age, address, additional_info)
        VALUES ($1, $2, $3, $4)
      `;
      const values = [user.name.firstname + ' ' + user.name.lastname, user.age, user.address, { sex: user.gender }];
      await client.query(query, values);
    }
    console.log('Data uploaded to PostgreSQL.');

    
  } catch (error) {
    console.error('Error:', error);
  }  try {

    const ageDistributionQuery = `
      SELECT
        CASE
          WHEN age < 20 THEN 'Below 20'
          WHEN age >= 20 AND age <= 40 THEN '20 to 40'
          WHEN age > 40 AND age <= 60 THEN '40 to 60'
          ELSE 'Above 60'
        END AS age_range,
        COUNT(*) as count
      FROM public.users
      GROUP BY age_range
      ORDER BY age_range
    `;
    const ageDistributionResult = await client.query(ageDistributionQuery);

    const totalCountQuery = `
      SELECT COUNT(*) as total_count
      FROM public.users
    `;
    const totalCountResult = await client.query(totalCountQuery);
    const totalUsers = totalCountResult.rows[0].total_count;

    console.log('Age Distribution Percentage:');

    const table = new Table({
      head: ['Age-group', '% Distribution'],
      colWidths: [20, 20]
    });

    for (const row of ageDistributionResult.rows) {
      const percentage = (row.count / totalUsers) * 100;
      table.push([row.age_range, `${percentage.toFixed(2)}%`]);
    }

    console.log(table.toString()); 
  } catch (error) {
    console.error('Error:', error);
  } finally {
    client.release();
    pool.end();
  }
}
);

