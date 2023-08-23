const csv = require('csv-to-array');
const { Pool } = require('pg');
const Table = require('cli-table3'); 
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'postgres',
  port: 5432, 
});

let columns = ['name.firstName', 'name.lastName', 'age', 'address.line1', 'address.line2', 'address.city', 'address.state', 'gender'];

csv(
  {
    file: 'users.csv',
    columns: columns
  },
  async function (err, array) {
    if (err) {
      console.error(err);
      return;
    }

    const formattedData = array.map(item => {
      return {
        name: {
          firstname: item['name.firstName'],
          lastname: item['name.lastName']
        },
        age: parseInt(item['age']),
        address: {
          line1: item['address.line1'],
          line2: item['address.line2'],
          city: item['address.city'],
          state: item['address.state']
        },
        gender: item['gender']
      };
    });

    const client = await pool.connect();
    try {
      for (const user of formattedData) {
        const query = `
          INSERT INTO public.users (name, age, address, additional_info)
          VALUES ($1, $2, $3, $4)
        `;
        const values = [user.name.firstname + ' ' + user.name.lastname, user.age, user.address, { sex: user.sex }];
        await client.query(query, values);
      }
      console.log('Data uploaded to PostgreSQL.');

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
