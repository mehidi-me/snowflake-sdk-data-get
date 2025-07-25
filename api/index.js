require("dotenv").config();
const express = require("express");
const snowflake = require("snowflake-sdk");

const app = express();
const port = 3000;

// Function to get Snowflake Data
function getSnowflakeData() {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
    });

    connection.connect((err) => {
      if (err) return reject(err);

      connection.execute({
        //     sqlText: `SELECT *
        // FROM DB_AMG.REPORT.V_CLICK_REVENUE_INCLUDING_INTRADAY`,
        sqlText: `SELECT *
    FROM DB_AMG.REPORT.V_CLICK_REVENUE_INCLUDING_INTRADAY
        WHERE TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())`,
        complete: (err, stmt, rows) => {
          connection.destroy();
          if (err) return reject(err);
          resolve(rows);
        },
      });
    });
  });
}

// API Endpoint
app.get("/api/snowflake-data", async (req, res) => {
  try {
    const data = await getSnowflakeData();
    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.listen(port, () => {
  console.log(`API is running on http://localhost:${port}`);
});
