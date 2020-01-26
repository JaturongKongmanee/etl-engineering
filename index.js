const fs = require("fs");
const csv = require("csv-parser");
const mysql = require("mysql");
const _ = require("lodash");

let map = {};

// Create the connection to MySQL Database
const con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "",
  database: "etl_db"
});

// Connect to DB
con.connect(function(err) {
  if (err) throw err;
  console.log("DB is connected...");
  // Create the DB
  const queryCreateDB = "CREATE DATABASE IF NOT EXISTS etl_db";
  queryDB(queryCreateDB, "Database is created...").then(msg =>
    console.log(msg)
  );

  // Create the table
  const queryCreateTable =
    "CREATE TABLE IF NOT EXISTS customers (created_at DATETIME(2), first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), latitude DECIMAL(9,6), longitude DECIMAL(9,6), ip VARBINARY(16))";
  queryDB(queryCreateTable, "Table is created...").then(msg => console.log(msg));
});

function getData(filePath, readMapper) {
  const results = [];
  return new Promise((resolve, reject) => {
    // async work
    const streamer = fs.createReadStream(filePath);
    const readMap = readMapper ? {} : { mapHeaders: transformData };
    streamer
      .pipe(csv(readMap))
      .on("data", data => results.push(data))
      .on("end", () => {
        resolve(results);
      });
  });
}

function createMapper(arr) {
  return new Promise((resolve, reject) => {
    // async work
    arr.map(file => {
      const mapper = getData(file, true);
      mapper.then(data => {
        data.map(d => {
          _.forEach(d, (value, key) => {
            if (map[key]) {
              Object.assign(map[key], JSON.parse(`{"${value}" : true}`));
            } else {
              Object.assign(map, JSON.parse(`{"${key}": {"${value}" : true}}`));
            }
          });
          resolve("Map is created...");
        });
      });
    });
  });
}

function transformData(obj) {
  // obj = {header, index}
  const key = _.findKey(map, obj.header);
  return (obj.header = key);
}

function loadDataToDb(data) {
  const value = `'${data.created_at}', '${data.first_name}', '${data.last_name}', '${data.email}', '${data.latitude}', '${data.longitude}', '${data.ip}'`;
  const sql = `INSERT INTO customers (created_at, first_name, last_name, email, latitude, longitude, ip) VALUES (${value})`;
  queryDB(sql, "1 record inserted")
  .then(m => console.log(m));
}

function queryDB(query, msg) {
  return new Promise((resolve, reject) => {
    // async work
    con.query(query, (err, result) => {
      if (err) throw err;
      resolve(msg);
    });
  });
}

async function runETL() {
  const msg = await createMapper(["./map1.csv", "./map2.csv"]);
  console.log(msg);

  const data = await Promise.all([
    getData("./data1_test.csv", false),
    getData("./data2_test.csv", false)
  ]);

  data.map(items => {
    items.map(item => {
      loadDataToDb(item);
    });
  });
}

runETL();
