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
  con.query("CREATE DATABASE IF NOT EXISTS etl_db", function(err, result) {
    if (err) throw err;
    console.log("Database is created...");
  });

  // Create the table
  const sql =
    "CREATE TABLE IF NOT EXISTS customers (created_at DATETIME(2), first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), latitude DECIMAL(9,6), longtitude DECIMAL(9,6), ip VARBINARY(16))";
  con.query(sql, function(err, result) {
    if (err) throw err;
    console.log("Table is created");
  });
});

function getData(filePath, readMapper) {
  const results = [];
  return new Promise((resolve, reject) => {
    // async work
    const streamer = fs.createReadStream(filePath);
    if (readMapper) {
      streamer
        .pipe(csv())
        .on("data", data => results.push(data))
        .on("end", () => {
          resolve(results);
        });
    } else {
      streamer
        .pipe(csv({ mapHeaders: transformData }))
        .on("data", data => results.push(data))
        .on("end", () => {
          resolve(results);
        });
    }
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
          resolve("map is created");
        });
      });
    });
  });
}

function transformData(obj) {
  // obj = {header, index}
  const key = _.findKey(map, obj.header);
  //console.log(map);

  return (obj.header = key);
}

function loadDataToDb(data) {
  //console.log("---------------test results-------------");
  const value = `'${data.created_at}', '${data.first_name}', '${data.last_name}', '${data.email}', '${data.latitude}', '${data.longtitude}', '${data.ip}'`;
  return new Promise((resolve, reject) => {
    // asyn work
    //console.log("-----------------sql-------------------");
    const sql = `INSERT INTO customers (created_at, first_name, last_name, email, latitude, longtitude, ip) VALUES (${value})`;
    con.query(sql, (err, result) => {
      if (err) throw err;
      resolve("1 record inserted");
    });
  });
}

async function runETL() {
  const msg = await createMapper(["./map1.csv", "./map2.csv"]);
  console.log(msg);

  const data = await Promise.all([
    getData("./data1.csv", false),
    getData("./data2.csv", false)
  ]);

  data.map(items => {
    items.map(item => {
      loadDataToDb(item).then(message => {
        console.log(message);
      });
    });
  }); 
}

runETL();


