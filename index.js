const fs = require("fs");
const csv = require("csv-parser");
const mysql = require("mysql");
const _ = require("lodash");

/*let map = {
  created_at: {
    SignUpDate: true,
    createDatetime: true
  },
  first_name: {
    First: true,
    firstName: true
  },
  last_name: {
    Last: true,
    lastName: true
  },
  email: {
    Email: true,
    emailAddress: true
  },
  latitude: {
    Latitude: true,
    geoLat: true
  },
  longtitude: {
    Longitude: true,
    geoLong: true
  },
  ip: {
    IP: true,
    ipAddress: true
  }
};*/

let map = {};

// map 1
// created_at	first_name	last_name	email	latitude	longitude	ip
// SignUpDate	First	Last	Email	Latitude	Longitude	IP

// map2
// created_at	ip	latitude	longitude	first_name	last_name	email
// createDatetime	ipAddress	geoLat	geoLong	firstName	lastName	emailAddress

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

}

function transformData(obj) {
  // obj = {header, index}
  const key = _.findKey(map, obj.header);

  return (obj.header = key);
}

function loadDataToDb(data) {
  //console.log("---------------test results-------------");
  //console.log(data);
  const value = `'${data.created_at}', '${data.first_name}', '${data.last_name}', '${data.email}', '${data.latitude}', '${data.longtitude}', '${data.ip}'`;
  return new Promise((resolve, reject) => {
    // asyn work
    //console.log(value);
    //console.log("-----------------sql-------------------");
    const sql = `INSERT INTO customers (created_at, first_name, last_name, email, latitude, longtitude, ip) VALUES (${value})`;
    con.query(sql, (err, result) => {
      if (err) throw err;
      resolve("1 record inserted");
    });
  });
}

async function runETL() {
  /*const map = await createMapper(["./map1.csv", "./map2.csv"]);
  console.log(map);*/

  const data = await Promise.all([
    getData("./data2_test.csv", false),
    getData("./data1_test.csv", false)
  ]);

  data.map(items => {
    items.map(item => {
      loadDataToDb(item).then(message => {
        console.log(message);
      });
    });
  });
}

//runETL();

createMapper(["./map1.csv", "./map2.csv"]);
