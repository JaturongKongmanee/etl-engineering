# etl-engineering

## Introduction
This hands-on exercise will create a solution to upload data from multiple sources in a single customers table using the mapping in the file provided.

## Getting started
 ### Prerequisites
 * Install [Node.js](https://nodejs.org/en/)
 * Install [lodash](https://www.npmjs.com/package/lodash), utility module
  ```javascript
npm i --save lodash
```
 * Install [mysql](https://www.npmjs.com/package/mysql), a node.js driver for mysql
 ```javascript
npm install mysql
```
* Install [csv-parser](https://www.npmjs.com/package/csv-parser) for streaming CSV parser that aims for maximum speed
```javascript
npm install csv-parser
```
* Install [MySQL](https://www.mysql.com/), a relational database management system(RDBMS). For the sake of simplicity, I installed [XAMPP](https://www.apachefriends.org/index.html), to run a development server on the local computer.

## Set Up & Run
In **index.js**, set up these parameters
```javascript
// Add the array of file paths of map formats
const msg = await createMapper(["./map1.csv", "./map2.csv"]);
```
```javascript
// Add the file paths of data to be ETL to getData function
const data = await Promise.all([
    getData("./data1.csv", false),
    getData("./data2.csv", false)
  ]);
```
At the terminal, run **index.js**
```javascript
node index.js
```

### Result
* #### After running, you should see these messages from the terminal.
![etl_run](https://github.com/JaturongKongmanee/etl-engineering/blob/master/images/etl_run.PNG) 
* #### You will see Data is inserted into Database.
![db](https://github.com/JaturongKongmanee/etl-engineering/blob/master/images/db.PNG) 

