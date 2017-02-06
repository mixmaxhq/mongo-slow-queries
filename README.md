# mongo-slow-queries
A module for pulling slow queries from Mongo, tagged with metadata as well.

## Install
```
$ npm install mongo-slow-queries --save
```

## Usage

### Initialization

To use `mongo-slow-queries` you just need to provide a Mongo DB reference when
creating an instance:

```js
const mongojs = require('mongojs');
const MongoSlowQueryChecker = require('mongo-slow-queries');

let db = mongojs('mongodb://localhost:27017/my-db');
let slowQueries = new MongoSlowQueryChecker({ db });
```


### Querying for slow queries
Querying for slow queries is then as simple as calling `get`:

```js
slowQueries.get((err, queries) => {
  if (err) {
    console.log('Failed to retrieve slow queries: ' + queries);
  } else {
    console.log('slow queries: ' + JSON.stringify(queries));
  }
});
```

### Query duration threshold
By default, `mongo-slow-queries` only returns queries that are currently
running that have already taken greater than five seconds to run. If you wish
to use a different threshold value, simply pass it in when you construct the
Mongo slow query checker:

```js
let slowQueries = new MongoSlowQueryChecker({
  db,
  queryThreshold: 2 // The unit of time is in seconds.
});
```
