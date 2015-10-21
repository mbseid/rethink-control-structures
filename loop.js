/**
 * [r = driver]
 *  
 * Import the rethinkdb node module. Driver for running queries and creating connections
 * npm install --save rethinkdb
 */
var r = require('rethinkdb');
/**
 * Aysnc.js library which is really helpful for callbacks and stuff.
 */
var async = require('async');

/**
 * [connection variable that holds the connection to rethinkdb]
 *
 * Connect to the local RethinkDB instance and run our function once the
 * connection is complete. If an error is throw, propagate it up.
 */
var connection = null;
r.connect({
  host: 'localhost',
  port: 28015,
  db: 'starwars'
}, function(err, conn) {
  if (err) throw err;
  connection = conn;
  run();
})

/**
 * As our system has grown and we have discovered more starships, we need to do some optimizations
 * to keep queries running fast. We have discovered so many starships that finding pilots who can fly
 * them has become burdensome. Let's mark all the people who are pilots so they can be easily found
 * to pilot other ships.
 *
 * We will denormalize that relationship for the sake of performance and run that migration using a
 * forEach loop. 
 */
var run = function() {
  r.table('starships').forEach(function(ship) {
    return ship('pilots').forEach(function(pilot) {
      return r.table('people').get(pilot).update({
        pilot: true
      })
    })
  }).run(connection, function() {
    r.table('people').filter({
      pilot: true
    }).run(connection, function(err, data) {
      data.toArray(function(err, pilots) {
        console.log("Newly declared pilots: " + pilots.length, pilots[0])
        process.exit();
      });

    })
  })
}