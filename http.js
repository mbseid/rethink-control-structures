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
  port: 28015
}, function(err, conn) {
  if (err) throw err;
  connection = conn;
  run();
})

/**
 * The Imperial army now requires all new characters to be reported to their latest database. As
 * the drone sends back information to our system, we will have to send it to an endpoint provided
 * to us by the Imperial army. 
 *
 * This is a great use-case of the RethinkDB Http command. Instead of persisting in our DB and then
 * sending the new information to Imperial army, we can have our database do it for us! Less round
 * trips the better.
 */
var run = function() {
  /**
   * The drone has just found me! I will be persisted in the database and reported to the Imperial
   * army. Luckily, I have nothing to hide, but I wish they didn't require it.
   */

  var me = {
    "edited": r.now(),
    "name": "Mike Seid",
    "created": r.now(),
    "gender": "male",
    "skin_color": "brown",
    "hair_color": "black",
    "height": "172",
    "eye_color": "brown",
    "mass": "77",
    "homeworld": 1,
    "birth_year": "19BBY"
  };

  r.db('starwars').table('people').insert(me, {returnChanges: true}).do(function(doc) {
      return r.http('https://httpbin.org/post', {
        method: 'POST',
        data: doc.toJSON()
      })
  }).run(connection, function(err, data) {
    console.log("Successfully reported to the Imperial Army.", data);
    process.exit();
  })
}