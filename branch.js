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
 * Imagine a probe flying around space collecting information on planets it flies by. During
 * it's perpetual journey through space, it will either find new planets, or update the planets
 * latest reading. 
 *
 * This is a perfect use case of RethinkDB branches. We will try to find the planet, and update
 * the planet if found, otherwise it will update it. Since everything is done on the RethinkDB
 * server, everything can be run fast. No need to communicate between the probe and outer space.
 */
var run = function() {
    /**
     * After flying by a planet, the probe will scan the planet's molecules. This hash will be
     * unique, and for the sake of the demo it will be easy to read integers
     *
     * The first planet flown by is Tatooine
     * The second plane flown by is Endor
     */

    var endor = {
      "id": 7,
      "edited": r.now(),
      "climate": "temperate",
      "surface_water": "8",
      "name": "Endor",
      "diameter": "4900",
      "rotation_period": "18",
      "created": r.now(),
      "terrain": "forests, mountains, lakes",
      "gravity": "0.85 standard",
      "orbital_period": "402",
      "population": "30000000"
    };

    var tatooine = {
      "id": 1,
      "edited": r.now(),
      "climate": "arid",
      "surface_water": "1",
      "name": "Tatooine",
      "diameter": "10465",
      "rotation_period": "23",
      "created": r.now(),
      "terrain": "dessert",
      "gravity": "1 standard",
      "orbital_period": "304",
      "population": "200000"
    }
    var updatePlanet = function(planetId, data) {
      return r.db('starwars').table('planets').get(planetId).replace(function(doc) {
        return r.branch(doc.eq(null),
          data,
          doc.merge(function() {
            var newObj = {}
            newObj["edited"] = r.now();
            return newObj;
          })
        )
      });
    }

    async.waterfall([
        function(callback) {
          updatePlanet(endor.id, endor).run(connection, callback)
        },
        function(endor, callback) {
          updatePlanet(tatooine.id, tatooine).run(connection, callback)
        }
      ], function(err, data) {
        console.log("We have updated two planets, beep boop")
        async.waterfall([
            function(callback) {
              r.db('starwars').table('planets').get(1).run(connection, function(err, data) {
                callback(null, data)
              })
            },
            function(planet1, callback) {
              r.db('starwars').table('planets').get(7).run(connection, function(err, data) {
                callback(null, [planet1, data]);
              })
            }
          ],
          function(err, data) {
            console.log("Planet 1", data[0])
            console.log("Planet 7", data[1])
            process.exit();
          });


        });

    }