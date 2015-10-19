var r = require('rethinkdb');
var async = require('async');
var lodash = require('lodash');

var DATABASE = 'starwars';
var TABLE = {
	PEOPLE: "people",
	PLANETS: "planets",
	FILMS: "films",
	STARSHIPS: "starships"
}


/**
 * @param  json {JSON} - This object is what is loaded from the fixtures.
 * @return json {JSON} - Cleaned up ready for insertion into RethinkDB
 */
var jsonToEntity = function(json) {
	var fields = json.fields;
	fields['id'] = json.pk;
	return fields;
}

var connection = null;
r.connect({
	host: 'localhost',
	port: 28015
}, function(err, conn) {
	if (err) throw err;
	connection = conn;
	run();
})

var run = function() {
	async.waterfall([
		function(callback) {
			console.log("Dropping the old database");
			r.dbDrop(DATABASE).run(connection, function(err, data) {
				/**
				 * We don't mind about an error here. If there is no database
				 * an error will be thrown no matter what.
				 */
				callback();
			});
		},
		function(callback) {
			console.log("Creating starwars database");
			r.dbCreate(DATABASE).run(connection, callback);
		},
		function(args, callback) {
			console.log("Using the startwars databse");
			connection.use(DATABASE);
			callback();
		},
		function(callback) {
			console.log("Creating the people table");
			r.tableCreate(TABLE.PEOPLE).run(connection, callback);
		},
		function(args, callback) {
			var people = require('./data/people.json');
			var cleanPeople = lodash.map(people, jsonToEntity);
			console.log("Inserting " + cleanPeople.length + " people");
			r.table(TABLE.PEOPLE).insert(cleanPeople).run(connection, callback);
		},
		function(args, callback) {
			console.log("Creating the planets table");
			r.tableCreate(TABLE.PLANETS).run(connection, callback);
		},
		function(args, callback) {
			var planets = require('./data/planets.json');
			var cleanPlanets = lodash.map(planets, jsonToEntity);
			console.log("Inserting " + cleanPlanets.length + " planets");
			r.table(TABLE.PLANETS).insert(cleanPlanets).run(connection, callback);
		},
		function(args, callback) {
			console.log("Creating the films table");
			r.tableCreate(TABLE.FILMS).run(connection, callback);
		},
		function(args, callback) {
			var films = require('./data/films.json');
			var cleanFilms = lodash.map(films, jsonToEntity);
			console.log("Inserting " + cleanFilms.length + " films");
			r.table(TABLE.FILMS).insert(cleanFilms).run(connection, callback);
		},
		function(args, callback) {
			console.log("Creating the starships table");
			r.tableCreate(TABLE.STARSHIPS).run(connection, callback);
		},
		function(args, callback) {
			var starships = require('./data/starships.json');
			var cleanStarships = lodash.map(starships, jsonToEntity);
			console.log("Inserting " + cleanStarships.length + " starships");
			r.table(TABLE.STARSHIPS).insert(cleanStarships).run(connection, callback);
		}
	], function(err, data) {
		if (err) {
			console.error(err);
		} else {
			console.log("Completed Insert")
			process.exit()
		}
	})

}