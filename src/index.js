'use strict';

const assert = require('assert');
const _ = require('underscore');

const fingerprint = require('./fingerprint');

/**
 * A utility class to simplify retrieving slow queries from Mongo.
 */
class MongoSlowQueryChecker {
  /**
   * Creates the MongoSlowQueryChecker.
   *
   * @param {Object} options Options to use to create the Mongo slow query
   *    checker.
   *    @property {Object} options.db The Mongo DB reference.
   *    @property {Number} options.queryThreshold The time threshold to use to
   *       classify a Mongo query as slow, given in seconds. This parameter is
   *       optional, and if not provided it defaults to five seconds.
   */
  constructor(options) {
    assert(options.db, 'Must provide a valid DB reference');
    
    this.db = options.db;
    this.queryThreshold = 5;
    if (options.queryThreshold) {
      this.queryThreshold = options.queryThreshold;
    }
  }

  /**
   * Returns all queries that have currently been running for longer than the
   * preset `queryThreshold`. It returns an array of objects of the format:
   *
   * ```
   * [{
   *   query: The slow query itself,
   *   fingerprint: A fingerprint of the query based on the keys in the query,
   *   collection: The collection this query was run against (or `no collection`),
   *   indexed: If this query was able to use an index or not,
   *   waitingForLock: True if the query is waiting for a lock, false otherwise
   * }]
   * ```
   *
   * @param {Function} done Node style callback.
   */
  get(done) {
    this.db.runCommand({
      currentOp: true,
      active: true,
      op: {
        $ne: 'none'
      },
      secs_running: {
        $gte: this.queryThreshold
      }
    }, (err, ops) => {
      let inprog = ops && ops.inprog;
      if (!inprog || !inprog.length) {
        // Short circuit early.
        done(null, []);
        return;
      }

      var processed = _.map(inprog, (query) => {
        return {
          query,
          fingerprint: fingerprint(query.query),
          collection: query.ns ? query.ns.replace(/.*\./, '') : '(no collection)',
          indexed: query.planSummary && (query.planSummary.indexOf('IXSCAN') !== -1),
          waitingForLock: query.waitingForLock
        };
      });
      done(null, processed);
    });
  }
}

module.exports = MongoSlowQueryChecker;