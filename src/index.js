'use strict';

const assert = require('assert');
const _ = require('underscore');

const fingerprint = require('./fingerprint');

const INDEXED_PLANS = ['IXSCAN', 'IDHACK'];

/**
 * @typedef {Object} CurrentOp
 * @property {?string} appName        The identifier of the client application
 * @property {string}  op             The type of operation e.g. 'update', 'insert', etc.
 * @property {string}  ns             The namespace the operation targets
 * @property {?string} planSummary    The query plan for the operation
 * @property {boolean} waitingForLock Whether the operation is waiting for a lock
 *
 * @see https://docs.mongodb.com/manual/reference/command/currentOp/#currentop-output-fields
 */

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

      var processed = _.map(inprog, (op) => {
        return {
          query: op,
          fingerprint: fingerprint(op.query),
          collection: op.ns ? op.ns.replace(/.*\./, '') : '(no collection)',
          indexed: op.planSummary && this.isIndexed(op),
          waitingForLock: op.waitingForLock
        };
      });
      done(null, processed);
    });
  }

  /**
   * Given an operation with a planSummary, determines whether or not it's using an index.
   *
   * @param {CurrentOp} op
   * @return {boolean} whether or not the given operation is using an index
   */
  isIndexed(op) {
    return INDEXED_PLANS.some((p) => op.planSummary.indexOf(p) !== -1);
  }
}

module.exports = MongoSlowQueryChecker;
