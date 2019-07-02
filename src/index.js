const { deferred } = require('promise-callbacks');
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
   *   waitingForLock: True if the query is waiting for a lock, false otherwise,
   *   appName: The client application responsible for this operation
   * }]
   * ```
   */
  async get() {
    const isMongoJS = typeof this.db.__getConnection === 'function';
    const command = {
      currentOp: true,
      active: true,
      op: {
        $ne: 'none'
      },
      secs_running: {
        $gte: this.queryThreshold
      }
    };

    let ops;
    if (isMongoJS) {
      const promise = deferred();
      this.db.runCommand(command, promise.defer());
      ops = await promise;
    } else if (typeof this.db.adminCommand === 'function') {
      ops = await this.db.adminCommand(command);
    } else {
      // Assume this.db is a reference to the 'admin' db
      ops = await this.db.runCommand(command);
    }
    return this.processOps(ops);
  }

  /**
   * Returns an array of ops given the result of a db command.
   *
   * @param      {Object}  ops     The result of an op query.
   * @return     {Array}   An array of formatted mongo ops with details about their run time..
   */
  processOps(ops) {
    const inprog = ops && ops.inprog;
    if (!inprog || !inprog.length) {
      // Short circuit early.
      return [];
    }

    const processed = _.map(inprog, (op) => {
      return {
        query: op,
        fingerprint: fingerprint(op.query),
        collection: op.ns ? op.ns.replace(/.*\./, '') : '(no collection)',
        indexed: op.planSummary && this.isIndexed(op),
        waitingForLock: op.waitingForLock,
        appName: op.appName
      };
    });
    return processed;
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
