const { deferred } = require('promise-callbacks');
const assert = require('assert');
const _ = require('lodash');

const fingerprint = require('./fingerprint');

const INDEXED_PLANS = ['IXSCAN', 'IDHACK'];

/**
 * @typedef {Object} CurrentOp
 * @property {?string} appName        The identifier of the client application
 * @property {string}  op             The type of operation e.g. 'update', 'insert', etc.
 * @property {string}  ns             The namespace the operation targets
 * @property {?string} planSummary    The query plan for the operation
 * @property {boolean} waitingForLock Whether the operation is waiting for a lock
 * @property {boolean} indexed        Whether the operation is using an index
 * @property {boolean} collscan       Whether the operation is using a collection scan
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
   *    @property {Boolean} options.useProfiling Whether to use system.profile;
   *       if not, we will only look at currently running queries. Profiling needs
   *       to be enabled for this to work. This parameter is optional, and it defaults
   *       to `false`.
   *    @property {Number} options.queryThreshold The time threshold to use to
   *       classify a Mongo query as slow, given in seconds. This parameter is
   *       optional, and if not provided it defaults to five seconds.
   *    @property {Boolean} options.reportAllCollscans If true and using system.profile,
   *       all queries using a COLLSCAN will be reported as slow queries, even if they
   *       didn't exceed `options.queryThreshold`. This parameter is optional, and it
   *       defaults to `false`.
   */
  constructor(options) {
    assert(options.db, 'Must provide a valid DB reference');

    this.db = options.db;
    this.useProfiling = options.useProfiling || false;
    this.queryThreshold = options.queryThreshold || 5;
    this.reportAllCollscans = options.reportAllCollscans || false;
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
    const ops = this.useProfiling
      ? await this.getFromSystemProfile()
      : await this.getCurrentlyRunningQueries();

    return this.processOps(ops);
  }

  /**
   * Get relevant (slow/COLLSCAN) queries from the `system.profile` table, ensuring we don't
   * repeatedly return the same queries in consecutive runs.
   */
  async getFromSystemProfile() {
    this.profile = this.profile || (await this.db.connect()).collection('system.profile');

    const conditions = [{ millis: { $gte: this.queryThreshold * 1000 } }];
    if (this.reportAllCollscans) conditions.push({ planSummary: { $regex: /COLLSCAN/ } });
    const profileQuery = { $or: conditions, ns: { $not: /system.profile/ } };
    if (this.lastTimestampInSystemProfile) {
      profileQuery.ts = { $gt: this.lastTimestampInSystemProfile };
    }

    const queries = await this.profile.find(profileQuery).toArray();

    this.lastTimestampInSystemProfile = _.max(_.map(queries, (q) => q.ts));
    return queries;
  }

  async getCurrentlyRunningQueries() {
    const isMongoJS = typeof this.db.__getConnection === 'function';
    const command = {
      currentOp: true,
      active: true,
      op: {
        $ne: 'none',
      },
      secs_running: {
        $gte: this.queryThreshold,
      },
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
    return ops && ops.inprog;
  }

  /**
   * Returns an array of ops given the result of a db command.
   *
   * @param      {Object}  queries     List of ops/queries.
   * @return     {Array}   An array of formatted mongo ops with details about their run time..
   */
  processOps(queries) {
    if (!queries) {
      return [];
    }

    const processed = _.map(queries, (op) => {
      const filter =
        op.query || op.command.q || op.command.filter || op.command.query || op.command.pipeline;

      return {
        query: op,
        fingerprint: fingerprint(filter),
        collection: op.ns ? op.ns.replace(/.*\./, '') : '(no collection)',
        indexed: op.planSummary && this.isIndexed(op),
        collscan: op.planSummary && this.isCollscan(op),
        waitingForLock: op.waitingForLock,
        appName: op.appName,
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

  /**
   * Given an operation with a planSummary, determines whether or not it's doing a collscan.
   *
   * @param {CurrentOp} op
   * @return {boolean} whether or not the given operation is doing a collscan
   */
  isCollscan(op) {
    return op.planSummary.indexOf('COLLSCAN') !== -1;
  }
}

module.exports = MongoSlowQueryChecker;
