var _ = require('underscore');


/**
 * arrayFingerprint generates a key based fingerprint for objects in an array.
 * @param  {Array} ra The array to fingerprint.
 * @return {String} The fingerprint for nested objects in the array.
 */
function arrayFingerprint(ra) {
  // See if it's just an array with null or nulls
  if (_.size(ra) !== _.size(_.compact(ra))) return '[ null ]';
  var fprint = '[ ';
  for (var i = 0; i < ra.length; i++) {
    var val = ra[i];
    if (_.isObject(val)) {
      fprint += fingerprint(val);
    }

    if (_.size(ra) > i + 1) fprint += ', ';
  }

  return fprint + ' ]';
}

/**
 * fingerprint generates a fingerprint based on the keys of the object.
 * @param  {Object} obj The object to generate a fingerprint for.
 * @return {String} The object key fingerprint.
 */
function fingerprint(obj) {
  var keys = _.keys(obj),
    fprint = '{ ';

  for (var i = 0; i < keys.length; i++) {
    var key = keys[i],
      val = obj[key];
    if (_.isArray(val)) {
      fprint += `${key}: ${arrayFingerprint(val)}`;
    } else if (_.isObject(val)) {
      fprint += `${key}: ${fingerprint(val)}`;
    } else {
      var addition = '';
      if (_.isUndefined(val)) addition = ': undefined';
      else if (_.isNull(val)) addition = ': null';
      fprint += `${key}${addition}`;
    }

    if (_.size(keys) > i + 1) fprint += ', ';
  }

  return fprint + ' }';
}

module.exports = fingerprint;