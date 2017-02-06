var fingerprint = require('../src/fingerprint');

describe('fingerprint', () => {
  it('creates a fingerprint for an object', () => {
    var obj = {
      _id: 1
    };
    expect(fingerprint(obj)).toEqual('{ _id }');
  });

  it('creates a fingerprint for a (nested) object', () => {
    var obj = {
      _id: 'yolo',
      $or: [{
        createdAt: {
          $gt: 1234567
        }
      }]
    };
    expect(fingerprint(obj)).toEqual('{ _id, $or: [ { createdAt: { $gt } } ] }');

    var raObj = {
      _id: 'yolo',
      $or: [{
        createdAt: {
          otherIds: ['34tfger']
        }
      }]
    };
    expect(fingerprint(raObj)).toEqual('{ _id, $or: [ { createdAt: { otherIds: [  ] } } ] }');
  });

  it('creates a fingerprint complex object', () => {
    var obj = {
      "find": "users",
      "filter": {
        "$or": [{
          "_id": {
            "$regex": "(^| |@)foo",
            "$options": "i"
          }
        }, {
          "name": {
            "$regex": "(^| |@)bar",
            "$options": "i"
          }
        }, {
          "email": {
            "$regex": "(^| |@)baz",
            "$options": "i"
          }
        }]
      },
      "sort": {
        "createdAt": -1
      },
      "projection": {
        "_id": 1,
        "name": 1,
        "email": 1
      },
      "limit": 10
    };
    expect(fingerprint(obj)).toEqual('{ find, filter: { $or: [ { _id: { $regex, $options } }, { name: { $regex, $options } }, { email: { $regex, $options } } ] }, sort: { createdAt }, projection: { _id, name, email }, limit }');

    var raObj = {
      _id: 'yolo',
      $or: [{
        createdAt: {
          otherIds: ['34tfger']
        }
      }]
    };
    expect(fingerprint(raObj)).toEqual('{ _id, $or: [ { createdAt: { otherIds: [  ] } } ] }');
  });

  it('specifically identifies arrays with nulls', () => {
    var obj = {
      _id: 'yolo',
      groupIds: [null]
    };
    expect(fingerprint(obj)).toEqual('{ _id, groupIds: [ null ] }');
  });

  it('specifically identifies null values', () => {
    var obj = {
      _id: 'yolo',
      groupIds: null
    };
    expect(fingerprint(obj)).toEqual('{ _id, groupIds: null }');
  });

  it('specifically identifies undefined values', () => {
    var obj = {
      _id: 'yolo',
      groupIds: undefined
    };
    expect(fingerprint(obj)).toEqual('{ _id, groupIds: undefined }');
  });
});

