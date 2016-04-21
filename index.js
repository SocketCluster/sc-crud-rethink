var _ = require('lodash');
var thinky = require('thinky');
var async = require('async');
var AccessControl = require('./access-control');
var jsonStableStringify = require('json-stable-stringify');

var SCCRUDRethink = function (worker, options) {
  var self = this;

  this.scServer = worker.scServer;
  this.options = options || {};
  this.models = {};

  this.schema = this.options.schema || {};
  this.thinky = thinky(this.options.thinkyOptions);

  this.maxPredicateDataCount = this.options.maxPredicateDataCount || 100;
  this.channelPrefix = 'crud>';

  if (!this.options.defaultPageSize) {
    this.options.defaultPageSize = 10;
  }

  Object.keys(this.schema).forEach(function (modelName) {
    var modelSchema = self.schema[modelName];
    self.models[modelName] = self.thinky.createModel(modelName, modelSchema.fields);
  });

  this.accessControl = new AccessControl(this.scServer, this.thinky, this.options);

  this.scServer.on('_handshake', function (socket) {
    self._attachSocket(socket);
  });
};

SCCRUDRethink.prototype._isValidView = function (type, viewName) {
  var typeSchema = this.schema[type] || {};
  var modelViews = typeSchema.views || {};
  return modelViews.hasOwnProperty(viewName);
};

SCCRUDRethink.prototype._getViewMetaData = function (type, viewName) {
  var typeSchema = this.schema[type] || {};
  var modelViews = typeSchema.views || {};
  var viewSchema = modelViews[viewName] || {};

  return {
    transform: viewSchema.transform
  };
};

SCCRUDRethink.prototype._constructTransformedRethinkQuery = function (ModelClass, type, viewName, predicateData) {
  var viewMetaData = this._getViewMetaData(type, viewName);
  var rethinkQuery = ModelClass;

  var sanitizedPredicateData;
  if (predicateData == undefined) {
    sanitizedPredicateData = null;
  } else {
    sanitizedPredicateData = predicateData;
  }

  var transformFn = viewMetaData.transform;
  if (transformFn) {
    rethinkQuery = transformFn(rethinkQuery, this.thinky.r, sanitizedPredicateData);
  }

  return rethinkQuery;
};

SCCRUDRethink.prototype._getDocumentViewOffsets = function (documentId, query, callback) {
  var self = this;
  var ModelClass = this.models[query.type];

  if (ModelClass) {

    var tasks = [];
    var optimizationMap = query.optimization;

    if (optimizationMap == null) {
      callback(null, {});
    } else {
      _.forOwn(optimizationMap, function (predicateDataList, viewName) {
        if (self._isValidView(query.type, viewName)) {
          if (!(predicateDataList instanceof Array)) {
            predicateDataList = [predicateDataList];
          }

          if (predicateDataList.length <= self.maxPredicateDataCount) {
            predicateDataList.forEach(function (predicateData) {
              tasks.push(function (cb) {
                var rethinkQuery = self._constructTransformedRethinkQuery(ModelClass, query.type, viewName, predicateData);

                rethinkQuery.offsetsOf(self.thinky.r.row('id').eq(documentId)).execute(function (err, documentOffsets) {
                  if (err) {
                    cb(err);
                  } else {
                    cb(null, {
                      view: viewName,
                      id: documentId,
                      predicateData: predicateData,
                      offset: (documentOffsets && documentOffsets.length) ? documentOffsets[0] : null
                    });
                  }
                });
              });
            });
          } else {
            tasks.push(function (cb) {
              var error = new Error('Optimization failure - The length of the predicate data array for the view ' + viewName +
                ' exceeded the maxPredicateDataCount of ' + self.maxPredicateDataCount);
              error.name = 'CRUDOptimizationError';
              cb(error);
            });
          }
        }
      });

      async.parallel(tasks, function (err, results) {
        var viewOffetMap = {};
        if (!err) {
          results.forEach(function (viewOffset) {
            if (viewOffset != null) {
              viewOffetMap[viewOffset.view] = viewOffset;
            }
          });
        }
        callback(err, viewOffetMap);
      });
    }
  }
};

SCCRUDRethink.prototype._isWithinRealtimeBounds = function (offset) {
  return this.options.maximumRealtimeOffset == null || offset <= this.options.maximumRealtimeOffset;
};

SCCRUDRethink.prototype._getViewChannelName = function (viewName, predicateData, type) {
  var predicateDataString;
  if (predicateData == null) {
    predicateDataString = '';
  } else {
    predicateDataString = jsonStableStringify(predicateData);
  }
  return this.channelPrefix + viewName + '(' + predicateDataString + '):' + type;
};

SCCRUDRethink.prototype.create = function (query, callback) {
  var self = this;

  if (!query) {
    query = {};
  }

  var ModelClass = this.models[query.type];

  var savedHandler = function (err, result) {
    if (err) {
      callback && callback(err);
    } else {
      if (query.optimization == null) {
        self.scServer.exchange.publish(self.channelPrefix + query.type, {
          type: 'create',
          id: result.id
        });
      } else {
        self._getDocumentViewOffsets(result.id, query, function (err, viewOffsets) {
          if (!err) {
            _.forOwn(viewOffsets, function (offsetData, viewName) {
              if (self._isWithinRealtimeBounds(offsetData.offset)) {
                self.scServer.exchange.publish(self._getViewChannelName(viewName, offsetData.predicateData, query.type), {
                  type: 'create',
                  id: result.id,
                  offset: offsetData.offset
                });
              }
            });
          }
        });
      }
      callback && callback(err, result.id);
    }
  };

  if (ModelClass == null) {
    var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    savedHandler(error);
  } if (typeof query.value == 'object') {
    var instance = new ModelClass(query.value);
    instance.save(savedHandler);
  } else {
    var error = new Error('Cannot create a document from a primitive - Must be an object');
    error.name = 'CRUDInvalidParams';
    savedHandler(error);
  }
};

SCCRUDRethink.prototype.read = function (query, callback) {
  var self = this;

  if (!query) {
    query = {};
  }

  var pageSize = query.pageSize || this.options.defaultPageSize;

  var loadedHandler = function (err, data, count) {
    if (err) {
      callback && callback(err);
    } else {
      var result;
      if (query.id) {
        if (query.field) {
          if (data == null) {
            data = {};
          }
          result = data[query.field];
        } else {
          result = data;
        }
      } else {
        var documentList = [];
        var resultCount = Math.min(data.length, pageSize);

        for (var i = 0; i < resultCount; i++) {
          documentList.push(data[i].id || null);
        }
        result = {
          data: documentList
        };

        if (query.getCount) {
          result.count = count;
        }

        if (data.length < pageSize + 1) {
          result.isLastPage = true;
        }
      }
      // Return null instead of undefined - That way the frontend will know
      // that the value was read but didn't exist (or was null).
      if (result === undefined) {
        result = null;
      }
      callback && callback(null, result);
    }
  };

  var ModelClass = self.models[query.type];
  if (ModelClass == null) {
    var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    loadedHandler(error);
  } else {
    if (query.id) {
      ModelClass.get(query.id).run(loadedHandler);
    } else {
      var rethinkQuery = self._constructTransformedRethinkQuery(ModelClass, query.type, query.view, query.predicateData);

      var tasks = [];

      if (query.offset) {
        tasks.push(function (cb) {
          // Get one extra record just to check if we have the last value in the sequence.
          rethinkQuery.slice(query.offset, query.offset + pageSize + 1).pluck('id').run(cb);
        });
      } else {
        tasks.push(function (cb) {
          // Get one extra record just to check if we have the last value in the sequence.
          rethinkQuery.limit(pageSize + 1).pluck('id').run(cb);
        });
      }

      if (query.getCount) {
        tasks.push(function (cb) {
          rethinkQuery.count().execute(cb);
        });
      }

      async.parallel(tasks, function (err, results) {
        if (err) {
          loadedHandler(err);
        } else {
          loadedHandler(err, results[0], results[1]);
        }
      });
    }
  }
};

SCCRUDRethink.prototype.update = function (query, callback) {
  var self = this;

  if (!query) {
    query = {};
  }

  var savedHandler = function (err, oldViewOffsets, queryResult) {
    if (!err) {
      if (query.field) {
        var cleanValue = query.value;
        if (cleanValue === undefined) {
          cleanValue = null;
        }
        self.scServer.exchange.publish(self.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'update',
          value: cleanValue
        });
      } else {
        _.forOwn(query.value, function (value, field) {
          if (value === undefined) {
            value = null;
          }
          self.scServer.exchange.publish(self.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'update',
            value: value
          });
        });
      }

      if (query.optimization == null) {
        self.scServer.exchange.publish(self.channelPrefix + query.type, {
          type: 'update',
          id: query.id
        });
      } else {
        self._getDocumentViewOffsets(query.id, query, function (err, newViewOffsets) {
          if (!err) {
            _.forOwn(newViewOffsets, function (newOffsetData, viewName) {
              var oldOffsetData = oldViewOffsets[viewName] || {};
              newOffsetData = newOffsetData || {};

              if (oldOffsetData.offset != newOffsetData.offset) {
                if (self._isWithinRealtimeBounds(oldOffsetData.offset)) {
                  self.scServer.exchange.publish(self._getViewChannelName(viewName, oldOffsetData.predicateData, query.type), {
                    type: 'update',
                    freshness: 'old',
                    id: query.id,
                    offset: oldOffsetData.offset
                  });
                }
                if (self._isWithinRealtimeBounds(newOffsetData.offset)) {
                  self.scServer.exchange.publish(self._getViewChannelName(viewName, newOffsetData.predicateData, query.type), {
                    type: 'update',
                    freshness: 'new',
                    id: query.id,
                    offset: newOffsetData.offset
                  });
                }
              }
            });
          }
        });
      }
    }
    callback && callback(err);
  };

  var ModelClass = this.models[query.type];
  if (ModelClass == null) {
    var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    savedHandler(error);
  } else if (query.id == null) {
    var error = new Error('Cannot update document without specifying an id');
    error.name = 'CRUDInvalidParams';
    savedHandler(error);
  } else {
    var tasks = [];

    if (query.field) {
      if (query.field == 'id') {
        var error = new Error('Cannot modify the id field of an existing document');
        error.name = 'CRUDInvalidOperation';
        savedHandler(error);
      } else {
        if (query.optimization != null) {
          tasks.push(function (cb) {
            self._getDocumentViewOffsets(query.id, query, cb);
          });
        }

        tasks.push(function (cb) {
          ModelClass.get(query.id).run().then(function (instance) {
            instance[query.field] = query.value;
            instance.save(cb);
          }).error(cb);
        });
      }
    } else {
      if (typeof query.value == 'object') {
        if (query.value.id == null) {
          query.value.id = query.id;
        }
        if (query.optimization != null) {
          tasks.push(function (cb) {
            self._getDocumentViewOffsets(query.id, query, cb);
          });
        }

        tasks.push(function (cb) {
          // Replace the whole document
          var error;
          try {
            var instance = new ModelClass(query.value);
            instance.validate();
          } catch (e) {
            error = e;
          }
          if (error) {
            savedHandler(error);
          } else {
            self.thinky.r.table(query.type).get(query.id).update(query.value, {returnChanges: true}).run(cb);
          }
        });
      } else {
        var error = new Error('Cannot replace document with a primitive - Must be an object');
        error.name = 'CRUDInvalidOperation';
        savedHandler(error);
      }
    }
    if (tasks.length) {
      async.series(tasks, function (err, results) {
        if (err) {
          savedHandler(err);
        } else {
          if (query.optimization == null) {
            savedHandler(null, null, results[0]);
          } else {
            savedHandler(null, results[0], results[1]);
          }
        }
      });
    }
  }
};

SCCRUDRethink.prototype.delete = function (query, callback) {
  var self = this;

  if (!query) {
    query = {};
  }

  var deletedHandler = function (err, viewOffsets, result) {
    if (!err) {
      if (query.field) {
        self.scServer.exchange.publish(self.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'delete'
        });
      } else {
        var change = result.changes[0] || {};
        var oldValue = change.old_val;

        _.forOwn(oldValue, function (value, field) {
          self.scServer.exchange.publish(self.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'delete'
          });
        });
      }

      if (query.optimization == null) {
        self.scServer.exchange.publish(self.channelPrefix + query.type, {
          type: 'delete',
          id: query.id
        });
      } else {
        _.forOwn(viewOffsets, function (offsetData, viewName) {
          if (self._isWithinRealtimeBounds(offsetData.offset)) {
            self.scServer.exchange.publish(self._getViewChannelName(viewName, offsetData.predicateData, query.type), {
              type: 'delete',
              id: query.id,
              offset: offsetData.offset
            });
          }
        });
      }
    }
    callback && callback(self._formatErrorResponse(err));
  };

  var ModelClass = this.models[query.type];
  if (ModelClass == null) {
    var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    deletedHandler(error);
  } else {
    var tasks = [];

    if (query.id == null) {
      var error = new Error('Cannot delete an entire collection - ID must be provided');
      error.name = 'CRUDInvalidParams';
      deletedHandler(error);
    } else {
      if (query.optimization != null) {
        tasks.push(function (cb) {
          self._getDocumentViewOffsets(query.id, query, cb);
        });
      }

      if (query.field == null) {
        tasks.push(function (cb) {
          ModelClass.get(query.id).delete({returnChanges: true}).run(cb);
        });
      } else {
        tasks.push(function (cb) {
          ModelClass.get(query.id).run().then(function (instance) {
            delete instance[query.field];
            instance.save(cb);
          }).error(cb);
        });
      }
      if (tasks.length) {
        async.series(tasks, function (err, results) {
          if (err) {
            deletedHandler(err);
          } else {
            if (query.optimization == null) {
              deletedHandler(null, null, results[0]);
            } else {
              deletedHandler(null, results[0], results[1]);
            }
          }
        });
      }
    }
  }
};

SCCRUDRethink.prototype._attachSocket = function (socket) {
  socket.on('create', this.create.bind(this));
  socket.on('read', this.read.bind(this));
  socket.on('update', this.update.bind(this));
  socket.on('delete', this.delete.bind(this));
};

module.exports.thinky = thinky;

module.exports.attach = function (worker, options) {
  return new SCCRUDRethink(worker, options);
};
