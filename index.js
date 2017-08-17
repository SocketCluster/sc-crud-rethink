var _ = require('lodash');
var thinky = require('thinky');
var async = require('async');
var Filter = require('./filter');
var Cache = require('./cache');
var jsonStableStringify = require('json-stable-stringify');
var constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
var parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;

var SCCRUDRethink = function (options) {
  var self = this;

  this.options = options || {};

  this.models = {};
  this.schema = this.options.schema || {};
  this.thinky = thinky(this.options.thinkyOptions);
  this.options.thinky = this.thinky;

  this.channelPrefix = 'crud>';

  if (!this.options.defaultPageSize) {
    this.options.defaultPageSize = 10;
  }

  Object.keys(this.schema).forEach(function (modelName) {
    var modelSchema = self.schema[modelName];
    self.models[modelName] = self.thinky.createModel(modelName, modelSchema.fields);
  });
  this.options.models = this.models;

  var cacheDisabled;
  if (this.options.worker) {
    this.scServer = this.options.worker.scServer;
    cacheDisabled = this.options.cacheDisabled || false;
  } else {
    this.scServer = null;
    if (this.options.hasOwnProperty('cacheDisabled')) {
      cacheDisabled = this.options.cacheDisabled || false;
    } else {
      // If worker is not defined and cacheDisabled isn't defined,
      // then by default we will disable the cache.
      cacheDisabled = true;
    }
  }

  this.cache = new Cache({
    cacheDisabled: cacheDisabled,
    cacheDuration: this.options.cacheDuration
  });
  this.options.cache = this.cache;

  this.cache.on('expire', this._cleanupResourceChannel.bind(this));
  this.cache.on('clear', this._cleanupResourceChannel.bind(this));

  this._resourceReadBuffer = {};

  if (this.scServer) {
    this.filter = new Filter(this.scServer, this.options);

    this.publish = this.scServer.exchange.publish.bind(this.scServer.exchange);

    this.scServer.on('_handshake', function (socket) {
      self._attachSocket(socket);
    });
  } else {
    // If no server is available, publish will be a no-op.
    this.publish = function () {};
  }
};

SCCRUDRethink.prototype._getResourceChannelName = function (resource) {
  return this.channelPrefix + resource.type + '/' + resource.id;
};

SCCRUDRethink.prototype._cleanupResourceChannel = function (resource) {
  var resourceChannelName = this._getResourceChannelName(resource);
  var resourceChannel = this.scServer.exchange.channel(resourceChannelName);
  resourceChannel.unsubscribe();
  resourceChannel.destroy();
};

SCCRUDRethink.prototype._handleResourceChange = function (resource) {
  this.cache.clear(resource);
};

SCCRUDRethink.prototype._getViews = function (type) {
  var typeSchema = this.schema[type] || {};
  return typeSchema.views || {};
};

SCCRUDRethink.prototype._isValidView = function (type, viewName) {
  var modelViews = this._getViews(type);
  return modelViews.hasOwnProperty(viewName);
};

SCCRUDRethink.prototype._getDocumentViewOffsets = function (document, query, callback) {
  var self = this;
  var ModelClass = this.models[query.type];

  if (ModelClass) {
    var tasks = [];

    var viewSchemaMap = this._getViews(query.type);

    _.forOwn(viewSchemaMap, function (viewSchema, viewName) {
      var viewParams;
      if (viewSchema.paramFields && viewSchema.paramFields.length) {
        viewParams = {};
        _.forEach(viewSchema.paramFields, function (fieldName) {
          viewParams[fieldName] = document[fieldName];
        });
      } else {
        viewParams = null;
      }

      tasks.push(function (cb) {
        var rethinkQuery = constructTransformedRethinkQuery(self.options, ModelClass, query.type, viewName, document);

        rethinkQuery.offsetsOf(self.thinky.r.row('id').eq(document.id)).execute(function (err, documentOffsets) {
          if (err) {
            cb(err);
          } else {
            cb(null, {
              view: viewName,
              id: document.id,
              viewParams: viewParams,
              offset: (documentOffsets && documentOffsets.length) ? documentOffsets[0] : null
            });
          }
        });
      });
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
};

SCCRUDRethink.prototype._isWithinRealtimeBounds = function (offset) {
  return this.options.maximumRealtimeOffset == null || offset <= this.options.maximumRealtimeOffset;
};

SCCRUDRethink.prototype._getViewChannelName = function (viewName, viewParams, type) {
  var viewParamsString;
  if (viewParams == null) {
    viewParamsString = '';
  } else {
    viewParamsString = jsonStableStringify(viewParams);
  }
  return this.channelPrefix + viewName + '(' + viewParamsString + '):' + type;
};

SCCRUDRethink.prototype.create = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var ModelClass = this.models[query.type];

  var savedHandler = function (err, result) {
    if (err) {
      callback && callback(err);
    } else {
      var resourceChannelName = self._getResourceChannelName({
        type: query.type,
        id: result.id
      });
      self.publish(resourceChannelName, 1);

      self._getDocumentViewOffsets(result, query, function (err, viewOffsets) {
        if (!err) {
          _.forOwn(viewOffsets, function (offsetData, viewName) {
            if (self._isWithinRealtimeBounds(offsetData.offset)) {
              self.publish(self._getViewChannelName(viewName, offsetData.viewParams, query.type), {
                type: 'create',
                id: result.id,
                offset: offsetData.offset
              });
            }
          });
        }
      });
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

SCCRUDRethink.prototype._processResourceReadBuffer = function (error, resourceChannelName, query, dataProvider) {
  var self = this;

  var callbackList = this._resourceReadBuffer[resourceChannelName] || [];
  if (error) {
    callbackList.forEach(function (callback) {
      callback(error);
    });
  } else {
    callbackList.forEach(function (callback) {
      self.cache.pass(query, dataProvider, callback);
    });
  }
  delete this._resourceReadBuffer[resourceChannelName];
};

SCCRUDRethink.prototype.read = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var pageSize = query.pageSize || this.options.defaultPageSize;

  var loadedHandler = function (err, data, count) {
    if (err) {
      callback && callback(err);
    } else {
      // If socket does not exist, then the CRUD operation comes from the server-side
      // and we don't need to pass it through a filter.
      var applyPostFilter;
      if (socket && self.filter) {
        applyPostFilter = self.filter.applyPostFilter.bind(self.filter);
      } else {
        applyPostFilter = function (req, next) {
          next();
        };
      }
      var filterRequest = {
        r: self.thinky.r,
        socket: socket,
        action: 'read',
        authToken: socket && socket.authToken,
        query: query,
        resource: data
      };
      applyPostFilter(filterRequest, function (err) {
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
      });
    }
  };

  var ModelClass = self.models[query.type];
  if (ModelClass == null) {
    var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
    error.name = 'CRUDInvalidModelType';
    loadedHandler(error);
  } else {
    if (query.id) {
      var dataProvider = function (cb) {
        ModelClass.get(query.id).run(cb);
      };
      var resourceChannelName = self._getResourceChannelName(query);

      var isSubscribedToResourceChannel = self.scServer.exchange.isSubscribed(resourceChannelName);
      var isSubscribedToResourceChannelOrPending = self.scServer.exchange.isSubscribed(resourceChannelName, true);
      var isSubcriptionPending = !isSubscribedToResourceChannel && isSubscribedToResourceChannelOrPending;

      if (!self._resourceReadBuffer[resourceChannelName]) {
        self._resourceReadBuffer[resourceChannelName] = [];
      }
      self._resourceReadBuffer[resourceChannelName].push(loadedHandler);

      if (isSubscribedToResourceChannel) {
        // If it is fully subscribed, we can process the request straight away since we are
        // confident that the data is up to date (in real-time).
        self._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
      } else if (!isSubcriptionPending) {
        // If there is no pending subscription, then we should create one and process the
        // buffer when we're subscribed.
        function handleResourceSubscribeFailure(err) {
          // TODO test failure case
          resourceChannel.removeListener('subscribe', handleResourceSubscribe);
          var error = new Error('Failed to subscribe to resource channel for the ' + query.type + ' model');
          error.name = 'FailedToSubscribeToResourceChannel';
          self._processResourceReadBuffer(error, resourceChannelName, query, dataProvider);
        }
        function handleResourceSubscribe() {
          resourceChannel.removeListener('subscribeFail', handleResourceSubscribeFailure);
          self._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
        }

        var resourceChannel = self.scServer.exchange.subscribe(resourceChannelName);
        resourceChannel.once('subscribe', handleResourceSubscribe);
        resourceChannel.once('subscribeFail', handleResourceSubscribeFailure);
        resourceChannel.watch(self._handleResourceChange.bind(self, query));
      }
    } else {
      var rethinkQuery = constructTransformedRethinkQuery(self.options, ModelClass, query.type, query.view, query.viewParams);

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

SCCRUDRethink.prototype.update = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var savedHandler = function (err, oldViewOffsets, queryResult) {
    if (!err) {
      var resourceChannelName = self._getResourceChannelName(query);
      self.publish(resourceChannelName, 1);

      if (query.field) {
        var cleanValue = query.value;
        if (cleanValue === undefined) {
          cleanValue = null;
        }
        self.publish(self.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'update',
          value: cleanValue
        });
      } else {
        _.forOwn(query.value, function (value, field) {
          if (value === undefined) {
            value = null;
          }
          self.publish(self.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'update',
            value: value
          });
        });
      }

      self._getDocumentViewOffsets(queryResult, query, function (err, newViewOffsets) {
        if (!err) {
          _.forOwn(newViewOffsets, function (newOffsetData, viewName) {
            var oldOffsetData = oldViewOffsets[viewName] || {};
            newOffsetData = newOffsetData || {};

            if (oldOffsetData.offset != newOffsetData.offset) {
              if (self._isWithinRealtimeBounds(oldOffsetData.offset)) {
                self.publish(self._getViewChannelName(viewName, oldOffsetData.viewParams, query.type), {
                  type: 'update',
                  freshness: 'old',
                  id: query.id,
                  offset: oldOffsetData.offset
                });
              }
              if (self._isWithinRealtimeBounds(newOffsetData.offset)) {
                self.publish(self._getViewChannelName(viewName, newOffsetData.viewParams, query.type), {
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

    // If socket does not exist, then the CRUD operation comes from the server-side
    // and we don't need to pass it through a filter.
    var applyPostFilter;
    if (socket && self.filter) {
      applyPostFilter = self.filter.applyPostFilter.bind(self.filter);
    } else {
      applyPostFilter = function (req, next) {
        next();
      };
    }

    var filterRequest = {
      r: self.thinky.r,
      socket: socket,
      action: 'update',
      authToken: socket && socket.authToken,
      query: query
    };

    var modelInstance;
    var loadModelInstanceAndGetViewOffsets = function (cb) {
      ModelClass.get(query.id).run().then(function (instance) {
        modelInstance = instance;
        self._getDocumentViewOffsets(modelInstance, query, cb);
      }).error(cb);
    };

    if (query.field) {
      if (query.field == 'id') {
        var error = new Error('Cannot modify the id field of an existing document');
        error.name = 'CRUDInvalidOperation';
        savedHandler(error);
      } else {
        tasks.push(loadModelInstanceAndGetViewOffsets);

        tasks.push(function (cb) {
          filterRequest.resource = modelInstance;
          applyPostFilter(filterRequest, function (err) {
            if (err) {
              cb(err);
            } else {
              modelInstance[query.field] = query.value;
              modelInstance.save(cb);
            }
          });
        });
      }
    } else {
      if (typeof query.value == 'object') {
        tasks.push(loadModelInstanceAndGetViewOffsets);

        tasks.push(function (cb) {
          filterRequest.resource = modelInstance;
          applyPostFilter(filterRequest, function (err) {
            if (err) {
              cb(err);
            } else {
              _.forOwn(query.value, function (value, field) {
                modelInstance[field] = value;
              });
              modelInstance.save(cb);
            }
          });
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
          savedHandler(null, results[0], results[1]);
        }
      });
    }
  }
};

SCCRUDRethink.prototype.delete = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var deletedHandler = function (err, viewOffsets, result) {
    if (!err) {
      if (query.field) {
        self.publish(self.channelPrefix + query.type + '/' + query.id + '/' + query.field, {
          type: 'delete'
        });
      } else {
        var deletedFields;
        var modelSchema = self.schema[query.type];
        if (modelSchema && modelSchema.fields) {
          deletedFields = modelSchema.fields;
        } else {
          deletedFields = result;
        }
        _.forOwn(deletedFields, function (value, field) {
          self.publish(self.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'delete'
          });
        });

        _.forOwn(viewOffsets, function (offsetData, viewName) {
          if (self._isWithinRealtimeBounds(offsetData.offset)) {
            self.publish(self._getViewChannelName(viewName, offsetData.viewParams, query.type), {
              type: 'delete',
              id: query.id,
              offset: offsetData.offset
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
    deletedHandler(error);
  } else {
    var tasks = [];

    if (query.id == null) {
      var error = new Error('Cannot delete an entire collection - ID must be provided');
      error.name = 'CRUDInvalidParams';
      deletedHandler(error);
    } else {
      var modelInstance;
      tasks.push(function (cb) {
        ModelClass.get(query.id).run().then(function (instance) {
          modelInstance = instance;
          self._getDocumentViewOffsets(modelInstance, query, cb);
        }).error(cb);
      });

      // If socket does not exist, then the CRUD operation comes from the server-side
      // and we don't need to pass it through a filter.
      var applyPostFilter;
      if (socket && self.filter) {
        applyPostFilter = self.filter.applyPostFilter.bind(self.filter);
      } else {
        applyPostFilter = function (req, next) {
          next();
        };
      }

      var filterRequest = {
        r: self.thinky.r,
        socket: socket,
        action: 'delete',
        authToken: socket && socket.authToken,
        query: query
      };

      if (query.field == null) {
        tasks.push(function (cb) {
          filterRequest.resource = modelInstance;
          applyPostFilter(filterRequest, function (err) {
            if (err) {
              cb(err);
            } else {
              modelInstance.delete(cb);
            }
          });
        });
      } else {
        tasks.push(function (cb) {
          filterRequest.resource = modelInstance;
          applyPostFilter(filterRequest, function (err) {
            if (err) {
              cb(err);
            } else {
              delete modelInstance[query.field];
              modelInstance.save(cb);
            }
          });
        });
      }
      if (tasks.length) {
        async.series(tasks, function (err, results) {
          if (err) {
            deletedHandler(err);
          } else {
            deletedHandler(null, results[0], results[1]);
          }
        });
      }
    }
  }
};

SCCRUDRethink.prototype._attachSocket = function (socket) {
  var self = this;
  socket.on('create', function (query, callback) {
    self.create(query, callback, socket);
  });
  socket.on('read', function (query, callback) {
    self.read(query, callback, socket);
  });
  socket.on('update', function (query, callback) {
    self.update(query, callback, socket);
  });
  socket.on('delete', function (query, callback) {
    self.delete(query, callback, socket);
  });
};

module.exports.thinky = thinky;
module.exports.SCCRUDRethink = SCCRUDRethink;

module.exports.attach = function (worker, options) {
  if (options) {
    options.worker = worker;
  } else {
    options = {worker: worker};
  }
  return new SCCRUDRethink(options);
};
