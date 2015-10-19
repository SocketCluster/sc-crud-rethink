var _ = require('lodash');
var thinky = require('thinky');
var async = require('async');

/*
  TODO: Allow specifying maximum realtime offset
  TODO: Better errors for the client-side
  TODO: Allow getting the count of a collection
*/

var SCCRUDRethink = function (worker, options) {
  var self = this;

  this.scServer = worker.scServer;
  this.options = options || {};
  this.models = {};

  this.schema = this.options.schema || {};
  this.orders = this.options.orders || {};

  this.thinky = thinky(this.options.thinkyOptions);

  if (!this.options.defaultPageSize) {
    this.options.defaultPageSize = 10;
  }

  this.modelFieldViewMap = {};

  Object.keys(this.schema).forEach(function (modelName) {
    var modelSchema = self.schema[modelName];
    self.models[modelName] = self.thinky.createModel(modelName, modelSchema.fields);

    var viewsMap = modelSchema.views;

    var fieldViewModel = {};
    self.modelFieldViewMap[modelName] = fieldViewModel;

    _.forOwn(viewsMap, function (viewData, viewName) {
      var filter = viewData.filter;
      if (filter) {
        var filterFields = filter.affectedFields;

        filterFields.forEach(function (fieldName) {
          if (fieldViewModel[fieldName] == null) {
            fieldViewModel[fieldName] = {};
          }
          fieldViewModel[fieldName][viewName] = true;
        });
      }

      var order = viewData.order;
      if (order) {
        var orderFields = order.affectedFields;

        orderFields.forEach(function (fieldName) {
          if (fieldViewModel[fieldName] == null) {
            fieldViewModel[fieldName] = {};
          }
          fieldViewModel[fieldName][viewName] = true;
        });
      }
    });
  });

  this.scServer.on('_handshake', function (socket) {
    self._attachSocket(socket);
  });
};

SCCRUDRethink.prototype._getAffectedViews = function (type, field) {
  var affectedViews = [];

  if (field) {
    var modelFieldMap = this.modelFieldViewMap[type];
    if (modelFieldMap) {
      var fieldViewMap = modelFieldMap[field];
      _.forOwn(fieldViewMap, function (value, viewName) {
        affectedViews.push(viewName);
      });
    }
  } else {
    var modelSchema = this.schema[type] || {};
    _.forOwn(modelSchema.views, function (value, viewName) {
      affectedViews.push(viewName);
    });
  }
  return affectedViews;
};

SCCRUDRethink.prototype._getViewMetaData = function (type, view) {
  var modelViews = this.schema[type].views || {};
  var viewSchema = modelViews[view] || {};

  return {
    filter:  viewSchema.filter || {},
    order: viewSchema.order || {}
  };
};

SCCRUDRethink.prototype._constructOrderedFilteredRethinkQuery = function (ModelClass, query, view) {
  var viewMetaData = this._getViewMetaData(query.type, view);
  var rethinkQuery = ModelClass;

  var filterFn = viewMetaData.filter.predicateFactory;
  if (filterFn) {
    var sanitizedFilterData;
    if (query.filterData == undefined) {
      sanitizedFilterData = null;
    } else {
      sanitizedFilterData = query.filterData;
    }
    var filter = filterFn(this.thinky.r, sanitizedFilterData);
    rethinkQuery = rethinkQuery.filter(filter);
  }

  var orderFn = viewMetaData.order.predicateFactory;
  if (orderFn) {
    var sanitizedOrderData;
    if (query.orderData == undefined) {
      sanitizedOrderData = null;
    } else {
      sanitizedOrderData = query.orderData;
    }
    var order = orderFn(this.thinky.r, sanitizedOrderData);
    rethinkQuery = rethinkQuery.orderBy(order);
  }
  return rethinkQuery;
};

SCCRUDRethink.prototype._getDocumentViewOffsets = function (documentId, query, callback) {
  var self = this;
  var ModelClass = this.models[query.type];

  if (ModelClass) {

    var affectedViews = this._getAffectedViews(query.type, query.field);
    var tasks = [];

    affectedViews.forEach(function (viewName) {
      tasks.push(function (cb) {
        var rethinkQuery = self._constructOrderedFilteredRethinkQuery(ModelClass, query, viewName);

        rethinkQuery.offsetsOf(self.thinky.r.row('id').eq(documentId)).execute(function (err, documentOffsets) {
          if (err) {
            cb(err);
          } else {
            cb(null, {
              view: viewName,
              id: documentId,
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

SCCRUDRethink.prototype.create = function (query, callback) {
  var self = this;

  var ModelClass = this.models[query.type];

  var savedHandler = function (err, result) {
    if (err) {
      callback && callback(err);
    } else {
      self._getDocumentViewOffsets(result.id, query, function (err, viewOffets) {
        if (!err) {
          _.forOwn(viewOffets, function (offsetData, viewName) {
            if (self._isWithinRealtimeBounds(offsetData.offset)) {
              self.scServer.global.publish(viewName + ':' + query.type, {
                type: 'create',
                id: offsetData.id,
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
    savedHandler('The ' + query.type + ' model type is not supported - It is not part of the schema');
  } if (typeof query.value == 'object') {
    var instance = new ModelClass(query.value);
    instance.save(savedHandler);
  } else {
    savedHandler('Cannot create a document from a primitive - Must be an object');
  }
};

SCCRUDRethink.prototype.read = function (query, callback) {
  var self = this;

  var pageSize = query.pageSize || this.options.defaultPageSize;
  var loadedHandler = function (err, data) {
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

        if (data.length < pageSize + 1) {
          result.isLastPage = true;
        }
      }

      callback && callback(null, result);
    }
  };

  var ModelClass = self.models[query.type];
  if (ModelClass == null) {
    loadedHandler('The ' + query.type + ' model type is not supported - It is not part of the schema');
  } else {
    if (query.id) {
      ModelClass.get(query.id).run(loadedHandler);
    } else {
      var rethinkQuery = self._constructOrderedFilteredRethinkQuery(ModelClass, query, query.view);

      if (query.offset) {
        rethinkQuery.slice(query.offset, query.offset + pageSize + 1).pluck('id').run(loadedHandler);
      } else {
        // Get one extra record just to check if we have the last value in the sequence.
        rethinkQuery.limit(pageSize + 1).pluck('id').run(loadedHandler);
      }
    }
  }
};

// TODO: Don't allow changing the id
SCCRUDRethink.prototype.update = function (query, callback) {
  var self = this;

  var savedHandler = function (err, oldViewOffsets, queryResult) {
    if (!err) {
      if (query.field) {
        self.scServer.global.publish(query.type + '/' + query.id + '/' + query.field, {
          type: 'update',
          value: query.value
        });
      } else {
        _.forOwn(queryResult, function (value, field) {
          self.scServer.global.publish(query.type + '/' + query.id + '/' + field, {
            type: 'update',
            value: value
          });
        });
      }

      self._getDocumentViewOffsets(query.id, query, function (err, newViewOffsets) {
        if (!err) {
          _.forOwn(newViewOffsets, function (newOffsetData, viewName) {
            var oldOffsetData = oldViewOffsets[viewName] || {};
            newOffsetData = newOffsetData || {};

            if (oldOffsetData.offset != newOffsetData.offset) {
              if (self._isWithinRealtimeBounds(oldOffsetData.offset)) {
                self.scServer.global.publish(viewName + ':' + query.type, {
                  type: 'update',
                  freshness: 'old',
                  id: query.id,
                  offset: oldOffsetData.offset
                });
              }
              if (self._isWithinRealtimeBounds(newOffsetData.offset)) {
                self.scServer.global.publish(viewName + ':' + query.type, {
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
    savedHandler('The ' + query.type + ' model type is not supported - It is not part of the schema');
  } else if (query.id == null) {
    savedHandler('Cannot update document without specifying an id');
  } else {
    var tasks = [];

    if (query.field) {
      tasks.push(function (cb) {
        self._getDocumentViewOffsets(query.id, query, cb);
      });

      tasks.push(function (cb) {
        ModelClass.get(query.id).run().then(function (instance) {
          instance[query.field] = query.value;
          instance.save(cb);
        }).error(cb);
      });
    } else {
      if (typeof query.value == 'object') {
        if (query.value.id == null) {
          query.value.id = query.id;
        }

        tasks.push(function (cb) {
          self._getDocumentViewOffsets(query.id, query, cb);
        });

        tasks.push(function (cb) {
          // Replace the whole document
          ModelClass.get(query.id).replace(query.value).run(cb);
        });
      } else {
        savedHandler('Cannot replace document with a primitive - Must be an object');
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

SCCRUDRethink.prototype.delete = function (query, callback) {
  var self = this;

  var deletedHandler = function (err, result) {
    if (!err) {
      if (query.field) {
        self.scServer.global.publish(query.type + '/' + query.id + '/' + query.field, {
          type: 'delete',
          value: query.value
        });
      } else {
        var change = result.changes[0] || {};
        var oldValue = change.old_val;

        _.forOwn(oldValue, function (value, field) {
          self.scServer.global.publish(query.type + '/' + query.id + '/' + field, {
            type: 'delete',
            value: value
          });
        });
      }
    }
    callback && callback(err);
  };

  var ModelClass = this.models[query.type];
  if (ModelClass == null) {
    deletedHandler('The ' + query.type + ' model type is not supported - It is not part of the schema');
  } else {
    if (query.id == null) {
      deletedHandler('Cannot delete an entire collection - ID must be provided');
    } else {
      self._getDocumentViewOffsets(query.id, query, function (err, viewOffets) {
        if (!err) {
          _.forOwn(viewOffets, function (offsetData, viewName) {
            if (self._isWithinRealtimeBounds(offsetData.offset)) {
              self.scServer.global.publish(viewName + ':' + query.type, {
                type: 'delete',
                id: query.id,
                offset: offsetData.offset
              });
            }
          });
        }
        ModelClass.get(query.id).delete({returnChanges: true}).run(deletedHandler);
      });
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
