var thinky = require('thinky');
var async = require('async');
var Filter = require('./filter');
var Cache = require('./cache');
var EventEmitter = require('events').EventEmitter;
var jsonStableStringify = require('json-stable-stringify');
var constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
var parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;

var SCCRUDRethink = function (options) {
  var self = this;
  EventEmitter.call(this);

  this.options = Object.assign({}, options);

  if (!this.options.logger) {
    this.options.logger = console;
  }
  if (!this.options.schema) {
    this.options.schema = {};
  }

  this.models = {};
  this.schema = this.options.schema;
  this.thinky = thinky(this.options.thinkyOptions);
  this.options.thinky = this.thinky;
  this.logger = this.options.logger;

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

SCCRUDRethink.prototype = Object.create(EventEmitter.prototype);

SCCRUDRethink.prototype._getResourceChannelName = function (resource) {
  return this.channelPrefix + resource.type + '/' + resource.id;
};

SCCRUDRethink.prototype._getResourcePropertyChannelName = function (resourceProperty) {
  return this.channelPrefix + resourceProperty.type + '/' + resourceProperty.id + '/' + resourceProperty.field;
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

SCCRUDRethink.prototype._getView = function (type, viewName) {
  var modelViews = this._getViews(type);
  return modelViews[viewName];
};

// Find the offset index of a document within each of its affected views.
// Later, we can use this information to determine how a change to a document's
// property affects each view within the overall system.
SCCRUDRethink.prototype._getDocumentViewOffsets = function (document, query, callback) {
  var self = this;
  var ModelClass = this.models[query.type];

  if (ModelClass) {
    var tasks = [];

    var changeSettings = {
      type: query.type,
      resource: document
    };
    if (query.field) {
      changeSettings.fields = [query.field];
    }

    var affectedViewSchemaMap = this.getAffectedViews(changeSettings);

    affectedViewSchemaMap.forEach(function (viewData) {
      var viewName = viewData.view;
      var viewParams = viewData.params;

      tasks.push(function (cb) {
        var rethinkQuery = constructTransformedRethinkQuery(self.options, ModelClass, query.type, viewName, document);

        // On an indexed result set, offsetsOf time complexity should be O(1).
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
      var viewOffsetMap = {};
      if (err) {
        self.emit('warning', err);
      } else {
        results.forEach(function (viewOffset) {
          if (viewOffset != null) {
            viewOffsetMap[viewOffset.view] = viewOffset;
          }
        });
      }
      callback(err, viewOffsetMap);
    });
  }
};

SCCRUDRethink.prototype._isWithinRealtimeBounds = function (offset) {
  return this.options.maximumRealtimeOffset == null || offset <= this.options.maximumRealtimeOffset;
};

SCCRUDRethink.prototype._getViewChannelName = function (viewName, viewParams, type) {
  var primaryParams;
  var viewSchema = this._getView(type, viewName);

  if (viewSchema && viewSchema.primaryKeys) {
    primaryParams = {};

    viewSchema.primaryKeys.forEach(function (field) {
      primaryParams[field] = viewParams[field] === undefined ? null : viewParams[field];
    });
  } else {
    primaryParams = viewParams || {};
  }

  var viewPrimaryParamsString = jsonStableStringify(primaryParams);
  return this.channelPrefix + viewName + '(' + viewPrimaryParamsString + '):' + type;
};

SCCRUDRethink.prototype._areViewParamsEqual = function (viewParamsA, viewParamsB) {
  var viewParamsStringA = jsonStableStringify(viewParamsA || {});
  var viewParamsStringB = jsonStableStringify(viewParamsB || {});
  return viewParamsStringA == viewParamsStringB;
};

SCCRUDRethink.prototype.getModifiedResourceFields = function (updateDetails) {
  var oldResource = updateDetails.oldResource || {};
  var newResource = updateDetails.newResource || {};
  var modifiedFieldsMap = {};

  Object.keys(oldResource).forEach(function (fieldName) {
    if (oldResource[fieldName] !== newResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  });
  Object.keys(newResource).forEach(function (fieldName) {
    if (!modifiedFieldsMap.hasOwnProperty(fieldName) && newResource[fieldName] !== oldResource[fieldName]) {
      modifiedFieldsMap[fieldName] = {before: oldResource[fieldName], after: newResource[fieldName]};
    }
  });

  return modifiedFieldsMap;
};

SCCRUDRethink.prototype.getAffectedViews = function (updateDetails) {
  var self = this;

  var affectedViews = [];
  var resource = updateDetails.resource || {};

  var viewSchemaMap = this._getViews(updateDetails.type);

  Object.keys(viewSchemaMap).forEach(function (viewName) {
    var viewSchema = viewSchemaMap[viewName];
    var paramFields = viewSchema.paramFields || [];

    var viewParams = {};
    paramFields.forEach(function (fieldName) {
      viewParams[fieldName] = resource[fieldName];
    });

    if (updateDetails.fields) {
      var updatedFields = updateDetails.fields;
      var affectingFields = viewSchema.affectingFields || [];
      var isViewAffectedByUpdate = false;

      var affectingFieldsLookup = {
        id: true
      };
      paramFields.forEach(function (fieldName) {
        affectingFieldsLookup[fieldName] = true;
      });
      affectingFields.forEach(function (fieldName) {
        affectingFieldsLookup[fieldName] = true;
      });

      var modifiedFieldsLength = updatedFields.length;
      for (var i = 0; i < modifiedFieldsLength; i++) {
        var fieldName = updatedFields[i];
        if (affectingFieldsLookup[fieldName]) {
          isViewAffectedByUpdate = true;
          break;
        }
      }

      if (isViewAffectedByUpdate) {
        affectedViews.push({
          view: viewName,
          type: updateDetails.type,
          params: viewParams
        });
      }
    } else {
      affectedViews.push({
        view: viewName,
        type: updateDetails.type,
        params: viewParams
      });
    }
  });
  return affectedViews;
};

/*
  If you update the database outside of sc-crud-rethink, you can use this method
  to clear sc-crud-rethink cache for a resource and notify all client subscribers
  about the change.

  The updateDetails argument must be an object with the following properties:
    type: The resource type which was updated (name of the collection).
    id: The id of the specific resource/document which was updated.
    fields (optional): Fields which were updated within the resource - Can be either
      an array of field names or an object where each key represents a field name
      and each value represents the new updated value for the field (providing
      updated values is a performance optimization).
*/
SCCRUDRethink.prototype.notifyResourceUpdate = function (updateDetails) {
  var self = this;

  var resourceChannelName = self._getResourceChannelName(updateDetails);
  // This will cause the resource cache to clear itself.
  self.publish(resourceChannelName);

  var updatedFields = updateDetails.fields || [];
  if (Array.isArray(updatedFields)) {
    updatedFields.forEach(function (fieldName) {
      var resourcePropertyChannelName = self._getResourcePropertyChannelName({
        type: updateDetails.type,
        id: updateDetails.id,
        field: fieldName
      });
      // Notify individual field subscribers about the change.
      self.publish(resourcePropertyChannelName);
    });
  } else {
    // Notify individual field subscribers about the change and provide the new value.
    Object.keys(updatedFields).forEach(function (fieldName) {
      var resourcePropertyChannelName = self._getResourcePropertyChannelName({
        type: updateDetails.type,
        id: updateDetails.id,
        field: fieldName
      });
      self.publish(resourcePropertyChannelName, {
        type: 'update',
        value: updatedFields[fieldName]
      });
    });
  }
};

/*
  If you update the database outside of sc-crud-rethink, you can use this method
  to clear sc-crud-rethink cache for a view and notify all client subscribers
  about the change.

  The updateDetails argument must be an object with the following properties:
    view: The name of the view.
    params: The predicate object/value which defines the affected view.
    type: The resource type which was updated (name of the collection).
    offsets (optional): An array of affected indexes within the view which
    were affected by the update (this is for performance optimization).
*/
SCCRUDRethink.prototype.notifyViewUpdate = function (updateDetails) {
  var self = this;

  var viewChannelName = self._getViewChannelName(
    updateDetails.view,
    updateDetails.params,
    updateDetails.type
  );
  var offsets = updateDetails.offsets || [];
  if (offsets.length) {
    offsets.forEach(function (offset) {
      self.publish(viewChannelName, {offset: offset});
    });
  } else {
    self.publish(viewChannelName);
  }
};

/*
  If you update the database outside of sc-crud-rethink, you can use this method
  to clear sc-crud-rethink cache and notify all client subscribers (both the resource
  and any affected views) about the change.

  The updateDetails argument must be an object with the following properties:
    type: The resource type which was updated (name of the collection).
    oldResource: The old document/resource before the update was made.
      If the resource did not exist before (newly created), then this
      should be set to null.
    newResource: The new document/resource after the update was made.
      If the resource no longer exists after the operation (deleted), then
      this should be set to null.
*/
SCCRUDRethink.prototype.notifyUpdate = function (updateDetails) {
  var self = this;

  var refResource = updateDetails.oldResource || updateDetails.newResource || {};
  var oldResource = updateDetails.oldResource || {};
  var newResource = updateDetails.newResource || {};

  var updatedFieldsMap = this.getModifiedResourceFields(updateDetails);
  var updatedFieldsList = Object.keys(updatedFieldsMap);

  if (!updatedFieldsList.length) {
    return;
  }

  this.notifyResourceUpdate({
    type: updateDetails.type,
    id: refResource.id,
    fields: updatedFieldsList
  });

  var oldViewParams = {};
  var oldResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: oldResource,
    fields: updatedFieldsList
  });
  oldResourceAffectedViews.forEach(function (viewData) {
    oldViewParams[viewData.view] = viewData.params;
    self.notifyViewUpdate({
      type: viewData.type,
      view: viewData.view,
      params: viewData.params
    });
  });

  var newResourceAffectedViews = this.getAffectedViews({
    type: updateDetails.type,
    resource: newResource,
    fields: updatedFieldsList
  });

  newResourceAffectedViews.forEach(function (viewData) {
    if (!self._areViewParamsEqual(oldViewParams[viewData.view], viewData.params)) {
      self.notifyViewUpdate({
        type: viewData.type,
        view: viewData.view,
        params: viewData.params
      });
    }
  });
};

// Add a new document to a collection. This will send a change notification to each
// affected view (taking into account the affected page number within each view).
// This allows views to update themselves on the front-end in real-time.
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
      self.publish(resourceChannelName);

      self._getDocumentViewOffsets(result, query, function (err, viewOffsets) {
        if (err) {
          self.emit('warning', err);
        } else {
          Object.keys(viewOffsets).forEach(function (viewName) {
            var offsetData = viewOffsets[viewName];
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

SCCRUDRethink.prototype._appendToResourceReadBuffer = function (resourceChannelName, loadedHandler) {
  if (!this._resourceReadBuffer[resourceChannelName]) {
    this._resourceReadBuffer[resourceChannelName] = [];
  }
  this._resourceReadBuffer[resourceChannelName].push(loadedHandler);
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

// Read either a collection of IDs, a single document or a single field
// within a document. To achieve efficient field-level granularity, a cache is used.
// A cache entry will automatically get cleared when sc-crud-rethink detects
// a real-time change to a field which is cached.
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
        ModelClass.get(query.id).run(function (err, data) {
          var error;
          if (err) {
            self.logger.error(err);
            error = new Error(`Failed to get resource with id ${query.id} from the database`);
          } else {
            error = null;
          }
          cb(error, data);
        });
      };
      var resourceChannelName = self._getResourceChannelName(query);

      var isSubscribedToResourceChannel = self.scServer.exchange.isSubscribed(resourceChannelName);
      var isSubscribedToResourceChannelOrPending = self.scServer.exchange.isSubscribed(resourceChannelName, true);
      var isSubcriptionPending = !isSubscribedToResourceChannel && isSubscribedToResourceChannelOrPending;

      self._appendToResourceReadBuffer(resourceChannelName, loadedHandler);

      if (isSubscribedToResourceChannel) {
        // If it is fully subscribed, we can process the request straight away since we are
        // confident that the data is up to date (in real-time).
        self._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
      } else if (!isSubcriptionPending) {
        // If there is no pending subscription, then we should create one and process the
        // buffer when we're subscribed.
        function handleResourceSubscribe() {
          resourceChannel.removeListener('subscribeFail', handleResourceSubscribeFailure);
          self._processResourceReadBuffer(null, resourceChannelName, query, dataProvider);
        }
        function handleResourceSubscribeFailure(err) {
          resourceChannel.removeListener('subscribe', handleResourceSubscribe);
          var error = new Error('Failed to subscribe to resource channel for the ' + query.type + ' model');
          error.name = 'FailedToSubscribeToResourceChannel';
          self._processResourceReadBuffer(error, resourceChannelName, query, dataProvider);
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
          var error = new Error(`Failed to generate view ${query.view} for type ${query.type} with viewParams ${JSON.stringify(query.viewParams)}`);
          self.logger.error(err);
          self.logger.error(error);
          loadedHandler(error);
        } else {
          loadedHandler(null, results[0], results[1]);
        }
      });
    }
  }
};

// Update a single whole document or one or more fields within a document.
// Whenever a document is updated, it may affect the ordering and pagination of
// certain views. This update operation will send notifications to all affected
// clients to let them know if a view that they are currently looking at
// has been affected by the update operation - This allows them to update
// themselves in real-time.
SCCRUDRethink.prototype.update = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var savedHandler = function (err, oldViewOffsets, queryResult) {
    if (err) {
      self.emit('warning', err);
    } else {
      var resourceChannelName = self._getResourceChannelName(query);
      self.publish(resourceChannelName);

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
        var queryValue = query.value || {};
        Object.keys(queryValue).forEach(function (field) {
          var value = queryValue[field];
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
        if (err) {
          self.emit('warning', err);
        } else {
          Object.keys(newViewOffsets).forEach(function (viewName) {
            var newOffsetData = newViewOffsets[viewName] || {};
            var oldOffsetData = oldViewOffsets[viewName] || {};

            var areViewParamsEqual = self._areViewParamsEqual(oldOffsetData.viewParams, newOffsetData.viewParams);
            if (areViewParamsEqual) {
              if (oldOffsetData.offset != newOffsetData.offset) {
                if (self._isWithinRealtimeBounds(oldOffsetData.offset) || self._isWithinRealtimeBounds(newOffsetData.offset)) {
                  self.publish(self._getViewChannelName(viewName, newOffsetData.viewParams, query.type), {
                    type: 'update',
                    action: 'move',
                    id: query.id,
                    oldOffset: oldOffsetData.offset,
                    newOffset: newOffsetData.offset
                  });
                }
              }
            } else {
              if (self._isWithinRealtimeBounds(oldOffsetData.offset)) {
                self.publish(self._getViewChannelName(viewName, oldOffsetData.viewParams, query.type), {
                  type: 'update',
                  action: 'remove',
                  id: query.id,
                  offset: oldOffsetData.offset
                });
              }
              if (self._isWithinRealtimeBounds(newOffsetData.offset)) {
                self.publish(self._getViewChannelName(viewName, newOffsetData.viewParams, query.type), {
                  type: 'update',
                  action: 'add',
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
              var queryValue = query.value || {};
              Object.keys(queryValue).forEach(function (field) {
                modelInstance[field] = queryValue[field];
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

// Delete a single document or field from a document.
// This will notify affected views so that they may update themselves
// in real-time.
SCCRUDRethink.prototype.delete = function (query, callback, socket) {
  var self = this;

  if (!query) {
    query = {};
  }

  var deletedHandler = function (err, viewOffsets, result) {
    if (err) {
      self.emit('warning', err);
    } else {
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
        Object.keys(deletedFields || {}).forEach(function (field) {
          self.publish(self.channelPrefix + query.type + '/' + query.id + '/' + field, {
            type: 'delete'
          });
        });

        viewOffsets = viewOffsets || {};
        Object.keys(viewOffsets).forEach(function (viewName) {
          var offsetData = viewOffsets[viewName];
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
