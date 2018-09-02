var constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
var parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;

var Filter = function (scServer, options) {
  // Setup SocketCluster middleware for access control and filtering
  var self = this;

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.thinky = this.options.thinky;
  this.cache = this.options.cache;

  this._getModelFilter = function (modelType, filterPhase) {
    var modelSchema = self.schema[modelType];
    if (!modelSchema) {
      return null;
    }
    var modelFilters = modelSchema.filters;
    if (!modelFilters) {
      return null;
    }
    return modelFilters[filterPhase] || null;
  };

  scServer.addMiddleware(scServer.MIDDLEWARE_EMIT, function (req, next) {
    if (req.event == 'create' || req.event == 'read' || req.event == 'update' || req.event == 'delete') {
      // If socket has a valid auth token, then allow emitting get or set events
      var authToken = req.socket.authToken;

      var preFilter = self._getModelFilter(req.data.type, 'pre');
      if (preFilter) {
        var crudRequest = {
          r: self.thinky.r,
          socket: req.socket,
          action: req.event,
          authToken: authToken,
          query: req.data
        };
        preFilter(crudRequest, function (err) {
          if (err) {
            if (typeof err == 'boolean') {
              err = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id);
              err.name = 'CRUDBlockedError';
              err.type = 'pre';
            }
            next(err);
          } else {
            next();
          }
        });
      } else {
        if (self.options.blockPreByDefault) {
          var crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id + ' - No filters found');
          crudBlockedError.name = 'CRUDBlockedError';
          crudBlockedError.type = 'pre';
          next(crudBlockedError);
        } else {
          next();
        }
      }
    } else {
      // This module is only responsible for CRUD-related filtering.
      next();
    }
  });

  scServer.addMiddleware(scServer.MIDDLEWARE_PUBLISH_IN, function (req, next) {
    var channelResourceQuery = parseChannelResourceQuery(req.channel);
    if (channelResourceQuery) {
      // Always block CRUD publish from outside clients.
      var crudPublishNotAllowedError = new Error('Cannot publish to a CRUD resource channel');
      crudPublishNotAllowedError.name = 'CRUDPublishNotAllowedError';
      next(crudPublishNotAllowedError);
    } else {
      next();
    }
  });

  scServer.addMiddleware(scServer.MIDDLEWARE_SUBSCRIBE, function (req, next) {
    var authToken = req.socket.authToken;
    var channelResourceQuery = parseChannelResourceQuery(req.channel);
    if (!channelResourceQuery) {
      next();
      return;
    }
    // Sometimes the real viewParams may be different from what can be parsed from
    // the channel name; this is because some view params don't affect the real-time
    // delivery of messages but they may still be useful in constructing the view.
    if (channelResourceQuery.view !== undefined && req.data && typeof req.data.viewParams == 'object') {
      channelResourceQuery.viewParams = req.data.viewParams;
    }

    var continueWithPostFilter = function () {
      var subscribePostRequest = {
        socket: req.socket,
        action: 'subscribe',
        query: channelResourceQuery,
        fetchResource: true
      };
      self.applyPostFilter(subscribePostRequest, next);
    };

    var preFilter = self._getModelFilter(channelResourceQuery.type, 'pre');
    if (preFilter) {
      var subscribePreRequest = {
        r: self.thinky.r,
        socket: req.socket,
        action: 'subscribe',
        authToken: authToken,
        query: channelResourceQuery
      };
      preFilter(subscribePreRequest, function (err) {
        if (err) {
          if (typeof err == 'boolean') {
            err = new Error('Cannot subscribe to ' + req.channel + ' channel');
            err.name = 'CRUDBlockedError';
            err.type = 'pre';
          }
          next(err);
        } else {
          continueWithPostFilter();
        }
      });
    } else {
      if (self.options.blockPreByDefault) {
        var crudBlockedError = new Error('Cannot subscribe to ' + req.channel + ' channel - No filters found');
        crudBlockedError.name = 'CRUDBlockedError';
        crudBlockedError.type = 'pre';
        next(crudBlockedError);
      } else {
        continueWithPostFilter();
      }
    }
  });
};

Filter.prototype.applyPostFilter = function (req, next) {
  var query = req.query;
  var postFilter = this._getModelFilter(query.type, 'post');

  if (postFilter) {
    var request = {
      r: this.thinky.r,
      socket: req.socket,
      action: req.action,
      authToken: req.socket && req.socket.authToken,
      query: query
    };
    if (!req.fetchResource) {
      request.resource = req.resource;
    }

    var continueWithPostFilter = function () {
      postFilter(request, function (err) {
        if (err) {
          if (typeof err == 'boolean') {
            err = new Error('You are not permitted to perform a CRUD operation on the ' + query.type + ' resource with ID ' + query.id);
            err.name = 'CRUDBlockedError';
            err.type = 'post';
          }
          next(err);
        } else {
          next();
        }
      });
    };

    if (req.fetchResource) {
      var pageSize = query.pageSize || this.options.defaultPageSize;
      var ModelClass = this.options.models[query.type];

      if (!ModelClass) {
        var error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
        error.name = 'CRUDInvalidModelType';
        next(error);
        return;
      }

      var queryResponseHandler = function (err, resource) {
        if (err) {
          next(err);
        } else {
          request.resource = resource;
          continueWithPostFilter();
        }
      };

      if (query.id) {
        var dataProvider = function (cb) {
          ModelClass.get(query.id).run(cb);
        };
        this.cache.pass(query, dataProvider, queryResponseHandler);
      } else {
        // For collections.
        var rethinkQuery = constructTransformedRethinkQuery(this.options, ModelClass, query.type, query.view, query.viewParams);
        if (query.offset) {
          rethinkQuery = rethinkQuery.slice(query.offset, query.offset + pageSize);
        } else {
          rethinkQuery = rethinkQuery.limit(pageSize);
        }
        rethinkQuery.run(queryResponseHandler);
      }

    } else {
      continueWithPostFilter();
    }
  } else {
    if (this.options.blockPostByDefault) {
      var crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + query.type + ' resource with ID ' + query.id + ' - No filters found');
      crudBlockedError.name = 'CRUDBlockedError';
      crudBlockedError.type = 'post';
      next(crudBlockedError);
    } else {
      next();
    }
  }
};

module.exports = Filter;
