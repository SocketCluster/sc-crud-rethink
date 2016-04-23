var _ = require('lodash');
var constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;

var Filter = function (scServer, options) {
  // Setup SocketCluster middleware for access control and filtering
  var self = this;

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.thinky = options.thinky;

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

  var channelViewPredicateRegex = /^([^\(]*)\((.*)\):([^:]*)$/;

  var getChannelResourceQuery = function (channelName) {
    var mainParts = channelName.split('>');
    if (mainParts[0] == 'crud' && mainParts[1]) {
      var resourceString = mainParts[1];

      if (resourceString.indexOf(':') != -1) {
        // If resource is a view.
        var viewMatches = resourceString.match(channelViewPredicateRegex);
        var viewResource = {
          view: viewMatches[1],
          type: viewMatches[3]
        }
        try {
          viewResource.predicateData = JSON.parse(viewMatches[2]);
        } catch (e) {}

        return viewResource;
      } else {
        // If resource is a simple model.
        var resourceParts = resourceString.split('/');
        var modelResource = {
          type: resourceParts[0]
        };
        if (resourceParts[1]) {
          modelResource.id = resourceParts[1];
        }
        if (resourceParts[2]) {
          modelResource.field = resourceParts[2];
        }
        return modelResource;
      }
    }
    return null;
  };

  scServer.addMiddleware(scServer.MIDDLEWARE_PUBLISH_IN, function (req, next) {
    var channelResourceQuery = getChannelResourceQuery(req.channel);
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
    var channelResourceQuery = getChannelResourceQuery(req.channel);
    if (!channelResourceQuery) {
      next();
      return;
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

      var rethinkQuery;

      if (query.id) {
        // For single documents.
        rethinkQuery = ModelClass.get(query.id);
      } else {
        // For collections.
        rethinkQuery = constructTransformedRethinkQuery(this.options, ModelClass, query.type, query.view, query.predicateData);
        if (query.offset) {
          rethinkQuery = rethinkQuery.slice(query.offset, query.offset + pageSize);
        } else {
          rethinkQuery = rethinkQuery.limit(pageSize);
        }
      }
      rethinkQuery.run(function (err, resource) {
        if (err) {
          next(err);
        } else {
          request.resource = resource;
          continueWithPostFilter();
        }
      });
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
