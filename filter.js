const constructTransformedRethinkQuery = require('./query-transformer').constructTransformedRethinkQuery;
const parseChannelResourceQuery = require('./channel-resource-parser').parseChannelResourceQuery;

let Filter = function (scServer, options) {
  // Setup SocketCluster middleware for access control and filtering

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.thinky = this.options.thinky;
  this.cache = this.options.cache;
  this.scServer = scServer;
  this.logger = this.options.logger;

  this._getModelFilter = (modelType, filterPhase) => {
    let modelSchema = this.schema[modelType];
    if (!modelSchema) {
      return null;
    }
    let modelFilters = modelSchema.filters;
    if (!modelFilters) {
      return null;
    }
    return modelFilters[filterPhase] || null;
  };

  scServer.addMiddleware(scServer.MIDDLEWARE_EMIT, (req, next) => {
    if (req.event === 'create' || req.event === 'read' || req.event === 'update' || req.event === 'delete') {
      // If socket has a valid auth token, then allow emitting get or set events
      let authToken = req.socket.authToken;

      let preFilter = this._getModelFilter(req.data.type, 'pre');
      if (preFilter) {
        let crudRequest = {
          r: this.thinky.r,
          socket: req.socket,
          action: req.event,
          authToken: authToken,
          query: req.data
        };
        preFilter(crudRequest, (err) => {
          if (err) {
            if (typeof err === 'boolean') {
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
        if (this.options.blockPreByDefault) {
          let crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id + ' - No filters found');
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

  scServer.addMiddleware(scServer.MIDDLEWARE_PUBLISH_IN, (req, next) => {
    let channelResourceQuery = parseChannelResourceQuery(req.channel);
    if (channelResourceQuery) {
      // Always block CRUD publish from outside clients.
      let crudPublishNotAllowedError = new Error('Cannot publish to a CRUD resource channel');
      crudPublishNotAllowedError.name = 'CRUDPublishNotAllowedError';
      next(crudPublishNotAllowedError);
    } else {
      next();
    }
  });

  scServer.addMiddleware(scServer.MIDDLEWARE_SUBSCRIBE, (req, next) => {
    let authToken = req.socket.authToken;
    let channelResourceQuery = parseChannelResourceQuery(req.channel);
    if (!channelResourceQuery) {
      next();
      return;
    }
    // Sometimes the real viewParams may be different from what can be parsed from
    // the channel name; this is because some view params don't affect the real-time
    // delivery of messages but they may still be useful in constructing the view.
    if (channelResourceQuery.view !== undefined && req.data && typeof req.data.viewParams === 'object') {
      channelResourceQuery.viewParams = req.data.viewParams;
    }

    let continueWithPostFilter = () => {
      let subscribePostRequest = {
        socket: req.socket,
        action: 'subscribe',
        query: channelResourceQuery,
        fetchResource: true
      };
      this.applyPostFilter(subscribePostRequest, next);
    };

    let preFilter = this._getModelFilter(channelResourceQuery.type, 'pre');
    if (preFilter) {
      let subscribePreRequest = {
        r: this.thinky.r,
        socket: req.socket,
        action: 'subscribe',
        authToken: authToken,
        query: channelResourceQuery
      };
      preFilter(subscribePreRequest, (err) => {
        if (err) {
          if (typeof err === 'boolean') {
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
      if (this.options.blockPreByDefault) {
        let crudBlockedError = new Error('Cannot subscribe to ' + req.channel + ' channel - No filters found');
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
  let query = req.query;
  let postFilter = this._getModelFilter(query.type, 'post');

  if (postFilter) {
    let request = {
      r: this.thinky.r,
      socket: req.socket,
      action: req.action,
      authToken: req.socket && req.socket.authToken,
      query: query
    };
    if (!req.fetchResource) {
      request.resource = req.resource;
    }

    let continueWithPostFilter = () => {
      postFilter(request, (err) => {
        if (err) {
          if (typeof err === 'boolean') {
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
      let pageSize = query.pageSize || this.options.defaultPageSize;
      let ModelClass = this.options.models[query.type];

      if (!ModelClass) {
        let error = new Error('The ' + query.type + ' model type is not supported - It is not part of the schema');
        error.name = 'CRUDInvalidModelType';
        next(error);
        return;
      }

      let queryResponseHandler = (err, resource) => {
        if (err) {
          this.logger.error(err);
          next(new Error('Executed an invalid query transformation'));
        } else {
          request.resource = resource;
          continueWithPostFilter();
        }
      };

      if (query.id) {
        let dataProvider = (cb) => {
          ModelClass.get(query.id).run(cb);
        };
        this.cache.pass(query, dataProvider, queryResponseHandler);
      } else {
        // For collections.
        let rethinkQuery = constructTransformedRethinkQuery(this.options, ModelClass, query.type, query.view, query.viewParams);
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
      let crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + query.type + ' resource with ID ' + query.id + ' - No filters found');
      crudBlockedError.name = 'CRUDBlockedError';
      crudBlockedError.type = 'post';
      next(crudBlockedError);
    } else {
      next();
    }
  }
};

module.exports = Filter;
