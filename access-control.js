var _ = require('lodash');

var AccessControl = function (scServer, thinky, options) {
  // Setup SocketCluster middleware for access control
  var self = this;

  this.options = options || {};
  this.schema = this.options.schema || {};
  this.thinky = thinky;

  this._getModelAccessRightsFilter = function (type, direction) {
    var modelSchema = self.schema[type];
    if (!modelSchema) {
      return null;
    }
    return modelSchema.accessControl || null;
  };

  scServer.addMiddleware(scServer.MIDDLEWARE_EMIT, function (req, next) {
    if (req.event == 'create' || req.event == 'read' || req.event == 'update' || req.event == 'delete') {
      // If socket has a valid auth token, then allow emitting get or set events
      var authToken = req.socket.getAuthToken();

      var accessFilter = self._getModelAccessRightsFilter(req.data.type);
      if (accessFilter) {
        var crudRequest = {
          r: self.thinky.r,
          socket: req.socket,
          action: req.event,
          authToken: authToken,
          query: req.data
        };
        accessFilter(crudRequest, function (err) {
          if (err) {
            if (typeof err == 'boolean') {
              err = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id);
              err.name = 'CRUDBlockedError';
            }
            next(err);
          } else {
            next();
          }
        });
      } else {
        if (self.options.blockInboundByDefault) {
          var crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id + ' - No access control rules found');
          crudBlockedError.name = 'CRUDBlockedError';
          next(crudBlockedError);
        } else {
          next();
        }
      }
    } else {
      // This module is only responsible for CRUD-related access control.
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
    var authToken = req.socket.getAuthToken();
    var channelResourceQuery = getChannelResourceQuery(req.channel);
    if (!channelResourceQuery) {
      next();
      return;
    }
    var accessFilter = self._getModelAccessRightsFilter(channelResourceQuery.type);
    if (accessFilter) {
      var subscribeRequest = {
        r: self.thinky.r,
        socket: req.socket,
        action: 'subscribe',
        authToken: authToken,
        query: channelResourceQuery
      };
      accessFilter(subscribeRequest, function (err) {
        if (err) {
          if (typeof err == 'boolean') {
            err = new Error('Cannot subscribe to ' + req.channel + ' channel');
            err.name = 'CRUDBlockedError';
          }
          next(err);
        } else {
          next();
        }
      });
    } else {
      if (self.options.blockInboundByDefault) {
        var crudBlockedError = new Error('Cannot subscribe to ' + req.channel + ' channel - No access control rules found');
        crudBlockedError.name = 'CRUDBlockedError';
        next(crudBlockedError);
      } else {
        next();
      }
    }
  });
};

module.exports = AccessControl;
