var _ = require('lodash');

module.exports.attach = function (scServer, options) {
  // Setup SocketCluster middleware for access control
  var self = this;

  this.options = options || {};
  this.schema = this.options.schema || {};

  var getModelAccessRightsFilter = function (type, action) {
    var modelSchema = self.schema[type];
    if (!modelSchema) {
      return null;
    }
    var modelAuthorization = modelSchema.authorizations;
    if (!modelAuthorization) {
      return null;
    }
    return modelAuthorization[action];
  };

  var getModelViewAccessRightsFilter = function (type, view, action) {
    var modelSchema = self.schema[type];
    if (!modelSchema) {
      return null;
    }
    var views = modelSchema.views;
    if (!views) {
      return null;
    }
    var viewSchema = views[view];
    if (!viewSchema) {
      return null;
    }
    var viewAuthorization = viewSchema.authorizations;
    if (!viewAuthorization) {
      return null;
    }
    return viewAuthorization[action];
  };

  var getAccessRightsFilter = function (type, resource, action) {
    if (resource.view) {
      return getModelViewAccessRightsFilter(type, resource.view, action);
    } else {
      return getModelAccessRightsFilter(type, action);
    }
  };

  scServer.addMiddleware(scServer.MIDDLEWARE_EMIT, function (req, next) {
    if (req.event == 'create' || req.event == 'read' || req.event == 'update' || req.event == 'delete') {
      // If socket has a valid auth token, then allow emitting get or set events
      var authToken = req.socket.getAuthToken();

      var accessFilter = getAccessRightsFilter(req.data.type, req.data, req.event);
      if (accessFilter) {
        accessFilter(req.socket, scServer.thinky.r, authToken, req.data, function (isAllowed) {
          if (isAllowed) {
            next();
          } else {
            var crudBlockedError = new Error('You are not permitted to perform a CRUD operation on the ' + req.data.type + ' resource with ID ' + req.data.id);
            crudBlockedError.name = 'CRUDBlockedError';
            next(crudBlockedError);
          }
        });
      } else {
        if (self.options.blockAccessByDefault) {
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

  var getChannelResource = function (channelName) {
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
    var authToken = req.socket.getAuthToken();
    var channelResource = getChannelResource(req.channel);
    if (!channelResource) {
      next();
      return;
    }
    var accessFilter = getAccessRightsFilter(channelResource.type, channelResource, 'publish');
    if (accessFilter) {
      if (req.allowAccess) {
        next();
      } else {
        accessFilter(req.socket, scServer.thinky.r, authToken, channelResource, function (isAllowed) {
          if (isAllowed) {
            next();
          } else {
            var crudBlockedError = new Error('Cannot publish to ' + req.channel + ' channel - Params: ' + JSON.stringify(req.data));
            crudBlockedError.name = 'CRUDBlockedError';
            next(crudBlockedError);
          }
        });
      }
    } else {
      if (self.options.blockAccessByDefault) {
        var crudBlockedError = new Error('Cannot publish to ' + req.channel + ' channel - Params: ' + JSON.stringify(req.data) + ' - No access control rules found');
        crudBlockedError.name = 'CRUDBlockedError';
        next(crudBlockedError);
      } else {
        next();
      }
    }
  });

  scServer.addMiddleware(scServer.MIDDLEWARE_SUBSCRIBE, function (req, next) {
    var authToken = req.socket.getAuthToken();
    var channelResource = getChannelResource(req.channel);
    if (!channelResource) {
      next();
      return;
    }
    var accessFilter = getAccessRightsFilter(channelResource.type, channelResource, 'subscribe');
    if (accessFilter) {
      if (req.allowAccess) {
        next();
      } else {
        accessFilter(req.socket, scServer.thinky.r, authToken, channelResource, function (isAllowed) {
          if (isAllowed) {
            next();
          } else {
            var crudBlockedError = new Error('Cannot subscribe to ' + req.channel + ' channel');
            crudBlockedError.name = 'CRUDBlockedError';
            next(crudBlockedError);
          }
        });
      }
    } else {
      if (self.options.blockAccessByDefault) {
        var crudBlockedError = new Error('Cannot subscribe to ' + req.channel + ' channel - No access control rules found');
        crudBlockedError.name = 'CRUDBlockedError';
        next(crudBlockedError);
      } else {
        next();
      }
    }
  });
};
