var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

var Cache = function (options) {
  var self = this;

  this._cache = {};
  this.options = options || {};
  this.cacheDuration = this.options.cacheDuration || 10000;
  this.brokerEngine = this.options.brokerEngine;
  this.cacheDisabled = !!this.options.cacheDisabled;

  if (!this.cacheDisabled) {
    this.brokerEngine.on('message', function (message) {
      self.update(message.channel, message.data);
    });
  }
};

Cache.prototype = Object.create(EventEmitter.prototype);

Cache.prototype._getResourcePath = function (query) {
  if (!query.type || !query.id) {
    return null;
  }
  return query.type + '/' + query.id;
};

Cache.prototype._simplifyQuery = function (query) {
  return {
    type: query.type,
    id: query.id
  };
};

Cache.prototype.set = function (query, entry, resourcePath) {
  var self = this;
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }

  this._cache[resourcePath] = entry;

  setTimeout(function () {
    delete self._cache[resourcePath];
    self.emit('expire', self._simplifyQuery(query), entry);
  }, this.cacheDuration);
};

Cache.prototype.clear = function (query) {
  var resourcePath = this._getResourcePath(query);

  var entry = this._cache[resourcePath];
  if (entry !== undefined) {
    delete this._cache[resourcePath];
    this.emit('clear', this._simplifyQuery(query), entry);
  }
};

Cache.prototype.get = function (query, resourcePath) {
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  return this._cache[resourcePath];
};

Cache.prototype.pass = function (query, provider, callback) {
  var self = this;

  if (this.cacheDisabled) {
    provider(callback);
    return;
  }

  var resourcePath = this._getResourcePath(query);
  if (!resourcePath) {
    // Bypass cache for unidentified resources.
    provider(callback);
    return;
  }

  var cacheEntry = this.get(query, resourcePath);

  if (cacheEntry) {
    this.emit('hit', query, cacheEntry);
    if (cacheEntry.pending) {
      cacheEntry.watchers.push(callback);
    } else {
      callback(null, cacheEntry.resource);
    }
  } else {
    this.emit('miss', query);
    cacheEntry = {
      pending: true,
      watchers: [callback],
      patch: {}
    };
    this.set(query, cacheEntry, resourcePath);
    this.emit('set', this._simplifyQuery(query), cacheEntry);

    provider(function (err, data) {
      if (err) {
        _.forEach(cacheEntry.watchers, function (watcher) {
          watcher(err);
        });
      } else {
        _.forOwn(cacheEntry.patch, function (value, field) {
          data[field] = value;
        });
        var freshCacheEntry = self.get(query, resourcePath);
        if (freshCacheEntry) {
          // Replace pending entry with a proper entry.
          // But keep old expiry as is.
          self._cache[resourcePath] = {
            resource: data
          };
        } else {
          // This is an unusual case if the pending cache entry expired
          // before the provider data was resolved.
          // Set a new cache entry with fresh expiry.
          var entry = {
            resource: data
          };
          self.set(query, entry, resourcePath);
          self.emit('set', self._simplifyQuery(query), entry);
        }
        _.forEach(cacheEntry.watchers, function (cb) {
          cb(null, data);
        });
      }
    });
  }
};

Cache.prototype.update = function (resourceChannelString, data) {
  if (!data || data.type != 'update' || !data.hasOwnProperty('value')) {
    return;
  }
  var parts = resourceChannelString.split('>');
  var crudString = parts[0];
  if (crudString != 'crud') {
    return;
  }
  var resourceChannel = parts[1];
  var resourceParts = resourceChannel.split('/');
  var field = resourceParts[2];

  var query = {
    type: resourceParts[0],
    id: resourceParts[1],
    field: field
  };

  if (query.type && query.id && field) {
    var resourcePath = this._getResourcePath(query);
    if (resourcePath) {
      var cacheEntry = this.get(query, resourcePath);
      if (cacheEntry) {
        var oldValue;
        if (cacheEntry.pending) {
          oldValue = null;
          cacheEntry.patch[field] = data.value;
        } else {
          oldValue = cacheEntry.resource[field];
          cacheEntry.resource[field] = data.value;
        }
        this.emit('update', query, cacheEntry, {
          oldValue: oldValue,
          newValue: data.value
        });
      }
    }
  }
};

module.exports = Cache;
