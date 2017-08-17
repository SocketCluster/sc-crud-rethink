var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

var Cache = function (options) {
  this._cache = {};
  this._watchers = {};
  this.options = options || {};
  this.cacheDuration = this.options.cacheDuration || 10000;
  this.cacheDisabled = !!this.options.cacheDisabled;
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

Cache.prototype.set = function (query, data, resourcePath) {
  var self = this;
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  var entry = {
    resource: data
  };

  var existingCache = this._cache[resourcePath];
  if (existingCache && existingCache.timeout) {
    clearTimeout(existingCache.timeout);
  }

  entry.timeout = setTimeout(function () {
    var freshEntry = self._cache[resourcePath] || {};
    delete self._cache[resourcePath];
    self.emit('expire', self._simplifyQuery(query), freshEntry);
  }, this.cacheDuration);

  this._cache[resourcePath] = entry;
};

Cache.prototype.clear = function (query) {
  var resourcePath = this._getResourcePath(query);

  var entry = this._cache[resourcePath];
  if (entry) {
    if (entry.timeout) {
      clearTimeout(entry.timeout);
    }
    delete this._cache[resourcePath];
    this.emit('clear', this._simplifyQuery(query), entry);
  }
};

Cache.prototype.get = function (query, resourcePath) {
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  var entry = this._cache[resourcePath] || {};
  return entry.resource;
};

Cache.prototype._pushWatcher = function (resourcePath, watcher) {
  if (!this._watchers[resourcePath]) {
    this._watchers[resourcePath] = [];
  }
  this._watchers[resourcePath].push(watcher);
};

Cache.prototype._processCacheWatchers = function (resourcePath, error, data) {
  var watcherList = this._watchers[resourcePath] || [];

  if (error) {
    watcherList.forEach(function (watcher) {
      watcher(error);
    });
  } else {
    watcherList.forEach(function (watcher) {
      watcher(null, data);
    });
  }
  delete this._watchers[resourcePath];
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

  this._pushWatcher(resourcePath, callback);

  if (cacheEntry) {
    this.emit('hit', query, cacheEntry);
    if (!cacheEntry.pending) {
      self._processCacheWatchers(resourcePath, null, cacheEntry.resource);
    }
  } else {
    this.emit('miss', query);
    cacheEntry = {
      pending: true,
      patch: {}
    };

    this.set(query, cacheEntry, resourcePath);
    this.emit('set', this._simplifyQuery(query), cacheEntry);

    provider(function (err, data) {
      if (!err) {
        var freshCacheEntry = self._cache[resourcePath];

        if (freshCacheEntry) {
          _.forOwn(freshCacheEntry.patch, function (value, field) {
            data[field] = value;
          });
        }

        var newCacheEntry = {
          resource: data
        };

        self.set(query, newCacheEntry, resourcePath);
        self.emit('set', self._simplifyQuery(query), newCacheEntry);
      }
      self._processCacheWatchers(resourcePath, err, data);
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
