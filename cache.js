const EventEmitter = require('events').EventEmitter;

let Cache = function (options) {
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
  if (!resourcePath) {
    resourcePath = this._getResourcePath(query);
  }
  let entry = {
    resource: data
  };

  let existingCache = this._cache[resourcePath];
  if (existingCache && existingCache.timeout) {
    clearTimeout(existingCache.timeout);
  }

  entry.timeout = setTimeout(() => {
    let freshEntry = this._cache[resourcePath] || {};
    delete this._cache[resourcePath];
    this.emit('expire', this._simplifyQuery(query), freshEntry);
  }, this.cacheDuration);

  this._cache[resourcePath] = entry;
};

Cache.prototype.clear = function (query) {
  let resourcePath = this._getResourcePath(query);

  let entry = this._cache[resourcePath];
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
  let entry = this._cache[resourcePath] || {};
  return entry.resource;
};

Cache.prototype._pushWatcher = function (resourcePath, watcher) {
  if (!this._watchers[resourcePath]) {
    this._watchers[resourcePath] = [];
  }
  this._watchers[resourcePath].push(watcher);
};

Cache.prototype._processCacheWatchers = function (resourcePath, error, data) {
  let watcherList = this._watchers[resourcePath] || [];

  if (error) {
    watcherList.forEach((watcher) => {
      watcher(error);
    });
  } else {
    watcherList.forEach((watcher) => {
      watcher(null, data);
    });
  }
  delete this._watchers[resourcePath];
};

Cache.prototype.pass = function (query, provider, callback) {
  if (this.cacheDisabled) {
    provider(callback);
    return;
  }

  let resourcePath = this._getResourcePath(query);
  if (!resourcePath) {
    // Bypass cache for unidentified resources.
    provider(callback);
    return;
  }

  let cacheEntry = this.get(query, resourcePath);

  this._pushWatcher(resourcePath, callback);

  if (cacheEntry) {
    this.emit('hit', query, cacheEntry);
    if (!cacheEntry.pending) {
      this._processCacheWatchers(resourcePath, null, cacheEntry.resource);
    }
  } else {
    this.emit('miss', query);
    cacheEntry = {
      pending: true,
      patch: {}
    };

    this.set(query, cacheEntry, resourcePath);
    this.emit('set', this._simplifyQuery(query), cacheEntry);

    provider((err, data) => {
      if (!err) {
        let freshCacheEntry = this._cache[resourcePath];

        if (freshCacheEntry) {
          let cacheEntryPatch = freshCacheEntry.patch || {};
          Object.keys(cacheEntryPatch).forEach((field) => {
            data[field] = cacheEntryPatch[field];
          });
        }

        let newCacheEntry = {
          resource: data
        };

        this.set(query, newCacheEntry, resourcePath);
        this.emit('set', this._simplifyQuery(query), newCacheEntry);
      }
      this._processCacheWatchers(resourcePath, err, data);
    });
  }
};

Cache.prototype.update = function (resourceChannelString, data) {
  if (!data || data.type !== 'update' || !data.hasOwnProperty('value')) {
    return;
  }
  let parts = resourceChannelString.split('>');
  let crudString = parts[0];
  if (crudString !== 'crud') {
    return;
  }
  let resourceChannel = parts[1];
  let resourceParts = resourceChannel.split('/');
  let field = resourceParts[2];

  let query = {
    type: resourceParts[0],
    id: resourceParts[1],
    field: field
  };

  if (query.type && query.id && field) {
    let resourcePath = this._getResourcePath(query);
    if (resourcePath) {
      let cacheEntry = this.get(query, resourcePath);
      if (cacheEntry) {
        let oldValue;
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
