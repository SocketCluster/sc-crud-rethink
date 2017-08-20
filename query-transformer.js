var getViewMetaData = function (options, type, viewName) {
  var typeSchema = options.schema[type] || {};
  var modelViews = typeSchema.views || {};
  var viewSchema = modelViews[viewName] || {};

  return {
    transform: viewSchema.transform
  };
};

module.exports.constructTransformedRethinkQuery = function (options, ModelClass, type, viewName, viewParams) {
  var viewMetaData = getViewMetaData(options, type, viewName);
  var rethinkQuery = ModelClass;

  var sanitizedViewParams;
  if (typeof viewParams != 'object') {
    sanitizedViewParams = {};
  } else {
    sanitizedViewParams = viewParams;
  }

  var transformFn = viewMetaData.transform;
  if (transformFn) {
    rethinkQuery = transformFn(rethinkQuery, options.thinky.r, sanitizedViewParams);
  }

  return rethinkQuery;
};
