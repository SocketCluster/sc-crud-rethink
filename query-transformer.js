var getViewMetaData = function (options, type, viewName) {
  var typeSchema = options.schema[type] || {};
  var modelViews = typeSchema.views || {};
  var viewSchema = modelViews[viewName] || {};

  return {
    transform: viewSchema.transform
  };
};

module.exports.constructTransformedRethinkQuery = function (options, ModelClass, type, viewName, predicateData) {
  var viewMetaData = getViewMetaData(options, type, viewName);
  var rethinkQuery = ModelClass;

  var sanitizedPredicateData;
  if (predicateData == undefined) {
    sanitizedPredicateData = null;
  } else {
    sanitizedPredicateData = predicateData;
  }

  var transformFn = viewMetaData.transform;
  if (transformFn) {
    rethinkQuery = transformFn(rethinkQuery, options.thinky.r, sanitizedPredicateData);
  }

  return rethinkQuery;
};
