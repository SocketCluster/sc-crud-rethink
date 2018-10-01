let getViewMetaData = function (options, type, viewName) {
  let typeSchema = options.schema[type] || {};
  let modelViews = typeSchema.views || {};
  let viewSchema = modelViews[viewName] || {};

  return Object.assign({}, viewSchema);
};

module.exports.constructTransformedRethinkQuery = function (options, ModelClass, type, viewName, viewParams) {
  let viewMetaData = getViewMetaData(options, type, viewName);
  let rethinkQuery = ModelClass;

  let sanitizedViewParams = {};
  if (typeof viewParams === 'object' && viewParams != null) {
    (viewMetaData.paramFields || []).forEach((field) => {
      let value = viewParams[field];
      sanitizedViewParams[field] = value === undefined ? null : value;
    });
  }

  let transformFn = viewMetaData.transform;
  if (transformFn) {
    rethinkQuery = transformFn(rethinkQuery, options.thinky.r, sanitizedViewParams);
  }

  return rethinkQuery;
};
