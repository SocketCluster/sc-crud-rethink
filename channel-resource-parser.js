var channelViewParamsRegex = /^([^\(]*)\((.*)\):([^:]*)$/;

module.exports.parseChannelResourceQuery = function (channelName) {
  var mainParts = channelName.split('>');
  if (mainParts[0] === 'crud' && mainParts[1]) {
    var resourceString = mainParts[1];

    if (resourceString.indexOf(':') !== -1) {
      // If resource is a view.
      var viewMatches = resourceString.match(channelViewParamsRegex);
      var viewResource = {
        view: viewMatches[1],
        type: viewMatches[3]
      }
      try {
        viewResource.viewParams = JSON.parse(viewMatches[2]);
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
