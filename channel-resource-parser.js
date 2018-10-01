let channelViewParamsRegex = /^([^\(]*)\((.*)\):([^:]*)$/;

module.exports.parseChannelResourceQuery = function (channelName) {
  let mainParts = channelName.split('>');
  if (mainParts[0] === 'crud' && mainParts[1]) {
    let resourceString = mainParts[1];

    if (resourceString.indexOf(':') !== -1) {
      // If resource is a view.
      let viewMatches = resourceString.match(channelViewParamsRegex);
      let viewResource = {
        view: viewMatches[1],
        type: viewMatches[3]
      }
      try {
        viewResource.viewParams = JSON.parse(viewMatches[2]);
      } catch (e) {}

      return viewResource;
    } else {
      // If resource is a simple model.
      let resourceParts = resourceString.split('/');
      let modelResource = {
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
