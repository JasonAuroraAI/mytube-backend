export default {
  async listVideos(req, { q } = {}) {
    // Later: read from DB and return CloudFront URLs.
    // Keep same shape as local provider.
    return [];
  }
};
