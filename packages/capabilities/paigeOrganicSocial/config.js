'use strict';

const { ANCHOR_CLIENT_ID } = require('./types');

function resolveAnchorMediaFolderId(clientConfig = {}) {
  const fromMeta = clientConfig?.metadata?.paige?.mediaLibrary?.driveFolderId
    || clientConfig?.metadata?.anchor?.mediaLibrary?.driveFolderId;
  if (fromMeta) return String(fromMeta).trim();
  const env = process.env.PAIGE_ANCHOR_MEDIA_DRIVE_FOLDER_ID
    || process.env.ANCHOR_MEDIA_DRIVE_FOLDER_ID;
  if (env) return String(env).trim();
  // Certified revenue/backup folder used for Anchor media dumps when unset.
  return '1pHiHFHQjTNVijXilJyrhLX-MAYxyGnXS';
}

function isAnchorOrganicSocialEnabled(clientId, clientConfig = {}) {
  if (Number(clientId) !== ANCHOR_CLIENT_ID) return false;
  if (process.env.PAIGE_ANCHOR_ORGANIC_SOCIAL_ENABLED === 'false') return false;
  const flag = clientConfig?.metadata?.paige?.organicSocial?.enabled;
  if (flag === false) return false;
  return true;
}

module.exports = {
  resolveAnchorMediaFolderId,
  isAnchorOrganicSocialEnabled,
};
