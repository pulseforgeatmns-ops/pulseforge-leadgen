'use strict';

const {
  inferMediaKind,
  extractJobGroupKey,
  extractVisualHints,
} = require('./assetHints');
const { resolveAnchorMediaFolderId } = require('./config');

async function defaultListDriveFiles({ folderId, pageToken }) {
  const { google } = require('googleapis');
  const credsJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!credsJson) throw new Error('google_service_account_required');
  const credentials = JSON.parse(credsJson);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/drive.readonly'],
  });
  const drive = google.drive({ version: 'v3', auth });
  const response = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false`,
    fields: 'nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, thumbnailLink, webViewLink)',
    pageSize: 200,
    pageToken: pageToken || undefined,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
  });
  return {
    files: response.data.files || [],
    nextPageToken: response.data.nextPageToken || null,
  };
}

async function syncAnchorMediaLibrary(input = {}, deps = {}) {
  const clientId = Number(input.clientId);
  const store = deps.store;
  if (!store) throw new Error('store_required');
  const listDriveFiles = deps.listDriveFiles || defaultListDriveFiles;
  const folderId = input.folderId || resolveAnchorMediaFolderId(input.clientConfig || {});
  const discovered = [];
  let pageToken = null;
  do {
    const page = await listDriveFiles({ folderId, pageToken });
    for (const file of page.files || []) {
      const mimeType = file.mimeType || null;
      const filename = file.name || 'untitled';
      const mediaKind = inferMediaKind(mimeType, filename);
      if (mediaKind === 'other') continue;
      const asset = await store.upsertMediaAsset({
        clientId,
        driveFileId: file.id,
        driveFolderId: folderId,
        filename,
        mimeType,
        mediaKind,
        byteSize: file.size ? Number(file.size) : null,
        createdTime: file.createdTime || null,
        modifiedTime: file.modifiedTime || null,
        thumbnailLink: file.thumbnailLink || null,
        webViewLink: file.webViewLink || null,
        jobGroupKey: extractJobGroupKey(filename),
        visualHints: extractVisualHints(filename),
        metadata: { drive: { id: file.id, name: filename } },
      });
      discovered.push(asset);
    }
    pageToken = page.nextPageToken;
  } while (pageToken);

  return {
    folderId,
    discoveredCount: discovered.length,
    assets: discovered,
  };
}

module.exports = {
  syncAnchorMediaLibrary,
  defaultListDriveFiles,
};
