'use strict';

const { createHash, randomUUID } = require('crypto');
const { CHART_SNAPSHOT } = require('./types');

/**
 * ScreenshotProcessor — turns raw image bytes into an immutable Observation.
 *
 * The original image never changes. Future extractors may reprocess the same
 * image; they never mutate the stored bytes or hash.
 */
class ScreenshotProcessor {
  /**
   * @param {object} [deps]
   * @param {() => string} [deps.idFactory]
   * @param {() => string} [deps.now]
   */
  constructor(deps = {}) {
    this._idFactory =
      typeof deps.idFactory === 'function' ? deps.idFactory : () => randomUUID();
    this._now =
      typeof deps.now === 'function'
        ? deps.now
        : () => new Date().toISOString();
  }

  /**
   * Normalize paste / drag-drop input into a frozen screenshot record + observation.
   *
   * Failures never throw for optional metadata — unknown values remain null.
   *
   * @param {object|Buffer|string} input
   * @param {object} [opts]
   * @param {string} [opts.subjectId]
   * @param {string} [opts.observedAt]
   * @param {string} [opts.tenantId]
   * @returns {{ screenshot: object, observation: object }}
   */
  process(input, opts = {}) {
    const normalized = normalizeImageInput(input);
    const observedAt = String(opts.observedAt || this._now());
    const screenshotId = String(
      (input && typeof input === 'object' && input.id) || this._idFactory()
    );

    const imageHash = hashImageBytes(normalized.bytes);
    const dimensions = readDimensions(normalized);

    const bytesCopy = Buffer.from(normalized.bytes);

    const screenshot = Object.freeze({
      id: screenshotId,
      imageHash,
      mimeType: normalized.mimeType,
      byteLength: bytesCopy.length,
      width: dimensions.width,
      height: dimensions.height,
      /** Original bytes copy — never rewritten; Buffer itself is not Object.freeze'd. */
      bytes: bytesCopy,
      encoding: normalized.encoding,
      source: normalized.source,
      createdAt: observedAt,
      immutable: true,
    });

    const observation = Object.freeze({
      id: `obs:chart_snapshot:${screenshotId}`,
      observationType: CHART_SNAPSHOT,
      type: CHART_SNAPSHOT,
      subjectId: opts.subjectId ? String(opts.subjectId) : null,
      observedAt,
      tenantId: opts.tenantId ? String(opts.tenantId) : null,
      screenshotId,
      payload: Object.freeze({
        screenshotId,
        imageHash,
        mimeType: normalized.mimeType,
        width: dimensions.width,
        height: dimensions.height,
        byteLength: normalized.bytes.length,
        chartImageHash: imageHash,
        screenshotDimensions: Object.freeze({
          width: dimensions.width,
          height: dimensions.height,
        }),
      }),
      immutable: true,
    });

    return { screenshot, observation };
  }
}

/**
 * @param {object|Buffer|string} input
 */
function normalizeImageInput(input) {
  if (input == null) {
    throw new Error('ScreenshotProcessor requires an image (paste or drag/drop)');
  }

  if (Buffer.isBuffer(input)) {
    return {
      bytes: input,
      mimeType: sniffMime(input),
      encoding: 'buffer',
      source: 'buffer',
    };
  }

  if (typeof input === 'string') {
    if (input.startsWith('data:')) {
      return parseDataUrl(input);
    }
    // Treat plain string as base64 or utf8 placeholder for tests / paste stubs.
    const asBase64 = tryBase64(input);
    if (asBase64) {
      return {
        bytes: asBase64,
        mimeType: sniffMime(asBase64) || 'application/octet-stream',
        encoding: 'base64',
        source: 'string',
      };
    }
    const bytes = Buffer.from(input, 'utf8');
    return {
      bytes,
      mimeType: 'text/plain',
      encoding: 'utf8',
      source: 'string',
    };
  }

  if (typeof input === 'object') {
    if (Buffer.isBuffer(input.bytes) || Buffer.isBuffer(input.buffer)) {
      const bytes = Buffer.from(input.bytes || input.buffer);
      return {
        bytes,
        mimeType: String(input.mimeType || input.contentType || sniffMime(bytes)),
        encoding: 'buffer',
        source: input.source || 'object',
      };
    }
    if (typeof input.dataUrl === 'string') {
      return parseDataUrl(input.dataUrl);
    }
    if (typeof input.base64 === 'string') {
      const bytes = Buffer.from(input.base64, 'base64');
      return {
        bytes,
        mimeType: String(input.mimeType || sniffMime(bytes) || 'application/octet-stream'),
        encoding: 'base64',
        source: input.source || 'object',
      };
    }
    if (typeof input.data === 'string') {
      return normalizeImageInput(input.data);
    }
  }

  throw new Error('Unsupported screenshot input');
}

/**
 * @param {string} dataUrl
 */
function parseDataUrl(dataUrl) {
  const match = /^data:([^;,]+)?(?:;charset=[^;,]+)?;base64,(.+)$/i.exec(dataUrl);
  if (!match) {
    throw new Error('Invalid data URL screenshot');
  }
  const mimeType = match[1] || 'application/octet-stream';
  const bytes = Buffer.from(match[2], 'base64');
  return {
    bytes,
    mimeType,
    encoding: 'base64',
    source: 'dataUrl',
  };
}

/**
 * @param {string} value
 * @returns {Buffer|null}
 */
function tryBase64(value) {
  if (!/^[A-Za-z0-9+/=\s]+$/.test(value) || value.length < 8) return null;
  try {
    const buf = Buffer.from(value.replace(/\s+/g, ''), 'base64');
    return buf.length > 0 ? buf : null;
  } catch {
    return null;
  }
}

/**
 * @param {Buffer} bytes
 */
function hashImageBytes(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

/**
 * Best-effort dimension sniff. Failures → null (never blocks capture).
 * @param {{ bytes: Buffer, mimeType: string }} normalized
 */
function readDimensions(normalized) {
  try {
    const png = readPngSize(normalized.bytes);
    if (png) return png;
    const jpeg = readJpegSize(normalized.bytes);
    if (jpeg) return jpeg;
  } catch {
    // ignore
  }
  return { width: null, height: null };
}

/**
 * @param {Buffer} buf
 */
function readPngSize(buf) {
  if (buf.length < 24) return null;
  if (buf[0] !== 0x89 || buf[1] !== 0x50 || buf[2] !== 0x4e || buf[3] !== 0x47) {
    return null;
  }
  return {
    width: buf.readUInt32BE(16),
    height: buf.readUInt32BE(20),
  };
}

/**
 * Minimal SOF0 scan for JPEG.
 * @param {Buffer} buf
 */
function readJpegSize(buf) {
  if (buf.length < 4 || buf[0] !== 0xff || buf[1] !== 0xd8) return null;
  let i = 2;
  while (i + 9 < buf.length) {
    if (buf[i] !== 0xff) {
      i += 1;
      continue;
    }
    const marker = buf[i + 1];
    if (marker === 0xc0 || marker === 0xc1 || marker === 0xc2) {
      return {
        height: buf.readUInt16BE(i + 5),
        width: buf.readUInt16BE(i + 7),
      };
    }
    const len = buf.readUInt16BE(i + 2);
    i += 2 + len;
  }
  return null;
}

/**
 * @param {Buffer} buf
 */
function sniffMime(buf) {
  if (!buf || buf.length < 4) return 'application/octet-stream';
  if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
    return 'image/png';
  }
  if (buf[0] === 0xff && buf[1] === 0xd8) return 'image/jpeg';
  if (buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) return 'image/gif';
  if (buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46) {
    return 'image/webp';
  }
  return 'application/octet-stream';
}

/**
 * @param {object} [deps]
 * @returns {ScreenshotProcessor}
 */
function createScreenshotProcessor(deps) {
  return new ScreenshotProcessor(deps);
}

module.exports = {
  ScreenshotProcessor,
  createScreenshotProcessor,
  normalizeImageInput,
  hashImageBytes,
  readDimensions,
};
