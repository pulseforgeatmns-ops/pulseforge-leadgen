'use strict';

const crypto = require('node:crypto');

function newComposerId(prefix = 'cmp') {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
}

module.exports = {
  newComposerId,
};
