'use strict';

const { inflateSync } = require('node:zlib');
const MAX_PIXELS = 12_000_000;
function dimensions(width, height) {
  if (!width || !height || width > 8192 || height > 8192 || width * height > MAX_PIXELS) throw new Error('Image dimensions exceed the supported limit.');
}
function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0);
  }
  return (crc ^ 0xffffffff) >>> 0;
}
function validatePng(bytes) {
  if (bytes.length < 45 || !bytes.subarray(0, 8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) throw new Error('Invalid PNG image.');
  let offset=8, header, ended=false; const compressed=[];
  while (offset < bytes.length) {
    if (offset + 12 > bytes.length) throw new Error('Truncated PNG image.');
    const length=bytes.readUInt32BE(offset), type=bytes.toString('ascii',offset+4,offset+8), end=offset+12+length;
    if (end>bytes.length || crc32(bytes.subarray(offset+4,end-4))!==bytes.readUInt32BE(end-4)) throw new Error('Invalid PNG chunk.');
    if (['acTL','fcTL','fdAT'].includes(type)) throw new Error('Animated photos are not supported.');
    if (!header && type!=='IHDR') throw new Error('Missing PNG dimensions.');
    if (type==='IHDR') {
      if (header || length!==13) throw new Error('Invalid PNG header.');
      const width=bytes.readUInt32BE(offset+8),height=bytes.readUInt32BE(offset+12),depth=bytes[offset+16],color=bytes[offset+17];
      dimensions(width,height);
      const channels={0:1,2:3,3:1,4:2,6:4}[color];
      const validDepth=({0:[1,2,4,8,16],2:[8,16],3:[1,2,4,8],4:[8,16],6:[8,16]})[color]?.includes(depth);
      if (!channels || !validDepth || bytes[offset+18] || bytes[offset+19] || bytes[offset+20]) throw new Error('Use a non-interlaced PNG or JPEG photo.');
      header={width,height,rowBytes:Math.ceil(width*channels*depth/8)};
    } else if (type==='IDAT') compressed.push(bytes.subarray(offset+8,end-4));
    else if (type==='IEND') {if(length || end!==bytes.length)throw new Error('Invalid PNG ending.');ended=true;break;}
    offset=end;
  }
  if (!ended || !compressed.length) throw new Error('Incomplete PNG image.');
  const expected=(header.rowBytes+1)*header.height;
  if(expected>96_000_000)throw new Error('Image is too large.');
  const decoded=inflateSync(Buffer.concat(compressed),{maxOutputLength:expected});
  if(decoded.length!==expected)throw new Error('Invalid PNG pixels.');
  for(let i=0;i<decoded.length;i+=header.rowBytes+1)if(decoded[i]>4)throw new Error('Invalid PNG filter.');
}
function validateJpeg(bytes) {
  if (bytes.length<20 || bytes[0]!==255 || bytes[1]!==216 || bytes[bytes.length-2]!==255 || bytes[bytes.length-1]!==217) throw new Error('Incomplete JPEG image.');
  let offset=2,foundDimensions=false,foundScan=false;
  while(offset<bytes.length-2) {
    if(bytes[offset++]!==255)throw new Error('Invalid JPEG marker.');
    while(bytes[offset]===255)offset++;
    const marker=bytes[offset++];
    if(marker===0xd9)break;
    if(offset+2>bytes.length)throw new Error('Truncated JPEG.');
    const length=bytes.readUInt16BE(offset);
    if(length<2 || offset+length>bytes.length)throw new Error('Invalid JPEG segment.');
    if([0xc0,0xc1,0xc2].includes(marker)) {
      if(length<8)throw new Error('Invalid JPEG dimensions.');
      dimensions(bytes.readUInt16BE(offset+5),bytes.readUInt16BE(offset+3));foundDimensions=true;
    }
    if(marker===0xda) {foundScan=true;break;}
    offset+=length;
  }
  if(!foundDimensions || !foundScan)throw new Error('Missing JPEG pixels.');
}
function validateWebp(bytes) {
  if(bytes.length<30 || bytes.toString('ascii',0,4)!=='RIFF' || bytes.toString('ascii',8,12)!=='WEBP' || bytes.readUInt32LE(4)+8!==bytes.length)throw new Error('Invalid WebP container.');
  let offset=12,found=false;
  while(offset<bytes.length) {
    if(offset+8>bytes.length)throw new Error('Truncated WebP.');
    const type=bytes.toString('ascii',offset,offset+4),length=bytes.readUInt32LE(offset+4),start=offset+8,end=start+length;
    if(end>bytes.length)throw new Error('Truncated WebP pixels.');
    if(type==='VP8 ') {
      if(length<10 || bytes[start+3]!==0x9d || bytes[start+4]!==0x01 || bytes[start+5]!==0x2a)throw new Error('Invalid WebP frame.');
      dimensions(bytes.readUInt16LE(start+6)&0x3fff,bytes.readUInt16LE(start+8)&0x3fff);found=true;
    } else if(type==='VP8L') {
      if(length<5 || bytes[start]!==0x2f)throw new Error('Invalid WebP frame.');
      const bits=bytes.readUInt32LE(start+1);dimensions((bits&0x3fff)+1,((bits>>>14)&0x3fff)+1);found=true;
    } else if(type==='VP8X') {
      if(length!==10 || (bytes[start]&2))throw new Error('Animated photos are not supported.');
      dimensions(bytes.readUIntLE(start+4,3)+1,bytes.readUIntLE(start+7,3)+1);
    }
    offset=end+(length%2);
  }
  if(!found || offset!==bytes.length)throw new Error('Missing WebP pixels.');
}
function validateRaster(bytes,type) {
  if(type==='png')validatePng(bytes);
  else if(type==='jpeg')validateJpeg(bytes);
  else if(type==='webp')validateWebp(bytes);
  else throw new Error('Unsupported image type.');
}
module.exports={validateRaster};
