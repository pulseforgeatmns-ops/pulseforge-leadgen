'use strict';
// 18 decimal places, integer arithmetic; API values are decimal strings.
const SCALE = 10n ** 18n;
function dec(value) {
  if (value === null || value === undefined) return null;
  const s = String(value);
  if (!/^-?\d+(\.\d{1,18})?$/.test(s)) throw new Error('invalid_decimal');
  const negative = s.startsWith('-'), [whole, fraction = ''] = s.replace('-', '').split('.');
  const n = BigInt(whole) * SCALE + BigInt(fraction.padEnd(18, '0'));
  return negative ? -n : n;
}
function str(n) {
  if (n === null) return null;
  const sign = n < 0n ? '-' : '', a = n < 0n ? -n : n;
  const f = (a % SCALE).toString().padStart(18, '0').replace(/0+$/, '');
  return sign + (a / SCALE).toString() + (f ? '.' + f : '');
}
function raw(amount, decimals) {
  if (!/^-?\d+$/.test(String(amount)) || !Number.isInteger(decimals) || decimals < 0 || decimals > 18) throw new Error('invalid_raw_amount');
  return BigInt(amount) * 10n ** BigInt(18 - decimals);
}
const mul = (a, b) => a === null || b === null ? null : a * b / SCALE;
const div = (a, b) => a === null || b === null || b === 0n ? null : a * SCALE / b;
function fromNumber(value){
  if(typeof value!=='number'||!Number.isFinite(value)||Math.abs(value)>=1e21||value!==0&&Math.abs(value)<1e-18)return null;
  return value.toFixed(18).replace(/0+$/,'').replace(/\.$/,'');
}
module.exports = {fromNumber,dec, str, raw, mul, div, SCALE};
