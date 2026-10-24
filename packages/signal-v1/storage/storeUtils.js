'use strict';

async function callStore(store, method, ...args) {
  const fn = store[method];
  if (typeof fn !== 'function') {
    throw new Error(`Signal store missing method: ${method}`);
  }
  const result = fn.apply(store, args);
  if (result && typeof result.then === 'function') {
    return await result;
  }
  return result;
}

function isAsyncStore(store) {
  return store && store.constructor && store.constructor.name === 'PostgresSignalStore';
}

module.exports = {
  callStore,
  isAsyncStore,
};
