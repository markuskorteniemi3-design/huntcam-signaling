'use strict';
const {createHash, timingSafeEqual} = require('crypto');
function requireText(value, name, pattern) {
  if (typeof value !== 'string' || !pattern.test(value)) throw new Error(`Invalid ${name}`);
  return value;
}
function deviceRegistry(env) {
  const rows = env.HUNTCAM_DEVICES_JSON ? JSON.parse(env.HUNTCAM_DEVICES_JSON) : [{
    device_id: env.HUNTCAM_DEVICE_ID || 'cm5-sisu-001', owner_uid: env.HUNTCAM_OWNER_UID,
    dog_name: env.HUNTCAM_DOG_NAME || 'Sisu', token: env.HUNTCAM_DEVICE_TOKEN,
  }];
  if (!Array.isArray(rows) || !rows.length) throw new Error('Device registry missing');
  const devices = new Map();
  for (const d of rows) {
    requireText(d.device_id, 'device_id', /^[a-zA-Z0-9_-]{1,80}$/);
    requireText(d.owner_uid, 'owner_uid', /^[a-zA-Z0-9_-]{1,128}$/);
    requireText(d.token, 'device token (minimum 32 characters)', /^[a-zA-Z0-9_-]{32,256}$/);
    if (devices.has(d.device_id)) throw new Error('Duplicate device ID');
    devices.set(d.device_id, Object.freeze({...d, dog_name: String(d.dog_name || '').slice(0,100)}));
  }
  return devices;
}
function authenticateDevice(devices, id, token) {
  const d = devices.get(id);
  if (!d || typeof token !== 'string') return null;
  const hash = x => createHash('sha256').update(x).digest();
  return timingSafeEqual(hash(token), hash(d.token)) ? d : null;
}
function observationId(deviceId, eventId) {
  requireText(eventId, 'event_id', /^[a-zA-Z0-9_-]{16,80}$/);
  return createHash('sha256').update(deviceId + ':' + eventId).digest('hex');
}
function normalizeDetection(data) {
  const names = {brown_bear:'Karhu', moose:'Hirvi', wild_boar:'Villisika', wolf:'Susi',
    karhu:'Karhu', hirvi:'Hirvi', villisika:'Villisika', susi:'Susi'};
  const animal = names[String(data.animal || '').toLowerCase()];
  if (!animal) throw new Error('Unknown animal');
  if (typeof data.confidence !== 'number' || !Number.isFinite(data.confidence) || data.confidence < 0 || data.confidence > 1) throw new Error('Confidence must be 0..1');
  if (typeof data.timestamp !== 'number' || !Number.isFinite(data.timestamp) || data.timestamp < 1577836800 || data.timestamp > Date.now()/1000 + 300) throw new Error('Invalid Unix timestamp');
  let location = null;
  if (data.latitude != null || data.longitude != null) {
    const a = data.latitude, b = data.longitude;
    if (typeof a !== 'number' || typeof b !== 'number' || !Number.isFinite(a) || !Number.isFinite(b) || Math.abs(a)>90 || Math.abs(b)>180) throw new Error('Invalid GPS');
    location = [a,b];
  }
  return {animal_type:animal, confidence:data.confidence, timestamp:data.timestamp, location};
}
module.exports = {deviceRegistry, authenticateDevice, observationId, normalizeDetection};
