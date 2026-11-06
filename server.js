'use strict';
const http = require('http');
const crypto = require('crypto');
const {pipeline} = require('stream/promises');
const {Transform} = require('stream');
const WebSocket = require('ws');
const {initializeApp, applicationDefault} = require('firebase-admin/app');
const {getFirestore, Timestamp, GeoPoint} = require('firebase-admin/firestore');
const {getStorage} = require('firebase-admin/storage');
const {getAuth} = require('firebase-admin/auth');
const {deviceRegistry, authenticateDevice, observationId, normalizeDetection} = require('./core');
const devices = deviceRegistry(process.env); // Fail closed: no unconfigured device connections.
const bucketName = process.env.FIREBASE_STORAGE_BUCKET;
if (!bucketName) throw new Error('FIREBASE_STORAGE_BUCKET missing');
initializeApp({credential:applicationDefault(), storageBucket:bucketName});
const db = getFirestore(), bucket = getStorage().bucket();
const json = (res, status, body) => {res.writeHead(status, {'Content-Type':'application/json', 'Cache-Control':'no-store'}); res.end(JSON.stringify(body));};
async function readJson(req) {
  let size=0; const chunks=[];
  for await (const chunk of req) {size+=chunk.length; if(size>16384) throw new Error('JSON too large'); chunks.push(chunk);}
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
function deviceForRequest(req) {
  const token = (req.headers.authorization || '').replace(/^Bearer /,'');
  return authenticateDevice(devices, req.headers['x-huntcam-device'], token);
}
async function upload(req, ref, kind, d, id) {
  const snapshot = await ref.get();
  if (!snapshot.exists || snapshot.data().device_id!==d.device_id || snapshot.data().owner_uid!==d.owner_uid) return 404;
  const max = kind==='image' ? 3*1024*1024 : 25*1024*1024;
  const length = Number(req.headers['content-length']);
  const type = kind==='image' ? 'image/jpeg' : 'video/mp4';
  if (!Number.isSafeInteger(length) || length<12 || length>max || req.headers['content-type']!==type) return 400;
  const path = `detections/${d.owner_uid}/${d.device_id}/${id}/${kind==='image'?'image.jpg':'video.mp4'}`;
  const file=bucket.file(path);
  // A retry after a committed upload must reuse its download token.
  let metadata;
  try { [metadata] = await file.getMetadata(); }
  catch(e) { if(Number(e.code)!==404) throw e; }
  if (!metadata) {
    const token=crypto.randomUUID(); let size=0, header=Buffer.alloc(0), checked=false;
    const guard=new Transform({transform(chunk, encoding, callback) {
      size+=chunk.length;
      if(size>max || size>length) return callback(new Error('Upload too large'));
      if(!checked) {
        header=Buffer.concat([header,chunk]);
        if(header.length<12) return callback();
        const valid=kind==='image' ? header[0]===255 && header[1]===216 && header[2]===255 : header.toString('ascii',4,8)==='ftyp';
        if(!valid) return callback(new Error('Invalid media header'));
        checked=true; this.push(header); header=null; return callback();
      }
      callback(null,chunk);
    }, flush(callback) {callback(size===length && checked ? null : new Error('Incomplete media'));}});
    try {
      await pipeline(req, guard, file.createWriteStream({resumable:false, validation:'crc32c',
        preconditionOpts:{ifGenerationMatch:0}, metadata:{contentType:type, cacheControl:'private, max-age=3600',
          metadata:{firebaseStorageDownloadTokens:token}}}));
    } catch(e) { if(Number(e.code)!==412) throw e; }
    [metadata]=await file.getMetadata();
  } else { req.resume(); }
  const token=metadata.metadata?.firebaseStorageDownloadTokens;
  if(!token) throw new Error('Media download token missing');
  const url=`https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/${encodeURIComponent(path)}?alt=media&token=${encodeURIComponent(token.split(',')[0])}`;
  await ref.update({[kind+'_url']:url, [kind+'_storage_path']:path, updated_at:Timestamp.now()});
  return 200;
}
const server=http.createServer(async(req,res)=>{
  req.setTimeout(120000,()=>req.destroy());
  try {
    const path=new URL(req.url,'http://localhost').pathname;
    if(path==='/health') return json(res,200,{status:'ok',version:'huntcam-storage-1'});
    if(!path.startsWith('/api/detections')) return json(res,404,{error:'Not found'});
    const d=deviceForRequest(req);
    if(!d) {req.resume(); return json(res,401,{error:'Unauthorized'});}
    if(req.method==='POST' && path==='/api/detections') {
      const data=await readJson(req), id=observationId(d.device_id,data.event_id), n=normalizeDetection(data);
      const doc={owner_uid:d.owner_uid,device_id:d.device_id,dog_name:d.dog_name,
        event_id:data.event_id,animal_type:n.animal_type,confidence:n.confidence,
        detected_at:Timestamp.fromMillis(n.timestamp*1000),created_at:Timestamp.now(),
        image_url:'',video_url:'',source:'CM5_V17'};
      if(n.location) doc.location=new GeoPoint(...n.location);
      try {await db.collection('detections').doc(id).create(doc);}
      catch(e) {if(Number(e.code)!==6 && e.code!=='already-exists') throw e;}
      return json(res,200,{id});
    }
    const match=path.match(/^\/api\/detections\/([a-f0-9]{64})\/(image|video)$/);
    if(req.method==='PUT' && match) {
      const status=await upload(req,db.collection('detections').doc(match[1]),match[2],d,match[1]);
      return json(res,status,status===200?{ok:true}:{error:'Invalid upload'});
    }
    req.resume(); return json(res,404,{error:'Not found'});
  }catch(e){
    console.error('Request failed:',e.code || e.name); // Never log credentials or media URLs.
    if(!res.headersSent) json(res,400,{error:'Request failed; check server logs and retry'});
  }
});
server.maxRequestsPerSocket=100;
const wss=new WebSocket.Server({server,maxPayload:256*1024,perMessageDeflate:false});
const send=(ws,data)=>{if(ws.readyState===WebSocket.OPEN && ws.bufferedAmount<1024*1024) ws.send(JSON.stringify(data));};
function group(d,role) {return [...wss.clients].filter(c=>c.device?.device_id===d.device_id && c.role===role);}
function announceViewer(viewer) {for(const video of group(viewer.device,'video')) send(video,{type:'viewer_ready',session_id:viewer.sessionId});}
wss.on('connection',ws=>{
  ws.isAlive=true; ws.on('pong',()=>ws.isAlive=true);
  const timeout=setTimeout(()=>ws.close(1008,'Authentication required'),10000);
  let busy=false, expires;
  ws.on('error',()=>{});
  ws.on('message',async(raw,isBinary)=>{
    if(isBinary || busy) return ws.close(1008,'Invalid message');
    busy=true;
    try {
      const data=JSON.parse(raw.toString());
      if(!ws.device) {
        if(data.type!=='auth') throw new Error('auth');
        if(data.role==='viewer') {
          const claims=await getAuth().verifyIdToken(data.id_token,true);
          const d=devices.get(data.device_id);
          if(!d || claims.uid!==d.owner_uid) throw new Error('access');
          ws.device=d; ws.role='viewer'; ws.sessionId=crypto.randomUUID();
          expires=setTimeout(()=>ws.close(1008,'Refresh authentication'),Math.max(1,claims.exp*1000-Date.now()));
        } else if(data.role==='sensor' || data.role==='video') {
          const d=authenticateDevice(devices,data.device_id,data.token);
          if(!d) throw new Error('access');
          if(group(d,data.role).length) throw new Error('Duplicate device role');
          ws.device=d; ws.role=data.role;
        } else throw new Error('role');
        clearTimeout(timeout); send(ws,{type:'auth_ok'});
        if(ws.role==='viewer') announceViewer(ws);
        if(ws.role==='video') for(const viewer of group(ws.device,'viewer')) send(ws,{type:'viewer_ready',session_id:viewer.sessionId});
        return;
      }
      const d=ws.device;
      if(ws.role==='sensor') {
        if(data.type==='detection') normalizeDetection(data);
        else if(data.type==='location') {
          if(typeof data.latitude!=='number' || typeof data.longitude!=='number' || !Number.isFinite(data.latitude) || !Number.isFinite(data.longitude) || Math.abs(data.latitude)>90 || Math.abs(data.longitude)>180) throw new Error('gps');
        }else return;
        // Persistence goes through authenticated HTTP API and durable CM5 outbox.
        for(const viewer of group(d,'viewer')) send(viewer,data);
      }else if(ws.role==='viewer') {
        if(data.type==='settings') for(const sensor of group(d,'sensor')) send(sensor,data);
        else if(data.type==='answer' || data.type==='candidate') for(const video of group(d,'video')) send(video,{...data,session_id:ws.sessionId});
      }else if(ws.role==='video' && (data.type==='offer' || data.type==='candidate')) {
        const viewer=group(d,'viewer').find(c=>c.sessionId===data.session_id);
        if(viewer) send(viewer,data);
      }
    }catch(e){ws.close(1008,'Authentication or message rejected');}
    finally {busy=false;}
  });
  ws.on('close',()=>{
    clearTimeout(timeout); clearTimeout(expires);
    if(ws.role==='viewer') for(const video of group(ws.device,'video')) send(video,{type:'viewer_gone',session_id:ws.sessionId});
    if(ws.role==='video') for(const viewer of group(ws.device,'viewer')) send(viewer,{type:'video_offline'});
  });
});
const heartbeat=setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive) ws.terminate(); else {ws.isAlive=false;ws.ping();}}},30000);
wss.on('close',()=>clearInterval(heartbeat));
server.listen(process.env.PORT || 10000,'0.0.0.0',()=>console.log('HuntCam authenticated server ready'));
