import assert from 'node:assert/strict';
import test from 'node:test';
import { Readable, Writable } from 'node:stream';
import { readFrames, writeFrame } from '../src/transports/stdio.ts';
import { encodeFrame, decodeFrame, validateControl } from '../src/protocols/session/binary.ts';
import { payload, type Request } from '../src/protocols/session/messages.ts';
const request=(operation:string):Request=>({session:'a'.repeat(32),activation:{incarnation:'1',fiber:'2',epoch:'3'},operation,command:'call',payload:payload({method:'echo'},new Uint8Array([0,255,128,0]))});
test('split headers and coalesced frames preserve opaque binary bodies',async()=>{
  const parts:Buffer[]=[];
  const output=new Writable({write(chunk,_encoding,done){parts.push(chunk);done();}});
  for(const n of ['1','2'])await writeFrame(output,encodeFrame(request(n)));
  const bytes=Buffer.concat(parts),messages=[];
  for await(const frame of readFrames(Readable.from([bytes.subarray(0,2),bytes.subarray(2,7),bytes.subarray(7)])))messages.push(frame);
  assert.deepEqual(messages.map(frame=>({...frame,payload:'payload' in frame?{...frame.payload,messages:frame.payload.messages.map(b=>new Uint8Array(b))}:undefined})),[request('1'),request('2')]);
});
test('truncated headers, bodies, malformed protobuf and excessive lengths fail closed',async()=>{
  for(const bytes of [Buffer.from([0,0]),Buffer.from([0,0,0,2,1]),Buffer.from([0,0,0,0]),Buffer.from([0,0,0,1,255]),Buffer.from([0,32,0,0])])
    await assert.rejects(async()=>{for await(const _ of readFrames(Readable.from([bytes])))assert.fail('invalid frame delivered');});
});
test('transport control is bounded and business bytes are opaque',()=>{
  const bytes=encodeFrame(request('1'));
  assert.throws(()=>decodeFrame(Uint8Array.from([...bytes,0x78,1]), 'audit'),/undeclared/);
  assert.throws(()=>validateControl({value:2**53}));
  assert.throws(()=>validateControl({value:'\ud800'}));
  const cycle:any=[];cycle.push(cycle);assert.throws(()=>validateControl(cycle),/nesting/);
  let nested:any=null;for(let n=0;n<16;n++)nested=[nested];validateControl(nested);assert.throws(()=>validateControl([nested]),/nesting/);
  assert.throws(()=>validateControl(Array(8192).fill(null)),/node/);
});
test('maximum control object depth, node count and body count fit the generated decoder',()=>{
  let nested:any=null;for(let n=0;n<16;n++)nested={nested};
  for(const control of [nested,Object.fromEntries(Array.from({length:8191},(_,n)=>[String(n),null]))]){
    const frame={...request('1'),payload:{control,messages:Array.from({length:4096},()=>new Uint8Array())}};
    for (const policy of ['contract','checked','audit'] as const)
      assert.deepEqual(decodeFrame(encodeFrame(frame),policy),frame);
  }
  assert.throws(()=>encodeFrame({...request('1'),payload:{control:null,messages:Array.from({length:4097},()=>new Uint8Array())}}));
});
test('coalesced frames use the policy established after the preceding frame', async () => {
  const parts: Buffer[] = [];
  const output = new Writable({write(chunk, _encoding, done) { parts.push(chunk); done(); }});
  await writeFrame(output, encodeFrame(request('1')));
  await writeFrame(output, Uint8Array.from([...encodeFrame(request('2')), 0x78, 1]));
  for (const selected of ['contract', 'checked', 'audit'] as const) {
    let policy: import('../src/core/validation.ts').Validation = 'audit';
    const reader = readFrames(Readable.from([Buffer.concat(parts)]), () => policy);
    assert.equal((await reader.next()).value?.operation, '1');
    policy = selected;
    if (selected === 'audit') await assert.rejects(reader.next(), /undeclared protobuf/);
    else assert.equal((await reader.next()).value?.operation, '2');
    await reader.return(undefined);
  }
});

test('fragmented large frames own their bodies across yields and source mutation', async () => {
  const original = {...request('1'), payload: payload(null, new Uint8Array(256 * 1024).fill(173))};
  const parts: Buffer[] = [];
  const output = new Writable({write(chunk, _encoding, done) { parts.push(chunk); done(); }});
  await writeFrame(output, encodeFrame(original));
  await writeFrame(output, encodeFrame(request('2')));
  const stream = Buffer.concat(parts);
  for (const size of [1, 4096, 65536, stream.length]) {
    const fragments = Array.from({length: Math.ceil(stream.length / size)}, (_, i) => stream.subarray(i * size, (i + 1) * size));
    const decoded = [];
    for await (const frame of readFrames(Readable.from(fragments))) decoded.push(frame);
    assert.equal(decoded.length, 2);
    assert.ok('payload' in decoded[0]);
    assert.deepEqual(Buffer.from(decoded[0].payload.messages[0]), Buffer.from(original.payload.messages[0]));
    assert.equal(decoded[1].operation, '2');
  }
  const source = Buffer.from(parts[0]);
  const iterator = readFrames(Readable.from([source]));
  const frame = (await iterator.next()).value as Request;
  source.fill(0);
  assert.deepEqual(Buffer.from(frame.payload.messages[0]), Buffer.from(original.payload.messages[0]));
  await iterator.return(undefined);
});
