import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { apply } from '../plugins/approval-feishu.mjs';

async function fixture(t, owned = true) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'dsh-approval-test-'));
  t.after(() => fs.rm(root, {recursive: true, force: true}));
  let listener;
  apply({on(event, fn, options) {
    assert.equal(event, 'approval/request');
    assert.equal(options.prepend, true);
    listener = fn;
  }}, {pipeDir: root, timeoutSeconds: 1, pollMs: 100});
  if (owned) await fs.writeFile(path.join(root, 'channels', 'session-a'), 'feishu');
  return {root, listener, request: {agent: {session: {id: 'session-a'}}, toolName: 'pwsh', callId: 'test-call'}};
}

async function requestFile(root) {
  for (let i = 0; i < 40; i++) {
    const files = await fs.readdir(path.join(root, 'pending'));
    const name = files.find(name => name.endsWith('.json'));
    if (name) {
      const claim = path.join(root, 'claim', name);
      try { await fs.access(claim); return name; } catch {}
    }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  assert.fail('pending request must remain visible while host waits');
}

test('owned request stays visible until the bridge decision arrives', async t => {
  const {root, listener, request} = await fixture(t);
  const result = listener(request, () => assert.fail('owned request delegated'));
  const name = await requestFile(root);
  const req = JSON.parse(await fs.readFile(path.join(root, 'pending', name), 'utf8'));
  assert.equal(req.sessionId, 'session-a');
  await fs.writeFile(path.join(root, 'decisions', name), JSON.stringify({id: req.id, outcome: 'allowed-once'}));
  assert.equal(await result, 'allowed-once');
  assert.deepEqual(await fs.readdir(path.join(root, 'pending')), []);
  assert.deepEqual(await fs.readdir(path.join(root, 'claim')), []);
});

test('unowned sessions keep their normal UI approval', async t => {
  const {root, listener, request} = await fixture(t, false);
  assert.equal(await listener(request, async () => 'delegated'), 'delegated');
  assert.deepEqual(await fs.readdir(path.join(root, 'pending')), []);
});

test('timeout rejects and removes stale request files', async t => {
  const {root, listener, request} = await fixture(t);
  assert.equal(await listener(request, () => assert.fail('delegated')), 'rejected');
  assert.deepEqual(await fs.readdir(path.join(root, 'pending')), []);
  assert.deepEqual(await fs.readdir(path.join(root, 'claim')), []);
});

test('cancellation rejects and releases the claim', async t => {
  const {root, listener, request} = await fixture(t);
  const controller = new AbortController();
  const result = listener({...request, signal: controller.signal}, () => assert.fail('delegated'));
  await requestFile(root);
  controller.abort();
  assert.equal(await result, 'rejected');
  assert.deepEqual(await fs.readdir(path.join(root, 'pending')), []);
  assert.deepEqual(await fs.readdir(path.join(root, 'claim')), []);
});
