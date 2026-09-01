// A session handoff must never be committed to this repo.
//
// A `HANDOFF.md` lived at the repo root and went stale three times — by 5 weeks
// and 41 builds (#284), with three claims outright wrong (3f9b842), and finally
// from 2026-08-18 to 2026-09-01 telling every cold tab "read it before touching
// anything" while describing a superseded build and an unmerged branch. Two
// separate audits filed it as OPEN and it kept misleading sessions anyway.
//
// Prose in a doc cannot enforce this; a failing test can. See CLAUDE.md.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const ROOT = execFileSync('git', ['rev-parse', '--show-toplevel'], { encoding: 'utf8' }).trim();
const git = (...args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' });

/** Root-level markdown whose name reads as a session handoff. */
const isHandoffPath = (p) => !p.includes('/') && /handoff/i.test(p) && /\.md$/i.test(p);

test('no session handoff is tracked at the repo root', () => {
  const tracked = git('ls-files').split('\n').filter(Boolean);
  const offenders = tracked.filter(isHandoffPath);
  assert.deepEqual(
    offenders,
    [],
    `A handoff doc is committed at the repo root: ${offenders.join(', ')}.\n` +
      'Handoffs go stale and then actively mislead the next session. Write it\n' +
      'outside the repo and hand over the absolute path instead. See CLAUDE.md.'
  );
});

test('git actually ignores handoff files at the root', () => {
  // Behavioural, not textual: ask git what it would do, so a reworded or
  // relocated .gitignore rule cannot pass this by accident.
  for (const name of ['HANDOFF.md', 'HANDOFF-2026-01-01.md', 'SIGNOVIVO-HANDOFF.md']) {
    let ignored = true;
    try {
      git('check-ignore', '-q', '--no-index', name);
    } catch {
      ignored = false; // exit 1 = not ignored
    }
    assert.equal(ignored, true, `${name} is not gitignored — it could be committed again.`);
  }
});

test('CLAUDE.md is tracked and states the rule, so a session reads it first', () => {
  const tracked = git('ls-files').split('\n').filter(Boolean);
  assert.ok(tracked.includes('CLAUDE.md'), 'CLAUDE.md must be committed — it is what every session reads.');
  const claude = readFileSync(join(ROOT, 'CLAUDE.md'), 'utf8').toLowerCase();
  assert.match(claude, /handoff/, 'CLAUDE.md must tell sessions where handoffs go.');
  assert.match(claude, /never committed|never be committed/, 'CLAUDE.md must state the rule.');
});
