#!/usr/bin/env node
//
// Generate a STAGING override of the proposed production PM2 config, and prove that it
// differs from it in exactly the five permitted ways.
//
// WHY A GENERATOR AND NOT A HAND-WRITTEN SECOND CONFIG.
// A staging validation of the production launcher is only worth anything if the config it
// runs is the production config. Hand-copying it would mean validating a file that merely
// resembles the one to be installed, and every future edit would have to be mirrored by
// hand. So the production config is REQUIRED and evaluated, and the overrides are applied
// to the object it produces.
//
// WHY NODE. PM2 reads these files with `require`. Text-matching a `.js` file is guessing
// at what it evaluates to; a config can compute `cwd` from `__dirname`, and ours does.
// Only evaluation gives the values PM2 will actually see — which is exactly how the
// missing-`cwd` defect was found.
//
// THE SIX PERMITTED DIFFERENCES, and nothing else:
//
//   apps[0].name                    production's app name may never be started here
//   apps[0].env.WOA23_PORT          production's port may never be bound here
//   apps[0].env.WOA23_ZARR_STORE    the synthetic store, never production's data
//   apps[0].env.WOA23_TLS           'off' — staging has no certificates and fabricates none
//   apps[0].env.WOA23_TLS_KEYFILE   REMOVED when TLS is off — see below
//   apps[0].env.WOA23_TLS_CERTFILE  REMOVED when TLS is off
//   apps[0].env.WOA23_PYTHON        THIS RUN'S staged venv, never the shared pyenv env
//   apps[0].{log_file,out_file,error_file}   a staging log directory
//
// WOA23_PYTHON is the sixth, and it is new. It was previously required ABSENT, which made
// production_app.sh fall back to the shared `.pyenv/versions/py311` environment — so pm2G
// built an isolated venv, manifested 58 packages for it, and then served from the shared
// env with ZERO libraries mapped from the venv it had just measured. Naming the interpreter
// here is what makes the staged venv the thing that actually runs (spec 016).
//
// A SEVENTH DIFFERENCE IS A STOP. `cwd`, `script`, `args`, `autorestart`, `kill_timeout`,
// `max_memory_restart`, `append_env_to_name`, the worker count, the TLS certificate paths
// and anything else must be byte-identical, because those are the properties the run
// exists to validate. If they can drift, the run validates something production will not
// have.
//
//   WOA23_PM2C_GRANTED=yes node deploy/make_staging_override.js \
//     --name <app> --port <n> --store <abs> --logdir <rel> --out <path>
//     --python <abs>
//
'use strict'

const fs = require('fs')
const path = require('path')

const HERE = __dirname
const PROD_CONFIG = path.join(HERE, 'ecosystem.production.config.js')

function die(...lines) {
  for (const l of lines) process.stderr.write(l + '\n')
  process.exit(2)
}

// --------------------------------------------------------------------------- the grant
// Enforced BOTH ways, as every VM24-touching tool in this campaign is: absent means
// refuse, and another run's grant means refuse. An authorisation for a benchmark is not
// an authorisation to generate and start a staging deployment config.
const OTHER_GRANTS = [
  'WOA23_S2PERF_GRANTED', 'WOA23_S2_C1_GRANTED', 'WOA23_S2_C2_GRANTED',
  'WOA23_D1_GRANTED', 'WOA23_D2A_GRANTED', 'WOA23_D2B_GRANTED',
  'WOA23_BASH5_VERIFY_GRANTED',
]
if (process.env.WOA23_PM2C_GRANTED !== 'yes') {
  die('REFUSING: WOA23_PM2C_GRANTED is not "yes".',
      '  This generates the config for a PM2 staging validation and needs its own grant.',
      '  No other grant is accepted in its place: ' + OTHER_GRANTS.join(', ') + '.')
}
for (const g of OTHER_GRANTS) {
  if (process.env[g] !== undefined && process.env[g] !== '') {
    die('REFUSING: ' + g + ' is set in this environment.',
        '  A PM2 staging validation must not run beside another run\'s grant.')
  }
}

// ------------------------------------------------------------------------ the arguments
const args = {}
for (let i = 2; i < process.argv.length; i += 2) {
  const k = process.argv[i]
  if (!k.startsWith('--')) die('unexpected argument: ' + k)
  const v = process.argv[i + 1]
  if (v === undefined) die('missing value for ' + k)
  args[k.slice(2)] = v
}
for (const required of ['name', 'port', 'store', 'logdir', 'out', 'python']) {
  if (!args[required] || !String(args[required]).trim()) {
    die('--' + required + ' is required and has no default.')
  }
}

// ------------------------------------------------------------------------- the guards
// The app name must not be production's. Even under an isolated PM2_HOME, an app called
// `woa23` makes a mistyped PM2_HOME ambiguous in the situation where ambiguity costs most.
if (args.name === 'woa23') {
  die('REFUSING: the staging app may not be named "woa23" — that is production\'s app.')
}
// The port must be a number, in range, and never production's.
if (!/^[0-9]+$/.test(args.port)) die('--port is not a number: ' + args.port)
const port = Number(args.port)
if (port < 1024 || port > 65535) die('--port out of range (1024-65535): ' + port)
for (const p of [8050, 8786, 8787]) {
  if (port === p) die('REFUSING to generate a config for port ' + p + ': that is PRODUCTION\'s.')
}
// The store must be absolute, must exist, and must not be production's or inside it.
// `path.resolve` collapses `..`, so a path that climbs out is caught rather than passed on.
if (!path.isAbsolute(args.store)) die('--store must be an absolute path: ' + args.store)
const store = path.resolve(args.store)
if (store !== args.store) {
  die('REFUSING: --store is not in normal form (it contains .. or a redundant segment):',
      '  given   : ' + args.store, '  resolves: ' + store)
}
const PROD_STORE = '/home/odbadmin/python/woa23/data'

// STORE MODE. Default `synthetic` keeps every existing caller's behaviour unchanged: the
// flag is absent from all of them, and absence means exactly what it always meant.
const storeMode = args['store-mode'] === undefined ? 'synthetic' : args['store-mode']
if (storeMode !== 'synthetic' && storeMode !== 'real-readonly') {
  die('--store-mode must be \'synthetic\' or \'real-readonly\' (got \'' + storeMode + '\')')
}

// THE LEXICAL GUARD IS NOT ENOUGH ON ITS OWN, and this is why the mode has to be explicit.
// `path.resolve` collapses `..` but DOES NOT follow symlinks, so a link inside the staging
// root pointing at production's store would sail past the check below unnoticed. Real mode
// therefore exists to be *declared*, not to be reached by a path that quietly resolves
// somewhere the guard cannot see.
const realStore = fs.existsSync(store) ? fs.realpathSync(store) : store
const insideProd = (p) => p === PROD_STORE || p.startsWith(PROD_STORE + path.sep)

if (storeMode === 'synthetic') {
  // Unchanged behaviour, plus the same test applied to the RESOLVED path -- a symlink into
  // production is refused here rather than silently accepted.
  if (insideProd(store) || insideProd(realStore)) {
    die('REFUSING: --store resolves inside the production store.',
        '  staging   : ' + store,
        '  realpath  : ' + realStore,
        '  production: ' + PROD_STORE,
        '  Staging uses its own synthetic store and never production\'s data.',
        '  If the real store is intended, say so: --store-mode real-readonly.')
  }
} else {
  // REAL READ-ONLY MODE. The store must resolve to production's store and to nothing else.
  // The guard is not disabled here; it is INVERTED and made exact, so this mode cannot be
  // used to reach any other path.
  if (realStore !== PROD_STORE) {
    die('REFUSING: --store-mode real-readonly, but the store does not resolve to the',
        '  authorised production store.',
        '  given     : ' + store,
        '  realpath  : ' + realStore,
        '  required  : ' + PROD_STORE)
  }
  if (!fs.existsSync(store)) {
    die('REFUSING: --store does not exist: ' + store)
  }
  // Writability is the caller's precondition, re-checked here because a config that names
  // a writable store would be a config that permits a write.
  //
  // `die` calls process.exit, it does not throw -- so the check is written as a plain
  // boolean rather than as control flow through a catch block, where the failure path
  // would be unreachable code sitting inside a guard.
  let storeIsWritable = false
  try { fs.accessSync(realStore, fs.constants.W_OK); storeIsWritable = true } catch (e) { storeIsWritable = false }
  if (storeIsWritable) {
    die('REFUSING: the production store is WRITABLE by this account.',
        '  Real read-only mode requires the KERNEL to refuse writes, not this script.')
  }
}
// The log directory is RELATIVE to the app's cwd and must stay inside it. A `..` here
// would write a staging run's logs over production's tmp/woa23*.log.
if (path.isAbsolute(args.logdir)) die('--logdir must be relative to the app cwd: ' + args.logdir)
if (args.logdir.split(path.sep).includes('..')) {
  die('REFUSING: --logdir escapes the app directory: ' + args.logdir)
}

// The interpreter must be an absolute path to a real executable, and it must be THIS run's
// venv rather than the shared environment. A relative path would resolve against whatever
// cwd PM2 happened to use, and the shared env is the exact thing this override exists to
// stop being used by default.
if (!path.isAbsolute(args.python)) {
  die('--python must be an absolute path: ' + args.python)
}
if (path.resolve(args.python) !== args.python) {
  die('REFUSING: --python is not in normal form (it contains .. or a redundant segment):',
      '  given   : ' + args.python, '  resolves: ' + path.resolve(args.python))
}
if (!fs.existsSync(args.python)) {
  die('REFUSING: --python does not exist: ' + args.python,
      '  The staged venv must be built BEFORE the run phase generates this config.')
}
try {
  fs.accessSync(args.python, fs.constants.X_OK)
} catch (e) {
  die('REFUSING: --python is not executable: ' + args.python)
}
if (/\/\.pyenv\/versions\/py311\//.test(args.python)) {
  die('REFUSING: --python names the SHARED pyenv environment: ' + args.python,
      '  That is the fallback pm2G exposed: a run builds an isolated venv, manifests it,',
      '  and then serves from somewhere else. Name this run\'s own venv interpreter.')
}

// ------------------------------------------------------- load and override the real config
if (!fs.existsSync(PROD_CONFIG)) die('the production config is missing: ' + PROD_CONFIG)
let prod
try {
  prod = require(PROD_CONFIG)
} catch (e) {
  die('the production config did not evaluate: ' + e.message)
}
if (!prod || !Array.isArray(prod.apps) || prod.apps.length !== 1) {
  die('the production config must define exactly one app; got ' +
      (prod && prod.apps ? prod.apps.length : 'none'))
}
const src = prod.apps[0]
// Fields the override depends on. A missing one means the production config changed shape,
// and generating from it blind would silently produce something else.
for (const [field, kind] of [['name', 'string'], ['cwd', 'string'], ['script', 'string'],
                             ['env', 'object'], ['log_file', 'string'],
                             ['out_file', 'string'], ['error_file', 'string']]) {
  if (src[field] === undefined) die('the production config has no "' + field + '" — refusing to guess.')
  const actual = Array.isArray(src[field]) ? 'array' : typeof src[field]
  if (actual !== kind) {
    die('the production config\'s "' + field + '" is a ' + actual + ', expected ' + kind + '.')
  }
}
for (const key of ['WOA23_PORT', 'WOA23_ZARR_STORE']) {
  if (typeof src.env[key] !== 'string') {
    die('the production config\'s env.' + key + ' is missing or not a string — refusing to guess.')
  }
}

const app = JSON.parse(JSON.stringify(src))
app.name = args.name
app.env.WOA23_PORT = String(port)
app.env.WOA23_ZARR_STORE = store
app.env.WOA23_TLS = 'off'
// TLS IS OFF, SO THE KEY AND CERTIFICATE PATHS ARE OMITTED ENTIRELY.
//
// They used to be carried over from the production config, which meant a STAGING process
// ran with production paths in its environment:
//   WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem
//   WOA23_TLS_KEYFILE =/home/odbadmin/python/woa23/conf/privkey.pem
// bs3v1 confirmed they were never opened -- zero file descriptors under /home/odbadmin --
// but non-USE is not non-EXPOSURE, and they were carried in. When TLS is off the paths
// are meaningless, so the honest representation is that they are not there.
//
// This does NOT change TLS behaviour anywhere. production_app.sh still defaults TLS ON,
// still refuses an unreadable key or certificate when it is on, and only an explicit
// WOA23_TLS=off disables it. Nothing here turns TLS off for production.
if (app.env.WOA23_TLS === 'off') {
  delete app.env.WOA23_TLS_KEYFILE
  delete app.env.WOA23_TLS_CERTFILE
}
app.env.WOA23_PYTHON = args.python
app.log_file = args.logdir + '/staging.outerr.log'
app.out_file = args.logdir + '/staging.log'
app.error_file = args.logdir + '/staging_err.log'
// `cwd` is computed from __dirname in the production config, so the evaluated value is an
// absolute path on THIS machine. It is written through unchanged and asserted identical:
// the whole point is that staging runs the production cwd/script relationship.

// ----------------------------------------------------------------------------- the diff
function flatten(obj, prefix, out) {
  out = out || {}
  for (const k of Object.keys(obj)) {
    const v = obj[k]
    const key = prefix ? prefix + '.' + k : k
    if (v && typeof v === 'object' && !Array.isArray(v)) flatten(v, key, out)
    else out[key] = JSON.stringify(v)
  }
  return out
}
const a = flatten(src, '')
const b = flatten(app, '')
const keys = Array.from(new Set(Object.keys(a).concat(Object.keys(b)))).sort()
const diffs = []
for (const k of keys) {
  if (a[k] !== b[k]) diffs.push({ key: k, from: a[k] === undefined ? '(absent)' : a[k],
                                  to: b[k] === undefined ? '(absent)' : b[k] })
}

const EXPECTED = ['name', 'env.WOA23_PORT', 'env.WOA23_ZARR_STORE', 'env.WOA23_TLS',
                  'env.WOA23_PYTHON',
                  'log_file', 'out_file', 'error_file']
  // With TLS off the two path variables are REMOVED, so their disappearance is itself an
  // expected difference. Listing it keeps the diff exhaustive rather than tolerant.
  .concat(app.env.WOA23_TLS === 'off'
            ? ['env.WOA23_TLS_KEYFILE', 'env.WOA23_TLS_CERTFILE'] : [])
  .sort()
const got = diffs.map(d => d.key).sort()

process.stdout.write('== differences from the production config ==\n')
for (const d of diffs) process.stdout.write('  ' + d.key + ': ' + d.from + ' -> ' + d.to + '\n')
process.stdout.write('\n')

const unexpected = got.filter(k => !EXPECTED.includes(k))
const missing = EXPECTED.filter(k => !got.includes(k))
if (unexpected.length) {
  die('', 'REFUSING: unexpected difference(s) from the production config:',
      ...unexpected.map(k => '  ' + k),
      '', '  Only the app name, port, store, WOA23_TLS, WOA23_PYTHON and the three log paths',
      '  may differ.',
      '  Anything else means staging would validate a configuration production will not have.')
}
if (missing.length) {
  die('', 'REFUSING: an expected override did not take effect:',
      ...missing.map(k => '  ' + k),
      '', '  A missing override means the generated config still carries production\'s value.')
}

// The properties the run exists to validate, asserted explicitly rather than left to the
// diff — so that a future change to EXPECTED cannot quietly let one through.
for (const k of ['cwd', 'script', 'args', 'autorestart', 'kill_timeout',
                 'max_memory_restart', 'append_env_to_name']) {
  if (JSON.stringify(src[k]) !== JSON.stringify(app[k])) {
    die('REFUSING: "' + k + '" differs, and it must not.')
  }
}
for (const k of ['WOA23_WORKERS']) {
  if (src.env[k] !== app.env[k]) die('REFUSING: env.' + k + ' differs, and it must not.')
}
// The TLS paths are judged by the mode, not by equality with production.
for (const k of ['WOA23_TLS_KEYFILE', 'WOA23_TLS_CERTFILE']) {
  if (app.env.WOA23_TLS === 'off') {
    if (k in app.env) die('REFUSING: env.' + k + ' is present while WOA23_TLS=off. ' +
                          'With TLS off the path is meaningless and must not be carried ' +
                          'into the staging environment.')
  } else if (src.env[k] !== app.env[k]) {
    die('REFUSING: env.' + k + ' differs, and with TLS on it must not.')
  }
}
if (src.pre_stop !== undefined || app.pre_stop !== undefined) {
  die('REFUSING: a pre_stop key is present. B1 removed it; it may not come back.')
}

const banner = [
  '// GENERATED by deploy/make_staging_override.js — do not edit.',
  '// Source: deploy/ecosystem.production.config.js',
  '// This is a STAGING override. It differs from the production config in exactly these',
  '// ways and no others: app name, port, store, WOA23_TLS=off, WOA23_PYTHON, the three',
  '// log paths, and the REMOVAL of WOA23_TLS_KEYFILE and WOA23_TLS_CERTFILE, which are',
  '// meaningless when TLS is off and must not carry production paths into staging.',
  '// The removals are counted as differences, not treated as exceptions. Every other key,',
  '// including cwd and script, is the production value — which is the point.',
  '',
].join('\n')
fs.writeFileSync(args.out, banner + 'module.exports = ' +
                 JSON.stringify({ apps: [app] }, null, 2) + '\n')

// The request speaks of SIX permitted items; the flattened diff shows EIGHT keys,
// because "log paths" is one item spanning log_file, out_file and error_file. Both
// numbers are printed so a reader comparing this output against the request does not
// have to reconcile them.
process.stdout.write('exactly ' + diffs.length + ' differing keys, across the 6 permitted' +
                     ' items (app name, port, store, WOA23_TLS, WOA23_PYTHON, log paths)\n')
process.stdout.write('python (this run\'s venv): ' + app.env.WOA23_PYTHON + '\n')
process.stdout.write('cwd    (unchanged): ' + app.cwd + '\n')
process.stdout.write('script (unchanged): ' + app.script + '\n')
process.stdout.write('written: ' + args.out + '\n')
