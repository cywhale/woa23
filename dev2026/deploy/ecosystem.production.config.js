// PROPOSED replacement for conf/ecosystem.config.js. NOT INSTALLED, NOT IN USE.
//
// Kept in dev2026/deploy/ because writing it into conf/ would be a production change,
// and that needs its own authorisation. Spec 011 covers the cutover this belongs to.
//
// ---------------------------------------------------------------------------------
// THE ONE THING THIS REMOVES: `pre_stop` (blocker B1)
//
// Production's config carries:
//
//   pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}'
//              | xargs -r kill -9"
//
// Three separate faults, any one of which is disqualifying:
//
//   1. it matches a COMMAND-LINE STRING, not a process this app owns. Anything on the
//      host whose argv contains `woa23_app` is killed — including production's own
//      master and workers when a second copy is stopped beside it;
//   2. `kill -9` cannot be caught, so a worker mid-response is destroyed rather than
//      drained. This campaign's cleanup policy forbids SIGKILL outright;
//   3. it matches ITSELF. `ps -ef` lists the shell running this very command, whose
//      argv contains `woa23_app`; `grep -v grep` does not exclude it. The pipeline can
//      therefore `kill -9` its own shell partway through its own PID list, so what it
//      actually kills depends on scheduling.
//
// After cutover it would also be matching a name the service no longer has, so it
// would quietly stop doing anything at all — a defect that stops looking like one.
//
// IT IS NOT REPLACED BY A BETTER pre_stop. It is removed, because it is unnecessary:
// production_app.sh `exec`s gunicorn, so PM2 tracks the master directly and its own
// stop signal reaches it. pm2B demonstrated this end to end — SIGINT to the master,
// both workers drained and gone, no pre_stop present.
//
// ---------------------------------------------------------------------------------
// WHY THIS ONE HAS AN `env` BLOCK WHEN THE STAGING CONFIG DOES NOT
//
// pm2A failed because a config `env` block overrode the environment `pm2 start` was
// given — PM2 merges the config over the command, and the config wins. The lesson is
// NOT "never use env". It is "the config wins, so what is in it must be the truth".
//
//   - staging values are per-run (a new port, tree and store each time), so they come
//     from the operator and the staging config carries no `env` block at all;
//   - production values are fixed properties of the deployment, so they belong in the
//     config, where they are visible, reviewable and identical on every restart.
//
// What made pm2A a failure was PLACEHOLDERS in that block — an empty store and a stale
// port. Every value below is real and explicit. None may be a placeholder, and the
// launcher refuses an empty one rather than defaulting it.
module.exports = {
  apps: [
    {
      name: 'woa23',

      // `cwd` IS REQUIRED, AND ITS ABSENCE WAS A DEFECT.
      //
      // The first version of this file set `script: './dev2026/deploy/production_app.sh'`
      // with no `cwd`, which cannot work. Two things depend on the working directory and
      // they pulled in opposite directions:
      //
      //   - that script path resolves only if PM2's cwd is the REPOSITORY ROOT;
      //   - `python -m gunicorn api.app:app` puts cwd on sys.path, and `api/` lives under
      //     `dev2026/`. From the repository root `api` is NOT importable — verified.
      //
      // So PM2 would have reported the app `online` and gunicorn would have died at
      // import. That is the B4 failure mode — a fault that surfaces after PM2 says
      // started — arriving through a different door, and no amount of reading the
      // launcher would have shown it.
      //
      // Fixed the way the staging config already did it, which pm2B proved end to end:
      // name the cwd explicitly, and make `script` relative to that cwd. `__dirname`
      // makes it correct wherever the tree is checked out, so staging and production
      // resolve by the same rule rather than by a hard-coded path.
      cwd: __dirname + '/..',
      script: './deploy/production_app.sh',
      args: '',

      env: {
        WOA23_PORT: '8050',
        WOA23_ZARR_STORE: '/home/odbadmin/python/woa23/data',

        // ABSOLUTE, and that is a consequence of `cwd` above. These used to be
        // `conf/privkey.pem`, relative — which resolved against production's old cwd
        // (the repository root). With cwd now `dev2026/`, the same relative path would
        // point at `dev2026/conf/privkey.pem`, which does not exist, and the launcher
        // would refuse to start with a TLS error.
        //
        // The launcher refusing is the correct behaviour and is exactly why it checks
        // certificate readability BEFORE claiming the port — but the right fix is to say
        // which files are meant rather than to rely on where PM2 happened to be started.
        //
        // THE PATH BELOW IS A PLACEHOLDER FOR THE INSTALL LOCATION and must be confirmed
        // against the host before any cutover; it is not confirmed today. `pm2C` runs
        // with WOA23_TLS=off and never reads these.
        WOA23_TLS_KEYFILE: '/home/odbadmin/python/woa23/conf/privkey.pem',
        WOA23_TLS_CERTFILE: '/home/odbadmin/python/woa23/conf/fullchain.pem',
        WOA23_WORKERS: '2',
      },

      // Unchanged from production, deliberately: a cutover changes the application,
      // not the restart policy or the memory ceiling. Two variables at once is two
      // explanations for one symptom.
      autorestart: true,
      max_memory_restart: '4G',
      watch: false,
      merge_logs: true,
      log_file: 'tmp/woa23.outerr.log',
      out_file: 'tmp/woa23.log',
      error_file: 'tmp/woa23_err.log',
      log_date_format: 'YYYY-MM-DD HH:mm Z',

      // Production sets this true. It appends the --env name to the app name, so a
      // `pm2 start --env production` would create `woa23-production` ALONGSIDE
      // `woa23` — two apps, one port, and the PM2 state for the original orphaned.
      // At a cutover, where an operator is more likely than usual to type --env, that
      // is a live hazard.
      append_env_to_name: false,

      // Room for the 120s request timeout plus the 10s graceful drain, so a stop is
      // never escalated by PM2. It only matters if the master ignores SIGINT, which
      // gunicorn does not.
      kill_timeout: 20000,
    },
  ],
}
