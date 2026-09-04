// PM2 config for CANDIDATE alternate-port staging on VM24. Not production's.
//
// conf/ecosystem.config.js could not be copied. Its `pre_stop` is
//
//     ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9
//
// which matches on a COMMAND-LINE STRING, not on a process this config started. Run
// beside production it would find production's workers — they contain `woa23_app` —
// and `kill -9` them. It is a production-outage command sitting in a stop hook, and it
// is also `kill -9`, which this campaign's cleanup policy forbids outright.
//
// There is no pre_stop here at all. PM2 signals the process it started, by PID, and
// gunicorn's --graceful-timeout does the rest. Nothing greps.
//
// PM2 STATE ISOLATION IS MANDATORY. Every command below sets PM2_HOME to a staging
// directory, so this app lives in its OWN PM2 daemon and process list and cannot
// appear in — or be reached from — production's:
//
//   export PM2_HOME=~/woa23-staging-pm2
//   pm2 start   dev2026/deploy/ecosystem.staging.config.js
//   pm2 logs    woa23-staging-candidate
//   pm2 restart woa23-staging-candidate
//   pm2 stop    woa23-staging-candidate
//   pm2 delete  woa23-staging-candidate
//
// FORBIDDEN, with or without PM2_HOME set: `pm2 delete all`, `pm2 restart all`,
// `pm2 stop all`, `pm2 kill`, and a global `pm2 save` or `pm2 resurrect`. Each of
// those operates on EVERY app in whichever daemon is addressed, and a single missing
// export would make that production's. Name the staging app explicitly, always.
//
module.exports = {
  apps: [
    {
      // NOT 'woa23'. A distinct name is what keeps `pm2 restart`, `pm2 stop` and
      // `pm2 delete` from ever reaching production's app by mistake.
      name: 'woa23-staging-candidate',
      script: './deploy/start_staging.sh',
      // dev2026/, so `api.app` is importable and the relative log paths resolve.
      cwd: __dirname + '/..',
      args: '',

      // THERE IS NO `env` BLOCK, AND ITS ABSENCE IS THE FIX.
      //
      // The pm2A run failed because there was one. PM2 merges the config's `env` over
      // the environment `pm2 start` was given, and the CONFIG WINS — so the block's
      // placeholders replaced the values the command passed. Read back from the app's
      // own `pm2 jlist`, the process received WOA23_STAGING_STORE='' and
      // WOA23_STAGING_PORT='18221' instead of the store path and 18231 it was started
      // with. The launcher refused the empty store and exited 2, which is the only
      // reason a spent port was not bound.
      //
      // With no `env` block PM2 passes the starting environment through unchanged, so
      // the three required variables must be supplied AT `pm2 start`:
      //
      //   WOA23_STAGING_PORT   WOA23_STAGING_STORE   WOA23_PRODUCTION_STORE
      //
      // and `deploy/start_staging.sh` refuses to start if any is missing, empty or
      // invalid. Nothing here defaults them: a default is what caused this.

      // Own logs, own directory. Production writes tmp/woa23*.log; nothing here
      // shares a path with it.
      merge_logs: true,
      log_file: 'tmp-staging/woa23-staging.outerr.log',
      out_file: 'tmp-staging/woa23-staging.log',
      error_file: 'tmp-staging/woa23-staging_err.log',
      log_date_format: 'YYYY-MM-DD HH:mm Z',

      // FALSE, unlike production. A staging crash must be visible as a crash: an
      // autorestart would mask exactly the startup and lifespan failures this
      // validation exists to observe. `pm2 restart` is still exercised explicitly.
      autorestart: false,
      watch: false,
      max_memory_restart: '4G',

      // Longer than gunicorn's --graceful-timeout 10, so PM2's SIGKILL fallback is
      // not what stops the process in the normal case.
      kill_timeout: 20000,
      append_env_to_name: false
    }
  ]
};
