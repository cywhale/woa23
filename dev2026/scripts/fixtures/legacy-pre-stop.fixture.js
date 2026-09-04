// REGRESSION FIXTURE — the historical `pre_stop` hook, kept OUT of every real config.
//
// WHAT THIS IS. Production's `conf/ecosystem.config.js` carried this line from 2024-06-18
// until Stage B removed it. It is preserved here, and ONLY here, so the pattern the B1 work
// exists to prevent can still be tested against — without leaving a dangerous-looking
// setting in a config anything might read, copy, or deploy.
//
// WHY IT IS NOT JUST A COMMENT. The generator's refusal guard
// (`make_staging_override.js`: "a pre_stop key is present. B1 removed it; it may not come
// back") needs a real config object carrying a real `pre_stop` to refuse. A commented-out
// string could not exercise it, and an inline string in one test could not be shared with
// the suites that assert the real configs are clean. This file is the single source of
// both.
//
// WHAT IT IS NOT:
//   - not deployable. It is not under deploy/, and it is not named ecosystem.*.config.js,
//     so it is neither mistaken for a real config nor excluded from the subject's file-list.
//   - not a description of production. Production no longer carries this line: VM24's
//     conf/ecosystem.config.js is ed5dec6c…2159 as of Stage B, and the repository source
//     was reconciled to match.
//   - NOT a claim that PM2 would ever have run it. Stage A established that PM2 5.4.2 has
//     no `pre_stop` hook at all — 0 occurrences in its source, absent from schema.json's 65
//     app keys — so the line was inert dead configuration. It is preserved because it READ
//     as an active SIGKILL safeguard, which is the mistake worth regression-testing.
//
// conf/simu.sh still contains the same technique, including a line that greps `tide_app`
// directly. That is deliberately untouched and is a separate matter; this fixture does not
// cover it and must not be read as covering it.

// The historical hook, verbatim as it stood in conf/ecosystem.config.js line 16.
const LEGACY_PRE_STOP =
  "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"

module.exports = {
  // The string on its own, for suites that need to assert the pattern is absent elsewhere.
  LEGACY_PRE_STOP,

  // A complete app object carrying it, for driving a refusal guard with real input.
  apps: [
    {
      name: 'woa23-legacy-fixture',
      script: './conf/start_app.sh',
      pre_stop: LEGACY_PRE_STOP,
    },
  ],
}
