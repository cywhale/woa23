module.exports = {
  apps : [
  {
    name: 'woa23',
    script: './conf/start_app.sh',
    args: '',
    merge_logs: true,
    autorestart: true,
    log_file: "tmp/woa23.outerr.log",
    out_file: "tmp/woa23.log",
    error_file: "tmp/woa23_err.log",
    log_date_format : "YYYY-MM-DD HH:mm Z",
    append_env_to_name: true,
    watch: false,
    max_memory_restart: '4G',
    pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
  }],
};
