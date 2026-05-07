const projectRoot = __dirname
const pythonBin = process.env.PYTHON_BIN || 'python3'
const syncHost = process.env.SYNC_HOST || '0.0.0.0'
const syncPort = process.env.SYNC_PORT || '8010'

module.exports = {
  apps: [
    {
      name: 'alphablocks-sync-data',
      cwd: projectRoot,
      script: `${projectRoot}/scripts/run_api_service.py`,
      interpreter: pythonBin,
      args: ['--host', syncHost, '--port', syncPort],
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '5s',
      env: {
        PYTHONUNBUFFERED: '1',
        SYNC_HOST: syncHost,
        SYNC_PORT: syncPort,
      },
    },
  ],
}
