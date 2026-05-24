const projectRoot = __dirname
const fs = require('node:fs')

const amazingDataPython = '/home/mubin/.miniconda3/envs/amazing_data/bin/python3'
const defaultPythonBin = fs.existsSync(amazingDataPython) ? amazingDataPython : 'python3'
const pythonBin = process.env.PYTHON_BIN || defaultPythonBin
const syncJobPythonBin = process.env.SYNC_JOB_PYTHON_BIN || pythonBin
const syncHost = process.env.SYNC_HOST || '0.0.0.0'
const syncPort = process.env.SYNC_PORT || '8010'
const runtimeConfig = process.env.SYNC_DATA_RUNTIME_CONFIG || `${projectRoot}/config/runtime.local.yaml`

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
        SYNC_JOB_PYTHON_BIN: syncJobPythonBin,
        SYNC_DATA_RUNTIME_CONFIG: runtimeConfig,
      },
    },
  ],
}
