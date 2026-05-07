const projectRoot = __dirname
const pythonBin = process.env.PYTHON_BIN || '/Users/zhao/miniconda3/envs/py313/bin/python'
const syncHost = process.env.SYNC_HOST || '127.0.0.1'
const syncPort = process.env.SYNC_PORT || '8010'

const bootstrap = `
import importlib.util
import pathlib
import sys

root = pathlib.Path(${JSON.stringify(projectRoot)}).resolve()
package_init = root / "__init__.py"

spec = importlib.util.spec_from_file_location(
    "sync_data_system",
    package_init,
    submodule_search_locations=[str(root)],
)
module = importlib.util.module_from_spec(spec)
sys.modules["sync_data_system"] = module
spec.loader.exec_module(module)

import uvicorn

uvicorn.run(
    "sync_data_system.service.api:app",
    host=${JSON.stringify(syncHost)},
    port=int(${JSON.stringify(syncPort)}),
    reload=False,
    access_log=True,
)
`.trim()

module.exports = {
  apps: [
    {
      name: 'alphablocks-sync-data',
      cwd: projectRoot,
      script: pythonBin,
      args: ['-c', bootstrap],
      interpreter: 'none',
      autorestart: true,
      watch: false,
      max_restarts: 10,
      min_uptime: '5s',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONPATH: projectRoot,
        SYNC_HOST: syncHost,
        SYNC_PORT: syncPort,
      },
    },
  ],
}
