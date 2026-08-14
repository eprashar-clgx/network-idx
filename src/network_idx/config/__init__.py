"""
Configuration for the network_idx package.

Configuration is split by concern into submodules so that each kind of setting
lives next to its peers: `environment` (which environment we run in), `paths`
(local filesystem directories), `gcs` (Google Cloud Storage settings), and
`bigquery` (all BigQuery dataset, table, and view identifiers). This package
re-exports every public name from those submodules, so existing imports such as
`from network_idx.config import RAW_DIR` continue to work unchanged. To add a new
setting, place it in the submodule that matches its concern; it becomes importable
from `network_idx.config` automatically.
"""
from dotenv import load_dotenv

# Load environment variables from a local .env file before any submodule reads
# them, so that values resolved via os.getenv pick up the developer's overrides.
load_dotenv()

from .environment import *  # noqa: E402,F401,F403
from .paths import *  # noqa: E402,F401,F403
from .gcs import *  # noqa: E402,F401,F403
from .bigquery import *  # noqa: E402,F401,F403
