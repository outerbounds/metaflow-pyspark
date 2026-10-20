# Metaflow has to finish initializing before any extension module is imported
# directly: metaflow's own plugin resolution imports the decorator modules, and
# reaching them first makes that circular.
import metaflow  # noqa: F401
