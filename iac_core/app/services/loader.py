import yaml
from pathlib import Path
from typing import Dict, Any
from deepmerge import Merger

def unique_list_merge(config, path, base, nxt):
    for item in nxt:
        if item not in base:
            base.append(item)
    return base

unique_merger = Merger(
    [(list, [unique_list_merge]), (dict, ["merge"]), (set, ["union"])],
    ["override"], ["override"]
)

def read_yaml(file_path: Path) -> Dict[str, Any]:
    """Reads a YAML file and returns a dictionary."""
    if not file_path.exists():
        return {} # Fail gracefully if a stage/site doesn't have a specific file
    
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML syntax in {file_path}: {exc}")

def load_configuration(inventory_root: Path, site: str, stage: str) -> Dict[str, Any]:
    """Loads and strictly merges YAML configurations."""
    config: Dict[str, Any] = {}

    # Load Globals
    global_dir = inventory_root / "environments" / "global"
    unique_merger.merge(config, read_yaml(global_dir / "01_global_vars.yml"))
    unique_merger.merge(config, read_yaml(global_dir / "02_service_catalog.yml"))
    unique_merger.merge(config, read_yaml(global_dir / "03_profiles.yml"))
    unique_merger.merge(config, read_yaml(global_dir / "04_agents.yml"))

    # Load Site
    site_dir = inventory_root / "environments" / "sites" / site
    unique_merger.merge(config, read_yaml(site_dir / "site_vars.yml"))
    unique_merger.merge(config, read_yaml(site_dir / "networks.yml"))
    unique_merger.merge(config, read_yaml(site_dir / "hardware.yml"))

    # Load Stage
    stage_dir = site_dir / "stages" / stage
    unique_merger.merge(config, read_yaml(stage_dir / "stage_vars.yml"))
    unique_merger.merge(config, read_yaml(stage_dir / "stage.yml"))
    unique_merger.merge(config, read_yaml(stage_dir / "hosts.yml"))

    return config