import json
import logging
import os
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

def write_json_if_changed(filepath: Path, new_data: dict) -> bool:
    """Writes JSON to disk ONLY if the data has actually changed."""
    if filepath.exists():
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                if existing_data == new_data:
                    logger.debug(f"No changes detected for {filepath}. Skipping write.")
                    return False
        except json.JSONDecodeError:
            pass

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(new_data, f, indent=2)
    
    logger.info(f"Updated Terraform vars: {filepath}")
    return True

def generate_terraform_state(config: Dict[str, Any], output_dir: Path) -> None:
    """Generates a terraform.tfvars.json file from the merged configuration."""
    tf_dir = output_dir / "terraform"
    tf_dir.mkdir(parents=True, exist_ok=True)

    tf_state: Dict[str, Any] = {
        "managed_nodes": {},
        "hardware_infrastructure": {}
    }

    # Extract Terraform data from hardware_hosts
    if "hardware_hosts" in config:
        for hostname, details in config["hardware_hosts"].items():
            if isinstance(details, dict) and "terraform" in details:
                if details["terraform"].get("is_used", False):
                    tf_state["hardware_infrastructure"][hostname] = details["terraform"]

    # Extract Terraform data from standard hosts (VMs/Containers)
    if "hosts" in config:
        for hostname, details in config["hosts"].items():
            if isinstance(details, dict) and "terraform" in details:
                if details["terraform"].get("is_managed", False):
                    tf_state["managed_nodes"][hostname] = details["terraform"]

    tfvars_path = tf_dir / "terraform.tfvars.json"
    write_json_if_changed(tfvars_path, tf_state)