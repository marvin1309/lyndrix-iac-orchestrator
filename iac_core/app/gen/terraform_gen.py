import json
import logging
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

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
    with open(tfvars_path, "w", encoding="utf-8") as f:
        json.dump(tf_state, f, indent=2)
    
    logger.info(f"Wrote Terraform vars: {tfvars_path}")