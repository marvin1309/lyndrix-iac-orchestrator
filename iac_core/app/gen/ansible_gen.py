import yaml
import os
from pathlib import Path
from typing import Dict, Any
from core.logger import get_logger

log = get_logger("IaC:Generator:AnsibleGen")

class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data): return True

def write_yaml_if_changed(filepath: Path, new_data: dict) -> bool:
    """
    Writes YAML to disk ONLY if the data has actually changed.
    Returns True if the file was updated, False if no changes were needed.
    """
    # 1. Read existing data if the file exists
    if filepath.exists():
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                existing_data = yaml.safe_load(f)
                
                # Check for equality. Python dict equality checks deep structure and values.
                if existing_data == new_data:
                    log.debug(f"No changes detected for {filepath}. Skipping write.")
                    return False
        except yaml.YAMLError:
            log.warning(f"Existing file {filepath} is corrupt or unreadable. Forcing overwrite.")
            pass # If the file is corrupt, proceed to overwrite it

    # 2. Write the file
    # We use sort_keys=False because your inventory structure relies on specific ordering
    # for readability, but dictionaries in Python 3.7+ preserve insertion order anyway.
    with open(filepath, 'w', encoding='utf-8') as f:
        yaml.dump(new_data, f, Dumper=NoAliasDumper, default_flow_style=False, sort_keys=False, width=1000)
    
    log.info(f"Updated state file: {filepath}")
    return True

def generate_ansible_state(config: Dict[str, Any], output_dir: Path) -> None:
    ansible_dir = output_dir / "ansible"
    ansible_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Aggregate group_vars/all.yml
    all_vars = {}
    for key in ["global_vars", "site_vars", "stage_vars"]:
        if key in config: 
            all_vars.update(config[key])

    gv_dir = ansible_dir / "group_vars"
    gv_dir.mkdir(parents=True, exist_ok=True)
    
    # NEW: Use the idempotent write function for variables
    write_yaml_if_changed(gv_dir / "all.yml", all_vars)

    # 2. Inventory Structure
    inventory = {"all": {"children": {}, "hosts": {}}}
    
    def process_host(hostname: str, details: Dict[str, Any]):
        if not isinstance(details, dict): return
        
        # Add host to the top-level 'all' hosts
        inventory["all"]["hosts"][hostname] = details
        
        # Calculate Groups
        groups = set(details.get("ansible_groups") or [])
        groups.update(details.get("baseline_roles") or [])
        groups.update(details.get("roles") or [])
        
        # System Groups
        if details.get("type"): groups.add(f"type_{details['type']}")
        if details.get("is_cluster") or details.get("is_part_of_cluster"):
            groups.add("cluster_node")
            if details.get("cluster_type"): 
                groups.add(f"type_{details['cluster_type']}")
        
        # Service Groups
        for svc in (details.get("services") or []):
            if isinstance(svc, dict) and "name" in svc:
                groups.add(f"service_{svc['name'].replace('-', '_')}")

        # CLEANUP: Remove reserved keyword 'roles' from host vars to fix Ansible warnings
        if "roles" in inventory["all"]["hosts"][hostname]:
            inventory["all"]["hosts"][hostname].pop("roles", None)

        # Map host to each group
        for grp in groups:
            if not grp: continue
            # FIX: Sanitize group names (Ansible warns about hyphens)
            safe_grp = grp.replace("-", "_")
            if safe_grp not in inventory["all"]["children"]:
                inventory["all"]["children"][safe_grp] = {"hosts": {}}
            inventory["all"]["children"][safe_grp]["hosts"][hostname] = None

    # 3. Process All Sources
    # Process Hardware (including Cluster Nodes)
    hw_hosts = config.get("hardware_hosts") or {}
    for name, details in hw_hosts.items():
        if isinstance(details, dict) and details.get("is_cluster") and "nodes" in details:
            for node_name, node_cfg in details["nodes"].items():
                process_host(node_name, node_cfg)
        else:
            process_host(name, details)

    # Process Standard Hosts
    std_hosts = config.get("hosts") or {}
    for name, details in std_hosts.items():
        process_host(name, details)

    # 4. Write File using the idempotent writer
    out_path = ansible_dir / "inventory.yml"
    write_yaml_if_changed(out_path, inventory)