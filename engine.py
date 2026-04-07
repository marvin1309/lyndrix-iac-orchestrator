import asyncio
import json
import yaml
import sys
import os
import importlib
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from core.logger import get_logger
from core.bus import bus

log = get_logger("IaC:Engine")

# --- UTILITIES ---
class StageResult:
    def __init__(self, success: bool, message: str = "", data: dict = None):
        self.success = success
        self.message = message
        self.data = data or {}

class PipelineLogBridge(logging.Handler):
    def __init__(self, state_list):
        super().__init__()
        self.state_list = state_list
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record):
        log_entry = self.format(record)
        component = record.name.split(':')[-1]
        self.state_list.append(f"[{component}] {log_entry}")

# --- PIPELINE STAGES ---

class BaseStage(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def run(self, engine, context: dict) -> StageResult:
        pass

class SyncRepoStage(BaseStage):
    def __init__(self, role_slug: str):
        super().__init__(f"Sync Repo: {role_slug}")
        self.role_slug = role_slug

    async def run(self, engine, context: dict) -> StageResult:
        if await engine.execute_git_sync(self.role_slug):
            return StageResult(True, f"Synced {self.role_slug}")
        return StageResult(False, f"Failed to sync {self.role_slug}")

class NativeGenerateStage(BaseStage):
    def __init__(self):
        super().__init__("Native State Generation")

    async def run(self, engine, context: dict) -> StageResult:
        try:
            await engine._execute_native_generation()
            return StageResult(True, "Infrastructure state generated.")
        except Exception as e:
            return StageResult(False, f"Generation failed: {str(e)}")

class CommitPushStage(BaseStage):
    def __init__(self, role_slug: str, message: str):
        super().__init__(f"Commit & Push: {role_slug}")
        self.role_slug = role_slug
        self.message = message

    async def run(self, engine, context: dict) -> StageResult:
        if await engine.execute_git_commit_push(self.role_slug, self.message):
            return StageResult(True, f"Pushed to {self.role_slug}")
        return StageResult(False, f"Push failed for {self.role_slug}")

# --- THE NEW ANSIBLE STAGE ---
class AnsiblePlaybookStage(BaseStage):
    def __init__(self, playbook_path: str, inventory_path: str):
        super().__init__(f"Ansible (DRY RUN): {playbook_path}")
        self.playbook_path = playbook_path
        self.inventory_path = inventory_path

    async def run(self, engine, context: dict) -> StageResult:
        success = await engine.execute_ansible_docker(self.playbook_path, self.inventory_path)
        if success:
            return StageResult(True, "Ansible execution completed.")
        return StageResult(False, "Ansible execution failed.")

# --- THE ENGINE ---

class DeploymentEngine:
    def __init__(self, ctx, state):
        self.ctx = ctx
        self.state = state
        self.base_git_dir = Path("/data/storage/git_repos")
        self.pending_syncs = {}
        bus.subscribe("git:status_update")(self._on_git_status)

    def get_default_pipeline(self):
        return [
            SyncRepoStage("iac_controller"),
            SyncRepoStage("inventory_state"),
            SyncRepoStage("config_engine"),  
            NativeGenerateStage(),
            CommitPushStage("inventory_state", "ci: automated state update"),
            

            AnsiblePlaybookStage(
                playbook_path="playbooks/cd_playbooks/cd_test_inventory.yml", 
                inventory_path="global/ansible/inventory.yml"
            )

        ]
    async def run_pipeline(self, payload: dict):
        if self.state.get("is_running"):
            log.warning("ENGINE: Execution already in progress.")
            return

        self.state["is_running"] = True
        self.state["latest_logs"] = ["[SYSTEM] Pipeline Started"]
        
        bridge = PipelineLogBridge(self.state["latest_logs"])
        target_logger = logging.getLogger("IaC")
        target_logger.addHandler(bridge)

        context = {"payload": payload}
        pipeline = self.get_default_pipeline()

        try:
            for stage in pipeline:
                self.state["latest_logs"].append(f"--- STAGE: {stage.name} ---")
                result = await stage.run(self, context)
                if not result.success:
                    raise RuntimeError(f"Stage '{stage.name}' failed: {result.message}")
                log.info(f"ENGINE: {result.message}")

            self.state["latest_logs"].append("[SYSTEM] Pipeline completed successfully.")
            self.state["last_deployment"] = "SUCCESS"
        except Exception as e:
            log.error(f"PIPELINE FATAL: {e}")
            self.state["latest_logs"].append(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
        finally:
            target_logger.removeHandler(bridge)
            self.state["is_running"] = False

    # --- HELPER METHODS ---
    async def _on_git_status(self, payload: dict):
        repo_id = payload.get("repo_id")
        if repo_id in self.pending_syncs and not self.pending_syncs[repo_id].done():
            self.pending_syncs[repo_id].set_result(payload)

    async def execute_git_sync(self, role_slug: str) -> bool:
        raw_config = self.ctx.get_secret(f"repo_{role_slug}_config")
        if not raw_config: return False
        config = json.loads(raw_config)
        url, token_key = config.get("url", ""), config.get("token_key", "")
        if not url: return True

        secret_value = self.ctx.get_secret(token_key) if token_key else ""
        auth_type = "ssh" if "git@" in url or "ssh" in url else "token"

        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[role_slug] = future

        self.ctx.emit("git:sync", {
            "repo_id": role_slug, "url": url, 
            "auth_type": auth_type, "secret_value": secret_value
        })

        try:
            result = await asyncio.wait_for(future, timeout=120.0)
            return result.get("status") == "synced"
        except asyncio.TimeoutError:
            return False
        finally:
            self.pending_syncs.pop(role_slug, None)

    async def execute_git_commit_push(self, role_slug: str, message: str) -> bool:
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[role_slug] = future
        self.ctx.emit("git:commit_push", {"repo_id": role_slug, "message": message, "is_local": False})
        try:
            result = await asyncio.wait_for(future, timeout=120.0)
            return result.get("status") in ["pushed", "committed_locally", "no_changes"]
        except asyncio.TimeoutError:
            return False
        finally:
            self.pending_syncs.pop(role_slug, None)

    async def _execute_native_generation(self):
        controller_dir = self.base_git_dir / "iac_controller"
        state_dir = self.base_git_dir / "inventory_state"
        iac_app_dir = Path(__file__).parent.resolve() / "iac_core" / "app"
        
        sys.path.insert(0, str(iac_app_dir))
        original_argv = sys.argv
        sys.argv = ["generator.py", "--inventory-dir", str(controller_dir), "--output-dir", str(state_dir)]
        try:
            generator_module = importlib.import_module("generator")
            await asyncio.get_event_loop().run_in_executor(None, generator_module.main)
        finally:
            if str(iac_app_dir) in sys.path: sys.path.remove(str(iac_app_dir))
            sys.argv = original_argv

    # --- NEW: ANSIBLE DOCKER EXECUTION ---
# --- NEW: ANSIBLE DOCKER EXECUTION ---
# --- NEW: ANSIBLE DOCKER EXECUTION ---
    async def execute_ansible_docker(self, playbook_subpath: str, inventory_subpath: str) -> bool:
        """Spawns an ephemeral Docker container to execute the playbook and streams the output."""
        import shutil
        import os
        
        # 1. Pre-flight Binary Check
        if not shutil.which("docker"):
            self.state["latest_logs"].append("!!! [FATAL] 'docker' CLI is not installed inside the Lyndrix container.")
            return False

        # 2. Authenticate to Custom Docker Registry (If configured)
        reg_url = self.ctx.get_secret("ansible_registry_url")
        reg_user = self.ctx.get_secret("ansible_registry_user")
        reg_token = self.ctx.get_secret("ansible_registry_token")

        if reg_url and reg_user and reg_token:
            self.state["latest_logs"].append(f"[SYSTEM] Authenticating to registry: {reg_url}")
            login_cmd = ["docker", "login", reg_url, "-u", reg_user, "--password-stdin"]
            
            login_proc = await asyncio.create_subprocess_exec(
                *login_cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            
            # Pass token securely via stdin
            stdout_data, _ = await login_proc.communicate(input=reg_token.encode('utf-8'))
            
            if login_proc.returncode != 0:
                error_msg = stdout_data.decode('utf-8').strip()
                self.state["latest_logs"].append(f"!!! [FATAL] Registry authentication failed: {error_msg}")
                return False
            self.state["latest_logs"].append("[SYSTEM] Registry authentication successful.")

        # 3. Prepare SSH Key from Vault
        ssh_key = self.ctx.get_secret("ansible_ssh_key")
        if not ssh_key:
            self.state["latest_logs"].append("!!! [ERROR] 'ansible_ssh_key' missing in Vault.")
            return False

        # FIX: Ensure the key is formatted correctly for OpenSSH
        # Replace literal escaped newlines just in case JSON serialization mangled them
        clean_key = ssh_key.replace('\\n', '\n').strip() + '\n'

        key_path = "/data/security/ansible_id_rsa"
        with open(key_path, "w") as f:
            f.write(clean_key)
        os.chmod(key_path, 0o600)

        # 4. Explicit Path Translation via Environment Variables
        container_git_dir = "/data/storage/git_repos"
        host_git_dir = os.environ.get("HOST_GIT_REPOS_DIR")
        host_sec_dir = os.environ.get("HOST_SECURITY_DIR")

        if not host_git_dir or not host_sec_dir:
            self.state["latest_logs"].append("!!! [FATAL] Missing required Env Vars: HOST_GIT_REPOS_DIR or HOST_SECURITY_DIR in .env")
            if os.path.exists(key_path):
                os.remove(key_path)
            return False

        full_playbook = f"{container_git_dir}/config_engine/{playbook_subpath}"
        full_inventory = f"{container_git_dir}/inventory_state/{inventory_subpath}"
        
        # FIX: Fetch the dynamic image configured in the UI, do not hardcode it.
        default_img = "registry.gitlab.int.fam-feser.de/iac-environment/iac-platform-assets/ansible-ci-image:latest"
        image_name = self.ctx.get_secret("ansible_docker_image") or default_img

        # 5. Construct the Docker Run Command (Dry Run & Verbose)
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{host_git_dir}:{container_git_dir}",
            "-v", f"{host_sec_dir}/ansible_id_rsa:/root/.ssh/id_rsa:ro",
            "-e", "ANSIBLE_HOST_KEY_CHECKING=False",
            image_name,
            "ansible-playbook", "-i", full_inventory, full_playbook,
            "-u", "ansible-agent", "--check", "-vvv"
        ]

        log.info(f"ENGINE: Executing Docker -> {' '.join(cmd)}")

        # 6. Execute and Stream Output to UI
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )

        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            decoded_line = line.decode('utf-8').rstrip()
            if decoded_line:
                self.state["latest_logs"].append(f"[Ansible] {decoded_line}")

        await process.wait()
        
        if os.path.exists(key_path):
            os.remove(key_path)

        return process.returncode == 0