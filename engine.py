import asyncio
import json
import sys
import os
import importlib
import logging
import shutil
import uuid
import yaml
from pathlib import Path
from core.logger import get_logger
from core.bus import bus
from deepdiff import DeepDiff
import re

from .stages.base import BaseStage
from .utils import StageResult, JobFileLogBridge
from .stages.git import (
    SyncRepoStage,
    CommitPushStage,
    CloneServiceRepoStage,
    SyncAllServicesStage
)
from .stages.ansible import (
    AnsiblePlaybookStage,
    AsyncBulkRolloutStage
)

log = get_logger("IaC:Engine")

# --- LOCAL STAGE DEFINITIONS ---
class NativeGenerateStage:
    def __init__(self):
        self.name = "Native Artifact Generation"
    async def run(self, engine, context: dict) -> StageResult:
        try:
            await engine._execute_native_generation()
            return StageResult(True, "Native artifacts generated.")
        except Exception as e:
            return StageResult(False, f"Native generation failed: {e}")

class DynamicRuleExecutionStage:
    def __init__(self, pipeline_type: str):
        self.name = f"Dynamic Rules: {pipeline_type}"
        self.pipeline_type = pipeline_type
    async def run(self, engine, context: dict) -> StageResult:
        return StageResult(True, f"Dynamic rules evaluated for {self.pipeline_type}")

class DetectDriftStage(BaseStage):
    def __init__(self):
        super().__init__("Detect State Drift")

    def _load_current_state_from_git(self, engine):
        """Parses all YAML files to build the current desired state."""
        # This is a simplified example. A real implementation would be more robust,
        # parsing profiles, sites, hosts, and services into one large dict.
        assignments = {}
        base_dir = engine.config.git_repos_dir / "iac_controller" / "environments"
        sites_dir = base_dir / "sites"
        if not sites_dir.exists(): return {}
        
        for yaml_file in sites_dir.rglob("*.yml"):
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    # Just using hostnames as keys for this example
                    hosts = {**data.get("hosts", {}), **data.get("hardware_hosts", {})}
                    for host_name, host_data in hosts.items():
                        if host_name not in assignments: assignments[host_name] = {}
                        assignments[host_name].update(host_data)
            except Exception:
                continue
        return assignments

    def _get_host_services(self, state_dict: dict, host_name: str) -> set:
        """Helper to extract a simple set of service names for a given host."""
        svcs = set()
        for s in state_dict.get(host_name, {}).get("services", []):
            if isinstance(s, dict) and s.get("name"): svcs.add(s["name"])
        return svcs

    async def run(self, engine, context: dict) -> StageResult:
        log.info("Comparing current desired state against last known good state...")
        
        current_desired_state = self._load_current_state_from_git(engine)
        if not current_desired_state:
            return StageResult(False, "Could not parse current desired state from Git.")

        # Save state to context so PersistStateStage can save it to the DB later
        context["current_desired_state"] = current_desired_state

        last_known_good_record = engine.db.get_state("last_known_good")
        last_known_good_state = last_known_good_record.get("data") if last_known_good_record else {}

        diff = DeepDiff(last_known_good_state, current_desired_state, ignore_order=True)

        if not diff:
            log.info("✅ No drift detected. Infrastructure is up to date.")
            context["stop_pipeline"] = True # Flag to stop the pipeline gracefully
            return StageResult(True, "No drift detected.")
        
        services_to_deploy = set()
        services_to_remove = set()

        # Intelligently parse the drift to find exactly WHICH services changed
        for change_type, changes in diff.items():
            paths = changes.keys() if isinstance(changes, dict) else changes
            for path in paths:
                m = re.match(r"root\['([^']+)'\](.*)", str(path))
                if not m: continue
                
                host_name, remainder = m.group(1), m.group(2)
                old_svcs = self._get_host_services(last_known_good_state, host_name)
                new_svcs = self._get_host_services(current_desired_state, host_name)

                if "['services']" in remainder:
                    # Only the services list changed on this host! 
                    # Find exactly which ones were added or removed
                    services_to_deploy.update(new_svcs - old_svcs)
                    services_to_remove.update(old_svcs - new_svcs)
                else:
                    # A core host property changed (like IP), we must redeploy all of its services
                    services_to_deploy.update(new_svcs)
        
        context["services_to_deploy"] = list(services_to_deploy)
        context["services_to_remove"] = list(services_to_remove)

        log.warning(f"DRIFT DETECTED: Deploying {len(services_to_deploy)} services, Cleaning up {len(services_to_remove)} services.")
        context["is_drift_run"] = True
        return StageResult(True, "Drift detected, proceeding with rollout.")

class CleanupOrphanedServicesStage(BaseStage):
    def __init__(self):
        super().__init__("Cleanup Orphaned Services")
        
    async def run(self, engine, context: dict) -> StageResult:
        to_remove = context.get("services_to_remove", [])
        if not to_remove:
            return StageResult(True, "No services require cleanup.")
            
        log.info(f"Placeholder: Would run cleanup playbook for removed services: {', '.join(to_remove)}")
        # FUTURE: await engine.execute_ansible_docker(playbook_subpath="playbooks/cleanup.yml", extra_vars={"services_to_kill": ",".join(to_remove)}, ...)
        return StageResult(True, "Placeholder cleanup completed.")

class PersistStateStage(BaseStage):
    def __init__(self):
        super().__init__("Persist State to DB")
    async def run(self, engine, context: dict) -> StageResult:
        log.info("Persisting new 'last_known_good' state to database...")
        new_state = context.get("current_desired_state")
        if new_state:
            # Use a placeholder 'latest' for commit hash for now
            engine.db.update_state("last_known_good", new_state, "latest")
        return StageResult(True, "State persisted.")

# --- THE ENGINE ---

class DeploymentEngine:
    def __init__(self, ctx, state, db, config):
        self.ctx = ctx
        self.state = state
        self.db = db
        self.config = config
        self.base_git_dir = self.config.git_repos_dir
        self.pending_syncs = {}
        bus.subscribe("git:status_update")(self._on_git_status)

    def get_default_ansible_stages(self, pipeline_type: str = "connectivity"):
        if pipeline_type == "rollout":
            return [AsyncBulkRolloutStage(inventory_path="global/ansible/inventory.yml", limit="all")]
        return [
            AnsiblePlaybookStage(
                name_override="CONNECTIVITY TEST", 
                playbook_path="playbooks/cd_playbooks/cd_test_inventory.yml", 
                inventory_path="global/ansible/inventory.yml", 
                limit="docker-hydra"
            )
        ]

    async def run_pipeline(self, payload: dict):
        if not payload.get("pipeline_type") and payload.get("object_kind") == "push":
            name, ref = payload.get("project", {}).get("name"), payload.get("ref", "")
            if name and ref.startswith("refs/heads/"):
                payload.update({"pipeline_type": "single_service", "service_name": name, "service_branch": ref.replace("refs/heads/", "")})

        pipeline_type = payload.get("pipeline_type", "connectivity")
        
        # Prevent concurrent bulk rollouts, but allow parallel single_service tasks
        if pipeline_type == "rollout":
            active_rollouts = [j for j in self.db.get_jobs_by_status("RUNNING") if j.pipeline_type == "rollout"]
            if active_rollouts:
                log.warning("ENGINE: A full rollout is already in progress.")
                return
                
        # Safely increment running job counter
        self.state["running_jobs"] = self.state.get("running_jobs", 0) + 1
        self.state["is_running"] = self.state["running_jobs"] > 0

        # Better tagging for filtering
        db_type = pipeline_type
        if pipeline_type == "single_service":
            db_type = f"single_service:{payload.get('service_name')}"
            
        current_job_id = self.db.create_job(db_type)
        
        # FILE LOGGING SETUP
        bridge = JobFileLogBridge(self.config.get_log_path(current_job_id))
        logging.getLogger("IaC").addHandler(bridge)
        
        log.info("[SYSTEM] Pipeline Started")
        log.info(f"[SYSTEM] Job #{current_job_id} registered in database.")

        context = {"payload": payload, "job_id": current_job_id}
        
        pipeline = [
            SyncRepoStage("iac_controller"), SyncRepoStage("inventory_state"), 
            SyncRepoStage("config_engine"), NativeGenerateStage(), 
            CommitPushStage("inventory_state", "ci: automated state update")
        ]
        
        if pipeline_type == "single_service":
            svc_name, svc_branch = payload.get("service_name"), payload.get("service_branch", "main")
            target_group = "stage_dev" if svc_branch == "dev" or str(svc_branch).endswith("-dev") else ("stage_test" if svc_branch == "test" else f"service_{str(svc_name).replace('-', '_')}")
            pipeline.extend([
                CloneServiceRepoStage(svc_name, svc_branch, payload), 
                AnsiblePlaybookStage(
                    name_override=f"Single Service: {svc_name} ({svc_branch})", 
                    playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml", 
                    inventory_path="global/ansible/inventory.yml", 
                    limit=target_group, 
                    extra_vars={"SERVICE_BRANCH": svc_branch, "target_service": svc_name, "target_group": target_group, "LOCAL_SERVICES_DIR": str(self.config.services_dir)}
                )
            ])
        elif pipeline_type == "rollout":
            pipeline.extend([
                DetectDriftStage(),
                SyncAllServicesStage(), 
                AsyncBulkRolloutStage(inventory_path="global/ansible/inventory.yml", limit="all"),
                CleanupOrphanedServicesStage(),
                DynamicRuleExecutionStage(pipeline_type),
                PersistStateStage()
            ])
        else:
            pipeline.append(DynamicRuleExecutionStage(pipeline_type))

        try:
            total_stages = len(pipeline)
            for idx, stage in enumerate(pipeline):
                # MACRO PROGRESS UPDATE
                pct = int((idx / total_stages) * 100)
                self.db.update_progress(current_job_id, progress=pct, current_step=f"Stage: {stage.name}")
                
                log.info(f"--- STAGE: {stage.name} ---")
                res = await stage.run(self, context)
                if not res.success:
                    raise RuntimeError(f"Stage '{stage.name}' failed: {res.message}")
                
                if context.get("stop_pipeline"):
                    log.info("[SYSTEM] Pipeline stopped gracefully by a stage.")
                    break
            
            log.info("[SYSTEM] Pipeline completed successfully.")
            self.state["last_deployment"] = "SUCCESS"
            self.db.update_progress(current_job_id, progress=100, current_step="Completed Successfully")
        except Exception as e:
            log.error(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
            self.db.update_progress(current_job_id, progress=None, current_step="Failed")
        finally:
            logging.getLogger("IaC").removeHandler(bridge)
            self.state["running_jobs"] = max(0, self.state.get("running_jobs", 0) - 1)
            self.state["is_running"] = self.state["running_jobs"] > 0
            self.db.update_job(job_id=current_job_id, status=self.state["last_deployment"])

    async def resume_bulk_rollout(self, job_id: int, pending_services: list[str]):
        if not pending_services: return
        
        self.state["running_jobs"] = self.state.get("running_jobs", 0) + 1
        self.state["is_running"] = self.state["running_jobs"] > 0
        
        self.db.update_progress(job_id, progress=None, current_step="Resuming Bulk Rollout...")
        bridge = JobFileLogBridge(self.config.get_log_path(job_id))
        logging.getLogger("IaC").addHandler(bridge)
        log.info(f"[SYSTEM] Resuming {len(pending_services)} pending services from job #{job_id}")
        
        context = {"payload": {}, "job_id": job_id}
        stage = AsyncBulkRolloutStage(inventory_path="global/ansible/inventory.yml", limit="all", target_services=pending_services)

        try:
            res = await stage.run(self, context)
            log.info("[SYSTEM] Resumed Pipeline completed.")
            self.state["last_deployment"] = "SUCCESS" if res.success else "FAILED"
            if res.success: self.db.update_progress(job_id, progress=100, current_step="Resume Completed")
        except Exception as e:
            log.error(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
            self.db.update_progress(job_id, progress=None, current_step="Resume Failed")
        finally:
            logging.getLogger("IaC").removeHandler(bridge)
            self.state["running_jobs"] = max(0, self.state.get("running_jobs", 0) - 1)
            self.state["is_running"] = self.state["running_jobs"] > 0
            self.db.update_job(job_id=job_id, status=self.state["last_deployment"])

    async def sync_core_repos(self):
        """Periodic/Startup task to keep core repositories up to date."""
        log.info("[SYSTEM] Initiating background sync for core repositories...")
        for repo in ["iac_controller", "inventory_state", "config_engine"]:
            success = await self.execute_git_sync(repo)
            if not success:
                log.warning(f"Failed to sync {repo} during background operation.")

    async def _on_git_status(self, payload: dict):
        repo_id = payload.get("repo_id")
        if repo_id in self.pending_syncs:
            for fut in self.pending_syncs[repo_id]:
                if not fut.done():
                    fut.set_result(payload)
            self.pending_syncs[repo_id].clear()

    async def execute_git_sync(self, role_slug: str) -> bool:
        raw_config = self.ctx.get_secret(f"repo_{role_slug}_config")
        if not raw_config: return False
        config = json.loads(raw_config)
        if not config.get("url"): return True
        
        future = asyncio.get_event_loop().create_future()
        if role_slug not in self.pending_syncs: self.pending_syncs[role_slug] = []
        self.pending_syncs[role_slug].append(future)
        
        self.ctx.emit("git:sync", {"repo_id": role_slug, "url": config.get("url"), "auth_type": "ssh" if "git@" in config.get("url") else "token", "secret_value": self.ctx.get_secret(config.get("token_key", "")) if config.get("token_key") else ""})
        try: return (await asyncio.wait_for(future, timeout=120.0)).get("status") == "synced"
        except asyncio.TimeoutError: return False
        finally: 
            if future in self.pending_syncs.get(role_slug, []): self.pending_syncs[role_slug].remove(future)

    async def execute_git_commit_push(self, role_slug: str, message: str) -> str:
        future = asyncio.get_event_loop().create_future()
        if role_slug not in self.pending_syncs: self.pending_syncs[role_slug] = []
        self.pending_syncs[role_slug].append(future)
        
        self.ctx.emit("git:commit_push", {"repo_id": role_slug, "message": message, "is_local": False})
        try: return (await asyncio.wait_for(future, timeout=120.0)).get("status", "failed")
        except asyncio.TimeoutError: return "timeout"
        finally: 
            if future in self.pending_syncs.get(role_slug, []): self.pending_syncs[role_slug].remove(future)

    async def _execute_native_generation(self):
        sys.path.insert(0, str(Path(__file__).parent.resolve() / "iac_core" / "app"))
        orig_argv, sys.argv = sys.argv, ["generator.py", "--inventory-dir", str(self.base_git_dir / "iac_controller"), "--output-dir", str(self.base_git_dir / "inventory_state")]
        try: await asyncio.get_event_loop().run_in_executor(None, importlib.import_module("generator").main)
        finally: sys.path.remove(sys.path[0]); sys.argv = orig_argv

    async def reconcile_orphaned_runners(self, job_id=None):
        try:
            proc = await asyncio.create_subprocess_exec("docker", "ps", "-a", "--filter", "name=^aac-runner-", "--format", "{{.Names}}", stdout=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            containers = [c.strip() for c in stdout.decode().split('\n') if c.strip()]
            if not containers: return

            log.info(f"Reconciliation: Found {len(containers)} orphaned runners. Reattaching...")
            self.state["is_running"] = True
            
            for c_name in containers:
                task_name = c_name.replace("aac-runner-", "")
                if "active_tasks" not in self.state: self.state["active_tasks"] = {}
                if task_name not in self.state["active_tasks"]:
                    self.state["active_tasks"][task_name] = {"status": "running_ansible", "logs": []}
                # Pass job_id=0 if we don't know it, log will output to job_0.log
                asyncio.create_task(self._watch_detached_runner(c_name, task_name, job_id=0))
        except Exception as e: log.error(f"Failed to reconcile: {e}")

    async def _watch_detached_runner(self, container_name: str, task_name: str, job_id: int):
        successful_hosts, failed_hosts = 0, 0
        log_file = self.config.get_log_path(job_id)
        
        try:
            log_proc = await asyncio.create_subprocess_exec("docker", "logs", "-f", container_name, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            while True:
                line = await log_proc.stdout.readline()
                if not line: break
                decoded = line.decode('utf-8', errors='replace').rstrip()
                if decoded:
                    # 1. WRITE TO DISK DIRECTLY (Solves the memory freeze)
                    with open(log_file, "a", encoding="utf-8") as f:
                        f.write(f"[{task_name}] {decoded}\n")

                    # 2. MICRO-PROGRESS SNIFFER
                    if "TASK [" in decoded:
                        ansible_task = decoded.split("TASK [")[1].split("]")[0]
                        self.db.update_progress(job_id, progress=None, current_step=f"Ansible: {ansible_task}")

                    # 3. LIGHTWEIGHT UI MEMORY (Keep only the last 50 lines for the active popup)
                    if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                        self.state["active_tasks"][task_name]["logs"].append(decoded)
                        if len(self.state["active_tasks"][task_name]["logs"]) > 50:
                            self.state["active_tasks"][task_name]["logs"].pop(0)

                    if "ok=" in decoded and "failed=" in decoded and ":" in decoded:
                        try:
                            sp = decoded.split(":")[1]
                            fc = int(sp.split("failed=")[1].split()[0])
                            uc = int(sp.split("unreachable=")[1].split()[0])
                            if fc > 0 or uc > 0: failed_hosts += 1
                            else: successful_hosts += 1
                        except Exception: pass

            wait_proc = await asyncio.create_subprocess_exec("docker", "wait", container_name, stdout=asyncio.subprocess.PIPE)
            stdout, _ = await wait_proc.communicate()
            success = int(stdout.decode().strip()) == 0
            
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "success" if success else "failed"
            await asyncio.create_subprocess_exec("docker", "rm", "-f", container_name)
            return success, {"successful_hosts": successful_hosts, "failed_hosts": failed_hosts}
            
        except Exception as e:
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "error"
            return False, {"successful_hosts": 0, "failed_hosts": 0}

    async def execute_ansible_docker(self, playbook_subpath: str, inventory_subpath: str, limit: str = None, extra_vars: dict = None, task_name: str = "global", job_id: int = 0):
        key_path = None # Safe default so the 'finally' block doesn't crash on early exit
        
        if not shutil.which("docker"): return False, {"successful_hosts": 0, "failed_hosts": 0}

        if "active_tasks" not in self.state: self.state["active_tasks"] = {}
        self.state["active_tasks"][task_name] = {
            "status": "pulling_image", 
            "logs": [],
            "job_id": job_id 
        }

        reg_url, reg_user, reg_token = self.ctx.get_secret("ansible_registry_url"), self.ctx.get_secret("ansible_registry_user"), self.ctx.get_secret("ansible_registry_token")
        if reg_url and reg_user and reg_token:
            proc = await asyncio.create_subprocess_exec("docker", "login", reg_url, "-u", reg_user, "--password-stdin", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await proc.communicate(input=reg_token.encode('utf-8'))

        ssh_key = self.ctx.get_secret("ansible_ssh_key")
        if not ssh_key: return False, {"successful_hosts": 0, "failed_hosts": 0}

        # Use services_dir for temporary key exchange because its volume mount is proven to be correctly mapped
        key_filename = f"ansible_id_rsa_{uuid.uuid4().hex[:8]}"
        key_dir = self.config.services_dir / ".iac_keys"
        key_dir.mkdir(parents=True, exist_ok=True)
        key_path = key_dir / key_filename
        
        with open(key_path, "w") as f: f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
        os.chmod(key_path, 0o600)

        try:
            # In a Docker-in-Docker setup, bind mounts require the physical host's path.
            h_git = self.config.host_git_repos_dir
            h_svc = self.config.host_services_dir
            
            safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
            c_name = f"aac-runner-{safe_task_name}"
            
            await asyncio.create_subprocess_exec("docker", "rm", "-f", c_name, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            cmd = [
                "docker", "run", "-d", "--name", c_name, "--pull", "always",
                "--label", f"iac_job_id={job_id}", "--label", f"iac_task_name={task_name}",
                "-e", f"IAC_JOB_ID={job_id}",
                "-v", f"{h_git}:/data/storage/git_repos", "-v", f"{h_svc}:/data/storage/services",
                "-e", "ANSIBLE_HOST_KEY_CHECKING=False", "-e", "PYTHONUNBUFFERED=1", "-e", "ANSIBLE_NOCOLOR=1", "-e", "ANSIBLE_DEPRECATION_WARNINGS=0", "-e", "ANSIBLE_INTERPRETER_PYTHON=auto_silent",
                "-e", "ANSIBLE_ROLES_PATH=/data/storage/git_repos/config_engine/roles", "-e", "PYTHONPATH=/opt/aac-template-engine/scripts",
                "--entrypoint", "",
                self.config.ansible_docker_image,
                "/bin/sh", "-c", 
                "mkdir -p /root/.ssh && cp \"$1\" /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa && shift && exec \"$@\"", 
                "--", f"/data/storage/services/.iac_keys/{key_filename}",
                "ansible-playbook", "-i", f"/data/storage/git_repos/inventory_state/{inventory_subpath}", f"/data/storage/git_repos/config_engine/{playbook_subpath}",
                "-u", "ansible-agent", "--diff" 
            ]
            if limit: cmd.extend(["--limit", limit])
            if extra_vars:
                for k, v in extra_vars.items(): cmd.extend(["-e", f"{k}={v}"])
            if not self.config.auto_apply: cmd.append("--check")

            log.info(f"Executing: {playbook_subpath} (Limit: {limit or 'None'})")
            self.state["active_tasks"][task_name]["status"] = "running_ansible"

            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await proc.communicate()
            
            return await self._watch_detached_runner(c_name, task_name, job_id)
        except Exception as e:
            log.error(f"Docker Execution Error: {e}")
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "error"
            return False, {"successful_hosts": 0, "failed_hosts": 0}
        finally:
            if key_path and os.path.exists(key_path):
                try: os.remove(key_path)
                except Exception: pass