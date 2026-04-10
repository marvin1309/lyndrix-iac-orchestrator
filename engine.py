import asyncio
import json
import yaml
import fnmatch
import sys
import os
import importlib
import logging
import shutil
import uuid
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

# THE NEW FILE LOGGER
class JobFileLogBridge(logging.Handler):
    def __init__(self, job_id: int):
        super().__init__()
        self.log_path = Path(f"/data/storage/logs/job_{job_id}.log")
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record):
        log_entry = self.format(record)
        component = record.name.split(':')[-1]
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"[{component}] {log_entry}\n")
        except Exception:
            pass


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
        status = await engine.execute_git_commit_push(self.role_slug, self.message)
        
        context[f"{self.role_slug}_commit_status"] = status
        
        if status in ["pushed", "committed_locally", "no_changes"]:
            return StageResult(True, f"Git status for {self.role_slug}: {status}")
        return StageResult(False, f"Push failed for {self.role_slug} with status: {status}")

class AnsiblePlaybookStage(BaseStage):
    def __init__(self, playbook_path: str, inventory_path: str, limit: str = None, name_override: str = None, extra_vars: dict = None):
        self.display_name = name_override or f"Ansible: {playbook_path}"
        super().__init__(self.display_name)
        self.playbook_path = playbook_path
        self.inventory_path = inventory_path
        self.limit = limit
        self.extra_vars = extra_vars or {}

    async def run(self, engine, context: dict) -> StageResult:
        # THE FIX: Explicitly passing arguments by name to prevent positional mixups
        success, stats = await engine.execute_ansible_docker(
            playbook_subpath=self.playbook_path, 
            inventory_subpath=self.inventory_path, 
            limit=self.limit,
            extra_vars=self.extra_vars,
            task_name=self.display_name,
            job_id=context.get("job_id", 0)
        )
        msg = "Ansible execution completed." if success else "Ansible execution failed."
        return StageResult(success, msg, data=stats)

class CloneServiceRepoStage(BaseStage):
    def __init__(self, service_name: str, branch: str, payload: dict):
        super().__init__(f"Clone Service Repo: {service_name}")
        self.service_name = service_name
        self.branch = branch
        self.payload = payload

    async def run(self, engine, context: dict) -> StageResult:
        services_dir = Path("/data/storage/services")
        services_dir.mkdir(parents=True, exist_ok=True)
        target_dir = services_dir / self.service_name
        
        repo_url = None
        
        if not repo_url:
            catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
            if catalog_file.exists():
                try:
                    with open(catalog_file, 'r') as f:
                        catalog_data = yaml.safe_load(f) or {}
                        catalog = catalog_data.get("service_catalog", {})
                        
                        git_mgmt = catalog.get("remote_git_management", {})
                        services_list = catalog.get("services", [])
                        
                        matched_svc = next((s for s in services_list if s.get("name") == self.service_name), None)
                        if matched_svc and git_mgmt.get("active"):
                            base_url = git_mgmt.get("remote_git_repository_toplevel", "").rstrip("/")
                            repo_name = matched_svc.get("repository_name", matched_svc.get("name"))
                            if base_url:
                                repo_url = f"{base_url}/{repo_name}.git"
                except Exception as e:
                    log.warning(f"Failed to parse source service_catalog: {e}")

        if not repo_url:
            repo_url = f"https://gitlab.int.fam-feser.de/aac-application-definitions/{self.service_name}.git"

        raw_svc_config = engine.ctx.get_secret("repo_service_repos_config")
        svc_token = None
        if raw_svc_config:
            try:
                svc_config = json.loads(raw_svc_config)
                token_key = svc_config.get("token_key")
                if token_key:
                    svc_token = engine.ctx.get_secret(token_key)
            except Exception:
                pass

        if repo_url.startswith("https://") and svc_token:
            repo_url = repo_url.replace("https://", f"https://gitlab-ci-token:{svc_token}@")

        auth_env = os.environ.copy()
        ssh_key = engine.ctx.get_secret("ansible_ssh_key")
        if ssh_key:
            key_path = "/data/security/ansible_id_rsa"
            os.makedirs(os.path.dirname(key_path), exist_ok=True)
            with open(key_path, "w") as f:
                f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
            os.chmod(key_path, 0o600)
            auth_env["GIT_SSH_COMMAND"] = f"ssh -i {key_path} -o StrictHostKeyChecking=no"

        try:
            if (target_dir / ".git").exists():
                git_cmds = [
                    ["git", "fetch", "origin", self.branch],
                    ["git", "checkout", "-f", self.branch],
                    ["git", "reset", "--hard", f"origin/{self.branch}"]
                ]
                for cmd in git_cmds:
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, cwd=str(target_dir), env=auth_env,
                        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                    )
                    await proc.communicate()
                    if proc.returncode != 0:
                        raise RuntimeError(f"Git sync failed at: {' '.join(cmd)}")
                return StageResult(True, f"Updated {self.service_name}")

            else:
                if target_dir.exists():
                    import shutil
                    shutil.rmtree(target_dir)

                proc = await asyncio.create_subprocess_exec(
                    "git", "clone", "-b", self.branch, repo_url, str(target_dir), env=auth_env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
                )
                stdout, _ = await proc.communicate()
                
                if proc.returncode != 0:
                    raise RuntimeError(f"Clone failed: {stdout.decode(errors='ignore')}")

                return StageResult(True, f"Successfully cloned {self.service_name}")

        except Exception as e:
            log.error(f"Git error for {self.service_name}: {e}")
            if (target_dir / "service.yml").exists():
                return StageResult(True, f"Using local fallback for {self.service_name}")
            return StageResult(False, f"Git failed and no valid service.yml found: {str(e)}")

class SyncAllServicesStage(BaseStage):
    def __init__(self):
        super().__init__("Bulk Sync Service Repositories")

    async def run(self, engine, context: dict) -> StageResult:
        catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
        
        if not catalog_file.exists():
            return StageResult(False, f"Source service_catalog.yml missing at {catalog_file}")

        try:
            with open(catalog_file, 'r') as f:
                catalog_data = yaml.safe_load(f) or {}
                catalog = catalog_data.get("service_catalog", {})
                
                services = catalog.get("services", [])
                git_mgmt = catalog.get("remote_git_management", {})
        except Exception as e:
            return StageResult(False, f"Raw catalog parse failed: {e}")

        if not services:
            log.warning("Parsed raw catalog, but no services found. Check YAML structure.")
            return StageResult(False, "No services found in raw catalog to sync.")

        default_branch = git_mgmt.get("remote_git_default_branch_prod", "main")
        log.info(f"Bulk syncing {len(services)} services (Branch: {default_branch})...")
        
        sem = asyncio.Semaphore(5) 
        
        async def bounded_sync(svc_name):
            async with sem:
                stage = CloneServiceRepoStage(svc_name, default_branch, {})
                result = await stage.run(engine, context)
                if not result.success:
                    log.error(f"Sync failed for {svc_name}: {result.message}")
                return result

        tasks = [bounded_sync(svc.get("name")) for svc in services if svc.get("name")]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        failures = 0
        for r in results:
            if isinstance(r, Exception):
                log.error(f"Bulk sync encountered an unhandled exception: {str(r)}")
                failures += 1
            elif not r.success:
                failures += 1
                
        msg = f"Bulk sync complete. {len(services) - failures} succeeded, {failures} failed."
        
        if failures > 0:
            return StageResult(False, f"CRITICAL: {failures} services failed to sync. Aborting rollout.")
        
        return StageResult(True, msg)

# THE UPDATED BULK ROLLOUT (MID-TIER PROGRESS)
class AsyncBulkRolloutStage(BaseStage):
    def __init__(self, inventory_path: str, limit: str = "all", target_services: list[str] = None):
        super().__init__(f"Async Bulk Rollout (Limit: {limit})")
        self.inventory_path = inventory_path
        self.limit = limit
        self.target_services = target_services

    async def run(self, engine, context: dict) -> StageResult:
        job_id = context.get("job_id", 0)

        if self.target_services is not None:
            svc_names = self.target_services
        else:
            catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
            if not catalog_file.exists():
                return StageResult(False, "Source service_catalog.yml missing.")
            try:
                with open(catalog_file, 'r') as f:
                    catalog_data = yaml.safe_load(f) or {}
                    services = catalog_data.get("service_catalog", {}).get("services", [])
                    svc_names = [svc.get("name") for svc in services if svc.get("name")]
            except Exception as e:
                return StageResult(False, f"Raw catalog parse failed: {e}")

        if not svc_names:
            return StageResult(False, "No services found to deploy.")

        log.info(f"Initiating Async Rollout for {len(svc_names)} services (Limit: {self.limit})...")
        
        pending_queue = list(svc_names)
        total_services = len(pending_queue) # For Progress Tracking

        try:
            if hasattr(engine.db, 'update_pending_tasks'):
                engine.db.update_pending_tasks(job_id, pending_queue)
        except Exception: pass

        sem = asyncio.Semaphore(5)
        report = {}
        failed_services = []

        async def bounded_deploy(svc_name):
            async with sem:
                sanitized_name = str(svc_name).replace("-", "_")
                svc_group = f"service_{sanitized_name}"
                eff_limit = f"{self.limit}:&{svc_group}" if self.limit and self.limit != "all" else svc_group
                
                stage = AnsiblePlaybookStage(
                    name_override=svc_name,
                    playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml",
                    inventory_path=self.inventory_path,
                    limit=eff_limit,
                    extra_vars={"target_service": svc_name, "target_group": eff_limit, "LOCAL_SERVICES_DIR": "/data/storage/services"}
                )
                res = await stage.run(engine, context)
                
                report[svc_name] = {"success": res.success, "successful_hosts": res.data.get("successful_hosts", 0), "failed_hosts": res.data.get("failed_hosts", 0)}
                if not res.success: failed_services.append(svc_name)

                # Pop from DB queue and UPDATE MID-TIER PROGRESS
                if svc_name in pending_queue:
                    pending_queue.remove(svc_name)
                    try:
                        if hasattr(engine.db, 'update_pending_tasks'):
                            engine.db.update_pending_tasks(job_id, pending_queue)
                            completed = total_services - len(pending_queue)
                            engine.db.update_progress(job_id, progress=None, current_step=f"Bulk Deploying ({completed}/{total_services})")
                    except Exception: pass

        await asyncio.gather(*[bounded_deploy(s) for s in svc_names])

        summary = ["\n" + "="*60, "🚀 BULK ROLLOUT DEPLOYMENT SUMMARY", "="*60]
        tot_succ, tot_fail = 0, 0
        for svc, stats in sorted(report.items()):
            status_icon = "✅" if stats['success'] else "❌"
            sh, fh = stats['successful_hosts'], stats['failed_hosts']
            host_str = f"{sh} hosts" + (f" | FAILED on {fh} hosts" if fh > 0 else "")
            summary.append(f"{status_icon} {svc.ljust(35)} : {host_str}")
            tot_succ += sh; tot_fail += fh

        summary.extend(["="*60, f"📊 TOTALS: {len(svc_names)} Services | {tot_succ} Successes | {tot_fail} Failures", "="*60 + "\n"])
        for line in summary: log.info(line)

        if failed_services: return StageResult(False, f"Bulk rollout had failures. See summary.", data=report)
        return StageResult(True, "Async bulk rollout completed successfully.", data=report)

class DynamicRuleExecutionStage(BaseStage):
    def __init__(self, pipeline_type: str):
        super().__init__("Dynamic Rule-Based Execution")
        self.pipeline_type = pipeline_type

    async def _get_changed_files(self, engine) -> list[str]:
        inventory_repo_path = engine.base_git_dir / "inventory_state"
        cmd = ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
        
        log.info(f"Checking for changes in: {inventory_repo_path}")
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=inventory_repo_path
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            err_msg = stderr.decode()
            log.error(f"Failed to get git diff from inventory_state: {err_msg}")
            if "fatal: bad object HEAD" in err_msg: return []
            raise RuntimeError(f"Git diff failed: {err_msg}")
        
        changed = stdout.decode().strip()
        return [f for f in changed.split('\n') if f] if changed else []

    def _extract_changed_services(self, changed_files: list[str]) -> list[str]:
        """Parses git paths to find specific targeted services."""
        changed_services = set()
        for filepath in changed_files:
            for part in filepath.split('/'):
                if part.startswith("aac-") or part in ["aria2", "renovate", "gitlab", "minio", "openbao"]: 
                    changed_services.add(part)
                    break
        return list(changed_services)

    async def run(self, engine, context: dict) -> StageResult:
        rules_file_path = engine.base_git_dir / "config_engine" / "pipeline_rules.yml"
        
        if not rules_file_path.exists():
            log.warning("pipeline_rules.yml not found. Falling back to default hardcoded behavior.")
            for stage in engine.get_default_ansible_stages(self.pipeline_type):
                res = await stage.run(engine, context)
                if not res.success: return res
            return StageResult(True, "Fallback pipeline executed successfully.")

        with open(rules_file_path, 'r') as f:
            try:
                rules_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                return StageResult(False, f"Error parsing pipeline_rules.yml: {e}")

        actions = []
        changed_services = []

        if self.pipeline_type == "connectivity":
            actions = rules_config.get("connectivity_test", [])
            log.info("Executing 'connectivity_test' action from rules file.")
        else:
            if context.get("inventory_state_commit_status") == "no_changes":
                log.info("No state changes generated. Bypassing execution.")
                return StageResult(True, "No state changes generated. Bypassing execution.")

            changed_files = await self._get_changed_files(engine)
            if not changed_files:
                return StageResult(True, "No changes in git diff, no actions required.")
            
            changed_services = self._extract_changed_services(changed_files)
            if changed_services:
                log.info(f"Differential deployment triggered for: {changed_services}")
            
            for rule in rules_config.get("rules", []):
                if any(fnmatch.fnmatch(f, p) for f in changed_files for p in rule.get("paths", [])):
                    actions = rule.get("actions", [])
                    break
                    
            if not actions:
                actions = rules_config.get("default", [])
            
        if not actions:
            return StageResult(True, "No actions performed.")

        for action in actions:
            target = action.get("playbook", "")
            if "cd_rollout_service.yml" in target:
                stage = AsyncBulkRolloutStage(
                    inventory_path=action.get("inventory"), 
                    limit=action.get("limit"), 
                    target_services=changed_services if changed_services else None
                )
            else:
                stage = AnsiblePlaybookStage(
                    name_override=action.get("name"), 
                    playbook_path=target, 
                    inventory_path=action.get("inventory"), 
                    limit=action.get("limit")
                )
                
            res = await stage.run(engine, context)
            if not res.success:
                return res

        return StageResult(True, "Dynamic rule-based execution completed.")

# --- THE ENGINE ---

class DeploymentEngine:
    def __init__(self, ctx, state, db):
        self.ctx = ctx
        self.state = state
        self.db = db
        self.base_git_dir = Path("/data/storage/git_repos")
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
        if self.state.get("is_running"):
            log.warning("ENGINE: Execution already in progress.")
            return
            
        self.state["is_running"] = True

        if not payload.get("pipeline_type") and payload.get("object_kind") == "push":
            name, ref = payload.get("project", {}).get("name"), payload.get("ref", "")
            if name and ref.startswith("refs/heads/"):
                payload.update({"pipeline_type": "single_service", "service_name": name, "service_branch": ref.replace("refs/heads/", "")})

        pipeline_type = payload.get("pipeline_type", "connectivity")
        # Better tagging for filtering
        db_type = pipeline_type
        if pipeline_type == "single_service":
            db_type = f"single_service:{payload.get('service_name')}"
            
        current_job_id = self.db.create_job(db_type)
        
        # FILE LOGGING SETUP
        bridge = JobFileLogBridge(current_job_id)
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
                    extra_vars={"SERVICE_BRANCH": svc_branch, "target_service": svc_name, "target_group": target_group, "LOCAL_SERVICES_DIR": "/data/storage/services"}
                )
            ])
        elif pipeline_type == "rollout":
            pipeline.extend([SyncAllServicesStage(), DynamicRuleExecutionStage(pipeline_type)])
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
            
            log.info("[SYSTEM] Pipeline completed successfully.")
            self.state["last_deployment"] = "SUCCESS"
            self.db.update_progress(current_job_id, progress=100, current_step="Completed Successfully")
        except Exception as e:
            log.error(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
            self.db.update_progress(current_job_id, progress=None, current_step="Failed")
        finally:
            logging.getLogger("IaC").removeHandler(bridge)
            self.state["is_running"] = False
            self.db.update_job(job_id=current_job_id, status=self.state["last_deployment"])

    async def resume_bulk_rollout(self, job_id: int, pending_services: list[str]):
        if not pending_services: return
        self.state["is_running"] = True
        
        self.db.update_progress(job_id, progress=None, current_step="Resuming Bulk Rollout...")
        bridge = JobFileLogBridge(job_id)
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
            self.state["is_running"] = False
            self.db.update_job(job_id=job_id, status=self.state["last_deployment"])

    async def _on_git_status(self, payload: dict):
        if payload.get("repo_id") in self.pending_syncs and not self.pending_syncs[payload.get("repo_id")].done():
            self.pending_syncs[payload.get("repo_id")].set_result(payload)

    async def execute_git_sync(self, role_slug: str) -> bool:
        raw_config = self.ctx.get_secret(f"repo_{role_slug}_config")
        if not raw_config: return False
        config = json.loads(raw_config)
        if not config.get("url"): return True
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[role_slug] = future
        self.ctx.emit("git:sync", {"repo_id": role_slug, "url": config.get("url"), "auth_type": "ssh" if "git@" in config.get("url") else "token", "secret_value": self.ctx.get_secret(config.get("token_key", "")) if config.get("token_key") else ""})
        try: return (await asyncio.wait_for(future, timeout=120.0)).get("status") == "synced"
        except asyncio.TimeoutError: return False
        finally: self.pending_syncs.pop(role_slug, None)

    async def execute_git_commit_push(self, role_slug: str, message: str) -> str:
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[role_slug] = future
        self.ctx.emit("git:commit_push", {"repo_id": role_slug, "message": message, "is_local": False})
        try: return (await asyncio.wait_for(future, timeout=120.0)).get("status", "failed")
        except asyncio.TimeoutError: return "timeout"
        finally: self.pending_syncs.pop(role_slug, None)

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
        log_file = Path(f"/data/storage/logs/job_{job_id}.log")
        
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

        # THE FIX: Split into two lines to avoid referencing before assignment
        key_filename = f"ansible_id_rsa_{uuid.uuid4().hex[:8]}"
        key_path = f"/data/security/{key_filename}"
        
        os.makedirs(os.path.dirname(key_path), exist_ok=True)
        with open(key_path, "w") as f: f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
        os.chmod(key_path, 0o600)

        try:
            h_git = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_GIT_REPOS_DIR", "/data/storage/git_repos")
            h_sec = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_SECURITY_DIR", "/data/security")
            h_svc = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_SERVICES_DIR", str(Path(h_git).parent / "services"))
            
            safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
            c_name = f"aac-runner-{safe_task_name}"
            
            await asyncio.create_subprocess_exec("docker", "rm", "-f", c_name, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            cmd = [
                "docker", "run", "-d", "--name", c_name, "--pull", "missing",
                "--label", f"iac_job_id={job_id}", "--label", f"iac_task_name={task_name}",
                "-e", f"IAC_JOB_ID={job_id}",
                "-v", f"{h_git}:/data/storage/git_repos", "-v", f"{h_svc}:/data/storage/services", "-v", f"{h_sec}/{key_filename}:/root/.ssh/id_rsa:ro",
                "-e", "ANSIBLE_HOST_KEY_CHECKING=False", "-e", "PYTHONUNBUFFERED=1", "-e", "ANSIBLE_NOCOLOR=1", "-e", "ANSIBLE_DEPRECATION_WARNINGS=0", "-e", "ANSIBLE_INTERPRETER_PYTHON=auto_silent",
                "-e", "ANSIBLE_ROLES_PATH=/data/storage/git_repos/config_engine/roles", "-e", "PYTHONPATH=/opt/aac-template-engine/scripts",
                self.ctx.get_secret("ansible_docker_image") or "registry.gitlab.int.fam-feser.de/aac-application-definitions/aac-template-engine:latest",
                "ansible-playbook", "-i", f"/data/storage/git_repos/inventory_state/{inventory_subpath}", f"/data/storage/git_repos/config_engine/{playbook_subpath}",
                "-u", "ansible-agent", "--diff" 
            ]
            if limit: cmd.extend(["--limit", limit])
            if extra_vars:
                for k, v in extra_vars.items(): cmd.extend(["-e", f"{k}={v}"])
            if str(self.ctx.get_secret("iac_auto_apply")).lower() != "true": cmd.append("--check")

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