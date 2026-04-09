import asyncio
import json
import yaml
import fnmatch
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
        status = await engine.execute_git_commit_push(self.role_slug, self.message)
        
        context[f"{self.role_slug}_commit_status"] = status
        
        if status in ["pushed", "committed_locally", "no_changes"]:
            return StageResult(True, f"Git status for {self.role_slug}: {status}")
        return StageResult(False, f"Push failed for {self.role_slug} with status: {status}")

class AnsiblePlaybookStage(BaseStage):
    def __init__(self, playbook_path: str, inventory_path: str, limit: str = None, name_override: str = None, extra_vars: dict = None):
        display_name = name_override or f"Ansible: {playbook_path}"
        super().__init__(display_name)
        self.playbook_path = playbook_path
        self.inventory_path = inventory_path
        self.limit = limit
        self.extra_vars = extra_vars or {}

    async def run(self, engine, context: dict) -> StageResult:
        # THE FIX: Unpack the new stats dictionary from the execution
        success, stats = await engine.execute_ansible_docker(
            self.playbook_path, 
            self.inventory_path, 
            limit=self.limit,
            extra_vars=self.extra_vars
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


class AsyncBulkRolloutStage(BaseStage):
    def __init__(self, inventory_path: str, limit: str = "all"):
        super().__init__(f"Async Bulk Rollout (Limit: {limit})")
        self.inventory_path = inventory_path
        self.limit = limit

    async def run(self, engine, context: dict) -> StageResult:
        catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
        if not catalog_file.exists():
            return StageResult(False, f"Source service_catalog.yml missing at {catalog_file}")

        try:
            with open(catalog_file, 'r') as f:
                catalog_data = yaml.safe_load(f) or {}
                services = catalog_data.get("service_catalog", {}).get("services", [])
        except Exception as e:
            return StageResult(False, f"Raw catalog parse failed: {e}")

        svc_names = [svc.get("name") for svc in services if svc.get("name")]
        if not svc_names:
            return StageResult(False, "No services found in raw catalog to deploy.")

        log.info(f"Initiating Async Global Rollout for {len(svc_names)} services (Limit: {self.limit})...")
        
        sem = asyncio.Semaphore(5)
        
        # THE FIX: Create a report dictionary to track exactly what happened
        report = {}
        failed_services = []

        async def bounded_deploy(svc_name):
            async with sem:
                sanitized_name = str(svc_name).replace("-", "_")
                svc_group = f"service_{sanitized_name}"
                effective_limit = f"{self.limit}:&{svc_group}" if self.limit and self.limit != "all" else svc_group
                
                stage = AnsiblePlaybookStage(
                    name_override=f"Deploy: {svc_name}",
                    playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml",
                    inventory_path=self.inventory_path,
                    limit=effective_limit,
                    extra_vars={
                        "target_service": svc_name,
                        "target_group": effective_limit,
                        "LOCAL_SERVICES_DIR": "/data/storage/services"
                    }
                )
                res = await stage.run(engine, context)
                
                # Save the parsed stats for our summary report
                report[svc_name] = {
                    "success": res.success,
                    "successful_hosts": res.data.get("successful_hosts", 0),
                    "failed_hosts": res.data.get("failed_hosts", 0)
                }
                
                if not res.success:
                    failed_services.append(svc_name)

        # Launch all playbooks asynchronously
        tasks = [bounded_deploy(svc) for svc in svc_names]
        await asyncio.gather(*tasks)

        # ======================================================================
        # THE SUMMARY PRINTER
        # ======================================================================
        summary = ["\n" + "="*60, "🚀 BULK ROLLOUT DEPLOYMENT SUMMARY", "="*60]
        total_success_hosts = 0
        total_fail_hosts = 0

        for svc, stats in sorted(report.items()):
            status_icon = "✅" if stats['success'] else "❌"
            succ_h = stats['successful_hosts']
            fail_h = stats['failed_hosts']
            
            host_str = f"{succ_h} hosts"
            if fail_h > 0:
                host_str += f" | FAILED on {fail_h} hosts"
                
            summary.append(f"{status_icon} {svc.ljust(35)} : {host_str}")
            total_success_hosts += succ_h
            total_fail_hosts += fail_h

        summary.append("="*60)
        summary.append(f"📊 TOTALS: {len(svc_names)} Services | {total_success_hosts} Successful Host Deployments | {total_fail_hosts} Host Failures")
        summary.append("="*60 + "\n")

        # Push the summary out to the logs and UI
        for line in summary:
            log.info(line)
            engine.state["latest_logs"].append(line)

        if failed_services:
            return StageResult(False, f"Bulk rollout had failures. See summary.", data=report)
        
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
        return changed.split('\n') if changed else []

    async def run(self, engine, context: dict) -> StageResult:
        rules_file_path = engine.base_git_dir / "config_engine" / "pipeline_rules.yml"
        
        if not rules_file_path.exists():
            log.warning("pipeline_rules.yml not found. Falling back to default hardcoded behavior.")
            fallback_stages = engine.get_default_ansible_stages(self.pipeline_type)
            for stage in fallback_stages:
                result = await stage.run(engine, context)
                if not result.success: return result
            return StageResult(True, "Fallback pipeline executed successfully.")

        with open(rules_file_path, 'r') as f:
            try:
                rules_config = yaml.safe_load(f)
                if not rules_config: return StageResult(False, "pipeline_rules.yml is empty or invalid.")
            except yaml.YAMLError as e:
                return StageResult(False, f"Error parsing pipeline_rules.yml: {e}")

        actions = []
        if self.pipeline_type == "connectivity":
            actions = rules_config.get("connectivity_test", [])
            log.info("Executing 'connectivity_test' action from rules file.")
        else:
            commit_status = context.get("inventory_state_commit_status")
            if commit_status == "no_changes":
                log.info("No state changes generated. Bypassing git diff and Ansible execution.")
                return StageResult(True, "No changes in inventory_state, no actions required.")

            changed_files = await self._get_changed_files(engine)
            if not changed_files: return StageResult(True, "No changes in git diff, no actions required.")
            log.info(f"Detected changes in inventory_state: {changed_files}")
            
            for rule in rules_config.get("rules", []):
                if any(fnmatch.fnmatch(f, p) for f in changed_files for p in rule.get("paths", [])):
                    log.info(f"Matched rule '{rule.get('name')}' for changes.")
                    actions = rule.get("actions", [])
                    break
            
            if not actions:
                log.info("No specific rule matched. Using 'default' rollout action.")
                actions = rules_config.get("default", [])
            
        if not actions: return StageResult(True, "No relevant changes or rules found, no actions performed.")

        for action in actions:
            playbook_target = action.get("playbook", "")
            if "cd_rollout_service.yml" in playbook_target:
                log.info(f"Intercepted monolithic rule '{action.get('name')}'. Upgrading to AsyncBulkRolloutStage.")
                stage = AsyncBulkRolloutStage(
                    inventory_path=action.get("inventory"),
                    limit=action.get("limit")
                )
            else:
                stage = AnsiblePlaybookStage(
                    name_override=action.get("name"), 
                    playbook_path=playbook_target, 
                    inventory_path=action.get("inventory"), 
                    limit=action.get("limit")
                )
                
            result = await stage.run(engine, context)
            if not result.success: return result

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
        stages = []
        if pipeline_type == "rollout":
            stages.append(AsyncBulkRolloutStage(
                inventory_path="global/ansible/inventory.yml",
                limit="all"
            ))
        else:
            stages.append(AnsiblePlaybookStage(
                name_override="CONNECTIVITY TEST",
                playbook_path="playbooks/cd_playbooks/cd_test_inventory.yml", 
                inventory_path="global/ansible/inventory.yml",
                limit="docker-hydra"
            ))
        return stages

    async def run_pipeline(self, payload: dict):
        if self.state.get("is_running"):
            log.warning("ENGINE: Execution already in progress.")
            return

        self.state["is_running"] = True
        self.state["latest_logs"] = ["[SYSTEM] Pipeline Started"]

        object_kind = payload.get("object_kind")
        if not payload.get("pipeline_type") and object_kind == "push":
            project_name = payload.get("project", {}).get("name")
            ref = payload.get("ref", "")
            if project_name and ref.startswith("refs/heads/"):
                payload["pipeline_type"] = "single_service"
                payload["service_name"] = project_name
                payload["service_branch"] = ref.replace("refs/heads/", "")
                log.info(f"ENGINE: Auto-detected single service push for {project_name} on {payload['service_branch']}")

        pipeline_type = payload.get("pipeline_type", "connectivity")
        current_job_id = self.db.create_job(pipeline_type)
        self.state["latest_logs"].append(f"[SYSTEM] Job #{current_job_id} registered in database.")
        
        bridge = PipelineLogBridge(self.state["latest_logs"])
        target_logger = logging.getLogger("IaC")
        target_logger.addHandler(bridge)

        context = {"payload": payload}
        
        pipeline = [
            SyncRepoStage("iac_controller"),
            SyncRepoStage("inventory_state"),
            SyncRepoStage("config_engine"),  
            NativeGenerateStage(),
            CommitPushStage("inventory_state", "ci: automated state update"),
        ]
        
        if pipeline_type == "single_service":
            svc_name = payload.get("service_name")
            svc_branch = payload.get("service_branch", "main")
            
            if not svc_name:
                raise RuntimeError("Service Name missing for single_service pipeline type.")
                
            target_group = "all"
            if svc_branch == "dev" or str(svc_branch).endswith("-dev"):
                target_group = "stage_dev"
            elif svc_branch == "test":
                target_group = "stage_test"
            else:
                sanitized_name = str(svc_name).replace("-", "_")
                target_group = f"service_{sanitized_name}"
                
            pipeline.append(CloneServiceRepoStage(svc_name, svc_branch, payload))
            pipeline.append(AnsiblePlaybookStage(
                name_override=f"Single Service: {svc_name} ({svc_branch})",
                playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml",
                inventory_path="global/ansible/inventory.yml",
                limit=target_group,
                extra_vars={
                    "SERVICE_BRANCH": svc_branch,
                    "target_service": svc_name,
                    "target_group": target_group,
                    "LOCAL_SERVICES_DIR": "/data/storage/services"
                }
            ))
            
        elif pipeline_type == "rollout":
            pipeline.append(SyncAllServicesStage())
            pipeline.append(DynamicRuleExecutionStage(pipeline_type))
            
        else:
            pipeline.append(DynamicRuleExecutionStage(pipeline_type))

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
            
            self.db.update_job(
                job_id=current_job_id, 
                status=self.state["last_deployment"], 
                logs_list=self.state["latest_logs"]
            )

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

    async def execute_git_commit_push(self, role_slug: str, message: str) -> str:
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[role_slug] = future
        self.ctx.emit("git:commit_push", {"repo_id": role_slug, "message": message, "is_local": False})
        try:
            result = await asyncio.wait_for(future, timeout=120.0)
            return result.get("status", "failed")
        except asyncio.TimeoutError:
            return "timeout"
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

    async def execute_ansible_docker(self, playbook_subpath: str, inventory_subpath: str, limit: str = None, extra_vars: dict = None):
        import shutil
        import os
        import asyncio
        import uuid # <--- ADD THIS IMPORT

        if not shutil.which("docker"):
            self.state["latest_logs"].append("!!! [FATAL] 'docker' CLI not found on host.")
            return False, {"successful_hosts": 0, "failed_hosts": 0}

        reg_url = self.ctx.get_secret("ansible_registry_url")
        reg_user = self.ctx.get_secret("ansible_registry_user")
        reg_token = self.ctx.get_secret("ansible_registry_token")

        if reg_url and reg_user and reg_token:
            login_cmd = ["docker", "login", reg_url, "-u", reg_user, "--password-stdin"]
            login_proc = await asyncio.create_subprocess_exec(
                *login_cmd, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )
            await login_proc.communicate(input=reg_token.encode('utf-8'))

        ssh_key = self.ctx.get_secret("ansible_ssh_key")
        if not ssh_key:
            self.state["latest_logs"].append("!!! [ERROR] No SSH Key found in Vault.")
            return False, {"successful_hosts": 0, "failed_hosts": 0}

        # THE FIX: Create a unique SSH key file for this specific async task
        task_id = uuid.uuid4().hex[:8]
        key_filename = f"ansible_id_rsa_{task_id}"
        key_path = f"/data/security/{key_filename}"
        
        os.makedirs(os.path.dirname(key_path), exist_ok=True)
        with open(key_path, "w") as f:
            f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
        os.chmod(key_path, 0o600)

        successful_hosts = 0
        failed_hosts = 0

        try:
            container_git_dir = "/data/storage/git_repos"
            container_services_dir = "/data/storage/services"
            host_git_dir = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_GIT_REPOS_DIR")
            host_sec_dir = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_SECURITY_DIR")
            host_services_dir = os.environ.get("PLUGIN_IAC_ORCHESTRATOR_SERVICES_DIR")
            
            if not host_services_dir and host_git_dir:
                host_services_dir = str(Path(host_git_dir).parent / "services")
            elif not host_services_dir:
                host_services_dir = "/data/storage/services"
            
            engine_scripts_path = "/opt/aac-template-engine/scripts"
            
            default_img = "registry.gitlab.int.fam-feser.de/aac-application-definitions/aac-template-engine:latest"
            image_name = self.ctx.get_secret("ansible_docker_image") or default_img
                
            full_playbook = f"{container_git_dir}/config_engine/{playbook_subpath}"
            full_inventory = f"{container_git_dir}/inventory_state/{inventory_subpath}"
            is_auto_apply = str(self.ctx.get_secret("iac_auto_apply")).lower() == "true"
            
            cmd = [
                "docker", "run", "--rm",
                "--pull", "missing",
                "-v", f"{host_git_dir}:{container_git_dir}",
                "-v", f"{host_services_dir}:{container_services_dir}",
                # THE FIX: Mount the unique key file specifically
                "-v", f"{host_sec_dir}/{key_filename}:/root/.ssh/id_rsa:ro",
                "-e", "ANSIBLE_HOST_KEY_CHECKING=False",
                "-e", "PYTHONUNBUFFERED=1",
                "-e", "ANSIBLE_NOCOLOR=1",
                "-e", "ANSIBLE_DEPRECATION_WARNINGS=0",
                "-e", "ANSIBLE_INTERPRETER_PYTHON=auto_silent",
                "-e", f"ANSIBLE_ROLES_PATH={container_git_dir}/config_engine/roles",
                "-e", f"PYTHONPATH={engine_scripts_path}",
                image_name,
                "ansible-playbook", "-i", full_inventory, full_playbook,
                "-u", "ansible-agent",
                "--diff" 
            ]

            if limit: 
                cmd.extend(["--limit", limit])
            if extra_vars:
                for k, v in extra_vars.items():
                    cmd.extend(["-e", f"{k}={v}"])
            if not is_auto_apply: 
                cmd.append("--check")

            self.state["latest_logs"].append(f"[SYSTEM] Executing: {playbook_subpath} (Limit: {limit or 'None'})")

            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
            )

            while True:
                line = await process.stdout.readline()
                if not line: 
                    break
                decoded_line = line.decode('utf-8', errors='replace').rstrip()
                if decoded_line:
                    self.state["latest_logs"].append(f"[Ansible] {decoded_line}")
                    
                    if "ok=" in decoded_line and "failed=" in decoded_line and ":" in decoded_line:
                        try:
                            stats_part = decoded_line.split(":")[1]
                            failed_count = int(stats_part.split("failed=")[1].split()[0])
                            unreachable_count = int(stats_part.split("unreachable=")[1].split()[0])
                            
                            if failed_count > 0 or unreachable_count > 0:
                                failed_hosts += 1
                            else:
                                successful_hosts += 1
                        except Exception:
                            pass

            await process.wait()
            success = process.returncode == 0
            
            return success, {"successful_hosts": successful_hosts, "failed_hosts": failed_hosts}

        except Exception as e:
            self.state["latest_logs"].append(f"!!! [ENGINE ERROR] Subprocess failed: {str(e)}")
            return False, {"successful_hosts": successful_hosts, "failed_hosts": failed_hosts}

        finally:
            if os.path.exists(key_path):
                try:
                    os.remove(key_path)
                except Exception as cleanup_err:
                    self.state["latest_logs"].append(f"!!! [WARNING] Failed to cleanup SSH key: {str(cleanup_err)}")