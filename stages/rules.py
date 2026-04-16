import asyncio
import fnmatch
import yaml
from pathlib import Path
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult
from .ansible import AnsiblePlaybookStage, AsyncBulkRolloutStage

log = get_logger("IaC:Engine:Rules")

class DynamicRuleExecutionStage(BaseStage):
    def __init__(self, pipeline_type: str):
        super().__init__("Dynamic Rule-Based Execution")
        self.pipeline_type = pipeline_type

    async def _get_changed_files(self, engine) -> list[str]:
        inventory_repo_path = engine.base_git_dir / "inventory_state"
        cmd = ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
        log.info(f"Checking for changes in: {inventory_repo_path}")
        process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=inventory_repo_path)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            err_msg = stderr.decode()
            log.error(f"Failed to get git diff from inventory_state: {err_msg}")
            if "fatal: bad object HEAD" in err_msg: return []
            raise RuntimeError(f"Git diff failed: {err_msg}")
        changed = stdout.decode().strip()
        return [f for f in changed.split('\n') if f] if changed else []

    def _extract_changed_services(self, changed_files: list[str]) -> list[str]:
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
            try: rules_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e: return StageResult(False, f"Error parsing pipeline_rules.yml: {e}")
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
            if not changed_files: return StageResult(True, "No changes in git diff, no actions required.")
            changed_services = self._extract_changed_services(changed_files)
            if changed_services: log.info(f"Differential deployment triggered for: {changed_services}")
            for rule in rules_config.get("rules", []):
                if any(fnmatch.fnmatch(f, p) for f in changed_files for p in rule.get("paths", [])):
                    actions = rule.get("actions", [])
                    break
            if not actions: actions = rules_config.get("default", [])
        if not actions: return StageResult(True, "No actions performed.")
        for action in actions:
            target = action.get("playbook", "")
            if "cd_rollout_service.yml" in target:
                stage = AsyncBulkRolloutStage(inventory_path=action.get("inventory"), limit=action.get("limit"), target_services=changed_services if changed_services else None)
            else:
                stage = AnsiblePlaybookStage(name_override=action.get("name"), playbook_path=target, inventory_path=action.get("inventory"), limit=action.get("limit"))
            res = await stage.run(engine, context)
            if not res.success: return res
        return StageResult(True, "Dynamic rule-based execution completed.")