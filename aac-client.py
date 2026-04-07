import httpx
from core.logger import get_logger

log = get_logger("IaC:AAC")

class AACClient:
    def __init__(self, ctx):
        self.ctx = ctx
        # Get config from Vault
        self.base_url = ctx.get_secret("aac_url") # e.g., https://awx.int.example.com
        self.token = ctx.get_secret("aac_token")

    async def trigger_job_template(self, template_id: int, extra_vars: dict = None):
        """Triggers an Ansible Job Template in AAC."""
        if not self.base_url or not self.token:
            log.error("AAC: Configuration missing in Vault (aac_url or aac_token).")
            return None

        url = f"{self.base_url}/api/v2/job_templates/{template_id}/launch/"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        payload = {"extra_vars": extra_vars or {}}

        async with httpx.AsyncClient(verify=False) as client: # verify=False for internal self-signed certs
            response = await client.post(url, headers=headers, json=payload)
            
            if response.status_code == 201:
                job_data = response.json()
                log.info(f"AAC: Job {job_data['job']} launched successfully for template {template_id}.")
                return job_data['job']
            else:
                log.error(f"AAC: Failed to launch job. Status: {response.status_code}, Error: {response.text}")
                return None

    async def cleanup_old_jobs(self, days_to_keep: int = 7):
        """Cleans up finished jobs older than X days to keep AAC database lean."""
        log.info(f"AAC: Initiating cleanup for jobs older than {days_to_keep} days...")
        # (Logic for DELETE /api/v2/jobs/?created__lt=...)
        # We'll implement this after we confirm the launch works.
        pass