import asyncio
from nicegui import ui, app as nicegui_app
from ui.layout import main_layout
from core.components.plugins.logic.models import ModuleManifest
from .api import iac_api_router, init_api
from .engine import DeploymentEngine
from .ui_dashboard import render_dashboard
from .ui_settings import render_settings_ui as modular_settings_ui
from .database import JobDatabase
from .models import IaCJob, Base
from core.components.database.logic.db_service import db_instance
from .config import IaCConfig

# ==========================================
# 1. MANIFEST
# ==========================================
manifest = ModuleManifest(
    id="lyndrix.plugin.iac_orchestrator",
    name="IaC Orchestrator",
    version="0.2.0",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch", 
    type="PLUGIN",
    ui_route="/iac",
    permissions={
        "subscribe": ["vault:ready_for_data", "iac:webhook_verified", "git:status_update", "db:connected"], 
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "git:sync", "git:commit_push"]
    }
)

# ==========================================
# 2. SHARED PLUGIN STATE
# ==========================================
plugin_state = {
    "auto_apply_enabled": False,
    "last_deployment": "Never",
    "latest_logs": [],
    "is_running": False,
    "active_tasks": {}
}

# ==========================================
# 3. SETTINGS INJECTION
# ==========================================
def render_settings_ui(ctx):
    """Glue function for settings injection."""
    modular_settings_ui(ctx, plugin_state)

# ==========================================
# 4. PLUGIN BOOT SEQUENCE
# ==========================================
async def setup(ctx):
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")
    
    config = IaCConfig(ctx)
    job_db = JobDatabase()
    engine = DeploymentEngine(ctx, plugin_state, job_db, config)
    
    # Initialize the API with the engine instance
    init_api(ctx, engine)
    
    # Attach router to NiceGUI directly (Fixes the previous ctx.app crash)
    nicegui_app.include_router(iac_api_router)
    
    def init_db_tables():
        if db_instance.is_connected and db_instance.engine:
            ctx.log.info("IaC Orchestrator: Verifying database tables...")
            try:
                Base.metadata.create_all(bind=db_instance.engine, checkfirst=True)
            except Exception as e:
                ctx.log.error(f"Failed to create tables: {e}")

    # Initial DB Check
    init_db_tables()

    # --- EVENT SUBSCRIPTIONS ---
    @ctx.subscribe('db:connected')
    async def on_db_connected(payload):
        init_db_tables()
    
    @ctx.subscribe('iac:webhook_verified')
    async def on_webhook(payload):
        asyncio.create_task(engine.run_pipeline(payload))
        
    # --- START RECONCILIATION ---
    async def run_reconciliation():
        await asyncio.sleep(2) # Give the DB a moment to wake up
        ctx.log.info("IaC Orchestrator: Checking for surviving Docker runners...")
        await engine.reconcile_orphaned_runners()

        # Resume any pending tasks in the database queue
        interrupted_jobs = job_db.get_jobs_by_status("RUNNING")
        for job in interrupted_jobs:
            remaining_services = job_db.get_pending_tasks(job.id)
            if remaining_services:
                asyncio.create_task(engine.resume_bulk_rollout(job.id, remaining_services))

    # Spawn reconciliation instantly in the background
    asyncio.create_task(run_reconciliation())
        
    # --- REGISTER UI ROUTE ---
    # Because this is inside setup(), it acts as a closure and permanently locks 
    # the 'ctx' and 'engine' instances to the route, surviving any hot-reloads.
    @ui.page('/iac')
    @main_layout('IaC Orchestrator')
    async def dashboard_page():
        await render_dashboard(ctx, plugin_state, engine, config)