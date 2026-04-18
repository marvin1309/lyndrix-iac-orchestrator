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
    version="0.2.2",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch", 
    type="PLUGIN",
    ui_route="/iac",
    permissions={
        "subscribe": ["vault:ready_for_data", "iac:webhook_verified", "git:status_update", "db:connected"], 
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "git:sync", "git:commit_push", "system:notify", "user:notify"]
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

# --- NEW: WIDGET INJECTION ---
def render_dashboard_widget(ctx):
    """Renders a status widget for the main dashboard."""
    with ui.column().classes('gap-2 w-full'):
        ui.label("IaC Orchestrator").classes("text-base font-bold text-slate-200")
        ui.separator().classes('my-1 opacity-20')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Last Deployment:").classes("text-xs text-slate-400")
            ui.label().classes("text-xs font-mono").bind_text_from(plugin_state, 'last_deployment')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Pipeline Active:").classes("text-xs text-slate-400")
            with ui.row().classes('items-center gap-2'):
                ui.spinner('dots', color='indigo').bind_visibility_from(plugin_state, 'is_running')
                ui.label().classes("text-xs font-mono").bind_text_from(plugin_state, 'is_running', lambda v: "Yes" if v else "No")

# ==========================================
# 4. PLUGIN BOOT SEQUENCE
# ==========================================
async def setup(ctx):
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")
    
    config = IaCConfig(ctx)
    job_db = JobDatabase()
    engine = DeploymentEngine(ctx, plugin_state, job_db, config)
    
    # Restore the Auto-Apply setting from Vault/Config on boot
    plugin_state["auto_apply_enabled"] = config.auto_apply

    # Initialize the API with the engine instance
    init_api(ctx, engine)
    
    # Attach router to NiceGUI directly (Fixes the previous ctx.app crash)
    nicegui_app.include_router(iac_api_router)
    
    def init_db_tables():
        if db_instance.is_connected and db_instance.engine:
            ctx.log.info("IaC Orchestrator: Verifying database tables...")
            try:
                Base.metadata.create_all(bind=db_instance.engine, checkfirst=True)
                
                # Restore the last deployment status for the UI on boot
                recent = job_db.get_recent_jobs(1)
                if recent:
                    plugin_state["last_deployment"] = recent[0]["status"]
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
            elif not any(t.get("job_id") == job.id for t in engine.state.get("active_tasks", {}).values()):
                # If the job has no pending tasks AND no orphaned runners reattached to it,
                # the container was completely destroyed. We must close it out so it doesn't hang.
                ctx.log.warning(f"IaC Orchestrator: Job #{job.id} is RUNNING but has no active runners. Marking as FAILED.")
                job_db.update_job(job.id, "FAILED")
                job_db.update_progress(job.id, progress=None, current_step="System Restart (Aborted)")
                plugin_state["last_deployment"] = "FAILED"

    # Spawn reconciliation instantly in the background
    asyncio.create_task(run_reconciliation())
        
    # --- REGISTER UI ROUTE ---
    # Because this is inside setup(), it acts as a closure and permanently locks 
    # the 'ctx' and 'engine' instances to the route, surviving any hot-reloads.
    @ui.page('/iac')
    @main_layout('IaC Orchestrator')
    async def dashboard_page():
        await render_dashboard(ctx, plugin_state, engine, config)