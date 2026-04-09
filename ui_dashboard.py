import os
import yaml
import asyncio
from pathlib import Path
from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles

async def render_dashboard(ctx, state, engine):
    last_active_task_keys = set()
    
    # --- HELPER: Load Catalog ---
    def load_catalog():
        catalog_path = Path("/data/storage/git_repos/iac_controller/environments/global/02_service_catalog.yml")
        if catalog_path.exists():
            try:
                with open(catalog_path, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    return data.get("service_catalog", {}).get("services", [])
            except Exception as e: ctx.log.error(f"UI: Failed to parse catalog: {e}")
        return []

    # --- HELPER: Kill Switch ---
    async def abort_execution():
        ctx.log.warning("UI: ABORT SEQUENCE INITIATED BY USER.")
        ui.notify("Aborting execution and destroying runner containers...", type="negative")
        for task_name in state.get("active_tasks", {}).keys():
            safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
            try: await asyncio.create_subprocess_exec("docker", "rm", "-f", f"aac-runner-{safe_task_name}")
            except Exception: pass
            
        recent = engine.db.get_recent_jobs(1)
        if recent and recent[0]["status"] == "RUNNING":
            job_id = recent[0]["id"]
            engine.db.update_job(job_id, "ABORTED")
            engine.db.update_progress(job_id, progress=None, current_step="Aborted by User")
            
        state["is_running"] = False
        state["active_tasks"] = {}
        ui.notify("Execution Aborted Successfully.", type="info")

    # --- HELPER: Auto-Pull Repos ---
    async def sync_catalog_repos():
        ui.notify("Starting background sync for all catalog services...", type="info")
        from .engine import SyncAllServicesStage
        async def bg_sync():
            stage = SyncAllServicesStage()
            res = await stage.run(engine, {})
            ui.notify(res.message, type="positive" if res.success else "negative")
        asyncio.create_task(bg_sync())

    with ui.column().classes('w-full gap-6'):
        
        # --- HEADER & TRIGGERS ---
        with ui.row().classes('w-full justify-between items-center'):
            ui.label('GitOps Dashboard').classes(UIStyles.TITLE_H2)
            
            with ui.row().classes('gap-3'):
                ui.button('Test Connectivity', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "connectivity", "manual": True}), icon='cable', color='blue-6').props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                def open_single_service_dialog():
                    with ui.dialog() as dialog, ui.card().classes(f'{UIStyles.CARD_GLASS} min-w-[300px] border border-zinc-700'):
                        ui.label('Deploy Single Service').classes('text-lg font-bold text-slate-100 mb-2')
                        svc_name_input = ui.input('Service Name').classes('w-full').props('outlined dense')
                        svc_branch_input = ui.input('Branch', value='main').classes('w-full mt-2').props('outlined dense')
                        
                        def trigger_deploy():
                            if not svc_name_input.value:
                                ui.notify("Service Name is required", type="negative")
                                return
                            ctx.emit("iac:webhook_verified", {"pipeline_type": "single_service", "service_name": svc_name_input.value.strip(), "service_branch": svc_branch_input.value.strip(), "manual": True})
                            dialog.close()
                            
                        with ui.row().classes('w-full justify-end mt-4 gap-2'):
                            ui.button('Cancel', on_click=dialog.close, color='zinc-500').props('flat rounded')
                            ui.button('Trigger Deploy', on_click=trigger_deploy, color='indigo').props('unelevated rounded')
                    dialog.open()

                ui.button('Deploy Service', on_click=open_single_service_dialog, icon='rocket', color='indigo-500').props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                ui.button('Run Full Rollout', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "manual": True}), icon='rocket_launch', color='emerald').props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                ui.button('ABORT', on_click=abort_execution, icon='dangerous', color='red-6').props('unelevated rounded').bind_visibility_from(state, 'is_running')

        # --- SUBNAVIGATION TABS ---
        with ui.tabs().classes('w-full') as tabs:
            overview_tab = ui.tab('Overview', icon='dashboard')
            catalog_tab = ui.tab('Service Catalog', icon='apps')
            history_tab = ui.tab('History & Logs', icon='history')

        with ui.tab_panels(tabs, value=overview_tab).classes('w-full bg-transparent p-0'):
            
            # ==========================================
            # TAB 1: OVERVIEW 
            # ==========================================
            with ui.tab_panel(overview_tab).classes('gap-6'):
                
                with ui.row().classes('w-full gap-6 flex-col md:flex-row items-stretch'):
                    with ui.card().classes(f'{UIStyles.CARD_GLASS} flex-1'):
                        ui.label('Deployment Engine').classes('text-lg font-bold mb-2 text-indigo-500')
                        ui.label().bind_text_from(state, 'is_running', backward=lambda x: "Running..." if x else "Status: Idle")
                    
                    with ui.card().classes(f'{UIStyles.CARD_GLASS} flex-1'):
                        ui.label('Last Result').classes('text-lg font-bold mb-2 text-rose-500')
                        ui.label().bind_text_from(state, 'last_deployment')
                
                with ui.card().classes(f'{UIStyles.CARD_GLASS} w-full').bind_visibility_from(state, 'is_running'):
                    ui.label('Active Deployment Progress').classes(f'{UIStyles.TITLE_H3} text-indigo-400 mb-2')
                    progress_bar = ui.linear_progress(value=0, show_value=False).props('size=15px color=indigo rounded')
                    with ui.row().classes('w-full justify-between mt-1'):
                        step_label = ui.label('Initializing...').classes(f'{UIStyles.TEXT_MUTED} font-mono')
                        pct_label = ui.label('0%').classes(f'{UIStyles.TEXT_MUTED} font-bold')

                with ui.column().classes('w-full mt-4'):
                    ui.label('Active Docker Runners').classes(UIStyles.TITLE_H3)
                    runner_container = ui.row().classes('w-full gap-4 items-stretch min-h-[100px]')
                    
                    with ui.dialog() as runner_log_dialog, ui.card().classes(f'{UIStyles.CARD_GLASS} w-full max-w-4xl h-[70vh] flex flex-col p-0'):
                        with ui.row().classes('w-full p-4 items-center justify-between border-b border-zinc-800 bg-zinc-950'):
                            runner_dialog_title = ui.label("Runner Logs").classes('text-lg font-bold text-indigo-400')
                            ui.button(icon='close', on_click=runner_log_dialog.close, color='zinc-600').props('flat round dense')
                        with ui.scroll_area().classes('w-full flex-grow bg-black p-4'):
                            runner_log_content = ui.label().classes('whitespace-pre-wrap font-mono text-xs text-green-400 break-words')

                    def open_runner_logs(task_name):
                        runner_dialog_title.set_text(f"Live Logs: {task_name}")
                        logs = state.get("active_tasks", {}).get(task_name, {}).get("logs", [])
                        runner_log_content.set_text('\n'.join(logs) if logs else "Waiting for output...")
                        runner_log_dialog.open()

            # ==========================================
            # TAB 2: SERVICE CATALOG GRID
            # ==========================================
            with ui.tab_panel(catalog_tab).classes('gap-4'):
                with ui.row().classes('w-full justify-between items-center mb-4'):
                    ui.label('Declared Infrastructure Services').classes(UIStyles.TITLE_H3)
                    ui.button('Sync Repositories', on_click=sync_catalog_repos, icon='cloud_download', color='secondary').props('unelevated rounded size=sm')

                catalog_services = load_catalog()
                
                # --- SERVICE HISTORY DIALOG ---
                with ui.dialog() as svc_history_dialog, ui.card().classes(f'{UIStyles.CARD_GLASS} w-full max-w-5xl p-0'):
                    with ui.row().classes('w-full p-4 items-center justify-between border-b border-zinc-800'):
                        svc_history_title = ui.label("Service History").classes('text-lg font-bold text-indigo-400')
                        ui.button(icon='close', on_click=svc_history_dialog.close, color='zinc-600').props('flat round dense')
                    
                    with ui.column().classes('w-full p-4'):
                        svc_history_table = ui.table(
                            columns=[
                                {'name': 'id', 'label': 'Job ID', 'field': 'id', 'align': 'left'},
                                {'name': 'pipeline_type', 'label': 'Type', 'field': 'pipeline_type', 'align': 'left'},
                                {'name': 'start_time', 'label': 'Started', 'field': 'start_time', 'align': 'left'},
                                {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                                {'name': 'action', 'label': 'Action', 'field': 'action', 'align': 'center'}
                            ], rows=[], row_key='id'
                        ).classes('w-full bg-transparent shadow-none')
                        svc_history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{ props.value }}</q-badge></q-td>''')
                        
                        # Bind the existing open_log_popup function to this table too
                        svc_history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" color="zinc-700" text-color="white" icon="folder_open" label="View Log" @click="() => $parent.$emit('view_logs_svc', props.row)" /></q-td>''')

                def open_service_history(svc_name):
                    svc_history_title.set_text(f"Deployment History: {svc_name}")
                    # Fetch history specific to this service
                    history_rows = engine.db.get_service_history(svc_name)
                    svc_history_table.rows = history_rows
                    svc_history_table.update()
                    svc_history_dialog.open()

                if not catalog_services:
                    ui.label("No services found. Ensure 'iac_controller' is synced and YAML is valid.").classes(f'{UIStyles.TEXT_MUTED} italic')
                else:
                    with ui.grid(columns='repeat(auto-fill, minmax(320px, 1fr))').classes('w-full gap-4'):
                        for svc in catalog_services:
                            svc_name = svc.get("name", "Unknown")
                            repo_name = svc.get("repository_name", svc_name)
                            branch = svc.get("branch", "main")
                            
                            # Extract deployment node/host info if available in your YAML schema
                            # (Adjust these keys if your YAML structure differs)
                            target_node = svc.get("target_environment", svc.get("host", "Auto-Assigned"))
                            deploy_type = svc.get("deploy_type", "Docker Compose")
                            
                            with ui.card().classes(f'{UIStyles.CARD_BASE} flex flex-col hover:border-indigo-500 transition-colors'):
                                with ui.row().classes('w-full justify-between items-start'):
                                    with ui.column().classes('gap-0'):
                                        ui.label(svc_name).classes('text-md font-bold truncate')
                                        ui.label(f"Repo: {repo_name}").classes(f'{UIStyles.TEXT_MUTED} text-[10px] truncate')
                                    
                                    # Visual Indicator for Docker Compose
                                    if "compose" in deploy_type.lower():
                                        ui.icon('view_in_ar', color='indigo-400').classes('text-xl').tooltip("Docker Compose")
                                    else:
                                        ui.icon('settings_applications', color='slate-400').classes('text-xl').tooltip(deploy_type)

                                ui.separator().classes('my-2 opacity-20')
                                
                                # Node & Branch Info
                                with ui.row().classes('w-full justify-between items-center'):
                                    with ui.row().classes('items-center gap-1'):
                                        ui.icon('dns', size='12px').classes('text-slate-400')
                                        ui.label(target_node).classes('text-xs text-slate-500 font-mono')
                                    
                                    with ui.row().classes('items-center gap-1'):
                                        ui.icon('call_split', size='12px').classes('text-slate-400')
                                        ui.label(branch).classes('text-xs text-slate-500 font-mono')
                                
                                ui.separator().classes('mt-auto mb-3 opacity-20')
                                
                                # Action Buttons
                                with ui.row().classes('w-full justify-between items-center gap-2'):
                                    ui.button(icon='history', color='zinc-600', on_click=lambda n=svc_name: open_service_history(n)).props('flat round size=sm').tooltip("View Deployment History")
                                    
                                    ui.button('Deploy', icon='rocket', color='indigo-500', on_click=lambda n=svc_name, b=branch: ctx.emit("iac:webhook_verified", {"pipeline_type": "single_service", "service_name": n, "service_branch": b, "manual": True})).props('unelevated rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

            # ==========================================
            # TAB 3: GLOBAL HISTORY & FILE LOGS
            # ==========================================
            with ui.tab_panel(history_tab).classes('gap-4 relative'):
                ui.button(icon='refresh', on_click=lambda: refresh_history()).props('flat round color=zinc-500').classes('absolute right-4 top-4 z-10')

                with ui.dialog() as log_dialog:
                    with ui.card().classes('w-full max-w-6xl h-[85vh] flex flex-col no-wrap bg-zinc-900 border border-zinc-700 p-0'):
                        with ui.row().classes('w-full p-4 items-center justify-between border-b border-zinc-800 bg-zinc-950'):
                            dialog_title = ui.label("Job Logs").classes('text-lg font-bold text-slate-200')
                            with ui.row().classes('gap-2 items-center'):
                                log_size_label = ui.label("").classes('text-xs text-zinc-500')
                                ui.button(icon='close', on_click=log_dialog.close, color='zinc-600').props('flat round dense')
                        with ui.scroll_area().classes('w-full flex-grow bg-black p-4'):
                            log_content = ui.label().classes('whitespace-pre-wrap font-mono text-xs text-green-400 break-words')
                        
                def open_log_popup(e):
                    job_data = e.args
                    job_id = job_data.get('id')
                    dialog_title.set_text(f"Job #{job_id} Logs ({job_data.get('status')})")
                    
                    log_path = Path(f"/data/storage/logs/job_{job_id}.log")
                    if log_path.exists():
                        size_mb = log_path.stat().st_size / (1024 * 1024)
                        log_size_label.set_text(f"File Size: {size_mb:.2f} MB")
                        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                            f.seek(0, 2)
                            file_size = f.tell()
                            read_size = min(file_size, 100000)
                            f.seek(file_size - read_size)
                            content = f.read()
                        prefix = "[TRUNCATED] Showing last 100kb of logs...\n\n" if file_size > 100000 else ""
                        log_content.set_text(prefix + content)
                    else:
                        log_size_label.set_text("File missing")
                        log_content.set_text(f"No log file found on disk at:\n{log_path}\n\n(This might be a legacy job stored only in the DB).")
                    
                    log_dialog.open()

                # Bind the service history table's "View Log" button to the same popup function
                svc_history_table.on('view_logs_svc', open_log_popup)

                columns = [
                    {'name': 'id', 'label': 'ID', 'field': 'id', 'sortable': True, 'align': 'left'},
                    {'name': 'pipeline_type', 'label': 'Type', 'field': 'pipeline_type', 'align': 'left'},
                    {'name': 'progress', 'label': 'Progress', 'field': 'progress', 'align': 'left'},
                    {'name': 'start_time', 'label': 'Started', 'field': 'start_time', 'align': 'left'},
                    {'name': 'end_time', 'label': 'Finished', 'field': 'end_time', 'align': 'left'},
                    {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                    {'name': 'action', 'label': 'Disk Logs', 'field': 'action', 'align': 'center'}
                ]

                history_table = ui.table(columns=columns, rows=engine.db.get_recent_jobs(), row_key='id').classes(f'{UIStyles.CARD_BASE} w-full mt-12')
                history_table.add_slot('body-cell-progress', '''<q-td :props="props"><q-linear-progress :value="(props.value || 0) / 100" color="indigo" class="mt-1" /><div class="text-xs mt-1 text-center text-slate-400">{{ props.value || 0 }}%</div></q-td>''')
                history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{ props.value }}</q-badge></q-td>''')
                history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" color="zinc-700" text-color="white" icon="folder_open" label="Read File" @click="() => $parent.$emit('view_logs', props.row)" /></q-td>''')
                history_table.on('view_logs', open_log_popup)

        def refresh_history():
            history_table.rows = engine.db.get_recent_jobs()
            history_table.update()

        history_refresh_counter = [0]
        
        def update_ui_loop():
            is_running = state.get("is_running", False)
            
            if is_running:
                recent = engine.db.get_recent_jobs(1)
                if recent:
                    job = recent[0]
                    p_val = job.get("progress", 0) or 0
                    progress_bar.set_value(p_val / 100.0)
                    pct_label.set_text(f"{p_val}%")
                    step_label.set_text(job.get("current_step", "Processing..."))
            
            nonlocal last_active_task_keys
            active_tasks = state.get("active_tasks", {})
            live_tasks = {k: v for k, v in active_tasks.items() if v["status"] in ["pulling_image", "running_ansible"]}
            current_keys = set(live_tasks.keys())

            if current_keys != last_active_task_keys:
                runner_container.clear()
                last_active_task_keys = current_keys
                with runner_container:
                    if not live_tasks:
                        ui.label("No active runners. Pool is idle.").classes(f'{UIStyles.TEXT_MUTED} italic p-4')
                    else:
                        for task_name, task_data in live_tasks.items():
                            status = task_data["status"]
                            border_color = "border-indigo-500" if status == "running_ansible" else "border-amber-500"
                            bg_color = "bg-indigo-500/10" if status == "running_ansible" else "bg-amber-500/10"
                            icon_name = "terminal" if status == "running_ansible" else "cloud_download"
                            
                            with ui.card().classes(f'{UIStyles.CARD_GLASS} flex-1 min-w-[200px] border {border_color} {bg_color} cursor-pointer transition-all hover:brightness-125').on('click', lambda t=task_name: open_runner_logs(t)):
                                with ui.row().classes('w-full items-center gap-2'):
                                    ui.icon(icon_name).classes('text-xl')
                                    ui.label(task_name).classes('font-bold truncate overflow-hidden')
                                
                                ui.label("Running..." if status == "running_ansible" else "Preparing...").classes('text-xs opacity-75 mt-2')
                                ui.spinner('dots', size='1em').classes('absolute bottom-2 right-2 opacity-50')
            
            history_refresh_counter[0] += 1
            threshold = 5 if is_running else 20
            if history_refresh_counter[0] >= threshold:
                refresh_history()
                history_refresh_counter[0] = 0

            if runner_log_dialog.value: 
                current_task = runner_dialog_title.text.replace("Live Logs: ", "")
                task_logs = state.get("active_tasks", {}).get(current_task, {}).get("logs", [])
                runner_log_content.set_text('\n'.join(task_logs))
        
        ui.timer(0.5, update_ui_loop)