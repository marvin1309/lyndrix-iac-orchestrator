import os
import yaml
import asyncio
from pathlib import Path
from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles

DOCKER_ICON = 'svg:M6.1,10L0,10.1V13h6.1V10z M13.1,10H7v3h6.1V10z M20.1,10H14v3h6.1V10z M13.1,3H7v3h6.1V3z'

async def render_dashboard(ctx, state, engine):
    active_job_cards = {} 

    def load_catalog():
        catalog_path = Path("/data/storage/git_repos/iac_controller/environments/global/02_service_catalog.yml")
        if catalog_path.exists():
            try:
                with open(catalog_path, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    return data.get("service_catalog", {}).get("services", [])
            except Exception as e: ctx.log.error(f"UI: Failed to parse catalog: {e}")
        return []

    def load_assignments():
        assignments = []
        base_dir = Path("/data/storage/git_repos/iac_controller/environments")
        sites_dir = base_dir / "sites"
        profiles_file = base_dir / "global" / "03_profiles.yml"
        
        # 1. Load Profiles
        profiles = {}
        if profiles_file.exists():
            try:
                with open(profiles_file, 'r') as f:
                    p_data = yaml.safe_load(f) or {}
                    profiles = p_data.get("profiles") or {}
            except Exception as e:
                ctx.log.error(f"UI: Failed to parse profiles YAML: {e}")

        if not sites_dir.exists(): return []

        # 2. Parse Hosts
        for yaml_file in sites_dir.rglob("*.yml"):
            parts = yaml_file.parts
            try:
                site = parts[parts.index("sites") + 1]
                stage = parts[parts.index("stages") + 1] if "stages" in parts else "common"
                
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    
                    hosts_data = data.get("hosts") or {}
                    hw_hosts_data = data.get("hardware_hosts") or {}
                    all_hosts = {**hosts_data, **hw_hosts_data}
                    
                    for host_name, host_data in all_hosts.items():
                        if not isinstance(host_data, dict): continue
                        
                        host_svcs = set()
                        
                        # Parse direct services
                        direct_services = host_data.get("services") or []
                        if isinstance(direct_services, list):
                            for s in direct_services:
                                if isinstance(s, dict) and s.get("name"): host_svcs.add(s.get("name"))
                                
                        # Parse profile-inherited services
                        host_profiles = host_data.get("profiles") or []
                        if isinstance(host_profiles, list):
                            for p in host_profiles:
                                profile_services = profiles.get(p, {}).get("services") or []
                                if isinstance(profile_services, list):
                                    for s in profile_services:
                                        if isinstance(s, dict) and s.get("name"): host_svcs.add(s.get("name"))
                                    
                        if host_svcs:
                            assignments.append({"site": site, "stage": stage, "host": host_name, "services": sorted(list(host_svcs))})
                            
            except (ValueError, IndexError):
                continue
            except Exception as e:
                ctx.log.error(f"UI: Failed to parse assignment YAML {yaml_file}: {e}")
                
        # Deduplicate and sort by site > stage > host
        unique_assignments = {f"{a['site']}-{a['stage']}-{a['host']}": a for a in assignments}
        return sorted(unique_assignments.values(), key=lambda x: (x['site'], x['stage'], x['host']))

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

    with ui.dialog() as log_viewer, ui.card().classes('w-full max-w-5xl h-[80vh] bg-black p-0 border border-zinc-800 flex flex-col no-wrap'):
        with ui.row().classes('w-full p-4 justify-between items-center border-b border-zinc-800 bg-zinc-900'):
            log_title = ui.label("Live Stream").classes('text-indigo-400 font-bold')
            ui.button(icon='close', on_click=log_viewer.close).props('flat round dense color=zinc-500')
        with ui.scroll_area().classes('w-full flex-grow bg-black p-4'):
            log_stream = ui.label().classes('whitespace-pre-wrap font-mono text-[11px] text-green-500 break-words')

    def open_live_logs(job_id):
        log_title.set_text(f"Live Pipeline Logs: Job #{job_id}")
        log_stream.set_text("Loading...")
        log_path = Path(f"/data/storage/logs/job_{job_id}.log")
        if log_path.exists():
            with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                f.seek(0, 2)
                file_size = f.tell()
                if file_size > 1048576:
                    f.seek(file_size - 100000)
                    content = f.read()
                    log_stream.set_text("[TRUNCATED] Showing last 100kb of logs...\n\n" + content)
                else:
                    f.seek(0)
                    log_stream.set_text(f.read())
        else:
            log_stream.set_text("No log file found on disk. (Legacy job or delayed write)")
        log_viewer.open()

    with ui.column().classes('w-full gap-6'):
        with ui.row().classes('w-full justify-between items-center'):
            ui.label('GitOps Dashboard').classes(UIStyles.TITLE_H2)
            with ui.row().classes('gap-3'):
                ui.button('Test Connect', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "connectivity", "manual": True}), icon='cable', color='blue-6').props('unelevated rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                ui.button('Deploy Service', on_click=lambda: ui.notify("Use the Service Catalog tab for targeted deploys", type="info"), icon='rocket', color='indigo-500').props('unelevated rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                ui.button('Run Rollout', on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "manual": True}), icon='rocket_launch', color='emerald').props('unelevated rounded size=sm').bind_enabled_from(state, 'is_running', backward=lambda x: not x)
                ui.button('ABORT', on_click=abort_execution, icon='dangerous', color='red-6').props('unelevated rounded size=sm').bind_visibility_from(state, 'is_running')

        with ui.tabs().classes('w-full border-b border-zinc-800') as tabs:
            overview_tab = ui.tab('Overview', icon='dashboard')
            catalog_tab = ui.tab('Service Catalog', icon='apps')
            assignment_tab = ui.tab('Assignments', icon='account_tree')
            history_tab = ui.tab('History & Logs', icon='history')

        with ui.tab_panels(tabs, value=overview_tab).classes('w-full bg-transparent p-0'):
            
            with ui.tab_panel(overview_tab).classes('gap-6 p-4'):
                ui.label("Active Pipelines").classes(UIStyles.TITLE_H3).bind_visibility_from(state, 'is_running')
                jobs_grid = ui.grid(columns='repeat(auto-fill, minmax(450px, 1fr))').classes('w-full gap-4')
                
                with ui.column().classes('w-full items-center py-32 opacity-30').bind_visibility_from(state, 'is_running', backward=lambda x: not x):
                    ui.icon('cloud_done', size='5em')
                    ui.label("Infrastructure is stable. No active jobs.").classes('text-xl font-bold')

            with ui.tab_panel(catalog_tab).classes('gap-4 p-4'):
                with ui.dialog() as svc_history_dialog, ui.card().classes('w-full max-w-4xl p-0 bg-zinc-950 border border-zinc-800'):
                    with ui.row().classes('p-4 w-full justify-between items-center border-b border-zinc-800'):
                        svc_history_title = ui.label("").classes('text-lg font-bold text-indigo-400')
                        ui.button(icon='close', on_click=svc_history_dialog.close).props('flat round dense color=zinc-500')
                    svc_history_table = ui.table(columns=[
                        {'name': 'id', 'label': 'Job ID', 'field': 'id', 'align': 'left'},
                        {'name': 'start_time', 'label': 'Date', 'field': 'start_time', 'align': 'left'},
                        {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                        {'name': 'action', 'label': 'Log', 'field': 'action', 'align': 'center'}
                    ], rows=[], row_key='id').classes('w-full bg-transparent shadow-none')
                    svc_history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{props.value}}</q-badge></q-td>''')
                    svc_history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" icon="article" color="primary" @click="() => $parent.$emit('view', props.row)" /></q-td>''')
                    svc_history_table.on('view', lambda e: show_job_logs_wrapper(e.args['id']))

                def show_job_logs_wrapper(jid):
                    svc_history_dialog.close()
                    open_live_logs(jid)

                with ui.row().classes('w-full justify-between items-end mb-4'):
                    with ui.column().classes('gap-0'):
                        ui.label('Service Catalog').classes(UIStyles.TITLE_H3)
                        ui.label('Available services from the global catalog.').classes(f'{UIStyles.TEXT_MUTED} text-xs')
                    with ui.row().classes('gap-2 items-center'):
                        catalog_search = ui.input('Search Service...').props('outlined dense clearable').classes('w-64')
                        ui.button(icon='refresh', on_click=lambda: catalog_container.refresh()).props('flat round color=zinc-500')

                @ui.refreshable
                def catalog_container():
                    catalog_services = load_catalog()
                    if not catalog_services:
                        ui.label("No services found. Ensure 'iac_controller' is synced and YAML is valid.").classes(f'{UIStyles.TEXT_MUTED} italic mt-4')
                        return
                    
                    catalog_grid = ui.grid(columns='repeat(auto-fill, minmax(320px, 1fr))').classes('w-full gap-4 mt-2')
                    
                    def render_catalog_cards(e=None):
                        catalog_grid.clear()
                        term = (catalog_search.value or "").lower()
                        with catalog_grid:
                            for svc in catalog_services:
                                name = svc.get("name", "Unknown")
                                repo_name = svc.get("repository_name", name)
                                branch = svc.get("branch", "main")
                                target_node = svc.get("target_environment", svc.get("host", "Auto-Assigned"))
                                deploy_type = svc.get("deploy_type", "Docker Compose")
                                
                                match = not term or term in name.lower() or term in repo_name.lower() or term in target_node.lower()
                                
                                if match:
                                    with ui.card().classes(f'{UIStyles.CARD_BASE} flex flex-col hover:border-indigo-500 transition-colors'):
                                        with ui.row().classes('w-full justify-between items-start'):
                                            with ui.column().classes('gap-0'):
                                                ui.label(name).classes('text-md font-bold truncate')
                                                ui.label(f"Repo: {repo_name}").classes(f'{UIStyles.TEXT_MUTED} text-[10px] truncate')
                                        
                                            if "compose" in deploy_type.lower():
                                                ui.html('<svg viewBox="0 0 24 24" width="24" height="24" fill="currentColor"><path d="M6.1,10L0,10.1V13h6.1V10z M13.1,10H7v3h6.1V10z M20.1,10H14v3h6.1V10z M13.1,3H7v3h6.1V3z"/></svg>').classes('text-indigo-400 w-6 h-6').tooltip("Docker Compose")
                                            else:
                                                ui.icon('settings_applications', color='slate-400').classes('text-xl').tooltip(deploy_type)

                                        ui.separator().classes('my-2 opacity-20')
                                            
                                        with ui.row().classes('w-full justify-between items-center'):
                                            with ui.row().classes('items-center gap-1'):
                                                ui.icon('dns', size='12px').classes('text-slate-400')
                                                ui.label(target_node).classes('text-xs text-slate-500 font-mono')
                                            
                                            with ui.row().classes('items-center gap-1'):
                                                ui.icon('call_split', size='12px').classes('text-slate-400')
                                                ui.label(branch).classes('text-xs text-slate-500 font-mono')
                                        
                                        ui.separator().classes('mt-auto mb-3 opacity-20')
                                        
                                        with ui.row().classes('w-full justify-between items-center gap-2'):
                                            ui.button(icon='history', on_click=lambda n=name: [svc_history_title.set_text(f"Deployment History: {n}"), setattr(svc_history_table, 'rows', engine.db.get_service_history(n)), svc_history_dialog.open()]).props('flat round size=sm color=zinc-500').tooltip("View Deployment History")
                                            ui.button('Deploy', icon='rocket', on_click=lambda n=name, b=branch: ctx.emit("iac:webhook_verified", {"pipeline_type": "single_service", "service_name": n, "service_branch": b, "manual": True})).props('unelevated rounded size=sm color=indigo').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                    catalog_search.on('update:model-value', render_catalog_cards)
                    render_catalog_cards()

                catalog_container()

            with ui.tab_panel(assignment_tab).classes('p-4'):
                with ui.row().classes('w-full justify-between items-end mb-4'):
                    with ui.column().classes('gap-0'):
                        ui.label('Infrastructure Topography').classes(UIStyles.TITLE_H3)
                        ui.label('Flattened view of mapped services across all sites and stages.').classes(f'{UIStyles.TEXT_MUTED} text-xs')
                    with ui.row().classes('gap-2 items-center'):
                        search_input = ui.input('Search Host or Service...').props('outlined dense clearable').classes('w-64')
                        ui.button(icon='refresh', on_click=lambda: assignment_container.refresh()).props('flat round color=zinc-500')

                @ui.refreshable
                def assignment_container():
                    assignments = load_assignments()
                    if not assignments:
                        ui.label("No assignments found. Ensure 'iac_controller/environments' is populated.").classes(f'{UIStyles.TEXT_MUTED} italic mt-4')
                        return

                    grid = ui.grid(columns='repeat(auto-fill, minmax(350px, 1fr))').classes('w-full gap-4 mt-2')
                    
                    def render_cards(e=None):
                        grid.clear()
                        term = (search_input.value or "").lower()
                        with grid:
                            for item in assignments:
                                site, stage, host, svcs = item['site'], item['stage'], item['host'], item['services']
                                
                                # Interactive search filter
                                match = not term or term in host.lower() or term in site.lower() or term in stage.lower() or any(term in s.lower() for s in svcs)
                                
                                if match:
                                    with ui.card().classes(f'{UIStyles.CARD_BASE} flex flex-col gap-2 hover:border-indigo-500/50 transition-all'):
                                        with ui.row().classes('w-full justify-between items-center border-b border-zinc-800/50 pb-2'):
                                            with ui.row().classes('items-center gap-2'):
                                                ui.icon('dns', size='18px').classes('text-slate-400')
                                                ui.label(host).classes('text-md font-bold text-slate-100 truncate max-w-[150px]').tooltip(host)
                                            with ui.row().classes('gap-1'):
                                                ui.chip(site.upper(), icon='domain').props('color=blue-900 text-color=blue-200 size=xs square')
                                                ui.chip(stage.upper(), icon='layers').props('color=emerald-900 text-color=emerald-200 size=xs square')
                                        
                                        with ui.row().classes('gap-1.5 pt-1'):
                                            for svc in svcs:
                                                ui.chip(svc, icon='apps', color='zinc-800').props('text-color=slate-300 size=sm')
                    
                    search_input.on('update:model-value', render_cards)
                    render_cards()
                    
                assignment_container()

            with ui.tab_panel(history_tab).classes('p-4'):
                history_table = ui.table(columns=[
                    {'name': 'id', 'label': 'ID', 'field': 'id', 'sortable': True, 'align': 'left'},
                    {'name': 'pipeline_type', 'label': 'Pipeline Type', 'field': 'pipeline_type', 'align': 'left'},
                    {'name': 'progress', 'label': 'Progress', 'field': 'progress', 'align': 'left'},
                    {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
                    {'name': 'action', 'label': 'Logs', 'field': 'action', 'align': 'center'}
                ], rows=engine.db.get_recent_jobs(), row_key='id').classes(f'{UIStyles.CARD_BASE} w-full mt-2')
                history_table.add_slot('body-cell-progress', '''<q-td :props="props"><q-linear-progress :value="(props.value || 0)/100" color="indigo" class="mt-2"/><div class="text-center text-[10px]">{{props.value || 0}}%</div></q-td>''')
                history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{props.value}}</q-badge></q-td>''')
                history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" color="zinc-700" icon="folder_open" @click="() => $parent.$emit('view', props.row)" /></q-td>''')
                history_table.on('view', lambda e: open_live_logs(e.args['id']))
        
        def update_ui_loop():
            running_jobs = engine.db.get_jobs_by_status("RUNNING")
            active_ids = [j.id for j in running_jobs]
            
            for jid in list(active_job_cards.keys()):
                if jid not in active_ids:
                    jobs_grid.remove(active_job_cards[jid]["card"])
                    del active_job_cards[jid]

            with jobs_grid:
                for job in running_jobs:
                    if job.id not in active_job_cards:
                        with ui.card().classes(f'{UIStyles.CARD_GLASS} border-indigo-500/50 flex flex-col p-4 shadow-2xl') as c:
                            with ui.row().classes('w-full justify-between items-start'):
                                with ui.column().classes('gap-0'):
                                    ui.label(f"Pipeline #{job.id}").classes('text-lg font-bold text-indigo-400')
                                    ui.label(job.pipeline_type).classes('text-[10px] uppercase text-slate-500 font-black tracking-widest')
                                ui.spinner('tail', size='2em', color='indigo')
                            
                            with ui.linear_progress(value=(job.progress or 0)/100.0, show_value=False).props('color=indigo rounded stripe size=20px').classes('mt-4 relative') as p_bar:
                                pct_lbl = ui.label(f"{int(job.progress or 0)}%").classes('absolute-center text-[11px] font-bold text-white drop-shadow-md')
                            with ui.row().classes('w-full mt-1'):
                                step_lbl = ui.label(job.current_step).classes('text-[11px] font-mono text-slate-300 truncate w-full')
                            
                            ui.label("Active Runners").classes('text-[10px] uppercase text-zinc-600 font-bold mt-4 mb-1')
                            runner_box = ui.column().classes('w-full gap-1 p-2 bg-black/40 rounded border border-zinc-800/50')
                            
                            with ui.row().classes('w-full mt-4 pt-2 border-t border-zinc-800 justify-between'):
                                ui.button('Live Logs', icon='terminal', on_click=lambda j=job.id: open_live_logs(j)).props('flat rounded size=sm color=green')
                                ui.button('Abort', icon='stop', on_click=abort_execution).props('flat rounded size=sm color=red')
                            
                        active_job_cards[job.id] = {"card": c, "bar": p_bar, "step": step_lbl, "pct": pct_lbl, "runners": runner_box}
                    else:
                        card_meta = active_job_cards[job.id]
                        card_meta["bar"].set_value((job.progress or 0) / 100.0)
                        card_meta["step"].set_text(job.current_step)
                        card_meta["pct"].set_text(f"{int(job.progress or 0)}%")
                        
                        card_meta["runners"].clear()
                        any_runners = False
                        for t_name, t_data in state.get("active_tasks", {}).items():
                            if t_data.get("job_id") == job.id and t_data.get("status") in ["pulling_image", "running_ansible"]:
                                any_runners = True
                                with card_meta["runners"]:
                                    with ui.row().classes('w-full items-center gap-2 px-1'):
                                        ui.icon('settings_input_component', size='12px', color='amber-500')
                                        ui.label(t_name).classes('text-[10px] text-slate-300 font-medium truncate w-4/5')
                                        ui.spinner('dots', size='xs', color='slate-600').classes('ml-auto')
                        
                        if not any_runners:
                            with card_meta["runners"]:
                                ui.label("Waiting for pool...").classes('text-[10px] text-zinc-600 italic px-1')

            if tabs.value == 'History & Logs':
                history_table.rows = engine.db.get_recent_jobs()
                history_table.update()

        ui.timer(1.0, update_ui_loop)