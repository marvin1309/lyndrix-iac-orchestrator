from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles
import json

async def render_dashboard(ctx, state, engine):
    # Track the last seen task names to avoid unnecessary UI clearing
    last_active_task_keys = set()

    with ui.column().classes('w-full gap-6'):
        
        # --- HEADER & TRIGGERS ---
        with ui.row().classes('w-full justify-between items-center'):
            ui.label('GitOps Dashboard').classes('text-2xl font-bold dark:text-zinc-100')
            
            with ui.row().classes('gap-3'):
                ui.button('Test Connectivity', 
                    on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "connectivity", "manual": True}),
                    icon='cable', color='blue-6'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                def open_single_service_dialog():
                    with ui.dialog() as dialog, ui.card().classes(UIStyles.CARD_GLASS + ' min-w-[300px] border border-zinc-700'):
                        ui.label('Deploy Single Service').classes('text-lg font-bold text-slate-100 mb-2')
                        svc_name_input = ui.input('Service Name').classes('w-full').props('outlined dense')
                        svc_branch_input = ui.input('Branch', value='main').classes('w-full mt-2').props('outlined dense')
                        
                        def trigger_deploy():
                            if not svc_name_input.value:
                                ui.notify("Service Name is required", type="negative")
                                return
                            ctx.emit("iac:webhook_verified", {
                                "pipeline_type": "single_service", 
                                "service_name": svc_name_input.value.strip(),
                                "service_branch": svc_branch_input.value.strip(),
                                "manual": True
                            })
                            dialog.close()
                            
                        with ui.row().classes('w-full justify-end mt-4 gap-2'):
                            ui.button('Cancel', on_click=dialog.close, color='zinc-500').props('flat rounded')
                            ui.button('Trigger Deploy', on_click=trigger_deploy, color='indigo').props('unelevated rounded')
                    dialog.open()

                ui.button('Deploy Service', on_click=open_single_service_dialog, icon='rocket', color='indigo-500'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                ui.button('Run Full Rollout', 
                    on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "manual": True}),
                    icon='rocket_launch', color='emerald'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

        # --- STATUS CARDS ---
        with ui.row().classes('w-full gap-6 flex-col md:flex-row items-stretch'):
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Deployment Engine').classes('text-lg font-bold mb-2 text-indigo-500')
                ui.label().bind_text_from(state, 'is_running', backward=lambda x: "Running..." if x else "Status: Idle")
            
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Last Result').classes('text-lg font-bold mb-2 text-rose-500')
                ui.label().bind_text_from(state, 'last_deployment')

        # --- ACTIVE DOCKER RUNNERS ---
        with ui.column().classes('w-full mb-4'):
            ui.label('Active Docker Runners').classes('text-lg font-bold text-slate-100')
            runner_container = ui.row().classes('w-full gap-4 items-stretch min-h-[100px]')
            
            with ui.dialog() as runner_log_dialog, ui.card().classes(UIStyles.CARD_GLASS + ' w-full max-w-4xl h-[70vh] flex flex-col p-0'):
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

            def update_runners_ui():
                nonlocal last_active_task_keys
                active_tasks = state.get("active_tasks", {})
                live_tasks = {k: v for k, v in active_tasks.items() if v["status"] in ["pulling_image", "running_ansible"]}
                current_keys = set(live_tasks.keys())

                # FIX: Only clear and rebuild if the SET of tasks has changed (new one started or one finished)
                # This prevents the UI from "blocking" or flickering every 0.5s
                if current_keys != last_active_task_keys:
                    runner_container.clear()
                    last_active_task_keys = current_keys
                    with runner_container:
                        if not live_tasks:
                            ui.label("No active runners. Pool is idle.").classes('text-sm text-zinc-500 italic p-4')
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

        # --- LIVE LOG WINDOW ---
        with ui.card().classes(UIStyles.CARD_GLASS + ' w-full mt-4 h-96'):
            log_window = ui.log(max_lines=10000).classes('w-full h-full bg-zinc-950 text-green-400 font-mono text-xs p-4 rounded border border-zinc-800')

        # --- HISTORY SECTION ---
        ui.separator().classes('mt-4 mb-2 w-full opacity-50')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Execution History").classes('text-2xl font-bold dark:text-zinc-100')
            ui.button(icon='refresh', on_click=lambda: refresh_history()).props('flat round color=zinc-500')

        with ui.dialog() as log_dialog:
            with ui.card().classes('w-full max-w-6xl h-[85vh] flex flex-col no-wrap bg-zinc-900 border border-zinc-700 p-0'):
                with ui.row().classes('w-full p-4 items-center justify-between border-b border-zinc-800 bg-zinc-950'):
                    dialog_title = ui.label("Job Logs").classes('text-lg font-bold text-slate-200')
                    ui.button(icon='close', on_click=log_dialog.close, color='zinc-600').props('flat round dense')
                with ui.scroll_area().classes('w-full flex-grow bg-black p-4'):
                    log_content = ui.label().classes('whitespace-pre-wrap font-mono text-xs text-green-400 break-words')
                
        def open_log_popup(e):
            job_data = e.args
            dialog_title.set_text(f"Job #{job_data.get('id')} ({job_data.get('status')})")
            raw_logs = engine.db.get_job_logs(job_data.get('id'))
            if raw_logs:
                try:
                    parsed = json.loads(raw_logs)
                    log_content.set_text('\n'.join(parsed) if isinstance(parsed, list) else str(parsed))
                except: log_content.set_text(str(raw_logs))
            else: log_content.set_text("No logs.")
            log_dialog.open()

        columns = [
            {'name': 'id', 'label': 'ID', 'field': 'id', 'sortable': True, 'align': 'left'},
            {'name': 'pipeline_type', 'label': 'Type', 'field': 'pipeline_type', 'align': 'left'},
            {'name': 'start_time', 'label': 'Started At', 'field': 'start_time', 'align': 'left'},
            {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
            {'name': 'action', 'label': 'Logs', 'field': 'action', 'align': 'center'}
        ]

        history_table = ui.table(columns=columns, rows=engine.db.get_recent_jobs(), row_key='id').classes('w-full mt-4 bg-zinc-900/50')
        history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{ props.value }}</q-badge></q-td>''')
        history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" color="info" icon="article" label="View" @click="() => $parent.$emit('view_logs', props.row)" /></q-td>''')
        history_table.on('view_logs', open_log_popup)

        def refresh_history():
            history_table.rows = engine.db.get_recent_jobs()
            history_table.update()

        last_log_index = [0]
        history_refresh_counter = [0]
        
        def update_ui_loop():
            current_logs = state.get("latest_logs", [])
            
            # 1. Update Global Logs
            if len(current_logs) < last_log_index[0]:
                last_log_index[0] = 0
                log_window.clear()
            if len(current_logs) > last_log_index[0]:
                new_lines = current_logs[last_log_index[0]:]
                last_log_index[0] = len(current_logs)
                log_window.push('\n'.join(new_lines))
            
            # 2. Update Active Runners (Uses internal check to avoid flicker)
            update_runners_ui()
            
            # 3. Update History Table (Refresh every 5 ticks while running, or every 20 when idle)
            history_refresh_counter[0] += 1
            threshold = 5 if state.get("is_running") else 20
            if history_refresh_counter[0] >= threshold:
                refresh_history()
                history_refresh_counter[0] = 0

            # 4. Live update of OPEN runner dialog
            if runner_log_dialog.value: 
                current_task = runner_dialog_title.text.replace("Live Logs: ", "")
                task_logs = state.get("active_tasks", {}).get(current_task, {}).get("logs", [])
                runner_log_content.set_text('\n'.join(task_logs))
        
        ui.timer(0.5, update_ui_loop)