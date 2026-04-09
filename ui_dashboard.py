from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles

async def render_dashboard(ctx, state, engine):
    with ui.column().classes('w-full gap-6'):
        
        # --- HEADER & TRIGGERS ---
        with ui.row().classes('w-full justify-between items-center'):
            ui.label('GitOps Dashboard').classes('text-2xl font-bold dark:text-zinc-100')
            
            # Action Buttons Row
            with ui.row().classes('gap-3'):
                # Button 1: The Quick Check
                ui.button(
                    'Test Connectivity', 
                    on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "connectivity", "manual": True}),
                    icon='cable', 
                    color='blue-6'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                # Button 1.5: Single Service Deploy Dialog
                def open_single_service_dialog():
                    with ui.dialog() as dialog, ui.card().classes(UIStyles.CARD_GLASS + ' min-w-[300px] border border-zinc-700'):
                        ui.label('Deploy Single Service').classes('text-lg font-bold text-slate-100 mb-2')
                        svc_name_input = ui.input('Service Name (e.g. my-app)').classes('w-full').props('outlined dense')
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

                ui.button(
                    'Deploy Service', 
                    on_click=open_single_service_dialog,
                    icon='rocket', 
                    color='indigo-500'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

                # Button 2: The Full Pruning Rollout
                ui.button(
                    'Run Full Rollout', 
                    on_click=lambda: ctx.emit("iac:webhook_verified", {"pipeline_type": "rollout", "manual": True}),
                    icon='rocket_launch', 
                    color='emerald'
                ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

        # --- STATUS CARDS ---
        with ui.row().classes('w-full gap-6 flex-col md:flex-row items-stretch'):
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Deployment Engine').classes('text-lg font-bold mb-2 text-indigo-500')
                ui.label().bind_text_from(state, 'is_running', backward=lambda x: "Running..." if x else "Status: Idle")
            
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Last Result').classes('text-lg font-bold mb-2 text-rose-500')
                ui.label().bind_text_from(state, 'last_deployment')

        # --- LIVE LOG WINDOW ---
        with ui.card().classes(UIStyles.CARD_GLASS + ' w-full mt-4 h-96'): # Added h-96 here
            with ui.column().classes('w-full h-full'): # Force column to fill the card
                with ui.row().classes('w-full justify-between items-center mb-2'):
                    ui.label('Live Execution Logs').classes('text-lg font-bold')
                    ui.button(icon='delete_sweep', on_click=lambda: log_window.clear(), color='zinc-500').props('flat round size=sm')
                
                # flex-grow ensures the log takes up all remaining space in the card
                log_window = ui.log(max_lines=1000).classes('w-full flex-grow bg-zinc-950 text-green-400 font-mono text-xs p-4 rounded border border-zinc-800')

        # --- HISTORY SECTION ---
        ui.separator().classes('mt-4 mb-2 w-full opacity-50')
        ui.label("Execution History").classes('text-2xl font-bold dark:text-zinc-100')

        with ui.dialog() as log_dialog:
            # Changed 'flex-col' and 'no-wrap' to ensure the log doesn't push the close button away
            with ui.card().classes('w-full max-w-5xl h-[80vh] flex flex-col no-wrap bg-zinc-900 border border-zinc-700 p-0'):
                # Header inside a fixed-height row
                with ui.row().classes('w-full p-4 items-center justify-between border-b border-zinc-800'):
                    dialog_title = ui.label("Job Logs").classes('text-lg font-bold text-slate-200')
                
                # Log content in a flex-grow area
                # Added 'overflow-hidden' to the log component classes to force its internal scrollbar
                log_content = ui.log(max_lines=5000).classes('w-full flex-grow bg-black text-green-400 p-4 font-mono text-xs overflow-hidden')
                
                # Footer inside a fixed-height row
                with ui.row().classes('w-full p-4 justify-end border-t border-zinc-800'):
                    ui.button("Close", on_click=log_dialog.close, color='red').props('unelevated rounded')
        def open_log_popup(e):
            job_data = e.args
            job_id = job_data.get('id')
            status = job_data.get('status', 'UNKNOWN')
            
            dialog_title.set_text(f"Execution Logs: Job #{job_id} ({status})")
            
            # CLEAR AND PUSH
            log_content.clear()
            logs = engine.db.get_job_logs(job_id)
            
            if not logs:
                log_content.push("No logs found for this job.")
            else:
                # Combine all logs into one push if possible, or iterate
                # If logs is a single string with newlines, split it
                if isinstance(logs, str):
                    logs = logs.splitlines()
                    
                for line in logs:
                    log_content.push(line)
                    
            log_dialog.open()

        columns = [
            {'name': 'id', 'label': 'Job ID', 'field': 'id', 'sortable': True, 'align': 'left'},
            {'name': 'pipeline_type', 'label': 'Type', 'field': 'pipeline_type', 'align': 'left'},
            {'name': 'start_time', 'label': 'Started At', 'field': 'start_time', 'align': 'left'},
            {'name': 'end_time', 'label': 'Finished At', 'field': 'end_time', 'align': 'left'},
            {'name': 'status', 'label': 'Status', 'field': 'status', 'align': 'left'},
            {'name': 'action', 'label': 'Logs', 'field': 'action', 'align': 'center'}
        ]

        history_table = ui.table(columns=columns, rows=engine.db.get_recent_jobs(), row_key='id').classes('w-full mt-4 bg-zinc-900/50')
        history_table.add_slot('body-cell-status', '''<q-td :props="props"><q-badge :color="props.value === 'SUCCESS' ? 'positive' : (props.value === 'RUNNING' ? 'warning' : 'negative')">{{ props.value }}</q-badge></q-td>''')
        history_table.add_slot('body-cell-action', '''<q-td :props="props"><q-btn size="sm" color="info" icon="article" label="View" @click="() => $parent.$emit('view_logs', props.row)" /></q-td>''')
        history_table.on('view_logs', open_log_popup)

        is_currently_running = [state.get("is_running", False)]
        last_log_index = [0] # Acts as our bookmark
        
        def update_ui_logs():
            current_logs = state.get("latest_logs", [])
            
            # Detect if a NEW pipeline started (the engine reset the list)
            if len(current_logs) < last_log_index[0]:
                last_log_index[0] = 0
                log_window.clear() # Clear the UI for the new run

            # If the list has grown past our bookmark, grab the new chunk
            if len(current_logs) > last_log_index[0]:
                new_lines = current_logs[last_log_index[0]:]
                last_log_index[0] = len(current_logs) # Move the bookmark forward
                
                # Push the chunk to the UI (No destructive clear!)
                log_window.push('\n'.join(new_lines))
            
            # Update history table only when status changes
            current_running = state.get("is_running", False)
            if is_currently_running[0] != current_running:
                history_table.rows = engine.db.get_recent_jobs()
                history_table.update()
                is_currently_running[0] = current_running
        
        ui.timer(0.3, update_ui_logs) # Slightly faster polling for a "live" feel