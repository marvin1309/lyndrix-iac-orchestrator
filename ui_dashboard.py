from nicegui import ui
from ui.layout import main_layout 
from ui.theme import UIStyles

async def render_dashboard(ctx, state, engine):
    """Renders the main GitOps Orchestrator dashboard."""
    with ui.column().classes('w-full gap-6'):
        
        # Header
        with ui.row().classes('w-full justify-between items-center'):
            ui.label('GitOps Dashboard').classes('text-2xl font-bold dark:text-zinc-100')
            ui.button(
                'Manual Trigger', 
                on_click=lambda: ctx.emit("iac:webhook_verified", {"manual": True}),
                icon='play_arrow', 
                color='emerald'
            ).props('unelevated rounded').bind_enabled_from(state, 'is_running', backward=lambda x: not x)

        # Status Cards
        with ui.row().classes('w-full gap-6 flex-col md:flex-row items-stretch'):
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Deployment Engine').classes('text-lg font-bold mb-2 text-indigo-500')
                ui.label().bind_text_from(state, 'is_running', backward=lambda x: "Running..." if x else "Status: Idle")
            
            with ui.card().classes(UIStyles.CARD_GLASS + ' flex-1'):
                ui.label('Last Result').classes('text-lg font-bold mb-2 text-rose-500')
                ui.label().bind_text_from(state, 'last_deployment')

        # Live Log Window
        with ui.card().classes(UIStyles.CARD_GLASS + ' w-full mt-4'):
            ui.label('Live Execution Logs').classes('text-lg font-bold mb-4')
            log_window = ui.log(max_lines=500).classes('w-full h-96 bg-zinc-900 text-green-400 font-mono text-xs p-4 rounded overflow-y-auto')

            # Log Poller: Moves logs from the background state to the UI component
            def update_ui_logs():
                while state["latest_logs"]:
                    line = state["latest_logs"].pop(0)
                    log_window.push(line)
            
            ui.timer(0.5, update_ui_logs)