import hmac
from fastapi import APIRouter, Request, Header, HTTPException
from nicegui import ui

iac_api_router = APIRouter(prefix="/api/iac", tags=["IaC Orchestrator"])

# Internal references to be set by init_api
_ctx = None
_engine = None

def init_api(ctx, engine):
    """Initializes the API module with the required context and execution engine."""
    global _ctx, _engine
    _ctx = ctx
    _engine = engine

@iac_api_router.post("/webhook/gitlab")
async def gitlab_webhook(request: Request, x_gitlab_token: str = Header(None)):
    """
    Endpoint for GitLab webhooks. Validates security tokens and triggers
    the internal event bus for processing.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")

    # 1. Security Check
    expected_token = _ctx.get_secret("gitlab_webhook_token")
    if not expected_token:
        _ctx.log.error("SECURITY HALT: Webhook token missing in Vault.")
        raise HTTPException(status_code=500, detail="Configuration Error")

    if not x_gitlab_token or not hmac.compare_digest(x_gitlab_token, expected_token):
        _ctx.log.warning("SECURITY REJECTION: Unauthorized webhook attempt.")
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 2. Payload Processing
    try:
        payload = await request.json()
        project_name = payload.get("project", {}).get("name", "unknown")
        _ctx.log.info(f"WEBHOOK: Verified push for project '{project_name}'.")
        
        # 3. Emit event to decouple request from execution
        _ctx.emit("iac:webhook_verified", payload)
        
        return {"status": "accepted", "message": "Webhook verified."}
    except Exception as e:
        _ctx.log.error(f"WEBHOOK ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail="Malformed JSON payload")
    
    
    
