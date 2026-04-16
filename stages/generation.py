from .base import BaseStage
from ..utils import StageResult

class NativeGenerateStage(BaseStage):
    def __init__(self):
        super().__init__("Native State Generation")

    async def run(self, engine, context: dict) -> StageResult:
        try:
            await engine._execute_native_generation()
            return StageResult(True, "Infrastructure state generated.")
        except Exception as e:
            return StageResult(False, f"Generation failed: {str(e)}")