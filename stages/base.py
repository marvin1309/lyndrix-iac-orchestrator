from abc import ABC, abstractmethod
from ..utils import StageResult

class BaseStage(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def run(self, engine, context: dict) -> StageResult:
        pass