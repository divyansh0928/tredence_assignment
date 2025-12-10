import asyncio
import uuid
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from enum import Enum
import traceback

class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class AsyncExecution:
    def __init__(self, execution_id: str, graph_id: str, initial_state: Dict[str, Any]):
        self.execution_id = execution_id
        self.graph_id = graph_id
        self.initial_state = initial_state
        self.status = ExecutionStatus.PENDING
        self.current_node: Optional[str] = None
        self.current_state: Dict[str, Any] = {}
        self.final_state: Optional[Dict[str, Any]] = None
        self.log: list = []
        self.error: Optional[str] = None
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.task: Optional[asyncio.Task] = None
        self.progress_callbacks: list = []

    def add_progress_callback(self, callback: Callable):
        self.progress_callbacks.append(callback)

    async def notify_progress(self, event_type: str, node: str, state: Dict[str, Any]):
        self.current_node = node
        self.current_state = dict(state)
        
        for callback in self.progress_callbacks:
            try:
                await callback(self.execution_id, event_type, node, state)
            except Exception as e:
                print(f"Error in progress callback: {e}")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "graph_id": self.graph_id,
            "status": self.status.value,
            "current_node": self.current_node,
            "current_state": self.current_state,
            "final_state": self.final_state,
            "log": self.log,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "progress": len(self.log)
        }

class AsyncExecutionManager:
    def __init__(self):
        self.executions: Dict[str, AsyncExecution] = {}
        self.max_concurrent_executions = 10

    async def start_execution(self, graph_id: str, initial_state: Dict[str, Any], 
                            engine_factory: Callable, node_registry: Dict[str, Any]) -> str:
        execution_id = str(uuid.uuid4())
        
        running_count = sum(1 for exec in self.executions.values() 
                          if exec.status in [ExecutionStatus.PENDING, ExecutionStatus.RUNNING])
        
        if running_count >= self.max_concurrent_executions:
            raise Exception(f"Maximum concurrent executions ({self.max_concurrent_executions}) reached")
        
        execution = AsyncExecution(execution_id, graph_id, initial_state)
        self.executions[execution_id] = execution
        
        # Create and start the async task
        task = asyncio.create_task(
            self._execute_graph(execution, engine_factory, node_registry)
        )
        execution.task = task
        
        return execution_id

    async def _execute_graph(self, execution: AsyncExecution, engine_factory: Callable, 
                           node_registry: Dict[str, Any]):
        try:
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.utcnow()
            
            graph_data = engine_factory(execution.graph_id, node_registry)
            if not graph_data:
                raise Exception("Graph not found")
            
            engine = graph_data["engine"]
            start_node = graph_data["start_node"]
            
            final_state = await engine.run_async(
                start_node, 
                dict(execution.initial_state),
                log=execution.log,
                progress_callback=execution.notify_progress
            )
            
            execution.final_state = final_state
            execution.status = ExecutionStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            
        except asyncio.CancelledError:
            execution.status = ExecutionStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            execution.error = "Execution was cancelled"
            
        except Exception as e:
            execution.status = ExecutionStatus.FAILED
            execution.completed_at = datetime.utcnow()
            execution.error = str(e)
            execution.log.append({
                "node": execution.current_node or "unknown",
                "error": str(e),
                "traceback": traceback.format_exc()
            })

    def get_execution(self, execution_id: str) -> Optional[AsyncExecution]:
        return self.executions.get(execution_id)

    def list_executions(self, status_filter: Optional[ExecutionStatus] = None) -> list:
        executions = list(self.executions.values())
        if status_filter:
            executions = [e for e in executions if e.status == status_filter]
        return sorted(executions, key=lambda x: x.created_at, reverse=True)

    async def cancel_execution(self, execution_id: str) -> bool:
        execution = self.executions.get(execution_id)
        if not execution:
            return False
        
        if execution.status in [ExecutionStatus.PENDING, ExecutionStatus.RUNNING]:
            if execution.task:
                execution.task.cancel()
            execution.status = ExecutionStatus.CANCELLED
            execution.completed_at = datetime.utcnow()
            return True
        
        return False

    def cleanup_completed(self, max_age_hours: int = 24):
        cutoff_time = datetime.utcnow().timestamp() - (max_age_hours * 3600)
        
        to_remove = []
        for execution_id, execution in self.executions.items():
            if (execution.status in [ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.CANCELLED] 
                and execution.completed_at 
                and execution.completed_at.timestamp() < cutoff_time):
                to_remove.append(execution_id)
        
        for execution_id in to_remove:
            del self.executions[execution_id]
        
        return len(to_remove)

async_executor = AsyncExecutionManager()