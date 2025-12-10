from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uuid
import sys
import os
import json
import asyncio
from datetime import datetime

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from graph_engine import GraphEngine
from app.database import save_graph, load_graph, list_graphs, delete_graph
from app.async_executor import async_executor, ExecutionStatus
from complete_data_quality_demo import DATA_QUALITY_NODES

app = FastAPI(title="Workflow Engine API")

RUNS: Dict[str, Dict[str, Any]] = {}

def test_node_1(state):
    print("Executing test_node_1")
    state["step"] = 1
    state["message"] = "Processed by node 1"
    state["timestamp"] = "2025-12-10T20:45:00Z"
    return state

def test_node_2(state):
    print("Executing test_node_2")
    state["step"] = 2
    state["message"] = "Processed by node 2"
    state["counter"] = state.get("counter", 0) + 10
    return state

def test_node_3(state):
    print("Executing test_node_3")
    state["step"] = 3
    state["message"] = "Processing completed"
    state["completed"] = True
    state["final_result"] = f"Processed {state.get('counter', 0)} items"
    return state

async def long_running_analysis(state):
    print("Starting long-running analysis...")
    state["analysis_status"] = "started"
    
    for i in range(5):
        await asyncio.sleep(2)  # Simulate 2 seconds of work
        progress = (i + 1) * 20
        state["analysis_progress"] = f"{progress}%"
        print(f"Analysis progress: {progress}%")
    
    state["analysis_status"] = "completed"
    state["analysis_result"] = "Complex analysis completed successfully"
    state["processing_time"] = "10 seconds"
    return state

async def async_data_processing(state):
    print("Processing data asynchronously...")
    state["processing_status"] = "running"
    
    tasks = []
    for i in range(3):
        task = asyncio.create_task(simulate_api_call(f"dataset_{i}"))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    
    state["processed_datasets"] = results
    state["processing_status"] = "completed"
    state["total_records"] = sum(len(result) for result in results)
    return state

async def simulate_api_call(dataset_name: str):
    await asyncio.sleep(1.5)
    return [f"{dataset_name}_record_{i}" for i in range(10)]

def heavy_computation(state):
    import time
    print("Starting heavy computation...")
    
    start_time = time.time()
    result = 0
    for i in range(1000000):
        result += i * i
    
    processing_time = time.time() - start_time
    state["computation_result"] = result
    state["computation_time"] = f"{processing_time:.2f} seconds"
    state["computation_status"] = "completed"
    return state

NODE_REGISTRY: Dict[str, Any] = {
    "test_node_1": test_node_1,
    "test_node_2": test_node_2,
    "test_node_3": test_node_3,
    "long_running_analysis": long_running_analysis,
    "async_data_processing": async_data_processing,
    "heavy_computation": heavy_computation,
    **DATA_QUALITY_NODES,
}

class EdgeSpec(BaseModel):
    from_node: str
    to_node: str

class GraphCreateRequest(BaseModel):
    nodes: List[str]
    edges: List[EdgeSpec]
    start_node: str

class GraphCreateResponse(BaseModel):
    graph_id: str

class GraphRunRequest(BaseModel):
    graph_id: str
    initial_state: Dict[str, Any] = {}

class StepLog(BaseModel):
    node: str
    state: Dict[str, Any]

class GraphRunResponse(BaseModel):
    run_id: str
    final_state: Dict[str, Any]
    log: List[StepLog]

class GraphStateResponse(BaseModel):
    run_id: str
    graph_id: str
    state: Dict[str, Any]
    log: Optional[List[StepLog]] = None

class GraphInfo(BaseModel):
    graph_id: str
    nodes: List[str]
    start_node: str
    created_at: str

class GraphListResponse(BaseModel):
    graphs: List[GraphInfo]

class WebSocketRunRequest(BaseModel):
    graph_id: str
    initial_state: Dict[str, Any] = {}

class StreamLogMessage(BaseModel):
    type: str  # "step", "completed", "error"
    run_id: str
    node: Optional[str] = None
    state: Optional[Dict[str, Any]] = None
    final_state: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class AsyncRunRequest(BaseModel):
    graph_id: str
    initial_state: Dict[str, Any] = {}

class AsyncRunResponse(BaseModel):
    execution_id: str
    status: str
    message: str

class AsyncExecutionResponse(BaseModel):
    execution_id: str
    graph_id: str
    status: str
    current_node: Optional[str] = None
    current_state: Dict[str, Any] = {}
    final_state: Optional[Dict[str, Any]] = None
    log: List[Dict[str, Any]] = []
    error: Optional[str] = None
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    progress: int = 0

class ExecutionListResponse(BaseModel):
    executions: List[AsyncExecutionResponse]
    total: int


@app.post("/graph/create", response_model=GraphCreateResponse)
def create_graph(req: GraphCreateRequest):
    engine = GraphEngine()
    
    for node_name in req.nodes:
        if node_name not in NODE_REGISTRY:
            raise HTTPException(status_code=400, detail=f"Unknown node: {node_name}")
        engine.add_node(node_name, NODE_REGISTRY[node_name])
    
    for edge in req.edges:
        engine.add_edge(edge.from_node, edge.to_node)
    
    if req.start_node not in req.nodes:
        raise HTTPException(status_code=400, detail=f"Start node '{req.start_node}' not in nodes list")
    
    graph_id = str(uuid.uuid4())
    
    edges_dict = [{"from_node": edge.from_node, "to_node": edge.to_node} for edge in req.edges]
    success = save_graph(graph_id, req.nodes, edges_dict, req.start_node, engine)
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to save graph to database")
    
    return GraphCreateResponse(graph_id=graph_id)


@app.post("/graph/run", response_model=GraphRunResponse)
def run_graph(req: GraphRunRequest):
    graph_data = load_graph(req.graph_id, NODE_REGISTRY)
    if graph_data is None:
        raise HTTPException(status_code=404, detail="Graph not found")
    
    engine: GraphEngine = graph_data["engine"]
    start_node: str = graph_data["start_node"]
    
    raw_log: List[Dict[str, Any]] = []
    final_state = engine.run(start_node, dict(req.initial_state), log=raw_log)
    
    run_id = str(uuid.uuid4())
    RUNS[run_id] = {
        "graph_id": req.graph_id,
        "final_state": final_state,
        "log": raw_log,
    }
    
    step_logs = [StepLog(node=entry["node"], state=entry["state"]) for entry in raw_log]
    
    return GraphRunResponse(
        run_id=run_id,
        final_state=final_state,
        log=step_logs,
    )


@app.get("/graph/state/{run_id}", response_model=GraphStateResponse)
def get_graph_state(run_id: str):
    run_data = RUNS.get(run_id)
    if run_data is None:
        raise HTTPException(status_code=404, detail="Run not found")
    
    step_logs = [StepLog(node=entry["node"], state=entry["state"]) for entry in run_data["log"]]
    
    return GraphStateResponse(
        run_id=run_id,
        graph_id=run_data["graph_id"],
        state=run_data["final_state"],
        log=step_logs,
    )


@app.get("/")
def root():
    return {"message": "Workflow Engine API is running"}


@app.get("/nodes")
def list_nodes():
    return {"available_nodes": list(NODE_REGISTRY.keys())}

@app.get("/graphs", response_model=GraphListResponse)
def list_all_graphs():
    graphs = list_graphs()
    graph_infos = [
        GraphInfo(
            graph_id=graph["graph_id"],
            nodes=graph["nodes"],
            start_node=graph["start_node"],
            created_at=graph["created_at"]
        )
        for graph in graphs
    ]
    return GraphListResponse(graphs=graph_infos)

@app.get("/graph/{graph_id}")
def get_graph_info(graph_id: str):
    graph_data = load_graph(graph_id, NODE_REGISTRY)
    if graph_data is None:
        raise HTTPException(status_code=404, detail="Graph not found")
    
    return {
        "graph_id": graph_id,
        "nodes": graph_data["nodes"],
        "edges": graph_data["edges"],
        "start_node": graph_data["start_node"]
    }

@app.delete("/graph/{graph_id}")
def delete_graph_endpoint(graph_id: str):
    success = delete_graph(graph_id)
    if not success:
        raise HTTPException(status_code=404, detail="Graph not found")
    
    return {"message": f"Graph {graph_id} deleted successfully"}

@app.post("/graph/run/async", response_model=AsyncRunResponse)
async def run_graph_async(req: AsyncRunRequest):
    try:
        execution_id = await async_executor.start_execution(
            req.graph_id,
            req.initial_state,
            lambda graph_id, node_registry: load_graph(graph_id, node_registry),
            NODE_REGISTRY
        )
        
        return AsyncRunResponse(
            execution_id=execution_id,
            status="started",
            message="Async execution started successfully"
        )
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/execution/{execution_id}", response_model=AsyncExecutionResponse)
async def get_execution_status(execution_id: str):
    execution = async_executor.get_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    data = execution.to_dict()
    return AsyncExecutionResponse(**data)

@app.get("/executions", response_model=ExecutionListResponse)
async def list_executions(status: Optional[str] = None, limit: int = 50):
    status_filter = None
    if status:
        try:
            status_filter = ExecutionStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
    
    executions = async_executor.list_executions(status_filter)[:limit]
    execution_responses = [AsyncExecutionResponse(**exec.to_dict()) for exec in executions]
    
    return ExecutionListResponse(
        executions=execution_responses,
        total=len(execution_responses)
    )

@app.post("/execution/{execution_id}/cancel")
async def cancel_execution(execution_id: str):
    success = await async_executor.cancel_execution(execution_id)
    if not success:
        raise HTTPException(status_code=404, detail="Execution not found or cannot be cancelled")
    
    return {"message": f"Execution {execution_id} cancelled successfully"}

@app.delete("/executions/cleanup")
async def cleanup_executions(max_age_hours: int = 24):
    cleaned_count = async_executor.cleanup_completed(max_age_hours)
    return {"message": f"Cleaned up {cleaned_count} old executions"}

async def run_graph_streaming(graph_id: str, initial_state: Dict[str, Any], websocket: WebSocket) -> str:
    run_id = str(uuid.uuid4())
    
    try:
        graph_data = load_graph(graph_id, NODE_REGISTRY)
        if graph_data is None:
            await websocket.send_text(json.dumps({
                "type": "error",
                "run_id": run_id,
                "error": "Graph not found"
            }))
            return run_id
        
        engine: GraphEngine = graph_data["engine"]
        start_node: str = graph_data["start_node"]
        
        current_node = start_node
        state = dict(initial_state)
        raw_log: List[Dict[str, Any]] = []
        
        if start_node not in engine.nodes:
            await websocket.send_text(json.dumps({
                "type": "error",
                "run_id": run_id,
                "error": f"Start node '{start_node}' not found"
            }))
            return run_id
        
        while current_node:
            func = engine.nodes[current_node]
            state = func(state)
            
            log_entry = {"node": current_node, "state": dict(state)}
            raw_log.append(log_entry)
            
            message = {
                "type": "step",
                "run_id": run_id,
                "node": current_node,
                "state": dict(state)
            }
            await websocket.send_text(json.dumps(message))
            
            await asyncio.sleep(0.1)
            
            next_node = None
            
            if current_node in engine.branch_conditions:
                for condition_func, target_node in engine.branch_conditions[current_node]:
                    if condition_func(state):
                        next_node = target_node
                        break
            
            if next_node is None:
                next_node = engine.edges.get(current_node)
            
            current_node = next_node
        
        RUNS[run_id] = {
            "graph_id": graph_id,
            "final_state": state,
            "log": raw_log,
        }
        
        completion_message = {
            "type": "completed",
            "run_id": run_id,
            "final_state": state
        }
        await websocket.send_text(json.dumps(completion_message))
        
        return run_id
        
    except Exception as e:
        error_message = {
            "type": "error",
            "run_id": run_id,
            "error": str(e)
        }
        await websocket.send_text(json.dumps(error_message))
        return run_id

@app.websocket("/ws/graph/run")
async def websocket_run_graph(websocket: WebSocket):
    await websocket.accept()
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                request_data = json.loads(data)
                graph_id = request_data.get("graph_id")
                initial_state = request_data.get("initial_state", {})
                
                if not graph_id:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "run_id": "unknown",
                        "error": "graph_id is required"
                    }))
                    continue
                
                run_id = await run_graph_streaming(graph_id, initial_state, websocket)
                
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "run_id": "unknown",
                    "error": "Invalid JSON format"
                }))
            except Exception as e:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "run_id": "unknown",
                    "error": f"Unexpected error: {str(e)}"
                }))
                
    except WebSocketDisconnect:
        print("WebSocket client disconnected")
    except Exception as e:
        print(f"WebSocket error: {e}")
        try:
            await websocket.close()
        except:
            pass

@app.websocket("/ws/execution/{execution_id}")
async def websocket_execution_monitor(websocket: WebSocket, execution_id: str):
    await websocket.accept()
    
    try:
        execution = async_executor.get_execution(execution_id)
        if not execution:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "Execution not found"
            }))
            await websocket.close()
            return
        
        async def progress_callback(exec_id: str, event_type: str, node: str, state: Dict[str, Any]):
            if exec_id == execution_id:
                message = {
                    "type": "progress",
                    "execution_id": exec_id,
                    "event_type": event_type,
                    "node": node,
                    "state": state,
                    "timestamp": datetime.utcnow().isoformat()
                }
                try:
                    await websocket.send_text(json.dumps(message))
                except:
                    pass
        
        execution.add_progress_callback(progress_callback)
        
        await websocket.send_text(json.dumps({
            "type": "status",
            "execution": execution.to_dict()
        }))
        
        while execution.status in [ExecutionStatus.PENDING, ExecutionStatus.RUNNING]:
            await asyncio.sleep(1)
            
            await websocket.send_text(json.dumps({
                "type": "status_update",
                "execution_id": execution_id,
                "status": execution.status.value,
                "current_node": execution.current_node,
                "progress": len(execution.log)
            }))
        
        await websocket.send_text(json.dumps({
            "type": "completed",
            "execution": execution.to_dict()
        }))
        
    except WebSocketDisconnect:
        print(f"WebSocket client disconnected from execution {execution_id}")
    except Exception as e:
        print(f"WebSocket error for execution {execution_id}: {e}")
        try:
            await websocket.close()
        except:
            pass