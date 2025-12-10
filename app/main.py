from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uuid
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from graph_engine import GraphEngine
from example_nodes import set_sample_code_node, analyze_code_node, report_results_node

app = FastAPI(title="Workflow Engine API")

GRAPHS: Dict[str, Dict[str, Any]] = {}
RUNS: Dict[str, Dict[str, Any]] = {}

NODE_REGISTRY: Dict[str, Any] = {
    "set_sample_code": set_sample_code_node,
    "analyze_code": analyze_code_node,
    "report_results": report_results_node,
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


@app.post("/graph/create", response_model=GraphCreateResponse)
def create_graph(req: GraphCreateRequest):
    """Create a new graph with specified nodes and edges."""
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
    GRAPHS[graph_id] = {"engine": engine, "start_node": req.start_node}
    
    return GraphCreateResponse(graph_id=graph_id)


@app.post("/graph/run", response_model=GraphRunResponse)
def run_graph(req: GraphRunRequest):
    """Execute a graph with given initial state."""
    graph_data = GRAPHS.get(req.graph_id)
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
    """Get the state and execution log for a completed run."""
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
    """Health check endpoint."""
    return {"message": "Workflow Engine API is running"}


@app.get("/nodes")
def list_nodes():
    """List all available node types."""
    return {"available_nodes": list(NODE_REGISTRY.keys())}