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
from app.database import save_graph, load_graph, list_graphs, delete_graph

app = FastAPI(title="Workflow Engine API")

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

class GraphInfo(BaseModel):
    graph_id: str
    nodes: List[str]
    start_node: str
    created_at: str

class GraphListResponse(BaseModel):
    graphs: List[GraphInfo]


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