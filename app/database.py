import sqlite3
import json
import pickle
import base64
from typing import Dict, Any, List, Optional
from contextlib import contextmanager
import os

DATABASE_PATH = "workflow_graphs.db"

def init_database():
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS graphs (
                graph_id TEXT PRIMARY KEY,
                nodes TEXT NOT NULL,
                edges TEXT NOT NULL,
                start_node TEXT NOT NULL,
                engine_data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def serialize_engine(engine) -> str:
    engine_data = {
        'nodes': list(engine.nodes.keys()),
        'edges': engine.edges,
        'branch_conditions': {}
    }
    pickled_data = pickle.dumps(engine_data)
    return base64.b64encode(pickled_data).decode('utf-8')

def deserialize_engine(engine_data: str, node_registry: Dict[str, Any]):
    from graph_engine import GraphEngine
    
    pickled_data = base64.b64decode(engine_data.encode('utf-8'))
    engine_structure = pickle.loads(pickled_data)
    
    engine = GraphEngine()
    
    for node_name in engine_structure['nodes']:
        if node_name in node_registry:
            engine.add_node(node_name, node_registry[node_name])
    
    for from_node, to_node in engine_structure['edges'].items():
        engine.add_edge(from_node, to_node)
    
    return engine

def save_graph(graph_id: str, nodes: List[str], edges: List[Dict[str, str]], 
               start_node: str, engine) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            nodes_json = json.dumps(nodes)
            edges_json = json.dumps(edges)
            engine_blob = serialize_engine(engine)
            
            cursor.execute("""
                INSERT INTO graphs (graph_id, nodes, edges, start_node, engine_data)
                VALUES (?, ?, ?, ?, ?)
            """, (graph_id, nodes_json, edges_json, start_node, engine_blob))
            
            conn.commit()
            return True
    except Exception as e:
        print(f"Error saving graph: {e}")
        return False

def load_graph(graph_id: str, node_registry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT nodes, edges, start_node, engine_data
                FROM graphs
                WHERE graph_id = ?
            """, (graph_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Deserialize data
            nodes = json.loads(row['nodes'])
            edges = json.loads(row['edges'])
            start_node = row['start_node']
            engine = deserialize_engine(row['engine_data'], node_registry)
            
            return {
                'engine': engine,
                'start_node': start_node,
                'nodes': nodes,
                'edges': edges
            }
    except Exception as e:
        print(f"Error loading graph: {e}")
        return None

def list_graphs() -> List[Dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT graph_id, nodes, start_node, created_at
                FROM graphs
                ORDER BY created_at DESC
            """)
            
            graphs = []
            for row in cursor.fetchall():
                graphs.append({
                    'graph_id': row['graph_id'],
                    'nodes': json.loads(row['nodes']),
                    'start_node': row['start_node'],
                    'created_at': row['created_at']
                })
            
            return graphs
    except Exception as e:
        print(f"Error listing graphs: {e}")
        return []

def delete_graph(graph_id: str) -> bool:
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM graphs WHERE graph_id = ?", (graph_id,))
            conn.commit()
            
            return cursor.rowcount > 0
    except Exception as e:
        print(f"Error deleting graph: {e}")
        return False

init_database()