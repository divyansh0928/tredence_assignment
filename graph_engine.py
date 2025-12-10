import asyncio
import inspect
from typing import Callable, Any, Dict, List, Optional, Union

class GraphEngine:
    
    def __init__(self):
        self.nodes = {}
        self.edges = {}
        self.branch_conditions = {}
    
    def add_node(self, name, func):
        self.nodes[name] = func
        print(f"Added node: {name}")
    
    def add_edge(self, from_node, to_node):
        if from_node not in self.nodes:
            raise ValueError(f"Node '{from_node}' not found")
        if to_node not in self.nodes:
            raise ValueError(f"Node '{to_node}' not found")
        
        self.edges[from_node] = to_node
        print(f"Added edge: {from_node} -> {to_node}")
    
    def add_branch(self, from_node, condition_func, target_node):
        if from_node not in self.nodes:
            raise ValueError(f"Node '{from_node}' not found")
        if target_node not in self.nodes:
            raise ValueError(f"Target node '{target_node}' not found")
        
        if from_node not in self.branch_conditions:
            self.branch_conditions[from_node] = []
        
        self.branch_conditions[from_node].append((condition_func, target_node))
        print(f"Added branch: {from_node} -> {target_node} (conditional)")
    
    def run(self, start_node, state, log=None):
        if start_node not in self.nodes:
            raise ValueError(f"Start node '{start_node}' not found")
        
        current_node = start_node
        
        while current_node:
            print(f"\nExecuting node: {current_node}")
            
            func = self.nodes[current_node]
            state = func(state)
            
            if log is not None:
                log.append({"node": current_node, "state": dict(state)})
            
            print(f"State after {current_node}: {state}")
            
            next_node = None
            
            if current_node in self.branch_conditions:
                for condition_func, target_node in self.branch_conditions[current_node]:
                    if condition_func(state):
                        next_node = target_node
                        print(f"  Branch condition met: {current_node} -> {target_node}")
                        break
            
            if next_node is None:
                next_node = self.edges.get(current_node)
                if next_node:
                    print(f"  Following edge: {current_node} -> {next_node}")
            
            current_node = next_node
        
        print("\nWorkflow execution completed!")
        return state
    
    async def run_async(self, start_node: str, state: Dict[str, Any], 
                       log: Optional[List[Dict[str, Any]]] = None,
                       progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        if start_node not in self.nodes:
            raise ValueError(f"Start node '{start_node}' not found")
        
        current_node = start_node
        
        while current_node:
            print(f"\nExecuting node: {current_node}")
            
            if progress_callback:
                await progress_callback("executing", current_node, state)
            
            func = self.nodes[current_node]
            
            if inspect.iscoroutinefunction(func):
                state = await func(state)
            else:
                state = await asyncio.get_event_loop().run_in_executor(None, func, state)
            
            if log is not None:
                log.append({"node": current_node, "state": dict(state)})
            
            print(f"State after {current_node}: {state}")
            
            if progress_callback:
                await progress_callback("completed", current_node, state)
            
            next_node = None
            
            if current_node in self.branch_conditions:
                for condition_func, target_node in self.branch_conditions[current_node]:
                    if inspect.iscoroutinefunction(condition_func):
                        condition_result = await condition_func(state)
                    else:
                        condition_result = condition_func(state)
                    
                    if condition_result:
                        next_node = target_node
                        print(f"  Branch condition met: {current_node} -> {target_node}")
                        break
            
            if next_node is None:
                next_node = self.edges.get(current_node)
                if next_node:
                    print(f"  Following edge: {current_node} -> {next_node}")
            
            current_node = next_node
        
        print("\nWorkflow execution completed!")
        return state