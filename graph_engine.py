class GraphEngine:
    
    def __init__(self):
        self.nodes = {}
        self.edges = {}
    
    def add_node(self, name, func):
        """Register a node function with a given name."""
        self.nodes[name] = func
        print(f"Added node: {name}")
    
    def add_edge(self, from_node, to_node):
        """Connect two nodes with an edge."""
        if from_node not in self.nodes:
            raise ValueError(f"Node '{from_node}' not found")
        if to_node not in self.nodes:
            raise ValueError(f"Node '{to_node}' not found")
        
        self.edges[from_node] = to_node
        print(f"Added edge: {from_node} -> {to_node}")
    
    def run(self, start_node, state):
        """Execute nodes sequentially starting from start_node."""
        if start_node not in self.nodes:
            raise ValueError(f"Start node '{start_node}' not found")
        
        current_node = start_node
        
        while current_node:
            print(f"\nExecuting node: {current_node}")
            
            func = self.nodes[current_node]
            state = func(state)
            
            print(f"State after {current_node}: {state}")
            
            current_node = self.edges.get(current_node)
        
        print("\nWorkflow execution completed!")
        return state