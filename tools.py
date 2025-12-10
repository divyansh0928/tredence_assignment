tools = {}


def register_tool(name, func):
    """Register a tool function under a given name."""
    tools[name] = func
    print(f"Registered tool: {name}")


def get_tool(name):
    """Retrieve a tool function by name. Raise a KeyError if not found."""
    if name not in tools:
        raise KeyError(f"Tool '{name}' not found in registry")
    return tools[name]


def detect_smells(code: str) -> dict:
    """
    Dummy implementation of a code smell detector.
    For now, just return a fixed number of issues or something based on simple rules.
    """
    issues = 0

    if "TODO" in code:
        issues += 1
    if "print(" in code:
        issues += 1
    if "FIXME" in code:
        issues += 1
    if len(code.split('\n')) > 50:  # Long functions
        issues += 1
    
    return {"issues": issues}

register_tool("detect_smells", detect_smells)