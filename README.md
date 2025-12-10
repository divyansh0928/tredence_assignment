# Workflow Engine

A production-ready workflow/graph engine in Python that executes nodes sequentially with support for branching, looping, and async operations. Demonstrated through a comprehensive Data Quality Pipeline.

## Description

The workflow engine allows you to:
- Register Python functions as workflow nodes
- Connect nodes through edges for sequential execution
- Add conditional branching and looping logic
- Execute workflows synchronously or asynchronously
- Monitor progress in real-time via REST API and WebSocket
- Store workflow definitions persistently in SQLite

The system is demonstrated through a **Data Quality Pipeline** that processes datasets through iterative quality improvement, detecting and fixing data anomalies until quality targets are met.

## How to Run

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the API Server
```bash
python -m uvicorn app.main:app --reload --port 8000
```

### 3. Run the Data Quality Pipeline Demo
```bash
python complete_data_quality_demo.py
```

The demo will:
- Create a 7-stage data quality workflow
- Run 3 test scenarios with different dataset sizes (300, 600, 1000 records)
- Show real-time progress monitoring
- Display quality improvement metrics and iteration history

### 4. API Endpoints
- **REST API**: `http://localhost:8000/docs` (Interactive documentation)
- **Create Graph**: `POST /graph/create`
- **Run Async**: `POST /graph/run/async`
- **Monitor Progress**: `GET /execution/{execution_id}`
- **WebSocket**: `ws://localhost:8000/ws/execution/{execution_id}`

## What the Workflow Engine Supports

### Core Features
- ✅ **Node Registration** - Python functions as workflow nodes
- ✅ **Sequential Execution** - Ordered processing through connected nodes
- ✅ **Conditional Branching** - Decision points based on state conditions
- ✅ **Iterative Looping** - Repeat processing until targets are met
- ✅ **State Management** - Shared state dictionary across all nodes

### Execution Modes
- ✅ **Synchronous Execution** - Immediate results via REST API
- ✅ **Asynchronous Execution** - Background processing with progress tracking
- ✅ **Concurrent Processing** - Multiple workflows running simultaneously
- ✅ **Real-time Monitoring** - Live progress via WebSocket and REST

### Advanced Capabilities
- ✅ **Mixed Async/Sync Nodes** - Supports both sync and async functions
- ✅ **Persistent Storage** - SQLite database for workflow definitions
- ✅ **Tool Registry** - Reusable functions accessible across workflows
- ✅ **Error Handling** - Comprehensive error management and recovery
- ✅ **Progress Callbacks** - Real-time execution notifications

### Data Quality Pipeline Demo
- ✅ **7-Stage Processing** - Initialize → Profile → Detect → Generate → Apply → Evaluate → Report
- ✅ **Quality Issue Detection** - Null values, invalid emails, outliers, inconsistencies
- ✅ **Iterative Improvement** - Automatic looping until quality targets achieved
- ✅ **Scalable Processing** - Consistent performance across dataset sizes
- ✅ **Comprehensive Reporting** - Detailed metrics and improvement tracking

## What I Would Improve With More Time

- **Visual Workflow Designer** - Web-based drag-and-drop interface for creating workflows with conditional edge logic, parallel execution support, workflow templates, and version control capabilities

- **Distributed Scalability** - Multi-node cluster support with job distribution, resource management, Redis caching layer, PostgreSQL scaling, and load balancing across multiple API instances

- **Enterprise Security & Monitoring** - Authentication and authorization system, comprehensive audit logging, real-time monitoring dashboard, alerting system for failures/completions, and API rate limiting

- **Enhanced Developer Experience** - SDK/client libraries for multiple languages, CLI tool for workflow management, built-in testing framework, auto-generated documentation, and IDE integration

- **Advanced Data Quality Intelligence** - ML-based anomaly detection, complete data lineage tracking, quality rule marketplace, direct database/API connectors, and automated self-healing remediation with confidence scoring