# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a distributed systems project implementing various communication and synchronization mechanisms using Python. The codebase simulates distributed processes with message passing, token ring mutual exclusion, and distributed synchronization barriers.

## Commands

### Running the System
```bash
cd references
python Launcher.py
```

### Configuration
The main configuration is in `Launcher.py:23`:
```python
launch(nbProcess=3, runningTime=3)  # 3 processes, 3 seconds runtime
```

## Architecture

### Core Components

- **Process.py**: Main process class implementing distributed algorithms
  - Inherits from `Thread` for concurrent execution
  - Uses PyEventBus3 for inter-process communication
  - Implements Lamport logical clocks for event ordering

- **Message.py**: Message hierarchy for different communication patterns
  - `BroadcastMessage`: Broadcasts to all processes
  - `DedicatedMessage`: Point-to-point communication
  - `TokenMessage`: Token circulation for mutual exclusion
  - `SyncMessage`: Synchronization barrier coordination

- **Token.py**: Simple token implementation for mutual exclusion
- **Launcher.py**: Entry point that creates and manages process lifecycle

### Communication Patterns

#### Event Bus Architecture
- Uses PyEventBus3 with `Mode.PARALLEL` for concurrent message handling
- Processes subscribe to specific message types using decorators
- All communication is asynchronous through publish/subscribe

#### Message Flow
1. **Broadcast**: `broadcast()` → `BroadcastMessage` → `on_broadcast()`
2. **Point-to-point**: `send_to()` → `DedicatedMessage` → `on_receive()`
3. **Token passing**: `send_token()` → `TokenMessage` → `on_token()`
4. **Synchronization**: `synchronize()` → `SyncMessage` → `on_sync()`

### Distributed Algorithms

#### Token Ring Mutual Exclusion
- Logical ring topology: P0 → P1 → P2 → ... → Pn-1 → P0
- Process states: `SC` (section critique request), `token` (possession)
- Algorithm: `request()` → wait for token → critical section → `release()` → `send_token()`

#### Centralized Synchronization Barrier
- Process 0 acts as coordinator
- Protocol: All processes send "READY" → Coordinator sends "SYNC_RELEASE" → All proceed
- Critical: Coordinator must handle its own SYNC_RELEASE to avoid deadlock

#### Lamport Logical Clocks
- Each process maintains `self.clock`
- Update rules:
  - Local events: `clock += 1`
  - Message reception: `clock = max(local_clock, message_timestamp) + 1`

### Threading Model
- Each process runs as independent thread
- Main loop in `run()` method with periodic actions
- Asynchronous message handlers via EventBus decorators
- Synchronization points use busy-waiting with `sleep(0.01)`

## Key Implementation Details

### Process Lifecycle
1. `__init__()`: Register with EventBus, start thread
2. `run()`: Main execution loop with timed actions
3. `stop()`: Set alive flag to false
4. `waitStopped()`: Join thread for cleanup

### Critical Sections
- Use `self.SC` flag to signal critical section requests
- Token possession required for critical section entry
- Always call `release()` after critical section to pass token

### Synchronization Barriers
- Only Process 0 counts READY messages and sends SYNC_RELEASE
- All processes (including coordinator) must wait for SYNC_RELEASE
- Reset `sync_ready_count = 0` after each barrier

## Dependencies

- `pyeventbus3`: Inter-process communication
- `threading`: Concurrent process execution
- `time`: Sleep and timing control