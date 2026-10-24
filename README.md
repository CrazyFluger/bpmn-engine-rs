# BPMN Engine

A BPMN 2.0 process engine implementation in Rust, built as a learning project to deeply understand BPMN 2.0 specification and reactive execution models.

## Features

### Currently Implemented

- **Domain Model** — Full BPMN 2.0 element coverage:
  - Events: StartEvent, EndEvent, IntermediateEvent
  - Tasks: Task, ServiceTask, UserTask
  - Gateways: ExclusiveGateway, ParallelGateway, InclusiveGateway
  - SubProcesses with recursive nesting
  - SequenceFlow with conditional expressions

- **XML Parser** — BPMN 2.0 XML parsing with validation:
  - Two-stage mapping (XML → Domain Model)
  - Reference validation (all flow references must be valid)
  - StartEvent presence validation
  - Uses `quick-xml` + `serde` for deserialization

- **Execution Engine** — Hybrid reactive execution model:
  - Each process instance runs as a separate tokio task
  - Event loop with token-based execution
  - State persistence after each transition
  - Support for multiple concurrent instances
  - Expression evaluation with `evalexpr` library (comparisons, logical operators, arithmetic, variables)

- **Persistence** — Trait-based abstraction:
  - `StateStore` trait for testability
  - `InMemoryStore` for tests
  - `PostgresStore` for production (PostgreSQL with sqlx)

- **Versioning** — Simple version management:
  - Running instances stay on their deployment version
  - New instances use the latest version
  - Multiple versions can coexist

## Requirements

- Rust 2024 edition
- PostgreSQL (for production use)
- Tokio runtime for async execution

## Building

```bash
cargo build
```

## Running Tests

```bash
cargo test
```

All tests use `InMemoryStore` and don't require PostgreSQL.

## Usage

### CLI Commands

Deploy a BPMN process:
```bash
cargo run -- deploy --file path/to/process.bpmn
```

Start a process instance:
```bash
cargo run -- start --id process-id
```

### Environment Variables

- `DATABASE_URL` — PostgreSQL connection string (default: `postgresql://localhost/bpmn`)

### Database Setup

```bash
psql $DATABASE_URL -f src/store/schema.sql
```

## Architecture

```
src/
├── main.rs              # CLI entry point
├── lib.rs               # Module exports
├── domain/              # BPMN domain model
│   ├── elements.rs      # FlowNode types (events, tasks, gateways)
│   ├── process.rs       # ProcessDefinition, SequenceFlow
│   ├── expression.rs    # Condition expressions (data structures only)
│   └── state.rs         # InstanceState, Token
├── parser/              # BPMN XML parsing
│   ├── xml_types.rs     # XML deserialization types
│   ├── mapper.rs        # XML → Domain mapping
│   └── validator.rs     # Process validation
├── engine/              # Execution engine
│   ├── handlers/        # Node execution handlers (modular structure)
│   │   ├── mod.rs       # HandleError enum and common types
│   │   ├── start_event.rs
│   │   ├── end_event.rs
│   │   ├── task.rs
│   │   ├── exclusive_gateway.rs
│   │   ├── parallel_gateway.rs
│   │   ├── inclusive_gateway.rs
│   │   ├── intermediate_event.rs
│   │   ├── service_task.rs
│   │   ├── user_task.rs
│   │   └── sub_process.rs
│   ├── instance.rs      # ProcessInstance, event loop
│   ├── expression_evaluator.rs  # Expression evaluation logic
│   └── executor.rs      # Engine::start(), instance spawning
├── store/               # Persistence layer
│   ├── store_trait.rs   # StateStore trait
│   ├── memory.rs        # InMemoryStore for tests
│   └── postgres.rs      # PostgresStore implementation
└── timer/               # Timer service
    └── service.rs       # Timer polling
```

### Handler Module Structure

Each handler is implemented in a separate module with integrated unit tests:
- **Modular design** — handlers are split into individual files for better organization
- **Unit tests included** — each handler module contains its own tests
- **Error handling** — all handlers return `Result<(), HandleError>` with consistent error types

Currently implemented handlers:
- ✅ `start_event` — handles StartEvent nodes
- ✅ `end_event` — handles EndEvent nodes with process completion
- ✅ `task` — handles Task nodes
- ✅ `exclusive_gateway` — handles ExclusiveGateway nodes with conditional routing and default flow fallback
- ✅ `parallel_gateway` — handles ParallelGateway nodes with fork (split), join (sync), and join+fork semantics; waits for all incoming tokens before activating outgoing flows
- ⏳ `inclusive_gateway` — planned: inclusive decision points

### Expression Engine

The engine supports BPMN 2.0 condition expressions using the `evalexpr` library. Expressions can reference process variables and use:

**Supported operations:**
- Comparisons: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical operators: `&&` (and), `||` (or), `!` (not)
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Variable access from process state

**Example expressions:**
```
amount > 1000
status == "approved"
amount > 1000 && status == "approved"
!(status == "rejected")
amount * 2 == 2000
(a > 0 && b < 10) || c == true
```

Variables are automatically converted from `serde_json::Value` to `evalexpr::Value`, supporting numbers, strings, booleans, and arrays. Nested objects are converted to their string representation (full nested object support is planned for future versions).

### Key Concepts

- **Token-based execution** — Tokens flow through the process graph, activating nodes
- **Event loop** — Each instance runs in its own tokio task with an internal event loop
- **State persistence** — State is saved to the database after each event transition
- **Trait abstractions** — `StateStore` trait allows swapping implementations for testing

## Roadmap

### Planned Features

- [ ] Timer events (intermediate catch events with delays)
- [ ] Message events and signal events
- [ ] ServiceTask implementation with external callbacks
- [ ] UserTask with wait/complete semantics
- [ ] Compensation and transaction handling
- [ ] Pools and lanes (collaboration diagrams)
- [ ] Process instance migration between versions
- [ ] REST API for process management
- [ ] Web-based process monitoring UI

### Learning Goals

- Deep understanding of BPMN 2.0 specification
- Reactive programming with tokio
- Async Rust patterns
- Database persistence with sqlx
- Test-driven development (TDD)

## Contributing

This is a learning project, but contributions and discussions are welcome! Feel free to open issues for:
- Bug reports
- Feature suggestions
- Questions about BPMN 2.0 implementation
- Code review feedback

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

Copyright 2026 Anthon S Litvinenko
