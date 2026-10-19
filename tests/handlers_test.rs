use std::collections::HashMap;

use bpmn_engine_rs::domain::elements::{EndEvent, FlowNode, StartEvent, Task};
use bpmn_engine_rs::domain::process::{ProcessDefinition, SequenceFlow};
use bpmn_engine_rs::domain::state::{InstanceState, Token};
use bpmn_engine_rs::engine::handlers::{HandleError, handle_end_event, handle_node, handle_start_event, handle_task};
use bpmn_engine_rs::engine::instance::{ProcessEvent, ProcessInstance};
use bpmn_engine_rs::store::memory::InMemoryStore;
use uuid::Uuid;

// Штатная проверка start event
#[tokio::test]
async fn test_handle_start_event() {
    // 1. Создать тестовый ProcessDefinition
    // 2. Создать ProcessInstance с этим определением
    let mut instance = create_process_instance(create_process_definition(), "start1");

    // 3. Вызвать handle_start_event(&mut instance, "start1")
    let result = handle_start_event(&mut instance, "start1").await;

    assert!(result.is_ok(), "Ожилася Ок, но получен Err:: {:?}", result.err());

    // 4. Проверить результаты:
    //    - Токен удалён со "start1" (state.current_tokens не содержит "start1")
    assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "start1"));

    //    - Токен добавлен на "task1" (state.current_tokens содержит "task1")
    assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));

    //    - Отправлено событие TokenArrived { node_id: "task1" }
    let event = instance.event_rx.recv().await.unwrap();
    match event {
        ProcessEvent::TokenArrived { node_id } => {
            assert_eq!(node_id, "task1");
        }
        _ => {}
    }
}

// Тест, когда нету Sequence Flow
#[tokio::test]
async fn test_handle_start_event_no_outgoing_flow() {
    // Создать ProcessDefinition без sequence flows
    let mut instance = create_process_instance(create_process_definition_without_sequence_flow(), "start1");
    // Вызвать handle_start_event
    let result = handle_start_event(&mut instance, "start1").await;

    // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
    assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
}

// Интеграционный тест start event через handle_node()
#[tokio::test]
async fn test_handle_node_start_event() {
    let mut instance = create_process_instance(create_process_definition(), "start1");
    let result = handle_node(&mut instance, "start1").await;
    // Проверки
    assert!(result.is_ok(), "Ожидался Ок, но получен Err:: {:?}", result.err());
    assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "start1"));
    assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));
    let event = instance.event_rx.recv().await.unwrap();
    match event {
        ProcessEvent::TokenArrived { node_id } => {
            assert_eq!(node_id, "task1");
        }
        _ => {}
    }
}

// Штатная проверка task
#[tokio::test]
async fn test_handle_task() {
    // 1. Создать тестовый ProcessDefinition
    // 2. Создать ProcessInstance с этим определением
    let mut instance = create_process_instance(create_process_definition(), "task1");

    // 3. Вызвать handle_task(&mut instance, "task1")
    let result = handle_task(&mut instance, "task1").await;

    assert!(result.is_ok(), "Ожилася Ок, но получен Err:: {:?}", result.err());

    // 4. Проверить результаты:
    //    - Токен удалён со "task1" (state.current_tokens не содержит "start1")
    assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));

    //    - Токен добавлен на "end1" (state.current_tokens содержит "end1")
    assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

    //    - Отправлено событие TokenArrived { node_id: "end1" }
    let event = instance.event_rx.recv().await.unwrap();
    match event {
        ProcessEvent::TokenArrived { node_id } => {
            assert_eq!(node_id, "end1");
        }
        _ => {}
    }
}

// Тест, когда нету Sequence Flow
#[tokio::test]
async fn test_handle_task_no_outgoing_flow() {
    // Создать ProcessDefinition без sequence flows
    let mut instance = create_process_instance(create_process_definition_without_sequence_flow(), "task1");
    // Вызвать handle_start_event
    let result = handle_start_event(&mut instance, "task1").await;
    // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
    assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
}

// Тест, когда много Sequence Flow
#[tokio::test]
async fn test_handle_task_multiple_outgoing_flow() {
    // Создать ProcessDefinition без sequence flows
    let mut instance = create_process_instance(create_process_definition_task_multiple_flows(), "task1");
    // Вызвать handle_start_event
    let result = handle_task(&mut instance, "task1").await;
    // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
    assert!(matches!(result, Err(HandleError::MultipleOutgoingFlows(_))));
}

// Интеграционный тест task через handle_node()
#[tokio::test]
async fn test_handle_node_task() {
    let mut instance = create_process_instance(create_process_definition(), "task1");
    let result = handle_node(&mut instance, "task1").await;
    // Проверки
    assert!(result.is_ok(), "Ожидался Ок, но получен Err:: {:?}", result.err());
    assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));
    assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));
    let event = instance.event_rx.recv().await.unwrap();
    match event {
        ProcessEvent::TokenArrived { node_id } => {
            assert_eq!(node_id, "end1");
        }
        _ => {}
    }
}

// Штатная проверка End Event
#[tokio::test]
async fn test_handle_end_event() {
    // 1. Создать тестовый ProcessDefinition
    // 2. Создать ProcessInstance с этим определением
    let mut instance = create_process_instance(create_process_definition(), "end1");

    // 3. Вызвать handle_task(&mut instance, "end1")
    let result = handle_end_event(&mut instance, "end1").await;

    assert!(result.is_ok(), "Ожилася Ок, но получен Err:: {:?}", result.err());

    // 4. Проверить результаты:
    //    - Токен удалён со "end1" (state.current_tokens не содержит "end1")
    assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

    // Все токены ушли
    assert_eq!(instance.state.current_tokens.len(), 0);

    //    - Отправлено событие TokenArrived { node_id: "end1" }
    let event = instance.event_rx.recv().await.unwrap();
    assert!(matches!(event, ProcessEvent::Complete));
}

// Проверка End Event на исходящие потоки
#[tokio::test]
async fn test_handle_end_event_has_flow() {
    let mut instance = create_process_instance(create_process_definition_end_event_has_outgoing_flow(), "end1");

    // 3. Вызвать handle_task(&mut instance, "end1")
    let result = handle_end_event(&mut instance, "end1").await;

    assert!(matches!(result, Err(HandleError::EndEventHasOutgoingFlows(_))));
}


// Создать тестовый ProcessDefinition
//    - StartEvent с id="start1"
//    - Task с id="task1"
//    - SequenceFlow: start1 → task1
fn create_process_definition() -> ProcessDefinition {
    let mut flow_nodes = HashMap::new();
    flow_nodes.insert(
        "start1".to_string(),
        FlowNode::StartEvent(StartEvent {
            id: "start1".to_string(),
        }),
    );
    flow_nodes.insert(
        "task1".to_string(),
        FlowNode::Task(Task {
            id: "task1".to_string(),
            name: None,
        }),
    );
    flow_nodes.insert(
        "end1".to_string(),
        FlowNode::EndEvent(EndEvent {
            id: "end1".to_string()
        }),
    );
    let mut sequence_flows = vec![];
    sequence_flows.push(SequenceFlow {
        id: "flow1".to_string(),
        source_ref: "start1".to_string(),
        target_ref: "task1".to_string(),
        condition: None,
    });
    sequence_flows.push(SequenceFlow {
        id: "flow2".to_string(),
        source_ref: "task1".to_string(),
        target_ref: "end1".to_string(),
        condition: None,
    });

    ProcessDefinition {
        id: "test-process".to_string(),
        version: 1,
        name: Some("Test".to_string()),
        flow_nodes,
        sequence_flows,
        start_events: vec!["start1".to_string()],
    }
}

// Создать тестовый ProcessDefinition без Sequence Flow
//    - StartEvent с id="start1"
//    - Task с id="task1"
fn create_process_definition_without_sequence_flow() -> ProcessDefinition {
    let mut flow_nodes = HashMap::new();
    flow_nodes.insert(
        "start1".to_string(),
        FlowNode::StartEvent(StartEvent {
            id: "start1".to_string(),
        }),
    );
    flow_nodes.insert(
        "task1".to_string(),
        FlowNode::Task(Task {
            id: "task1".to_string(),
            name: None,
        }),
    );
    let sequence_flows = vec![];

    ProcessDefinition {
        id: "test-process".to_string(),
        version: 1,
        name: Some("Test".to_string()),
        flow_nodes,
        sequence_flows,
        start_events: vec!["start1".to_string()],
    }
}

// Создать тестовый ProcessDefinition с несколькими flow для Task
fn create_process_definition_task_multiple_flows() -> ProcessDefinition {
    let mut flow_nodes = HashMap::new();
    flow_nodes.insert(
        "start1".to_string(),
        FlowNode::StartEvent(StartEvent {
            id: "start1".to_string(),
        }),
    );
    flow_nodes.insert(
        "task1".to_string(),
        FlowNode::Task(Task {
            id: "task1".to_string(),
            name: None,
        }),
    );
    flow_nodes.insert(
        "end1".to_string(),
        FlowNode::EndEvent(EndEvent {
            id: "end1".to_string()
        }),
    );
    let mut sequence_flows = vec![];
    sequence_flows.push(SequenceFlow {
        id: "flow1".to_string(),
        source_ref: "start1".to_string(),
        target_ref: "task1".to_string(),
        condition: None,
    });
    sequence_flows.push(SequenceFlow {
        id: "flow2".to_string(),
        source_ref: "task1".to_string(),
        target_ref: "end1".to_string(),
        condition: None,
    });
    sequence_flows.push(SequenceFlow {
        id: "flow3".to_string(),
        source_ref: "task1".to_string(),
        target_ref: "end1".to_string(),
        condition: None,
    });

    ProcessDefinition {
        id: "test-process".to_string(),
        version: 1,
        name: Some("Test".to_string()),
        flow_nodes,
        sequence_flows,
        start_events: vec!["start1".to_string()],
    }
}

// Создать тестовый ProcessDefinition с несколькими flow для Task
fn create_process_definition_end_event_has_outgoing_flow() -> ProcessDefinition {
    let mut flow_nodes = HashMap::new();
    flow_nodes.insert(
        "end1".to_string(),
        FlowNode::EndEvent(EndEvent {
            id: "end1".to_string()
        }),
    );
    let mut sequence_flows = vec![];
    sequence_flows.push(SequenceFlow {
        id: "flow1".to_string(),
        source_ref: "end1".to_string(),
        target_ref: "none".to_string(),
        condition: None,
    });

    ProcessDefinition {
        id: "test-process".to_string(),
        version: 1,
        name: Some("Test".to_string()),
        flow_nodes,
        sequence_flows,
        start_events: vec!["start1".to_string()],
    }
}

// Создать ProcessInstance с определением из create_process_definition
//    - Использовать InMemoryStore
//    - Добавить начальный токен на "start1"
fn create_process_instance(definition: ProcessDefinition, start_node_id: &str) -> ProcessInstance {
    let store = Box::new(InMemoryStore::new());
    let state = InstanceState {
        current_tokens: vec![Token {
            node_id: start_node_id.to_string(),
        }],
        variables: HashMap::new(),
    };

    ProcessInstance::restore(
        Uuid::new_v4(),
        definition,
        store,
        state,
    )
}
