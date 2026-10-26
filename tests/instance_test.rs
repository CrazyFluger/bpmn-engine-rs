use std::{collections::HashMap, sync::Arc};

use bpmn_engine_rs::domain::state::InstanceState;
use bpmn_engine_rs::store::store_trait::StateStore;
use bpmn_engine_rs::{
    domain::{
        elements::{EndEvent, FlowNode, StartEvent, Task},
        process::{ProcessDefinition, SequenceFlow},
    }, engine::instance::{ProcessError, ProcessInstance}, store::{memory::InMemoryStore, store_trait::ArcStore},
};
use uuid::Uuid;

#[tokio::test]
async fn test_run_simple_process() {
    // Создаём StateStore снаружи
    let store = Arc::new(InMemoryStore::new());
    let store_for_test = store.clone();

    // Создаём ProcessInstance c ProcessDefinition: Start → Task → End
    let instance = create_process_instance(create_process_definition(), store);
    assert!(instance.is_ok());
    let mut instance = instance.unwrap();
    
    // 2. Запустить run()
    let result = instance.start().await;
    assert!(result.is_ok(), "{}", result.unwrap_err());

    // 3. Проверить:
    //    - current_tokens пустой
    assert_eq!(instance.state.current_tokens.len(), 0);
    //    - Состояние сохранено в StateStore
    //      Проверяем через store_for_test   
    let (_, _, state) = store_for_test.load_instance(instance.id).await.unwrap();
    assert_eq!(state.current_tokens.len(), 0);
}

// Создать тестовый ProcessDefinition
//    - StartEvent с id="start1"
//    - Task с id="task1"
//    - SequenceFlow: start1 → task1 → end1
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

/// Создать ProcessInstance
///    - с определением из create_process_definition
///    - Использовать Arc<InMemoryStore>
fn create_process_instance(
    definition: ProcessDefinition,
    store: Arc<InMemoryStore>,
) -> Result<ProcessInstance, ProcessError> {
    let id = Uuid::new_v4();
    let state = InstanceState {
        current_tokens: vec![],
        variables: HashMap::new(),
    };
    let store = Box::new(ArcStore(store));
    let process = ProcessInstance::new(
        id,
        definition.clone(),
        state,
        store,
    )?;

    Ok(process)
}