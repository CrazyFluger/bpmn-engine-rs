use std::collections::HashMap;

use bpmn_engine_rs::domain::state::Token;
use bpmn_engine_rs::domain::{process::ProcessDefinition, state::InstanceState};
use bpmn_engine_rs::store::{memory::InMemoryStore, store_trait::StateStore};
use uuid::Uuid;

fn get_definition(id: &str, version: u32, name: &str) -> ProcessDefinition {
    ProcessDefinition {
        id: id.to_string(),
        version,
        name: Some(name.to_string()),
        flow_nodes: HashMap::new(),
        sequence_flows: vec![],
        start_events: vec![],
    }
}

#[tokio::test]
async fn test_save_definition() {
    let store = InMemoryStore::new();
    let process = get_definition("process1", 1, "Process number 1");

    let save = store.save_definition(&process).await;
    assert!(save.is_ok())
}

#[tokio::test]
async fn test_load_definition() {
    let store = InMemoryStore::new();
    let _ = store.save_definition(&get_definition("process1", 1, "Process number 1 ver 1")).await;
    let _ = store.save_definition(&get_definition("process1", 2, "Process number 1 ver 2")).await;
    let _ = store.save_definition(&get_definition("process2", 1, "Process number 2 ver 1")).await;

    let result = store.load_definition("process1", 2).await;
    assert!(result.is_ok());

    let process = result.unwrap();
    assert_eq!(process.id, "process1".to_string());
    assert_eq!(process.version, 2);
    assert_eq!(process.name, Some("Process number 1 ver 2".to_string()));
}


fn get_instance_state() -> InstanceState {
    InstanceState {
        current_tokens: vec![],
        variables: HashMap::new(),
    }
}

#[tokio::test]
async fn test_create_definition() {
    let store = InMemoryStore::new();
    let id = Uuid::new_v4();
    let definition_id = "process1";
    let definition_version = 1;
    let state = get_instance_state();

    let save = store.create_instance(id, definition_id, definition_version, &state).await;
    assert!(save.is_ok())   
}

#[tokio::test]
async fn test_load_instance() {
    let store = InMemoryStore::new();
    let id1 = Uuid::new_v4();
    let _ = store.create_instance(id1, "process1", 1, &get_instance_state()).await;

    let id2 = Uuid::new_v4();
    let _ = store.create_instance(id2, "process1", 2, &get_instance_state()).await;

    let id3 = Uuid::new_v4();
    let _ = store.create_instance(id3, "process2", 1, &get_instance_state()).await;

    let result = store.load_instance(id2).await;
    assert!(result.is_ok());

    let (id, version, _) = result.unwrap();
    assert_eq!(id, "process1".to_string());
    assert_eq!(version, 2);
}

#[tokio::test]
async fn test_update_instance() {
    let store = InMemoryStore::new();
    let id1 = Uuid::new_v4();
    let _ = store.create_instance(id1, "process1", 1, &get_instance_state()).await;

    let id2 = Uuid::new_v4();
    let _ = store.create_instance(id2, "process1", 2, &get_instance_state()).await;

    let id3 = Uuid::new_v4();
    let _ = store.create_instance(id3, "process2", 1, &get_instance_state()).await;

    let state = InstanceState {
        current_tokens: vec![Token { node_id: "node1".to_string() }],
        variables: HashMap::new(),
    };

    let result = store.update_instance_state(id3, &state).await;
    assert!(result.is_ok());

    let result = store.load_instance(id3).await;
    assert!(result.is_ok());

    let (_, _, state) = result.unwrap();
    assert_eq!(state.current_tokens.len(), 1);

    let token = &state.current_tokens[0];
    assert_eq!(token.node_id, "node1");
}

#[tokio::test]
async fn test_get_latest_version() {
    let store = InMemoryStore::new();
    
    // Сохраняем несколько версий одного процесса
    let _ = store.save_definition(&get_definition("process1", 1, "v1")).await;
    let _ = store.save_definition(&get_definition("process1", 2, "v2")).await;
    let _ = store.save_definition(&get_definition("process1", 3, "v3")).await;
    
    // Сохраняем другой процесс
    let _ = store.save_definition(&get_definition("process2", 1, "v1")).await;
    
    // Проверяем последнюю версию process1
    let result = store.get_latest_version("process1").await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(3));
    
    // Проверяем последнюю версию process2
    let result = store.get_latest_version("process2").await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(1));
    
    // Проверяем несуществующий процесс
    let result = store.get_latest_version("process3").await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}