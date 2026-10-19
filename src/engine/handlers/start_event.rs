use crate::{
    engine::handlers::HandleError,
    engine::instance::{ProcessEvent, ProcessInstance},
};

pub async fn handle_start_event(
    instance: &mut ProcessInstance,
    node_id: &str,
) -> Result<(), HandleError> {
    // 1. Удалить токен с узла StartEvent
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flow = instance
        .definition
        .sequence_flows
        .iter()
        .find(|flow| flow.source_ref == node_id);

    // 3.1 Если не нашли исходящий поток, кидаем ошибку
    let flow = outgoing_flow.ok_or_else(|| HandleError::NoOutgoingFlow(node_id.to_string()))?;

    // 3.2 Если нашли исходящий поток, перемещаем токен дальше
    let target_id = flow.target_ref.clone();

    // 4. Добавляем токен на следующий узел
    instance.add_token(&target_id);

    // 5. Отправляем событие TokenArrived для следующего узла
    instance
        .event_tx
        .send(ProcessEvent::TokenArrived {
            node_id: target_id.clone(),
        })
        .await
        // Отлавливаем ошибку, если канал закрыт
        .map_err(|_| HandleError::ChannelClosed)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::elements::{FlowNode, StartEvent, Task};
    use crate::domain::process::{ProcessDefinition, SequenceFlow};
    use crate::domain::state::{InstanceState, Token};
    use crate::engine::instance::ProcessInstance;
    use crate::store::memory::InMemoryStore;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn create_test_process_definition() -> ProcessDefinition {
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

        let sequence_flows = vec![SequenceFlow {
            id: "flow1".to_string(),
            source_ref: "start1".to_string(),
            target_ref: "task1".to_string(),
            condition: None,
        }];

        ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows,
            start_events: vec!["start1".to_string()],
        }
    }

    fn create_test_process_instance(
        definition: ProcessDefinition,
        start_node_id: &str,
    ) -> ProcessInstance {
        let store = Box::new(InMemoryStore::new());
        let state = InstanceState {
            current_tokens: vec![Token {
                node_id: start_node_id.to_string(),
            }],
            variables: HashMap::new(),
        };

        ProcessInstance::restore(Uuid::new_v4(), definition, store, state)
    }

    #[tokio::test]
    async fn test_handle_start_event() {
        let mut instance = create_test_process_instance(create_test_process_definition(), "start1");

        // Вызвать handle_start_event
        let result = handle_start_event(&mut instance, "start1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён со "start1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "start1"));

        // Токен добавлен на "task1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));

        // Отправлено событие TokenArrived { node_id: "task1" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "task1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
    }

    #[tokio::test]
    async fn test_handle_start_event_no_outgoing_flow() {
        let mut flow_nodes = HashMap::new();
        flow_nodes.insert(
            "start1".to_string(),
            FlowNode::StartEvent(StartEvent {
                id: "start1".to_string(),
            }),
        );

        let definition = ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows: vec![],
            start_events: vec!["start1".to_string()],
        };

        let mut instance = create_test_process_instance(definition, "start1");

        // Вызвать handle_start_event
        let result = handle_start_event(&mut instance, "start1").await;

        // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
        assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
    }
}
