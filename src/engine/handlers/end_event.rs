use crate::{
    engine::handlers::HandleError,
    engine::instance::{ProcessEvent, ProcessInstance},
};

pub async fn handle_end_event(
    instance: &mut ProcessInstance,
    node_id: &str,
) -> Result<(), HandleError> {
    // 1. Убираем токен с EndEvent
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flows: Vec<_> = instance
        .definition
        .sequence_flows
        .iter()
        .filter(|flow| flow.source_ref == node_id)
        .collect();

    // 3. Проверить количество flows
    match outgoing_flows.len() {
        // если нет исходящих потоков, то Ок
        0 => {}
        // если есть, то выкидываем ошибку
        _ => return Err(HandleError::EndEventHasOutgoingFlows(node_id.to_string())),
    };

    // 4. Проверить, есть ли ещё токены в процессе:
    if instance.state.current_tokens.is_empty() {
        // Процесс завершён!
        // 5. Отправляем событие Complete
        instance
            .event_tx
            .send(ProcessEvent::Complete)
            .await
            // Отлавливаем ошибку, если канал закрыт
            .map_err(|_| HandleError::ChannelClosed)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::elements::{EndEvent, FlowNode, StartEvent, Task};
    use crate::domain::process::{ProcessDefinition, SequenceFlow};
    use crate::domain::state::{InstanceState, Token};
    use crate::engine::handlers::handle_node;
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
        flow_nodes.insert(
            "end1".to_string(),
            FlowNode::EndEvent(EndEvent {
                id: "end1".to_string(),
            }),
        );

        let sequence_flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "task1".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];

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
    async fn test_handle_end_event() {
        let mut instance = create_test_process_instance(create_test_process_definition(), "start1");

        // Выполнить процесс до end1
        let _ = handle_node(&mut instance, "start1").await;
        let _ = handle_node(&mut instance, "task1").await;

        // Вызвать handle_end_event
        let result = handle_end_event(&mut instance, "end1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён с "end1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

        // Все токены ушли
        assert_eq!(
            instance.state.current_tokens.len(),
            0,
            "Остались токены: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие Complete
        // Получаем все события из канала (их будет 3: от start1, task1 и end1)
        let event1 = instance.event_rx.recv().await.unwrap();
        let event2 = instance.event_rx.recv().await.unwrap();
        let event3 = instance.event_rx.recv().await.unwrap();
        
        // Первое событие должно быть TokenArrived для task1
        match event1 {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "task1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
        
        // Второе событие должно быть TokenArrived для end1
        match event2 {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
        
        // Третье событие должно быть Complete
        assert!(matches!(event3, ProcessEvent::Complete));
    }

    #[tokio::test]
    async fn test_handle_end_event_has_flow() {
        let mut flow_nodes = HashMap::new();
        flow_nodes.insert(
            "end1".to_string(),
            FlowNode::EndEvent(EndEvent {
                id: "end1".to_string(),
            }),
        );

        let definition = ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows: vec![SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "end1".to_string(),
                target_ref: "task1".to_string(),
                condition: None,
            }],
            start_events: vec![],
        };

        let mut instance = create_test_process_instance(definition, "end1");

        // Вызвать handle_end_event
        let result = handle_end_event(&mut instance, "end1").await;

        // Проверить, что вернулась ошибка HandleError::EndEventHasOutgoingFlows
        assert!(matches!(result, Err(HandleError::EndEventHasOutgoingFlows(_))));
    }
}
