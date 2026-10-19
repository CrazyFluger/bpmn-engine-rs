use crate::{
    engine::handlers::HandleError,
    engine::instance::{ProcessEvent, ProcessInstance},
};

pub async fn handle_task(instance: &mut ProcessInstance, node_id: &str) -> Result<(), HandleError> {
    // 1. Удалить токен с текущего узла (Task)
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
        0 => return Err(HandleError::NoOutgoingFlow(node_id.to_string())),
        // если только 1, продолжим
        1 => {}
        // если более 1, то выкидываем ошибку
        _ => return Err(HandleError::MultipleOutgoingFlows(node_id.to_string())),
    };

    // 4. Переместить токен на следующий узел
    let flow = &outgoing_flows[0];
    let target_id = flow.target_ref.clone();
    instance.add_token(&target_id);

    // 5. Отправить событие TokenArrived { node_id: target } через event_tx
    instance
        .event_tx
        .send(ProcessEvent::TokenArrived {
            node_id: target_id.clone(),
        })
        .await
        // Обработать ошибку, если нет исходящего flow или канал закрыт
        .map_err(|_| HandleError::ChannelClosed)?;

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
    async fn test_handle_task() {
        let mut instance = create_test_process_instance(create_test_process_definition(), "start1");

        // Выполнить процесс до task1
        let _ = handle_node(&mut instance, "start1").await;

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён с "task1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));

        // Токен добавлен на "end1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

        // Отправлено событие TokenArrived { node_id: "end1" }
        // Получаем все события из канала (их будет 2: одно от start1, одно от task1)
        let event1 = instance.event_rx.recv().await.unwrap();
        let event2 = instance.event_rx.recv().await.unwrap();
        
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
    }

    #[tokio::test]
    async fn test_handle_task_no_outgoing_flow() {
        let mut flow_nodes = HashMap::new();
        flow_nodes.insert(
            "task1".to_string(),
            FlowNode::Task(Task {
                id: "task1".to_string(),
                name: None,
            }),
        );

        let definition = ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows: vec![],
            start_events: vec![],
        };

        let mut instance = create_test_process_instance(definition, "task1");

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
        assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
    }

    #[tokio::test]
    async fn test_handle_task_multiple_outgoing_flow() {
        let mut flow_nodes = HashMap::new();
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
        flow_nodes.insert(
            "end2".to_string(),
            FlowNode::EndEvent(EndEvent {
                id: "end2".to_string(),
            }),
        );

        let sequence_flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "end2".to_string(),
                condition: None,
            },
        ];

        let definition = ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows,
            start_events: vec![],
        };

        let mut instance = create_test_process_instance(definition, "task1");

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        // Проверить, что вернулась ошибка HandleError::MultipleOutgoingFlows
        assert!(matches!(result, Err(HandleError::MultipleOutgoingFlows(_))));
    }
}
