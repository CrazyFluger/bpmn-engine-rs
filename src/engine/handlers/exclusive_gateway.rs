use std::collections::HashMap;

use crate::domain::elements::FlowNode;
use crate::domain::expression::Expression;
use crate::engine::handlers::HandleError;
use crate::engine::instance::ProcessEvent;
use crate::engine::instance::ProcessInstance;

pub async fn handle_exclusive_gateway(
    instance: &mut ProcessInstance,
    node_id: &str,
) -> Result<(), HandleError> {
    // 1. Удалить токен с gateway
    instance.remove_token(node_id);

    // 2. Найти все исходящие flows
    let outgoing_flows: Vec<_> = instance
        .definition
        .sequence_flows
        .iter()
        .filter(|flow| flow.source_ref == node_id)
        .collect();

    // 3. Для каждого flow:
    // - Если есть condition:
    //     - Проверить условие (пока просто: condition == "true")
    //     - Если истинно — выбрать этот flow, прекратить поиск
    let condition_flow = outgoing_flows
        .iter()
        .copied()
        .find(|flow| match &flow.condition {
            Some(condition) => evaluate_condition(condition, &instance.state.variables),
            None => false,
        });

    // Находим default_flow
    let default_flow = match instance.definition.flow_nodes.get(node_id) {
        Some(FlowNode::ExclusiveGateway(node)) => node.default_flow.as_ref()
            .and_then(|default_id| outgoing_flows.iter().find(|flow| flow.id == *default_id))
            .copied(),
        _ => None,
    };

    // 4. Определяем исходящий flow
    let outgoing_flow = condition_flow
        // - Если ни одно условие не истинно, то берём default_flow
        .or(default_flow)
        // - Если нет default_flow — вернуть ошибку HandleError::NoConditionMet
        .ok_or_else(|| HandleError::NoConditionMet(node_id.to_string()))?;

    let target_id = outgoing_flow.target_ref.clone();

    // 5. Добавить токен на target выбранного flow
    instance.add_token(&target_id);

    // 6. Отправить TokenArrived событие
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

fn evaluate_condition(
    condition: &Expression,
    _variables: &HashMap<String, serde_json::Value>,
) -> bool {
    // Упрощённая версия: просто проверить строку
    match condition {
        Expression::Simple(s) => s == "true",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::elements::{EndEvent, ExclusiveGateway, FlowNode, StartEvent};
    use crate::domain::process::{ProcessDefinition, SequenceFlow};
    use crate::domain::state::{InstanceState, Token};
    use crate::engine::instance::ProcessInstance;
    use crate::store::memory::InMemoryStore;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn create_test_process_definition(
        nodes: Vec<FlowNode>,
        sequence_flows: Vec<SequenceFlow>,
    ) -> ProcessDefinition {
        let mut flow_nodes = HashMap::new();

        for node in nodes {
            flow_nodes.insert(node.id().to_string(), node);
        }

        ProcessDefinition {
            id: "test-process".to_string(),
            version: 1,
            name: Some("Test Process".to_string()),
            flow_nodes,
            sequence_flows,
            start_events: vec!["exclusive_gateway".to_string()],
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

    // Тест с истинным условием — одно условие "true", проверить, что токен пошёл по этому пути
    #[tokio::test]
    async fn test_handle_exclusive_gateway_one_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::ExclusiveGateway(ExclusiveGateway { id: "exclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end_event1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "exclusive_gateway");

        let result = handle_exclusive_gateway(&mut instance, "exclusive_gateway").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён со "exclusive_gateway"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "exclusive_gateway"));

        // Токен добавлен на "end_event1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end_event1"));

        // Отправлено событие TokenArrived { node_id: "end_event1" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end_event1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }

    }

    // Тест с несколькими условиями — несколько условий, но только одно "true", проверить выбор
    #[tokio::test]
    async fn test_handle_exclusive_gateway_two_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::ExclusiveGateway(ExclusiveGateway { id: "exclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end_event1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end_event2".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event2".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            }
        ];
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "exclusive_gateway");

        let result = handle_exclusive_gateway(&mut instance, "exclusive_gateway").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён со "exclusive_gateway"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "exclusive_gateway"));

        // Токен добавлен на "end_event1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end_event1"));

        // Отправлено событие TokenArrived { node_id: "end_event1" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end_event1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
    }

    // Тест с default flow — все условия "false", но есть default_flow, проверить, что используется default
    #[tokio::test]
    async fn test_handle_exclusive_gateway_default_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::ExclusiveGateway(ExclusiveGateway { id: "exclusive_gateway".to_string(), default_flow: Some("flow3".to_string()) }),
            FlowNode::EndEvent(EndEvent { id: "end_event1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end_event2".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end_event3".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event1".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event2".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event3".to_string(),
                condition: None,
            },
        ];
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "exclusive_gateway");

        let result = handle_exclusive_gateway(&mut instance, "exclusive_gateway").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён со "exclusive_gateway"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "exclusive_gateway"));

        // Токен добавлен на "end_event3"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end_event3"));

        // Отправлено событие TokenArrived { node_id: "end_event3" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end_event3");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
    }

    // Тест без условий и default flow — все условия "false", нет default_flow, проверить, что возвращается ошибка NoConditionMet
    #[tokio::test]
    async fn test_handle_exclusive_gateway_no_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::ExclusiveGateway(ExclusiveGateway { id: "exclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end_event1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end_event2".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event1".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "exclusive_gateway".to_string(),
                target_ref: "end_event2".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
        ];
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "exclusive_gateway");

        let result = handle_exclusive_gateway(&mut instance, "exclusive_gateway").await;

        // Проверить, что вернулась ошибка HandleError::NoConditionMet
        assert!(matches!(result, Err(HandleError::NoConditionMet(_))));
    }
}
