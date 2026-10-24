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
        .definition.get_outgoing_flow_for(node_id)
        // 2.1 Если не нашли исходящий поток, кидаем ошибку
        .ok_or_else(|| HandleError::NoOutgoingFlow(node_id.to_string()))?;

    // 3. Добавляем токен на следующий узел
    let target_id = outgoing_flow.target_ref.clone();
    instance.add_token(&target_id);

    // 4. Отправляем событие TokenArrived для следующего узла
    instance
        .event_tx
        .send(ProcessEvent::TokenArrived { node_id: target_id })
        .await
        // Отлавливаем ошибку, если канал закрыт
        .map_err(|_| HandleError::ChannelClosed)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::elements::{EndEvent, FlowNode, StartEvent};
    use crate::domain::process::SequenceFlow;
    use crate::engine::handlers::test_utils::create_test_process_definition;
    use crate::engine::handlers::test_utils::create_test_process_instance;

    
    #[tokio::test]
    async fn test_handle_start_event() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "start1");

        // Вызвать handle_start_event
        let result = handle_start_event(&mut instance, "start1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён со "start1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "start1"));

        // Токен добавлен на "end1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

        // Отправлено событие TokenArrived { node_id: "end1" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
    }

    #[tokio::test]
    async fn test_handle_start_event_no_outgoing_flow() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
        ];

        let flows = vec![];

        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), "start1");

        // Вызвать handle_start_event
        let result = handle_start_event(&mut instance, "start1").await;

        // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
        assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
    }
}
