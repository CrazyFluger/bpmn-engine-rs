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
    use crate::domain::elements::{EndEvent, FlowNode};
    use crate::domain::elements::StartEvent;
    use crate::domain::process::SequenceFlow;
    use crate::engine::handlers::test_utils::create_test_process_definition;
    use crate::engine::handlers::test_utils::create_test_process_instance;


    #[tokio::test]
    async fn test_handle_end_event() {
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
        let current_tokens = Some(vec!["end1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Вызвать handle_end_event
        let result = handle_end_event(&mut instance, "end1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён с "end1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

        // Все токены ушли
        assert_eq!(
            instance.state.current_tokens.len(), 0,
            "Остались токены: {:?}", instance.state.current_tokens
        );

        // Отправлено событие Complete
        let event = instance.event_rx.recv().await.unwrap();
        
        // Третье событие должно быть Complete
        assert!(matches!(event, ProcessEvent::Complete));
    }

    /// Финальное событие имеет исходящие потоки - это ошибка
    #[tokio::test]
    async fn test_handle_end_event_has_flow() {
        let nodes = vec![
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "end1".to_string(),
                target_ref: "task1".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["end1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Вызвать handle_end_event
        let result = handle_end_event(&mut instance, "end1").await;

        // Проверить, что вернулась ошибка HandleError::EndEventHasOutgoingFlows
        assert!(matches!(result, Err(HandleError::EndEventHasOutgoingFlows(_))));
    }
}
