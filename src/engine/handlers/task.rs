use crate::{
    engine::handlers::HandleError,
    engine::instance::ProcessInstance,
};

pub async fn handle_task(
    instance: &mut ProcessInstance,
    node_id: &str,
) -> Result<(), HandleError> {
    // 1. Удалить токен с текущего узла (Task)
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flows: Vec<_> = instance
        .definition
        .get_outgoing_flows_for(node_id);

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
    instance.add_token(&target_id).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::elements::{EndEvent, FlowNode, Task};
    use crate::domain::process::SequenceFlow;
    use crate::engine::handlers::test_utils::*;
    use crate::engine::instance::ProcessEvent;


    #[tokio::test]
    async fn test_handle_task() {
        let nodes = vec![
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["task1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        assert!(result.is_ok(), "Ожидался Ok, но получен Err: {:?}", result.err());

        // Токен удалён с "task1"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "task1"));

        // Токен добавлен на "end1"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "end1"));

        // Отправлено событие TokenArrived { node_id: "end1" }
        let event = instance.event_rx.recv().await.unwrap();
        
        // Событие TokenArrived для end1
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "end1");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }
    }

    #[tokio::test]
    async fn test_handle_task_no_outgoing_flow() {
        let nodes = vec![
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
        ];
        let flows = vec![];
        let current_tokens = Some(vec!["task1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        // Проверить, что вернулась ошибка HandleError::NoOutgoingFlow
        assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));
    }

    #[tokio::test]
    async fn test_handle_task_multiple_outgoing_flow() {
        let nodes = vec![
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end2".to_string() }),
        ];
        let flows = vec![
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
        let current_tokens = Some(vec!["task1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Вызвать handle_task
        let result = handle_task(&mut instance, "task1").await;

        // Проверить, что вернулась ошибка HandleError::MultipleOutgoingFlows
        assert!(matches!(result, Err(HandleError::MultipleOutgoingFlows(_))));
    }
}
