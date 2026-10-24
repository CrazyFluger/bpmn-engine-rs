use crate::engine::handlers::HandleError;
use crate::engine::instance::ProcessInstance;


pub async fn handle_parallel_gateway(
    instance: &mut ProcessInstance,
    node_id: &str,
) -> Result<(), HandleError> {
    // 1. Подсчитать количество входящих flows (incoming_count)
    let incoming_flows_count = instance.definition.get_incoming_flows_for(node_id).len();

    // 2. Подсчитать текущие токены на gateway (tokens_at_gateway)
    // (включая только что прибывший)
    let gateway_tokens_count = instance.count_tokens_for(node_id);

    // 3. Если tokens_at_gateway < incoming_count:
    //  - Это JOIN, но ещё не все токены пришли
    //  - Просто выйти (токен уже добавлен вызывающим кодом)
    //  - Ждать следующих событий
    if gateway_tokens_count < incoming_flows_count  {
        return Ok(())
    }
    // 4. Если tokens_at_gateway == incoming_count:
    //  - Это JOIN и все токены собрались
    //  - ИЛИ это FORK (один входящий, несколько исходящих)
    else if gateway_tokens_count == incoming_flows_count {
        // → Удалить все токены с gateway
        instance.remove_token(node_id);

        // → Для каждого исходящего flow:
        let outgoing_flows_id: Vec<_> = instance
            .definition
            .get_outgoing_flows_for(node_id)
            .iter()
            .map(|f| f.target_ref.clone())
            .collect();

        // Если нету исходящих потоков, выкидываем ошибку
        if outgoing_flows_id.is_empty() {
            return Err(HandleError::NoOutgoingFlow(node_id.to_string()))
        }

        for target_id in outgoing_flows_id {
            //    - Добавить токен на target
            instance.add_token(&target_id).await?;
        }            

        return Ok(())
    }
    // 4. Если tokens_at_gateway > incoming_count:
    //  - Это ошибка
    else {
        return Err(HandleError::TooMuchParallelGatewayTokens(gateway_tokens_count, incoming_flows_count));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

use super::*;
    use crate::engine::handlers::test_utils::*;
    use crate::domain::elements::{FlowNode, ParallelGateway, StartEvent, Task};
    use crate::domain::process::SequenceFlow;
    use crate::engine::handlers::handle_node;
    use crate::engine::instance::ProcessEvent;

    // Тест Fork — один входящий flow, два исходящих:
    //     Проверить, что токен удалён с gateway
    //     Проверить, что токены добавлены на ОБА target
    //     Проверить, что отправлены ОБА события TokenArrived
    #[tokio::test]
    async fn test_handle_parallel_gateway_fork() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::Task(Task { id: "task2".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task1".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task2".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["parallel_gateway"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;

        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // Токен удалён со "parallel_gateway"
        assert!(!instance.state.current_tokens.iter().any(|t| t.node_id == "parallel_gateway"));

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

        // Токен добавлен на "task2"
        assert!(instance.state.current_tokens.iter().any(|t| t.node_id == "task2"));

        // Отправлено событие TokenArrived { node_id: "task2" }
        let event = instance.event_rx.recv().await.unwrap();
        match event {
            ProcessEvent::TokenArrived { node_id } => {
                assert_eq!(node_id, "task2");
            }
            ProcessEvent::Complete => panic!("Unexpected Complete event"),
        }

    }

    // Тест Join — первый токен — два входящих, один исходящий:
    //     Начальное состояние: 0 токенов на gateway
    //     Добавить 1 токен на gateway
    //     Вызвать handle_parallel_gateway
    //     Проверить, что токен остался на gateway (ждём второй)
    //     Проверить, что НЕ отправлено событие TokenArrived
    #[tokio::test]
    async fn test_handle_parallel_gateway_join() {
        let nodes = vec![
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::Task(Task { id: "task2".to_string(), name: None }),
            FlowNode::Task(Task { id: "task3".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task2".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task3".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["task1", "task2"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "task1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать parallel_gateway
        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        // Событий нет
        assert!(instance.event_rx.try_recv().is_err());
        // Токен должен остаться на parallel_gateway
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "parallel_gateway"),
            "It was expected that the token would remain on the parallel_gateway."
        );
        // НЕ должно быть токена на task3
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "task3"),
            "It was expected that the token would NOT move to task3"
        );

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "task2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать parallel_gateway
        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // parallel_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "parallel_gateway"),
            "It was expected that NO tokens would remain on the parallel_gateway."
        );

        // Токен должен перейти на task3
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "task3"),
            "It was expected that the token would moved to task3: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "task3" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "task3");

    }

    // Тест Join — второй токен — два входящих, один исходящий:
    //     Начальное состояние: 1 токен уже на gateway
    //     Добавить 2-й токен на gateway
    //     Вызвать handle_parallel_gateway
    //     Проверить, что ВСЕ токены удалены с gateway
    //     Проверить, что токен добавлен на target
    //     Проверить, что отправлено событие TokenArrived
    #[tokio::test]
    async fn test_handle_parallel_gateway_join_second_token() {
        let nodes = vec![
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::Task(Task { id: "task2".to_string(), name: None }),
            FlowNode::Task(Task { id: "task3".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task2".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task3".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["task2", "parallel_gateway"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "task2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать parallel_gateway
        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // parallel_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "parallel_gateway"),
            "It was expected that NO tokens would remain on the parallel_gateway."
        );

        // Токен должен перейти на task3
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "task3"),
            "It was expected that the token would moved to task3: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "task3" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "task3");
    }

    // Тест Join + Fork — gateway одновременно и join, и fork:
    //     Два входящих flow, два исходящих
    //     Когда все токены собрались — удалить их и создать токены на ОБА исходящих
    #[tokio::test]
    async fn test_handle_parallel_gateway_join_and_fork() {
        let nodes = vec![
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::Task(Task { id: "task2".to_string(), name: None }),
            FlowNode::Task(Task { id: "task3".to_string(), name: None }),
            FlowNode::Task(Task { id: "task4".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task2".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task3".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow4".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task4".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["task1", "task2"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "task1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "task2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать parallel_gateway
        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // parallel_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "parallel_gateway"),
            "It was expected that NO tokens would remain on the parallel_gateway."
        );

        // Токен должен перейти на task3
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "task3"),
            "It was expected that the token would moved to task3: {:?}",
            instance.state.current_tokens
        );

        // Токен должен перейти на task4
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "task4"),
            "It was expected that the token would moved to task4: {:?}",
            instance.state.current_tokens
        );

        let mut arrived_tokens = HashSet::new();
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());

        // Проверяем, что оба target присутствуют (порядок не важен)
        assert!(arrived_tokens.contains("task3"));
        assert!(arrived_tokens.contains("task4"));
        assert_eq!(arrived_tokens.len(), 2);

    }

    // Тестирует ошибку при превышении количества токенов над входящими потоками
    #[tokio::test]
    async fn test_handle_parallel_gateway_too_much_error() {
        let nodes = vec![
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
            FlowNode::Task(Task { id: "task2".to_string(), name: None }),
            FlowNode::Task(Task { id: "task3".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "task2".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "parallel_gateway".to_string(),
                target_ref: "task3".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["parallel_gateway", "parallel_gateway", "parallel_gateway"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(matches!(result, Err(HandleError::TooMuchParallelGatewayTokens(_, _))));

    }

    // Тестирует ошибку при отсутствии исходящих потоков
    #[tokio::test]
    async fn test_handle_parallel_gateway_no_outgoing_flows() {
        let nodes = vec![
            FlowNode::ParallelGateway(ParallelGateway { id: "parallel_gateway".to_string() }),
            FlowNode::Task(Task { id: "task1".to_string(), name: None }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "task1".to_string(),
                target_ref: "parallel_gateway".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["parallel_gateway"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        let result = handle_parallel_gateway(&mut instance, "parallel_gateway").await;
        assert!(matches!(result, Err(HandleError::NoOutgoingFlow(_))));

    }
}
