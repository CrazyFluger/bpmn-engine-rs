use crate::domain::elements::FlowNode;
use crate::engine::expression_evaluator::ExpressionEvaluator;
use crate::engine::handlers::HandleError;
use crate::engine::instance::ProcessInstance;


// Примечание: упрощённая реализация — ждём все входящие токены
// TODO: В полной спецификации BPMN 2.0 нужно ждать только токены от активированных путей
pub async fn handle_inclusive_gateway(
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
        // 4.1 Удалить все токены с gateway
        instance.remove_token(node_id);

        // 4.2 Найти все исходящие flows
        let outgoing_flows: Vec<_> = instance.definition.get_outgoing_flows_for(node_id);

        // Если нету исходящих потоков, выкидываем ошибку
        if outgoing_flows.is_empty() {
            return Err(HandleError::NoOutgoingFlow(node_id.to_string()))
        }

        // 4.3 Находим все исходящие flow, которые проходят проверку
        let mut condition_target_ids: Vec<String> = vec![];
        for flow in &outgoing_flows {
            // Если есть condition:
            if let Some(expression) = &flow.condition {
                // Проверить условие с помощью ExpressionEvaluator::evaluate()
                match ExpressionEvaluator::evaluate(&expression, &instance.state.variables) {
                    // Если истинно — добавить flow
                    Ok(true) => condition_target_ids.push(flow.target_ref.clone()),
                    // Условие ложно, идём дальше
                    Ok(false) => continue,
                    // Ошибка оценки
                    Err(e) => return Err(HandleError::ExpressionError(e.to_string())),
                }
            }
        }

        // 4.4 Определяем default_flow
        let default_target_id: Option<String> = match instance.definition.flow_nodes.get(node_id) {
            Some(FlowNode::InclusiveGateway(node)) => {
                if let Some(default_id) = &node.default_flow {
                    outgoing_flows.iter().copied()
                        .find(|flow| &flow.id == default_id)
                        .map(|f| f.target_ref.clone())
                } else {
                    None
                }
            },
            _ => None,
        };

        // 4.5. Определяем актуальные target id
        let outgoing_target_id: Vec<_> = match condition_target_ids.is_empty() {
            false => condition_target_ids,
            true => match default_target_id {
                Some(target_id) => vec![target_id],
                None => return Err(HandleError::NoConditionMet(node_id.to_string())),
            }
        };

        for target_id in outgoing_target_id {
            // Добавить токен на target
            instance.add_token(&target_id).await?;
        }            

        return Ok(())
    }
    // 4. Если tokens_at_gateway > incoming_count:
    //  - Это ошибка
    else {
        return Err(HandleError::TooMuchGatewayTokens(gateway_tokens_count, incoming_flows_count));
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

use super::*;
    use crate::domain::elements::{EndEvent, FlowNode, InclusiveGateway, StartEvent};
    use crate::domain::expression::Expression;
    use crate::domain::process::SequenceFlow;
    use crate::engine::handlers::{handle_node, test_utils::*};

    // Fork-тесты:
    // Одно истинное условие — активируется один путь
    #[tokio::test]
    async fn test_handle_inclusive_gateway_fork_one_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let current_tokens = Some(vec!["start1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Вызываем inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти на end1
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "end1" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "end1");
    }

    // Два истинных условия — активируются оба пути
    #[tokio::test]
    async fn test_handle_inclusive_gateway_fork_two_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end2".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end2".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let current_tokens = Some(vec!["start1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Вызываем inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти на end1 и на end2
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end2"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено два события TokenArrived { node_id: "end1" } и TokenArrived { node_id: "end2" }
        let mut arrived_tokens = HashSet::new();
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());

        // Проверяем, что оба target присутствуют (порядок не важен)
        assert!(arrived_tokens.contains("end1"));
        assert!(arrived_tokens.contains("end2"));
        assert_eq!(arrived_tokens.len(), 2);
    }

    // Все условия ложны, есть default — активируется default
    #[tokio::test]
    async fn test_handle_inclusive_gateway_fork_default_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: Some("flow4".to_string()) }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end2".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end3".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end2".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow4".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end3".to_string(),
                condition: None,
            },
        ];
        let current_tokens = Some(vec!["start1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Вызываем inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти по дефолтному пути на end3
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end3"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "end3" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "end3");
    }

    // Все условия ложны, нет default — ошибка
    #[tokio::test]
    async fn test_handle_inclusive_gateway_fork_no_way() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end2".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end2".to_string(),
                condition: Some(Expression::Simple("false".to_string())),
            },
        ];
        let current_tokens = Some(vec!["start1"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // Вызываем inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(matches!(result, Err(HandleError::NoConditionMet(_))), "Expected NoConditionMet Error");
    }

    // Тест Join — первый токен — два входящих, один исходящий:
    //     Начальное состояние: 0 токенов на gateway
    //     Добавить 1 токен на gateway
    //     Вызвать handle_inclusive_gateway
    //     Проверить, что токен остался на gateway (ждём второй)
    //     Проверить, что НЕ отправлено событие TokenArrived
    #[tokio::test]
    async fn test_handle_inclusive_gateway_join() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::StartEvent(StartEvent { id: "start2".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "start2".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let current_tokens = Some(vec!["start1", "start2"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        // Событий нет
        assert!(instance.event_rx.try_recv().is_err());
        // Токен должен остаться на inclusive_gateway
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that the token would remain on the inclusive_gateway."
        );
        // НЕ должно быть токена на end1
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would NOT move to end1"
        );

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "start2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти на end1
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "end1" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "end1");

    }


    // Join-тесты:
    // Тест Join — второй токен — два входящих, один исходящий:
    //     Начальное состояние: 1 токен уже на gateway
    //     Добавить 2-й токен на gateway
    //     Вызвать handle_inclusive_gateway
    //     Проверить, что ВСЕ токены удалены с gateway
    //     Проверить, что токен добавлен на target
    //     Проверить, что отправлено событие TokenArrived
    #[tokio::test]
    async fn test_handle_inclusive_gateway_join_second_token() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::StartEvent(StartEvent { id: "start2".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "start2".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let current_tokens = Some(vec!["start2", "inclusive_gateway"]);
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "start2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Пробуем вызвать inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти на end1
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено событие TokenArrived { node_id: "end1" }
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());
        assert_eq!(token.unwrap(), "end1");
    }

    // Комбинированные тесты:
    // Join + Fork — ждать все входящие → активировать несколько исходящих
    #[tokio::test]
    async fn test_handle_inclusive_gateway_fork_and_join() {
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::StartEvent(StartEvent { id: "start2".to_string() }),
            FlowNode::InclusiveGateway(InclusiveGateway { id: "inclusive_gateway".to_string(), default_flow: None }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end2".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow2".to_string(),
                source_ref: "start2".to_string(),
                target_ref: "inclusive_gateway".to_string(),
                condition: None,
            },
            SequenceFlow {
                id: "flow3".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end1".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
            SequenceFlow {
                id: "flow4".to_string(),
                source_ref: "inclusive_gateway".to_string(),
                target_ref: "end2".to_string(),
                condition: Some(Expression::Simple("true".to_string())),
            },
        ];
        let current_tokens = None;
        let mut instance = create_test_process_instance(create_test_process_definition(nodes, flows), current_tokens);

        // Прокидываем первый токен
        let result = handle_node(&mut instance, "start1").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Прокидываем второй токен
        let result = handle_node(&mut instance, "start2").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());
        let token = get_arrived_token(&mut instance).await;
        assert!(token.is_ok(), "Expected Ok, but an error occurred: {}", token.unwrap_err());

        // Вызываем inclusive_gateway
        let result = handle_inclusive_gateway(&mut instance, "inclusive_gateway").await;
        assert!(result.is_ok(), "Expected Ok, but an error occurred: {}", result.unwrap_err());

        // inclusive_gateway должен быть пуст
        assert!(
            !instance.state.current_tokens.iter().any(|t| t.node_id == "inclusive_gateway"),
            "It was expected that NO tokens would remain on the inclusive_gateway."
        );

        // Токен должен перейти на end1 и на end2
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end1"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );
        assert!(
            instance.state.current_tokens.iter().any(|t| t.node_id == "end2"),
            "It was expected that the token would moved to end1: {:?}",
            instance.state.current_tokens
        );

        // Отправлено два события TokenArrived { node_id: "end1" } и TokenArrived { node_id: "end2" }
        let mut arrived_tokens = HashSet::new();
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());
        arrived_tokens.insert(get_arrived_token(&mut instance).await.unwrap());

        // Проверяем, что оба target присутствуют (порядок не важен)
        assert!(arrived_tokens.contains("end1"));
        assert!(arrived_tokens.contains("end2"));
        assert_eq!(arrived_tokens.len(), 2);
    }
}