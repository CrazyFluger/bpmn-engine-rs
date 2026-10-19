use crate::{domain::elements::FlowNode, engine::instance::{ProcessEvent, ProcessInstance}};

/// Ошибка обработчика
#[derive(Debug)]
pub enum HandleError {
    /// Канал закрыт
    ChannelClosed,
    /// У End Event имеются исходящие потоки
    EndEventHasOutgoingFlows(String),
    /// Узел имеет несколько исходящих потоков
    MultipleOutgoingFlows(String),
    /// У узла нету исходящих потоков
    NoOutgoingFlow(String),
    /// Обработчик ещё не реализован
    NotImplemented,
    /// Ошибка хранилища
    StoreError(String),
}

impl std::fmt::Display for HandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandleError::NoOutgoingFlow(node_id) => {
                write!(f, "Node \"{}\" has no outgoing flow", node_id)
            }
            HandleError::ChannelClosed => {
                write!(f, "Event channel is closed")
            }
            HandleError::NotImplemented => {
                write!(f, "Node not yet implemented")
            }
            HandleError::MultipleOutgoingFlows(node_id) => {
                write!(f, "Node \"{}\" has multiple outgoing flows", node_id)
            }
            HandleError::EndEventHasOutgoingFlows(node_id) => {
                write!(f, "End node \"{}\" has outgoing flows", node_id)
            }
            HandleError::StoreError(msg) => {
                write!(f, "StateStore error: {}", msg)
            }
        }
    }
}

impl std::error::Error for HandleError {}

/// Обработчик узла
pub async fn handle_node(instance: &mut ProcessInstance, node_id: &str) -> Result<(), HandleError> {
    let node = instance.definition.flow_nodes.get(node_id);
    
    match node {
        Some(FlowNode::EndEvent(_)) => handle_end_event(instance, node_id).await,
        Some(FlowNode::ExclusiveGateway(_)) => handle_exclusive_gateway(instance, node_id).await,
        Some(FlowNode::InclusiveGateway(_)) => handle_inclusive_gateway(instance, node_id).await,
        Some(FlowNode::IntermediateEvent(_)) => handle_intermediate_event(instance, node_id).await,
        Some(FlowNode::ParallelGateway(_)) => handle_parallel_gateway(instance, node_id).await,
        Some(FlowNode::ServiceTask(_)) => handle_service_task(instance, node_id).await,
        Some(FlowNode::StartEvent(_)) => handle_start_event(instance, node_id).await,
        Some(FlowNode::Task(_)) => handle_task(instance, node_id).await,
        Some(FlowNode::UserTask(_)) => handle_user_task(instance, node_id).await,
        Some(FlowNode::SubProcess(_)) => handle_sub_process(instance, node_id).await,
        None => Err(HandleError::NotImplemented),
    }
}

pub async fn handle_end_event(instance: &mut ProcessInstance, node_id: &str) -> Result<(), HandleError> {
    // 1. Убираем токен с EndEvent
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flows: Vec<_> = instance.definition.sequence_flows
        .iter()
        .filter(|flow| flow.source_ref == node_id)
        .collect();

    // 3. Проверить количество flows
    match outgoing_flows.len() {
        // если нет исходящих потоков, то Ок
        0 => {},
        // если есть, то выкидываем ошибку
        _ => return Err(HandleError::EndEventHasOutgoingFlows(node_id.to_string())),
    };

    // 2. Проверить, есть ли ещё токены в процессе:
    if instance.state.current_tokens.is_empty() {
        // Процесс завершён!
        // 3. Отправляем событие Complete для следующего узла
        instance.event_tx.send(ProcessEvent::Complete)
            .await
            // Отлавливаем ошибку, если канал закрыт
            .map_err(|_| HandleError::ChannelClosed)?;
    }
       
    Ok(())
}

pub async fn handle_exclusive_gateway(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_inclusive_gateway(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_intermediate_event(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_parallel_gateway(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_service_task(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_start_event(instance: &mut ProcessInstance, node_id: &str) -> Result<(), HandleError> {
    // 1. Удалить токен с узла StartEvent
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flow = instance.definition.sequence_flows
        .iter()
        .find(|flow| flow.source_ref == node_id);

    // 3.1 Если не нашли исходящий поток, кидаем ошибку
    let flow = outgoing_flow.ok_or_else(|| {
        HandleError::NoOutgoingFlow(node_id.to_string())
    })?;

    // 3.2 Если нашли исходящий поток, перемещаем токен дальше
    let target_id = flow.target_ref.clone();
    
    // 4. Добавляем токен на следующий узел
    instance.add_token(&target_id);
    
    // 5. Отправляем событие TokenArrived для следующего узла
    instance.event_tx.send(ProcessEvent::TokenArrived { node_id: target_id.clone() })
        .await
        // Отлавливаем ошибку, если канал закрыт
        .map_err(|_| HandleError::ChannelClosed)?;

    Ok(())
}

pub async fn handle_task(instance: &mut ProcessInstance, node_id: &str) -> Result<(), HandleError> {
    // 1. Удалить токен с текущего узла (Task)
    instance.remove_token(node_id);

    // 2. Найти исходящий flow — SequenceFlow, у которого source_ref == node_id
    let outgoing_flows: Vec<_> = instance.definition.sequence_flows
        .iter()
        .filter(|flow| flow.source_ref == node_id)
        .collect();

    // 3. Проверить количество flows
    match outgoing_flows.len() {
        0 => return Err(HandleError::NoOutgoingFlow(node_id.to_string())),
        // если только 1, продолжим
        1 => {},
        // если более 1, то выкидываем ошибку
        _ => return Err(HandleError::MultipleOutgoingFlows(node_id.to_string())),
    };

    // 4. Переместить токен на следующий узел
    let flow = &outgoing_flows[0];
    let target_id = flow.target_ref.clone();
    instance.add_token(&target_id);

    // 5. Отправить событие TokenArrived { node_id: target } через event_tx
    instance.event_tx.send(ProcessEvent::TokenArrived {
            node_id: target_id.clone()
        })
        .await
        // Обработать ошибку, если нет исходящего flow или канал закрыт
        .map_err(|_| HandleError::ChannelClosed)?;

    Ok(())
}

pub async fn handle_user_task(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}

pub async fn handle_sub_process(_instance: &mut ProcessInstance, _node_id: &str) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}
