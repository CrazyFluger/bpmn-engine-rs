use crate::{domain::elements::FlowNode, engine::instance::ProcessInstance};

mod start_event;
mod end_event;
mod task;

mod exclusive_gateway;
mod inclusive_gateway;
mod intermediate_event;
mod parallel_gateway;
mod service_task;
mod user_task;
mod sub_process;

#[cfg(test)]
pub mod test_utils;

pub use start_event::handle_start_event;
pub use end_event::handle_end_event;
pub use task::handle_task;

pub use exclusive_gateway::handle_exclusive_gateway;
pub use inclusive_gateway::handle_inclusive_gateway;
pub use intermediate_event::handle_intermediate_event;
pub use parallel_gateway::handle_parallel_gateway;
pub use service_task::handle_service_task;
pub use user_task::handle_user_task;
pub use sub_process::handle_sub_process;

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
    /// Нет истинных условий и нет default flow
    NoConditionMet(String),
    /// Ошибка обработки условия
    ExpressionError(String),
    /// Слишком много токенов у Gateway
    TooMuchGatewayTokens(usize, usize),
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
            HandleError::NoConditionMet(node_id) => {
                write!(f, "No condition met for gateway \"{}\" and no default flow", node_id)
            }
            HandleError::ExpressionError(msg) => {
                write!(f, "Expression error: {}", msg)
            }
            HandleError::TooMuchGatewayTokens(expected, arrived) => {
                write!(f, "Gateway expected {} tokens, but arrived {}", expected, arrived)
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
