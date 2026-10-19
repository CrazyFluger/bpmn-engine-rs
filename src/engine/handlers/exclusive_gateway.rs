use crate::{engine::handlers::HandleError, engine::instance::ProcessInstance};

pub async fn handle_exclusive_gateway(
    _instance: &mut ProcessInstance,
    _node_id: &str,
) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}
