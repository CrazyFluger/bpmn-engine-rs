use crate::{engine::handlers::HandleError, engine::instance::ProcessInstance};

pub async fn handle_service_task(
    _instance: &mut ProcessInstance,
    _node_id: &str,
) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}
