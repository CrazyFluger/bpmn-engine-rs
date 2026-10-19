use crate::{engine::handlers::HandleError, engine::instance::ProcessInstance};

pub async fn handle_sub_process(
    _instance: &mut ProcessInstance,
    _node_id: &str,
) -> Result<(), HandleError> {
    Err(HandleError::NotImplemented)
}
