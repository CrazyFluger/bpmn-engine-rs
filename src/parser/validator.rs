use anyhow::Result;
use crate::domain::process::ProcessDefinition;

pub fn validate_process(process: &ProcessDefinition) -> Result<()> {
    // Проверка наличия start event
    if process.start_events.is_empty() {
        anyhow::bail!("Process must have at least one start event");
    }
    
    // Проверка валидности всех ссылок sequence flow
    for flow in &process.sequence_flows {
        if !process.flow_nodes.contains_key(&flow.source_ref) {
            anyhow::bail!("Sequence flow \"{}\" references non-existent source \"{}\"", flow.id, flow.source_ref);
        }
        if !process.flow_nodes.contains_key(&flow.target_ref) {
            anyhow::bail!("Sequence flow \"{}\" references non-existent target \"{}\"", flow.id, flow.target_ref);
        }
    }
    
    Ok(())
}