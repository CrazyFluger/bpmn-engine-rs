use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::domain::{elements::FlowNode, expression::Expression};

/// Контейнер для всего процесса

/// Связь между узлами (рёбра)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceFlow {
    pub id: String,
    /// ID узла истоника
    pub source_ref: String,
    /// ID узла цели
    pub target_ref: String,
    /// Условие для перехода
    pub condition: Option<Expression>,
}

/// Определение процесса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessDefinition {
    pub id: String,
    /// Версия процесса
    pub version: u32,
    /// Наименование процесса
    pub name: Option<String>,
    /// Все узлы процесса
    pub flow_nodes: HashMap<String, FlowNode>,
    /// Все связи узлов процесса
    pub sequence_flows: Vec<SequenceFlow>,
    /// ID StartEvent, для быстрого доступа
    pub start_events: Vec<String>,
}

impl ProcessDefinition {
    pub fn new(
        id: String,
        version: u32,
        name: Option<String>,
        flow_nodes: HashMap<String, FlowNode>,
        sequence_flows: Vec<SequenceFlow>,
    ) -> Self {
        let start_events = flow_nodes
            .iter()
            .filter_map(|(id, node)| {
                if matches!(node, FlowNode::StartEvent(_)) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        
        Self {
            id,
            version,
            name,
            flow_nodes,
            sequence_flows,
            start_events,
        }
    }
}