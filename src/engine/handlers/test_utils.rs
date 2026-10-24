use std::collections::HashMap;

use uuid::Uuid;

use crate::domain::elements::FlowNode;
use crate::domain::process::{ProcessDefinition, SequenceFlow};
use crate::domain::state::{InstanceState, Token};
use crate::engine::instance::ProcessInstance;
use crate::store::memory::InMemoryStore;

pub fn create_test_process_definition(
    nodes: Vec<FlowNode>,
    sequence_flows: Vec<SequenceFlow>,
) -> ProcessDefinition {
    let mut flow_nodes = HashMap::new();

    for node in nodes {
        flow_nodes.insert(node.id().to_string(), node);
    }

    let start_events = flow_nodes
        .values()
        .filter_map(|node| {
            if matches!(node, FlowNode::StartEvent(_)) {
                Some(node.id().to_string())
            } else {
                None
            }
        })
        .collect();

    ProcessDefinition {
        id: "test-process".to_string(),
        version: 1,
        name: Some("Test Process".to_string()),
        flow_nodes,
        sequence_flows,
        start_events,
    }
}

pub fn create_test_process_instance(
    definition: ProcessDefinition,
    current_tokens: Option<Vec<&str>>,
) -> ProcessInstance {
    let store = Box::new(InMemoryStore::new());
    let current_tokens: Vec<Token> = current_tokens.map_or(
        vec![],
        |nodes| nodes.iter().map(|node_id| Token { node_id: node_id.to_string() }).collect(),
    );
    let state = InstanceState {
        current_tokens,
        variables: HashMap::new(),
    };

    ProcessInstance::restore(Uuid::new_v4(), definition, store, state)
}
