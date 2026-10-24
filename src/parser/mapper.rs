use anyhow::Error;
use quick_xml::{DeError, de::from_str};
use std::collections::HashMap;
use std::fmt;

use crate::domain::elements::*;
use crate::domain::process::ProcessDefinition;
use crate::domain::process::SequenceFlow;
use crate::parser::validator::validate_process;
use crate::parser::xml_types::*;

#[derive(Debug)]
pub enum BpmnParserError {
    Serialize(DeError),
    EmptyProcess,
    TooMuchProcess,
    Validation(Error),
}

impl fmt::Display for BpmnParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BpmnParserError::Serialize(err) => write!(f, "Ошибка разбора XML {:?}", err),
            BpmnParserError::EmptyProcess => write!(f, "Не найдено ни одного процесса"),
            BpmnParserError::TooMuchProcess => write!(f, "Слишком много головных процессов"),
            BpmnParserError::Validation(err) => write!(f, "Ошибка валидации: {:?}", err),
        }
    }
}


#[derive(Debug)]
pub struct BpmnParser;

impl BpmnParser {
    pub fn parse(xml: &str) -> Result<ProcessDefinition, BpmnParserError> {
        let serialized: Result<BpmnDefinitions, DeError> = from_str(xml);

        match serialized {
            Ok(definitions) => match &definitions.processes[..] {
                [] => Err(BpmnParserError::EmptyProcess),
                [process] => BpmnParser::map_processe(process),
                [..] => Err(BpmnParserError::TooMuchProcess),
            },
            Err(err) => Err(BpmnParserError::Serialize(err)),
        }
    }

    fn map_processe(process: &BpmnProcess) -> Result<ProcessDefinition, BpmnParserError> {
        let id = process.id.clone();
        let name = process.name.clone();
        let version = 1;
        let mut flow_nodes = HashMap::new();
        let mut sequence_flows = vec![];
        // let mut start_events = vec![];

        for event in &process.start_events {
            let node = BpmnParser::map_start_event(&event);
            let id = node.id().to_string();
            flow_nodes.insert(id.clone(), node);
            // start_events.push(id);
        }

        for event in &process.end_events {
            let node = BpmnParser::map_end_event(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.tasks {
            let node = BpmnParser::map_task(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.user_tasks {
            let node = BpmnParser::map_user_task(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.service_tasks {
            let node = BpmnParser::map_service_task(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.exclusive_gateways {
            let node = BpmnParser::map_exclusive_gateway(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.inclusive_gateways {
            let node = BpmnParser::map_inclusive_gateway(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.parallel_gateways {
            let node = BpmnParser::map_parallel_gateway(&event);
            flow_nodes.insert(node.id().to_string(), node);
        }

        for event in &process.sub_processes {
            let node = BpmnParser::map_sub_processe(&event)?;
            flow_nodes.insert(node.id().to_string(), node);
        }

        for xml_flow in &process.sequence_flows {
            let flow = BpmnParser::map_sequence_flow(&xml_flow);
            sequence_flows.push(flow);
        }

        let process = ProcessDefinition::new(
            id,
            version,
            name,
            flow_nodes,
            sequence_flows,
        );

        validate_process(&process).map(|_| process).map_err(BpmnParserError::Validation)
    }

    fn map_sub_processe(process: &BpmnProcess) -> Result<FlowNode, BpmnParserError> {
        let process = BpmnParser::map_processe(process)?;
        Ok(FlowNode::SubProcess(process))
    }

    fn map_start_event(event: &BpmnStartEvent) -> FlowNode {
        FlowNode::StartEvent(StartEvent {
            id: event.id.clone(),
        })
    }

    fn map_end_event(event: &BpmnEndEvent) -> FlowNode {
        FlowNode::EndEvent(EndEvent {
            id: event.id.clone(),
        })
    }

    fn map_task(event: &BpmnTask) -> FlowNode {
        FlowNode::Task(Task {
            id: event.id.clone(),
            name: event.name.clone(),
        })
    }

    fn map_user_task(event: &BpmnUserTask) -> FlowNode {
        FlowNode::UserTask(UserTask {
            id: event.id.clone(),
            name: event.name.clone(),
        })
    }

    fn map_service_task(event: &BpmnServiceTask) -> FlowNode {
        FlowNode::ServiceTask(ServiceTask {
            id: event.id.clone(),
            name: event.name.clone(),
        })
    }

    fn map_exclusive_gateway(event: &BpmnExclusiveGateway) -> FlowNode {
        FlowNode::ExclusiveGateway(ExclusiveGateway {
            id: event.id.clone(),
            default_flow: event.default_flow.clone(),
        })
    }

    fn map_inclusive_gateway(event: &BpmnInclusiveGateway) -> FlowNode {
        FlowNode::InclusiveGateway(InclusiveGateway {
            id: event.id.clone(),
            default_flow: event.default_flow.clone(),
        })
    }

    fn map_parallel_gateway(event: &BpmnParallelGateway) -> FlowNode {
        FlowNode::ParallelGateway(ParallelGateway {
            id: event.id.clone(),
            // incoming: event.incoming.clone(),
            // outgoing: event.outgoing.clone(),
        })
    }

    fn map_sequence_flow(event: &BpmnSequenceFlow) -> SequenceFlow {
        SequenceFlow {
            id: event.id.clone(),
            source_ref: event.source_ref.clone(),
            target_ref: event.target_ref.clone(),
            condition: None,
        }
    }
}
