use bpmn_engine_rs::parser::mapper::{BpmnParser, BpmnParserError};

#[test]
fn test_parse_simple_bpmn() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
        <process id="process1" name="Test Process">
            <startEvent id="start1"/>
            <task id="task1" name="Do something"/>
            <endEvent id="end1"/>
            <sequenceFlow id="flow1" sourceRef="start1" targetRef="task1"/>
            <sequenceFlow id="flow2" sourceRef="task1" targetRef="end1"/>
        </process>
    </definitions>"#;

    let process = BpmnParser::parse(xml).unwrap();

    let node = process.flow_nodes.get("start1").unwrap();
    assert_eq!(node.id(), "start1");

    let node = process.flow_nodes.get("task1").unwrap();
    assert_eq!(node.id(), "task1");

    let node = process.flow_nodes.get("end1").unwrap();
    assert_eq!(node.id(), "end1");

    let flow = process.sequence_flows.iter().find(|f| f.id == "flow1").unwrap();
    assert_eq!(flow.source_ref, "start1");
    assert_eq!(flow.target_ref, "task1");

    let flow = process.sequence_flows.iter().find(|f| f.id == "flow2").unwrap();
    assert_eq!(flow.source_ref, "task1");
    assert_eq!(flow.target_ref, "end1");
}

#[test]
fn test_map_bpmn_to_domain() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
        <process id="process1" name="Test Process">
            <startEvent id="start1"/>
            <task id="task1" name="Do something"/>
            <endEvent id="end1"/>
            <sequenceFlow id="flow1" sourceRef="start1" targetRef="task1"/>
            <sequenceFlow id="flow2" sourceRef="task1" targetRef="end1"/>
        </process>
    </definitions>"#;

    let process = BpmnParser::parse(xml).unwrap();
    assert_eq!(process.id, "process1");
    assert_eq!(process.version, 1);
    assert_eq!(process.flow_nodes.len(), 3);
    assert_eq!(process.sequence_flows.len(), 2);
}

#[test]
fn test_error_serialize() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
        <process id="process1" name="Test Process 1">
    </definitions>"#;

    let result = BpmnParser::parse(xml);
    assert!(matches!(result, Err(BpmnParserError::Serialize(_))))
}

#[test]
fn test_error_empty_process() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
    </definitions>"#;

    let result = BpmnParser::parse(xml);
    assert!(matches!(result, Err(BpmnParserError::EmptyProcess)))
}

#[test]
fn test_error_too_much_process() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
        <process id="process1" name="Test Process 1">
        </process>
        <process id="process2" name="Test Process 2">
        </process>
    </definitions>"#;

    let result = BpmnParser::parse(xml);
    assert!(matches!(result, Err(BpmnParserError::TooMuchProcess)))
}

#[test]
fn test_error_validation() {
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
    <definitions>
        <process id="process1" name="Test Process">
            <startEvent id="start1"/>
            <task id="task1" name="Do something"/>
            <endEvent id="end1"/>
            <sequenceFlow id="flow1" sourceRef="start2" targetRef="task1"/>
            <sequenceFlow id="flow2" sourceRef="task1" targetRef="end1"/>
        </process>
    </definitions>"#;

    let result = BpmnParser::parse(xml);
    assert!(matches!(result, Err(BpmnParserError::Validation(_))))
}
