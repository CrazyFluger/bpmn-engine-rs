use bpmn_engine_rs::parser::xml_types::BpmnDefinitions;
use quick_xml::de::from_str;

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
    
    let definitions: BpmnDefinitions = from_str(xml).unwrap();
    assert_eq!(definitions.processes.len(), 1);
    assert_eq!(definitions.processes[0].id, "process1");
    assert_eq!(definitions.processes[0].name, Some("Test Process".to_string()));
    
    let process = &definitions.processes[0];
    assert_eq!(process.start_events.len(), 1);
    assert_eq!(process.start_events[0].id, "start1");
    
    assert_eq!(process.tasks.len(), 1);
    assert_eq!(process.tasks[0].id, "task1");
    assert_eq!(process.tasks[0].name, Some("Do something".to_string()));
    
    assert_eq!(process.end_events.len(), 1);
    assert_eq!(process.end_events[0].id, "end1");
    
    assert_eq!(process.sequence_flows.len(), 2);
    assert_eq!(process.sequence_flows[0].source_ref, "start1");
    assert_eq!(process.sequence_flows[0].target_ref, "task1");
}