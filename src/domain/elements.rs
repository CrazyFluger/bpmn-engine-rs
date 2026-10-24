use serde::{Deserialize, Serialize};

use crate::domain::process::ProcessDefinition;

/// Начальное событие
/// — это графический элемент в виде тонкого круга, который указывает на то, где именно начинается бизнес-процесс
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartEvent {
    pub id: String,
}

/// Конечное событие
/// — в нотации BPMN — это графический элемент, который показывает точку, где завершается бизнес-процесс или его отдельная ветвь
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndEvent {
    pub id: String,
}

/// Промежуточное событие
/// — это элемент процесса, который происходит между стартовым и конечным событиями, влияя на ход выполнения задачи, задерживая поток или фиксируя отправку/получение информации
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntermediateEvent {
    pub id: String,
}

/// Задача
/// — это элементарная, неделимая единица работы в рамках бизнес-процесса
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub name: Option<String>,
}

/// Сервисная задача
/// — это элемент автоматического действия, которое выполняется системой без участия человека с помощью внешних программ, веб-сервисов или API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTask {
    pub id: String,
    pub name: Option<String>,
}

/// Пользовательская задача
/// — это элемент процесса, который выполняет человек с помощью информационной системы (например, интерфейса BPMS — системы управления бизнес-процессами)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTask {
    pub id: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubProcess {
    pub id: String,
    pub name: Option<String>,
}

/// Эксклюзивный шлюз «ИЛИ/ИЛИ» или XOR-шлюз
/// — это элемент нотации BPMN, который разделяет или объединяет поток процесса, выбирая только один путь из нескольких возможных на основе условий
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExclusiveGateway {
    pub id: String,
    pub default_flow: Option<String>,
}

/// Включающий или неисключающий шлюз
/// — это элемент управления потоком процесса в виде ромба с кругом внутри, который позволяет разветвлять процесс на один или несколько параллельных путей в зависимости от выполнения условий («и/или»)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InclusiveGateway {
    pub id: String,
    pub default_flow: Option<String>,
}

/// Параллельный шлюз «И» / AND
/// — это элемент управления потоком процесса, который разделяет один поток на несколько параллельных веток либо объединяет несколько веток в одну без проверки условий
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelGateway {
    pub id: String,
    // pub incoming: Vec<String>,
    // pub outgoing: Vec<String>,
}

/// Узел (BPMN Node / Flow Object)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowNode {
    /// Конечное событие
    EndEvent(EndEvent),
    /// Эксклюзивный шлюз «ИЛИ/ИЛИ» или XOR-шлюз
    ExclusiveGateway(ExclusiveGateway),
    /// Включающий или неисключающий шлюз
    InclusiveGateway(InclusiveGateway),
    /// Промежуточное событие
    IntermediateEvent(IntermediateEvent),
    /// Параллельный шлюз «И» / AND
    ParallelGateway(ParallelGateway),
    /// Сервисная задача
    ServiceTask(ServiceTask),
    /// Начальное событие
    StartEvent(StartEvent),
    /// Задача
    Task(Task),
    /// Пользовательская задача
    UserTask(UserTask),
    /// Подпроцесс
    /// — это составной элемент (действие), который содержит внутри себя детальную последовательность других задач, шлюзов и событий
    SubProcess(ProcessDefinition),
}

impl FlowNode {
    pub fn id(&self) -> &str {
        match self {
            FlowNode::StartEvent(e ) => &e.id,
            FlowNode::EndEvent(e ) => &e.id,
            FlowNode::IntermediateEvent(e ) => &e.id,
            FlowNode::Task(t ) => &t.id,
            FlowNode::ServiceTask(t ) => &t.id,
            FlowNode::UserTask(t ) => &t.id,
            FlowNode::ExclusiveGateway(g ) => &g.id,
            FlowNode::ParallelGateway(g ) => &g.id,
            FlowNode::InclusiveGateway(g ) => &g.id,
            FlowNode::SubProcess(p ) => &p.id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_flow_node_id() {
        let id = "start".to_string();
        let n = FlowNode::StartEvent(StartEvent { id: id.clone() });
        assert_eq!(n.id(), id);
        let id = "end".to_string();
        let n = FlowNode::EndEvent(EndEvent { id: id.clone() });
        assert_eq!(n.id(), id);
        let id = "intermediate".to_string();
        let n = FlowNode::IntermediateEvent(IntermediateEvent { id: id.clone() });
        assert_eq!(n.id(), id);
        let id = "task".to_string();
        let n = FlowNode::Task(Task { id: id.clone(), name: None });
        assert_eq!(n.id(), id);
        let id = "service task".to_string();
        let n = FlowNode::ServiceTask(ServiceTask { id: id.clone(), name: None });
        assert_eq!(n.id(), id);
        let id = "user task".to_string();
        let n = FlowNode::UserTask(UserTask { id: id.clone(), name: None });
        assert_eq!(n.id(), id);
        let id = "exclusive gateway".to_string();
        let n = FlowNode::ExclusiveGateway(ExclusiveGateway { id: id.clone(), default_flow: None });
        assert_eq!(n.id(), id);
        let id = "parallel gateway".to_string();
        let n = FlowNode::ParallelGateway(ParallelGateway { id: id.clone()/*, incoming: vec![], outgoing: vec![]*/});
        assert_eq!(n.id(), id);
        let id = "inclusive gateway".to_string();
        let n = FlowNode::InclusiveGateway(InclusiveGateway { id: id.clone(), default_flow: None });
        assert_eq!(n.id(), id);
        let id = "sub process".to_string();
        let n = FlowNode::SubProcess(ProcessDefinition::new(
            id.clone(),
            1,
            None,
            HashMap::new(),
            vec![],
        ));
        assert_eq!(n.id(), id);
    }
}