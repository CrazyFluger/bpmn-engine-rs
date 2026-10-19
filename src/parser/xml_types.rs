use serde::Deserialize;

#[derive(Deserialize)]
pub struct BpmnDefinitions {
    #[serde(rename = "process", default)]
    pub processes: Vec<BpmnProcess>,
}

#[derive(Deserialize)]
pub struct BpmnProcess {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@name")]
    pub name: Option<String>,

    #[serde(rename = "startEvent", default)]
    pub start_events: Vec<BpmnStartEvent>,

    // TODO: intermediateCatchEvent
    // TODO: intermediateThrowEvent

    #[serde(rename = "endEvent", default)]
    pub end_events: Vec<BpmnEndEvent>,

    #[serde(rename = "task", default)]
    pub tasks: Vec<BpmnTask>,

    #[serde(rename = "serviceTask", default)]
    pub service_tasks: Vec<BpmnServiceTask>,

    #[serde(rename = "userTask", default)]
    pub user_tasks: Vec<BpmnUserTask>,

    #[serde(rename = "subProcess", default)]
    pub sub_processes: Vec<BpmnProcess>,

    #[serde(rename = "exclusiveGateway", default)]
    pub exclusive_gateways: Vec<BpmnExclusiveGateway>,

    #[serde(rename = "inclusiveGateway", default)]
    pub inclusive_gateways: Vec<BpmnInclusiveGateway>,

    #[serde(rename = "parallelGateway", default)]
    pub parallel_gateways: Vec<BpmnParallelGateway>,

    #[serde(rename = "sequenceFlow", default)]
    pub sequence_flows: Vec<BpmnSequenceFlow>,
}

/// Начальное событие
#[derive(Deserialize)]
pub struct BpmnStartEvent {
    #[serde(rename = "@id")]
    pub id: String,
}

/// Конечное событие
#[derive(Deserialize)]
pub struct BpmnEndEvent {
    #[serde(rename = "@id")]
    pub id: String,
}

/// Промежуточное событие
#[derive(Deserialize)]
pub struct BpmnIntermediateEvent {
    #[serde(rename = "@id")]
    pub id: String,
}

/// Задача
#[derive(Deserialize)]
pub struct BpmnTask {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@name")]
    pub name: Option<String>,
}

/// Сервисная Задача
#[derive(Deserialize)]
pub struct BpmnServiceTask {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@name")]
    pub name: Option<String>,

    /// способ реализации службы (например, ##WebService, ##Unspecified или кастомная строка).
    /// Часто на практике в движках (Camunda, Zeebe, Flowable) подменяется специфическими расширениями (namespace-атрибутами),
    /// такими как camunda:delegateExpression, camunda:class или camunda:type="http"
    #[serde(rename = "@implementation")]
    pub implementation: Option<String>,

    /// ссылка на конкретную операцию (operation),
    /// описанную в интерфейсе (interface) в секции определения данных/сервисов (WSDL/Contract)
    #[serde(rename = "@operationRef")]
    pub operation_ref: Option<String>,

    #[serde(rename = "incoming", default)]
    pub incoming: Vec<String>,

    #[serde(rename = "outgoing", default)]
    pub outgoing: Vec<String>,
}

/// Пользовательская задача
#[derive(Deserialize)]
pub struct BpmnUserTask {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@name")]
    pub name: Option<String>,

    #[serde(rename = "incoming", default)]
    pub incoming: Vec<String>,

    #[serde(rename = "outgoing", default)]
    pub outgoing: Vec<String>,
}

/// Связь между узлами (рёбра)
#[derive(Deserialize)]
pub struct BpmnSequenceFlow {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@sourceRef")]
    pub source_ref: String,

    #[serde(rename = "@targetRef")]
    pub target_ref: String,
}

/// Эксклюзивный шлюз
#[derive(Deserialize)]
pub struct BpmnExclusiveGateway {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@default")]
    pub default_flow: Option<String>,
}

/// Включающий или неисключающий шлюз
#[derive(Deserialize)]
pub struct BpmnInclusiveGateway {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@default")]
    pub default_flow: Option<String>,
}

/// Параллельный шлюз «И»
#[derive(Deserialize)]
pub struct BpmnParallelGateway {
    #[serde(rename = "@id")]
    pub id: String,

    #[serde(rename = "@name")]
    pub name: Option<String>,

    #[serde(rename = "incoming", default)]
    pub incoming: Vec<String>,

    #[serde(rename = "outgoing", default)]
    pub outgoing: Vec<String>,

}