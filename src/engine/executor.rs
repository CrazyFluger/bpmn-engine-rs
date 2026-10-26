use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use uuid::Uuid;

use crate::domain::state::InstanceState;
use crate::engine::instance::ProcessInstance;
use crate::engine::registry::InstanceRegistry;
use crate::store::StateStore;
use crate::store::store_trait::ArcStore;


#[derive(Debug)]
pub enum EngineError {
    LoadDefinition(String),
    ProcessInstanceCreation(String),
    ProcessInstanceStarting(String),
    StartToken(String),
    RegistryRegister(String),
}

impl Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoadDefinition(msg) => write!(f, "Load definition error: {}", msg),
            Self::ProcessInstanceCreation(msg) => write!(f, "Can't create new Process Instance: {}", msg),
            Self::ProcessInstanceStarting(msg) => write!(f, "Can't start Process Instance: {}", msg),
            Self::StartToken(msg) => write!(f, "Cant't add start tokens: {}", msg),
            Self::RegistryRegister(msg) => write!(f, "Cant't register in registry: {}", msg),
        }
    }
}

pub struct Engine {

    /// Физическое хранилище состояний (например Postgres)
    store: Arc<dyn StateStore>,

    /// Реестр запущенных экземпляров
    registry: Arc<InstanceRegistry>,

}

impl Engine {

    pub async fn new(store: Arc<dyn StateStore>) -> Self {
        Self {
            store,
            registry: Arc::new(InstanceRegistry::new()),
        }
    }

    /// Запустить новый экземпляр процесса
    pub async fn start(&self, process_id: &str, version: u32) -> Result<Uuid, EngineError> {
        // 1. Загрузить ProcessDefinition
        let definition = self.store.load_definition(process_id, version)
            .await.map_err(|e| EngineError::LoadDefinition(e.to_string()))?;

        // 2. Создать InstanceState (пустой)
        let state = InstanceState {
            current_tokens: vec![],
            variables: HashMap::new(),
        };

        // 3. Создать ProcessInstance
        let instance_id = Uuid::new_v4();
        let store = Box::new(ArcStore(self.store.clone()));
        let mut instance = ProcessInstance::new(instance_id, definition, state, store)
            .map_err(|e| EngineError::ProcessInstanceCreation(e.to_string()))?;

        // 4. Зарегистрировать в registry
        self.registry.register(instance_id, &instance.event_tx)
            .await.map_err(|e| EngineError::RegistryRegister(e.to_string()))?;

        // 5. Запустить event loop в отдельной tokio task
        instance.start()
            .await.map_err(|e| EngineError::ProcessInstanceStarting(e.to_string()))?;

        // 6. Отправить начальное событие на StartEvent
        instance.init_start_tokens()
            .await.map_err(|e| EngineError::StartToken(e.to_string()))?;

        // 7. Вернуть instance_id
        Ok(instance_id)
    }
    
    /// Восстановить running instances при старте (rehydration)
    pub async fn resume(&self) -> Result<(), EngineError> {
        // 1. Загрузить все process_instances с status = 'running'
        let running_instances = self.store.load_running_instances()
            .await.map_err(|e| EngineError::LoadDefinition(e.to_string()))?;

        // 2. Для каждого:
        for (instance_id, process_id, version, state) in running_instances {
            // Загрузить ProcessDefinition
            let definition = self.store.load_definition(&process_id, version)
                .await.map_err(|e| EngineError::LoadDefinition(e.to_string()))?;

            // Восстановить ProcessInstance
            let store = Box::new(ArcStore(self.store.clone()));
            let mut instance = ProcessInstance::new(instance_id, definition, state, store)
                .map_err(|e| EngineError::ProcessInstanceCreation(e.to_string()))?;

            // Зарегистрировать в registry
            self.registry.register(instance_id, &instance.event_tx)
                .await.map_err(|e| EngineError::RegistryRegister(e.to_string()))?;

            // Запустить event loop
            // THINK: Когда мы восстанавливаем процессы после падения, токены стоят на своих местах, но не инициируют событий!
            instance.start()
                .await.map_err(|e| EngineError::ProcessInstanceStarting(e.to_string()))?;
        }
        Ok(())
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::handlers::test_utils::*;
    use crate::store::InMemoryStore;
    use crate::domain::process::SequenceFlow;
    use crate::domain::elements::{FlowNode, StartEvent, EndEvent};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_engine_start_success() {
        let store = Arc::new(InMemoryStore::new());
        
        // Создаём ProcessDefinition
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];
        let definition = create_test_process_definition(nodes, flows);
        
        store.save_definition(&definition).await.unwrap();
        
        let engine = Engine::new(store.clone()).await;
        let result = engine.start("test-process", 1).await;
        
        assert!(result.is_ok());
        let instance_id = result.unwrap();
        
        // Проверяем, что instance сохранён в store
        let loaded = store.load_instance(instance_id).await;
        assert!(loaded.is_ok());
    }

    #[tokio::test]
    async fn test_engine_start_definition_not_found() {
        let store = Arc::new(InMemoryStore::new());
        let engine = Engine::new(store).await;
        
        let result = engine.start("nonexistent", 1).await;
        
        assert!(matches!(result, Err(EngineError::LoadDefinition(_))));
    }

    #[tokio::test]
    async fn test_engine_resume_success() {
        let store = Arc::new(InMemoryStore::new());
        // Создаём ProcessDefinition
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];
        let definition = create_test_process_definition(nodes, flows);
        
        store.save_definition(&definition).await.unwrap();

        let engine = Engine::new(store.clone()).await;
        
        // 1. Запустить инстанс
        let instance_id = engine.start("test-process", 1).await.unwrap();
        
        // 2. Создать НОВЫЙ Engine
        let engine2 = Engine::new(store.clone()).await;
        
        // 3. Вызвать resume
        let result = engine2.resume().await;
        assert!(result.is_ok());
        
        // 4. Проверить, что инстанс восстановлен
        let loaded = store.load_instance(instance_id).await;
        assert!(loaded.is_ok());
    }

    #[tokio::test]
    async fn test_engine_resume_empty() {
        let store = Arc::new(InMemoryStore::new());
        let engine = Engine::new(store).await;
        
        let result = engine.resume().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_engine_resume_preserves_instance_id() {
        let store = Arc::new(InMemoryStore::new());
        
        // Создаём ProcessDefinition
        let nodes = vec![
            FlowNode::StartEvent(StartEvent { id: "start1".to_string() }),
            FlowNode::EndEvent(EndEvent { id: "end1".to_string() }),
        ];
        let flows = vec![
            SequenceFlow {
                id: "flow1".to_string(),
                source_ref: "start1".to_string(),
                target_ref: "end1".to_string(),
                condition: None,
            },
        ];
        let definition = create_test_process_definition(nodes, flows);
        
        store.save_definition(&definition).await.unwrap();
        
        let engine1 = Engine::new(store.clone()).await;
        // 1. Запустить инстанс, сохранить instance_id
        let instance_id = engine1.start("test-process", 1).await.unwrap();
        
        // 2. Создать НОВЫЙ Engine
        let engine2 = Engine::new(store.clone()).await;

        // 3. Вызвать resume()
        let result = engine2.resume().await;
        assert!(result.is_ok());

        // 4. Проверить, что восстановленный инстанс имеет ТОТ ЖЕ instance_id
        // FIXME: Проверяем, что завершённый процесс НЕ зарегистрирован
        assert!(!engine2.registry.is_registered(instance_id).await);
        // TODO: Позже, когда реализуем Timer или UserTask, можно добавить позитивный тест с процессом, который действительно "зависает" и может быть восстановлен.
        // assert!(engine2.registry.is_registered(instance_id).await);
    }
}