use std::collections::HashMap;

use anyhow::{Ok, Result};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::domain::process::ProcessDefinition;
use crate::domain::state::InstanceState;
use crate::store::store_trait::StateStore;


#[derive(Clone, PartialEq)]
pub enum InMemoryState {
    Running,
    Complete,
}

pub struct InMemoryStore {
    definitions: Mutex<HashMap<(String, u32), ProcessDefinition>>,
    instances: Mutex<HashMap<Uuid, (InMemoryState, String, u32, InstanceState)>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            definitions: Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StateStore for InMemoryStore {
    async fn save_definition(&self, v: &ProcessDefinition) -> Result<()> {
        let mut definitions = self.definitions.lock().await;
        let k = (v.id.clone(), v.version);
        definitions.insert(k, v.clone());
        Ok(())
    }

    async fn load_definition(&self, id: &str, version: u32) -> Result<ProcessDefinition> {
        let definitions = self.definitions.lock().await;
        let k = (id.to_string(), version);
        match definitions.get(&k) {
            Some(process) => Ok(process.clone()),
            None => anyhow::bail!("Не удалось найти процесс по ID: {}, версии {}", id, version),
        }
    }

    async fn get_latest_version(&self, id: &str) -> Result<Option<u32>> {
        let definitions = self.definitions.lock().await;
        let versions: Vec<u32> = definitions.keys()
            .filter(|(cid, _)| cid == id)
            .map(|(_, version)| *version)
            .collect();
        Ok(versions.into_iter().max())
    }

    async fn create_instance(&self, id: Uuid, definition_id: &str, definition_version: u32, state: &InstanceState) -> Result<()> {
        let mut instances = self.instances.lock().await;
        let v = (
            InMemoryState::Running,
            definition_id.to_string(),
            definition_version,
            state.clone()
        );
        instances.insert(id, v);
        Ok(())
    }

    async fn update_instance_state(&self, id: Uuid, state: &InstanceState) -> Result<()> {
        let mut instances = self.instances.lock().await;
        match instances.get_mut(&id) {
            Some((status, _, _, current_state)) => {
                *current_state = state.clone();
                // Обновляем статус на основе токенов
                *status = if state.current_tokens.is_empty() {
                    InMemoryState::Complete
                } else {
                    InMemoryState::Running
                };
                Ok(())
            },
            None => anyhow::bail!("Не удалось найти экземпляр процесса по ID: {}", id),
        }
    }

    async fn load_instance(&self, id: Uuid) -> Result<(String, u32, InstanceState)> {
        let instances = self.instances.lock().await;
        match instances.get(&id) {
            Some(v) => Ok((v.1.clone(), v.2, v.3.clone())),
            None => anyhow::bail!("Не удалось найти экземпляр процесса по ID: {}", id),
        }
    }

    async fn load_running_instances(&self) ->  Result<Vec<(Uuid, String, u32, InstanceState)>> {
        let instances = self.instances.lock().await;
        let results = instances
            .iter()
            .filter(|(_, (state, _, _, _,))| *state == InMemoryState::Running)
            .map(|(id, v)| (
                id.clone(),
                v.1.clone(),
                v.2,
                v.3.clone(),
            ))
            .collect();

        Ok(results)
    }
}