use std::collections::HashMap;

use anyhow::{Ok, Result};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::domain::process::ProcessDefinition;
use crate::domain::state::InstanceState;
use crate::store::store_trait::StateStore;

pub struct InMemoryStore {
    definitions: Mutex<HashMap<(String, u32), ProcessDefinition>>,
    instances: Mutex<HashMap<Uuid, (String, u32, InstanceState)>>,
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
            definition_id.to_string(),
            definition_version,
            state.clone()
        );
        instances.insert(id, v);
        Ok(())
    }

    async fn load_instance(&self, id: Uuid) -> Result<(String, u32, InstanceState)> {
        let instances = self.instances.lock().await;
        match instances.get(&id) {
            Some(v) => Ok(v.clone()),
            None => anyhow::bail!("Не удалось найти экземпляр процесса по ID: {}", id),
        }
    }

    async fn update_instance_state(&self, id: Uuid, state: &InstanceState) -> Result<()> {
        let mut instances = self.instances.lock().await;
        match instances.get_mut(&id) {
            Some((_, _, current_state)) => {
                *current_state = state.clone();
                Ok(())
            },
            None => anyhow::bail!("Не удалось найти экземпляр процесса по ID: {}", id),
        }
    }
}