use std::sync::Arc;

use crate::domain::process::ProcessDefinition;
use crate::domain::state::InstanceState;
use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait StateStore: Send + Sync {
    // Работа с определениями процессов
    async fn save_definition(&self, def: &ProcessDefinition) -> Result<()>;
    async fn load_definition(&self, id: &str, version: u32) -> Result<ProcessDefinition>;
    async fn get_latest_version(&self, id: &str) -> Result<Option<u32>>;

    // Работа с экземплярами процессов
    async fn create_instance(&self, id: Uuid, definition_id: &str, definition_version: u32, state: &InstanceState) -> Result<()>;
    async fn update_instance_state(&self, id: Uuid, state: &InstanceState) -> Result<()>;
    async fn load_instance(&self, id: Uuid) -> Result<(String, u32, InstanceState)>;
}

pub struct ArcStore(pub Arc<dyn StateStore>);

#[async_trait]
impl StateStore for ArcStore {
    async fn save_definition(&self, def: &ProcessDefinition) -> anyhow::Result<()> {
        self.0.save_definition(def).await
    }
    
    async fn load_definition(&self, id: &str, version: u32) -> anyhow::Result<ProcessDefinition> {
        self.0.load_definition(id, version).await
    }
    
    async fn get_latest_version(&self, id: &str) -> anyhow::Result<Option<u32>> {
        self.0.get_latest_version(id).await
    }
    
    async fn create_instance(&self, id: Uuid, definition_id: &str, definition_version: u32, state: &InstanceState) -> anyhow::Result<()> {
        self.0.create_instance(id, definition_id, definition_version, state).await
    }
    
    async fn update_instance_state(&self, id: Uuid, state: &InstanceState) -> anyhow::Result<()> {
        self.0.update_instance_state(id, state).await
    }
    
    async fn load_instance(&self, id: Uuid) -> anyhow::Result<(String, u32, InstanceState)> {
        self.0.load_instance(id).await
    }
}