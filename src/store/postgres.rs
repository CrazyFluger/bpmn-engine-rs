use std::collections::HashMap;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::types::Json;
use uuid::Uuid;

use crate::domain::process::ProcessDefinition;
use crate::domain::state::{InstanceState, Token};
use crate::store::StateStore;


pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(database_url).await?;

        // Применяем миграции при старте
        sqlx::migrate!("./migrations").run(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl StateStore for PostgresStore {

    async fn save_definition(&self, def: &ProcessDefinition) -> anyhow::Result<()> {
        // Сериализуем ProcessDefinition в JSON
        let definition = serde_json::to_value(def)?;

        sqlx::query("INSERT INTO process_definitions (id, version, name, definition) VALUES ($1, $2, $3, $4)")
            .bind(&def.id)
            .bind(def.version as i32) // u32 → i32 для PostgreSQL
            .bind(&def.name)
            .bind(&definition)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn load_definition(&self, id: &str, version: u32) -> anyhow::Result<ProcessDefinition> {
        let row: (Json<ProcessDefinition>,) = sqlx::query_as("SELECT definition FROM process_definitions WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(version as i32)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => anyhow!("Process definition '{}' version {} not found", id, version),
                _ => e.into(),
            })?;
        Ok(row.0.0)
    }

    async fn get_latest_version(&self, id: &str) -> anyhow::Result<Option<u32>> {
        let row = sqlx::query("SELECT MAX(version) as max_version FROM process_definitions WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
        
        let version: Option<i32> = row.get("max_version");
        Ok(version.map(|v| v as u32))
    }

    async fn create_instance(
        &self, 
        id: Uuid, 
        definition_id: &str, 
        definition_version: u32, 
        state: &InstanceState,
    ) -> anyhow::Result<()> {
        let tokens_json = serde_json::to_value(&state.current_tokens)?;
        let variables_json = serde_json::to_value(&state.variables)?;
        
        sqlx::query(
                "INSERT INTO process_instances 
                (id, definition_id, definition_version, status, current_tokens, variables) 
                VALUES ($1, $2, $3, 'running', $4::jsonb, $5::jsonb)"
            )
            .bind(id)
            .bind(definition_id)
            .bind(definition_version as i32)
            .bind(&tokens_json)
            .bind(&variables_json)
            .execute(&self.pool)
            .await?;
        
        Ok(())
    }

    async fn update_instance_state(&self, id: Uuid, state: &InstanceState) -> anyhow::Result<()> {
        let tokens_json = serde_json::to_value(&state.current_tokens)?;
        let variables_json = serde_json::to_value(&state.variables)?;
        
        // Определяем статус: если токенов нет — процесс завершён
        let status = if state.current_tokens.is_empty() { 
            "completed" 
        } else { 
            "running" 
        };
        
        let result = sqlx::query(
                "UPDATE process_instances 
                SET current_tokens = $1::jsonb, 
                    variables = $2::jsonb, 
                    status = $3, 
                    updated_at = CURRENT_TIMESTAMP 
                WHERE id = $4"
            )
            .bind(&tokens_json)
            .bind(&variables_json)
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            anyhow::bail!("Instance '{}' not found", id);
        }        

        Ok(())
    }

    async fn load_instance(&self, id: Uuid) -> anyhow::Result<(String, u32, InstanceState)> {
        let (definition_id, definition_version, current_tokens, variables) = sqlx::query_as::<_, (String, i32, Json<Vec<Token>>, Json<HashMap<String, Value>>)>(
                "SELECT definition_id, definition_version, current_tokens, variables 
                FROM process_instances 
                WHERE id = $1"
            )
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| match e {
                sqlx::Error::RowNotFound => anyhow!("Instance '{}' not found", id),
                _ => e.into(),
            })?;
        let state = InstanceState { 
                current_tokens: current_tokens.0,
                variables: variables.0,
            };
        
        Ok((
            definition_id,
            definition_version as u32,
            state,
        ))
    }

    async fn load_running_instances(&self) -> anyhow::Result<Vec<(Uuid, String, u32, InstanceState)>> {
        let records = sqlx::query_as::<_, (Uuid, String, i32, Json<Vec<Token>>, Json<HashMap<String, Value>>)>(
                "SELECT id, definition_id, definition_version, current_tokens, variables 
                FROM process_instances 
                WHERE status = $1"
            )
            .bind("running".to_string())
            .fetch_all(&self.pool)
            .await?;

        Ok(records
            .iter()
            .map(|(id, definition_id, definition_version, current_tokens, variables)| (
                id.clone(),
                definition_id.clone(),
                *definition_version as u32,
                InstanceState { 
                    current_tokens: current_tokens.0.clone(),
                    variables: variables.0.clone(),
                }
            ))
            .collect())
    }
}
