#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use bpmn_engine_rs::domain::state::Token;
    use uuid::Uuid;

    use bpmn_engine_rs::domain::process::ProcessDefinition;
    use bpmn_engine_rs::domain::state::InstanceState;
    use bpmn_engine_rs::store::PostgresStore;
    use bpmn_engine_rs::store::StateStore;

    // Требуется запущенный PostgreSQL
    // Установите DATABASE_URL в .env или переменные окружения
    
    async fn setup_store() -> PostgresStore {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://bpmn:bpmn@localhost/bpmn".to_string());
        PostgresStore::new(&database_url).await.unwrap()
    }

    fn create_test_definition(id: &str, version: u32) -> ProcessDefinition {
        ProcessDefinition {
            id: id.to_string(),
            version,
            name: None,
            flow_nodes: HashMap::new(),
            sequence_flows: vec![],
            start_events: vec![],
        }
    }

    fn create_test_state() -> InstanceState {
        let current_tokens = vec![ Token { node_id: "test-token-1".to_string() }];
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1500));

        InstanceState {
            current_tokens,
            variables,
        }
    }

    fn create_process_id() -> String {
        format!("__test-process-{}", Uuid::new_v4())
    }

    async fn cleanup(store: &PostgresStore, process_id: &str) {
        // Сначала удаляем instances (из-за FK constraints)
        sqlx::query("DELETE FROM process_instances WHERE definition_id = $1")
            .bind(process_id)
            .execute(store.pool())
            .await
            .unwrap();
        
        // Затем удаляем definitions
        sqlx::query("DELETE FROM process_definitions WHERE id = $1")
            .bind(process_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_postgres_save_and_load_definition() {
        let store = setup_store().await;
        let id = create_process_id();
        
        // Создаём тестовое определение
        let def = create_test_definition(&id, 1);
        
        // Сохраняем
        store.save_definition(&def).await.unwrap();
        
        // Загружаем
        let loaded = store.load_definition(&id, 1).await.unwrap();
        
        assert_eq!(loaded.id, def.id);
        assert_eq!(loaded.version, def.version);
        assert_eq!(loaded.name, def.name);
        assert_eq!(loaded.flow_nodes.len(), def.flow_nodes.len());
        assert_eq!(loaded.sequence_flows.len(), def.sequence_flows.len());
        cleanup(&store, &id).await;  // Очистка после теста
    }

    #[tokio::test]
    async fn test_postgres_get_latest_version_empty() {
        let store = setup_store().await;
        
        let version = store.get_latest_version("nonexistent").await.unwrap();
        
        assert_eq!(version, None);
    }

    #[tokio::test]
    async fn test_postgres_get_latest_version_for_single() {
        let store = setup_store().await;
        let id = create_process_id();

        // Сохраняем одну версию
        store.save_definition(&create_test_definition(&id, 1)).await.unwrap();
        
        let version = store.get_latest_version(&id).await.unwrap();
        
        assert_eq!(version, Some(1));
        cleanup(&store, &id).await;  // Очистка после теста
    }

    #[tokio::test]
    async fn test_postgres_get_latest_version_for_multiple() {
        let store = setup_store().await;
        let id = create_process_id();

        // Сохраняем несколько версий
        store.save_definition(&create_test_definition(&id, 1)).await.unwrap();
        store.save_definition(&create_test_definition(&id, 2)).await.unwrap();
        store.save_definition(&create_test_definition(&id, 3)).await.unwrap();
        
        let version = store.get_latest_version(&id).await.unwrap();
        
        assert_eq!(version, Some(3));
        cleanup(&store, &id).await;  // Очистка после теста
    }

    #[tokio::test]
    async fn test_postgres_create_and_load_instance() {
        let store = setup_store().await;
        let id = create_process_id();

        // Сначала сохраняем определение
        let def = create_test_definition(&id, 1);
        store.save_definition(&def).await.unwrap();
        
        // Создаём instance
        let instance_id = Uuid::new_v4();
        let state = create_test_state();
        store.create_instance(instance_id, &id, 1, &state).await.unwrap();
        
        // Загружаем instance
        let (def_id, version, loaded_state) = store.load_instance(instance_id).await.unwrap();
        
        assert_eq!(def_id, id);
        assert_eq!(version, 1);
        assert_eq!(loaded_state.current_tokens, state.current_tokens);
        cleanup(&store, &id).await;  // Очистка после теста
    }

    #[tokio::test]
    async fn test_postgres_update_instance_state() {
        let store = setup_store().await;
        let id = create_process_id();
        
        // Создаём definition и instance
        let def = create_test_definition(&id, 1);
        store.save_definition(&def).await.unwrap();
        
        let instance_id = Uuid::new_v4();
        let state = create_test_state();
        store.create_instance(instance_id, &id, 1, &state).await.unwrap();
        
        // Обновляем состояние (удаляем все токены)
        let new_state = InstanceState {
            current_tokens: vec![],
            variables: state.variables,
        };
        store.update_instance_state(instance_id, &new_state).await.unwrap();
        
        // Загружаем и проверяем
        let (_, _, loaded_state) = store.load_instance(instance_id).await.unwrap();
        assert!(loaded_state.current_tokens.is_empty());
        cleanup(&store, &id).await;  // Очистка после теста
    }

    #[tokio::test]
    async fn test_postgres_load_nonexistent_definition() {
        let store = setup_store().await;
        
        let result = store.load_definition("nonexistent", 1).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_postgres_load_nonexistent_instance() {
        let store = setup_store().await;
        
        let result = store.load_instance(Uuid::new_v4()).await;
        
        assert!(result.is_err());
    }
}