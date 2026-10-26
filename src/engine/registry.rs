use std::{collections::HashMap, fmt::Display};

use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

use crate::engine::instance::ProcessEvent;


#[derive(Debug)]
pub enum InstanceRegistryError {
    InstanceRegistred(Uuid),
    InstanceNotRegistred(Uuid),
    CantSendEvent(Uuid, String),
}

impl Display for InstanceRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InstanceRegistred(id) => write!(f, "Instance with ID \"{}\" already registred", id),
            Self::InstanceNotRegistred(id) => write!(f, "Instance \"{}\" NOT registred", id),
            Self::CantSendEvent(id, msg) => write!(f, "Can't send event for instance \"{}\": {}", id, msg),
        }
    }
}

pub struct InstanceRegistry {
    instances: RwLock<HashMap<Uuid, mpsc::Sender<ProcessEvent>>>,
}

impl InstanceRegistry {

    pub fn new() -> Self {
        Self {
            instances: RwLock::new(HashMap::new()),
        }
    }

    /// Зарегистрировать запущенный экземпляр
    pub async fn register(&self, instance_id: Uuid, event_tx: &mpsc::Sender<ProcessEvent>) -> Result<(), InstanceRegistryError> {
        let mut wg = self.instances.write().await;
        match wg.insert(instance_id, event_tx.clone()) {
            Some(_) => Err(InstanceRegistryError::InstanceRegistred(instance_id)),
            None => Ok(()),
        }
    }

    /// Аннулировать
    pub async fn unregister(&self, instance_id: Uuid) {
        let mut wg = self.instances.write().await;
        wg.remove(&instance_id);
    }

    /// Проверить, что экземпляр зарегестрирован
    pub async fn is_registered(&self, instance_id: Uuid) -> bool {
        let rg = self.instances.read().await;
        rg.contains_key(&instance_id)
    }


    /// Отправить событие для запущенного экземпляра
    pub async fn send_event(&self, instance_id: Uuid, event: ProcessEvent) -> Result<(), InstanceRegistryError> {
        let rg = self.instances.read().await;
        match rg.get(&instance_id) {
            Some(tx) => tx.send(event).await
                .map_err(|e| InstanceRegistryError::CantSendEvent(instance_id, e.to_string())),
            None => Err(InstanceRegistryError::InstanceNotRegistred(instance_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_registry_register_success() {
        let registry = InstanceRegistry::new();
        let (tx, _rx) = mpsc::channel(10);
        let instance_id = Uuid::new_v4();
        
        let result = registry.register(instance_id, &tx).await;
        
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_registry_register_duplicate() {
        let registry = InstanceRegistry::new();
        let (tx, _rx) = mpsc::channel(10);
        let instance_id = Uuid::new_v4();
        
        registry.register(instance_id, &tx).await.unwrap();
        let result = registry.register(instance_id, &tx).await;
        
        assert!(matches!(result, Err(InstanceRegistryError::InstanceRegistred(_))));
    }

    #[tokio::test]
    async fn test_registry_unregister() {
        let registry = InstanceRegistry::new();
        let (tx, _rx) = mpsc::channel(10);
        let instance_id = Uuid::new_v4();
        
        registry.register(instance_id, &tx).await.unwrap();
        registry.unregister(instance_id).await;
        
        // Проверяем, что instance удалён (через send_event)
        let result = registry.send_event(instance_id, ProcessEvent::Complete).await;
        assert!(matches!(result, Err(InstanceRegistryError::InstanceNotRegistred(_))));
    }

    #[tokio::test]
    async fn test_registry_send_event_success() {
        let registry = InstanceRegistry::new();
        let (tx, mut rx) = mpsc::channel(10);
        let instance_id = Uuid::new_v4();
        
        registry.register(instance_id, &tx).await.unwrap();
        
        let event = ProcessEvent::Complete;
        registry.send_event(instance_id, event).await.unwrap();
        
        // Проверяем, что событие получено
        let received = rx.recv().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_registry_send_event_not_found() {
        let registry = InstanceRegistry::new();
        let instance_id = Uuid::new_v4();
        
        let result = registry.send_event(instance_id, ProcessEvent::Complete).await;
        
        assert!(matches!(result, Err(InstanceRegistryError::InstanceNotRegistred(_))));
    }
}