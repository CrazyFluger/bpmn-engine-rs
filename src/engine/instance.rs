use std::collections::HashMap;

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::domain::process::ProcessDefinition;
use crate::domain::state::{InstanceState, Token};
use crate::engine::handlers::{HandleError, handle_node};
use crate::store::store_trait::StateStore;

#[derive(Debug)]
pub enum ProcessError {
    HasNoStartingEvents,
    
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessError::HasNoStartingEvents => {
                write!(f, "The process has no starting events")
            }
        }
    }
}

pub enum ProcessEvent {
    /// Доставка токена на узел
    TokenArrived { node_id: String },
    /// Завершение процесса
    Complete,
    // TODO: TimerFired
    // TODO: ExternalSignal
}

pub struct ProcessInstance {
    pub id: Uuid,
    pub definition: ProcessDefinition,
    pub state: InstanceState,
    pub event_tx: mpsc::Sender<ProcessEvent>,
    pub event_rx: mpsc::Receiver<ProcessEvent>,
    pub store: Box<dyn StateStore>,
}

impl ProcessInstance {

    /// Создать новый экземпляр процесса 
    pub fn new(
        id: Uuid,
        definition: ProcessDefinition,
        store: Box<dyn StateStore>,
    ) -> Result<Self, ProcessError> {
        let (tx, rx) = mpsc::channel(100);

        // TODO: Стартовых событий может быть несколько и каждое из них является тригером по спецификации BPMN 2.0
        let state = if let Some(start_event_id) = definition.start_events.first() {
                InstanceState {
                    current_tokens: vec![
                        Token { node_id: start_event_id.clone() }
                    ],
                    variables: HashMap::new(),
                }
            } else {
                return Err(ProcessError::HasNoStartingEvents);
            };

        Ok(Self {
            id,
            definition,
            state,
            event_tx: tx,
            event_rx: rx,
            store: store,
        })
    }

    /// Восстановить экземпляр процесса
    pub fn restore(
        id: Uuid,
        definition: ProcessDefinition,
        store: Box<dyn StateStore>,
        state: InstanceState,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            id,
            definition,
            state,
            event_tx: tx,
            event_rx: rx,
            store: store,
        }
    }

    /// Стартовать процесс
    pub async fn start(&mut self) -> Result<(), HandleError> {
        // Инициализировать состояние в StateStore
        self.store.create_instance(self.id, &self.definition.id, self.definition.version, &self.state)
            .await
            .map_err(|e| HandleError::StoreError(e.to_string()))?;
        
        // Запустить процесс
        self.run().await
    }

    // TODO: реализовать метод start_from(start_event_id: &str) для явного выбора стартового события

    /// Запустить процесс
    async fn run(&mut self) -> Result<(), HandleError> {
        // Автоматически отправляем начальное событие для StartEvent
        if let Some(start_event_id) = self.definition.start_events.first() {
            self.event_tx
                .send(ProcessEvent::TokenArrived { node_id: start_event_id.clone() })
                .await
                .map_err(|_| HandleError::ChannelClosed)?;
        }
        // Цикл обработки событий
        while let Some(event) = self.event_rx.recv().await {
            match event {
                ProcessEvent::TokenArrived { node_id } => {
                    // Обработать прибытие токена
                    handle_node(self, &node_id).await?;
                    
                    // Сохранить состояние в StateStore
                    self.store.update_instance_state(
                        self.id, 
                        &self.state
                    ).await.map_err(|e| HandleError::StoreError(e.to_string()))?;
                }
                ProcessEvent::Complete => {
                    // Процесс завершён
                    println!("Процесс #{} завершён.", self.id);
                    break;
                }
                // Если event_rx.recv() вернёт None (канал закрыт), цикл while let Some(event) просто завершится.
                // Это может быть корректным поведением, но стоит подумать — что означает закрытый канал? Ошибку или нормальное завершение?
            }
        }
        
        Ok(())
    }

    /// Добавить токен на узел
    pub async fn add_token(&mut self, node_id: &str) -> Result<(), HandleError> {
        // Добавляем токен в список
        self.state.current_tokens.push(Token { node_id: node_id.to_string() });
        // Вызываем событие ProcessEvent::TokenArrived
        self.event_tx
            .send(ProcessEvent::TokenArrived { 
                node_id: node_id.to_string() 
            })
            .await
            // Отлавливаем ошибку, если канал закрыт
            .map_err(|_| HandleError::ChannelClosed)?;
        Ok(())
    }

    /// Удалить токен с узла
    pub fn remove_token(&mut self, node_id: &str) {
        self.state.current_tokens.retain(|t| t.node_id != node_id);
    }

    /// Кол-во токенов на узле
    pub fn count_tokens_for(&mut self, node_id: &str) -> usize {
        self.state.current_tokens
            .iter()
            .filter(|t| t.node_id == node_id)
            .count()
    }

}
