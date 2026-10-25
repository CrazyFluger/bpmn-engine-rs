use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Токен в определённом узле
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Token {
    /// ID узла
    pub node_id: String,
}

/// Текущее состояние экземпляра
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceState {
    /// Текущие токены процесса
    pub current_tokens: Vec<Token>,
    /// Переменные процесса
    pub variables: HashMap<String, serde_json::Value>,
}