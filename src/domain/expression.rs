use serde::{Deserialize, Serialize};

/// Условие для перехода
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Simple(String),
}