use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// Условие для перехода
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Simple(String),
}

#[derive(Debug)]
pub enum ExpressionError {
    ContextError(String),
    EvaluationError(String),
    ParseError(String),
}

impl Display for ExpressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionError::ContextError(s) => {
                write!(f, "Context creation error: {}", s)
            }
            ExpressionError::EvaluationError(s) => {
                write!(f, "Evaluation error: {}", s)
            }
            ExpressionError::ParseError(s) => {
                write!(f, "Parse error: {}", s)
            }
        }
    }
}

impl std::error::Error for ExpressionError {}
