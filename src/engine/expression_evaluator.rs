use std::collections::HashMap;

use evalexpr::ContextWithMutableVariables;
use evalexpr::HashMapContext;
use evalexpr::eval_boolean_with_context;
use evalexpr::Value as EvalValue;

use serde_json::Value as JsonValue;

use crate::domain::expression::{Expression, ExpressionError};


pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Оценить выражение
    /// 
    /// Возвращает `true`, если условие выполнено, `false` в противном случае.
    pub fn evaluate(
        expression: &Expression,
        variables: &HashMap<String, serde_json::Value>
    ) -> Result<bool, ExpressionError> {
        match expression {
            Expression::Simple(expr) => {
                // Создать контекст из переменных
                let context = create_context(variables)
                    .map_err(|e| ExpressionError::ContextError(e.to_string()))?;

                // Оценить выражение
                eval_boolean_with_context(expr, &context)
                    .map_err(|e| ExpressionError::EvaluationError(e.to_string()))
            }
        }
    }

    // TODO: проверки синтаксиса без оценки (например: при парсинге BPMN)
    // pub fn validate(&self) -> Result<_, ExpressionError> {}
}

// TODO: Если выражение оценивается многократно с одними переменными, можно кэшировать контекст.

/// Создать контекст для eval_boolean_with_context
fn create_context(variables: &HashMap<String, serde_json::Value>) -> Result<HashMapContext, ExpressionError> {
    let mut context = HashMapContext::new();

    for (identifier, value) in variables {
        context.set_value(identifier.clone(), json_to_eval(value))
            .map_err(|e| ExpressionError::ContextError(format!("Failed to set variable '{}': {}", identifier, e)))?;
    }

    Ok(context)
}

/// Преобразовать serde_json::Value в evalexpr::Value
fn json_to_eval(json_value: &JsonValue) -> EvalValue {
    match json_value {
        JsonValue::Bool(b) => EvalValue::Boolean(*b),
        JsonValue::Null => EvalValue::Empty,
        JsonValue::String(s) => EvalValue::String(s.clone()),
        JsonValue::Array(arr) => EvalValue::Tuple(arr.iter().map(json_to_eval).collect()),
        JsonValue::Number(n) => {
            // evalexpr различает Int и Float
            if let Some(i) = n.as_i64() {
                EvalValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                EvalValue::Float(f)
            } else {
                // Fallback для очень больших чисел
                EvalValue::String(n.to_string())
            }
        },
        JsonValue::Object(_) => {
            // Для объектов используем строковое представление
            // TODO: реализовать вложенный контекст
            EvalValue::String(json_value.to_string())            
        }
    }
}

// Тесты
#[cfg(test)]
mod tests {
    use super::*;

    // amount > 1000
    #[test]
    fn test_expression_amount() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1500));

        let expr = Expression::Simple("amount > 1000".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }

    // amount > 1000 && status == 'approved'
    #[test]
    fn test_expression_logical_and() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1500));
        variables.insert("status".to_string(), serde_json::json!("approved"));

        let expr = Expression::Simple("amount > 1000 && status == \"approved\"".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }

    // status == 'pending' || status == 'review'
    #[test]
    fn test_expression_logical_or() {
        let mut variables = HashMap::new();
        variables.insert("status".to_string(), serde_json::json!("review"));

        let expr = Expression::Simple("status == \"pending\" || status == \"review\"".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }

    // !(status == 'rejected')
    #[test]
    fn test_expression_logical_not() {
        let mut variables = HashMap::new();
        variables.insert("status".to_string(), serde_json::json!("approved"));

        let expr = Expression::Simple("!(status == \"rejected\")".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }

    // Невалидное выражение: "amount >"
    #[test]
    fn test_expression_invalid() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1));

        let expr = Expression::Simple("amount >".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(matches!(result, Err(ExpressionError::EvaluationError(_))));
    }

    // Отсутствующая переменная: "unknown_var > 100"
    #[test]
    fn test_expression_unknown_variable() {
        let variables = HashMap::new();

        let expr = Expression::Simple("unknown_var > 1000".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(matches!(result, Err(ExpressionError::EvaluationError(_))));
    }

    // Выражение не возвращает boolean: "amount + 100"
    #[test]
    fn test_expression_not_boolean() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(50));

        let expr = Expression::Simple("amount + 100".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(matches!(result, Err(ExpressionError::EvaluationError(_))));
    }

    // Вложенные скобки: "(amount > 1000) && (status == \"approved\" || status == \"review\")"
    #[test]
    fn test_expression_complex() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1500));
        variables.insert("status".to_string(), serde_json::json!("approved"));

        let expr = Expression::Simple("(amount > 1000) && (status == \"approved\" || status == \"review\")".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }

    // Арифметика: "amount * 2 == 2000"
    #[test]
    fn test_expression_calculation() {
        let mut variables = HashMap::new();
        variables.insert("amount".to_string(), serde_json::json!(1000));

        let expr = Expression::Simple("amount * 2 == 2000".to_string());
        let result = ExpressionEvaluator::evaluate(&expr, &variables);
        assert!(result.is_ok());

        assert_eq!(result.unwrap(), true);
    }
}
