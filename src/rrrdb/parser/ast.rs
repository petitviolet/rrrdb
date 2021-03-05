use crate::rrrdb::schema::Column;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Statement {
    Select(Query),
    Insert(Insert),
    CreateDatabase(CreateDatabase),
    CreateTable(CreateTable),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Query {
    pub projections: Vec<Projection>,
    pub froms: Vec<Table>,
    pub predicate: Predicate,
}
impl Query {
    pub fn new(projections: Vec<Projection>, froms: Vec<Table>, predicate: Predicate) -> Self {
        Self {
            projections,
            froms,
            predicate,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Projection {
    Expression(Expression),
    Wildcard,
}
type Table = String;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Predicate {
    expression: Option<Expression>,
}
impl Predicate {
    pub fn empty() -> Self {
        Self { expression: None }
    }
    pub fn new(expression: Expression) -> Self {
        Self {
            expression: Some(expression),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Insert {
    // TODO
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CreateDatabase {
    pub(crate) name: String,
}
impl CreateDatabase {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CreateTable {
    pub(crate) database_name: String,
    pub(crate) table_name: String,
    pub(crate) column_definitions: Vec<ColumnDefinition>,
}
impl CreateTable {
    pub fn new(
        database_name: String,
        table_name: String,
        column_definitions: Vec<ColumnDefinition>,
    ) -> Self {
        Self {
            database_name,
            table_name,
            column_definitions,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ColumnDefinition {
    name: String,
    column_type: String,
}
impl ColumnDefinition {
    pub fn new(name: String, column_type: String) -> Self {
        Self { name, column_type }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Expression {
    Ident(String),
    Value(Value),
    BinOperator {
        lhs: Box<Expression>,
        rhs: Box<Expression>,
        op: BinaryOperator,
    },
}

impl Expression {
    pub fn ident(i: &str) -> Expression {
        Self::Ident(i.to_string())
    }
    pub fn number(n: &str) -> Expression {
        Self::Value(Value::Number(n.to_string()))
    }
    pub fn quoted_string(s: &str) -> Expression {
        Self::Value(Value::QuotedString(s.to_string()))
    }
    pub fn boolean(b: bool) -> Expression {
        Self::Value(Value::Boolean(b))
    }
    pub fn null() -> Expression {
        Self::Value(Value::Null)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Value {
    Number(String),
    QuotedString(String),
    Boolean(bool),
    Null,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

impl BinaryOperator {
    pub fn build(self, left: Expression, right: Expression) -> Expression {
        Expression::BinOperator {
            lhs: Box::new(left),
            rhs: Box::new(right),
            op: self,
        }
    }
}
