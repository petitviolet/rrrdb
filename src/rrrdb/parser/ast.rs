#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Statement {
    Select(Query),
    Insert(Insert),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Query {
    projections: Vec<Projection>,
    from: Option<Table>,
    predicates: Vec<Predicate>,
}
impl Query {
    pub fn new(
        projections: Vec<Projection>,
        from: Option<Table>,
        predicates: Vec<Predicate>,
    ) -> Self {
        Self {
            projections,
            from,
            predicates,
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
pub(crate) enum Predicate {
    Expression(Expression),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Insert {
    // TODO
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Expression {
    Ident(String),
    Wildcard,
    Value(Value),
    Operator(Operator),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Value {
    Number(String),
    QuotedString(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Operator {
    BinOperator {
        lhs: Box<Expression>,
        rhs: Box<Expression>,
        op: BinaryOperator,
    },
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
