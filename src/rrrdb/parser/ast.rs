pub(crate) enum Statement {
  Select(Query),
  Insert(Insert),
}
pub(crate) struct Query { 
  projections: Vec<Projection>, 
  from: Table,
  predicates: Vec<Predicate>,
}
pub(crate) enum Projection {
  Expression(Expression),
  Wildcard,
}
type Table = String;
pub(crate) enum Predicate {
  Expression(Expression),
}
pub(crate) struct Insert { 
  // TODO
}
pub(crate) enum Expression { 
  Ident(String),
  Wildcard, 
  Value(Value),
  Operator(Operator),
}

pub(crate) enum Value {
  Number(String),
  QuotedString(String),
  Boolean(bool),
  Null,
}

pub(crate) enum Operator {
  BinOperator {
    lhs: Box<Expression>,
    rhs: Box<Expression>,
    op: BinaryOperator,
  }
}

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