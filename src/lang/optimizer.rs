///
/// 
/// 
/// so the optimizer is going to take from the AST
/// for the static part, there seems to be only 2 main goals
/// projection pushdown firstly
/// optimize the order of predicate expressions stored in a node
/// 
/// is join ordering optimization going to be a thing here?
/// deciding the join algorithm is not something that can be done at this level (given no information concerning the DB layout)
/// 
/// we dont have anything on nested subqueries, that should be fine?