/// Construct the AST given the parsed statements
/// 
/// So what do we want to do 
/// 
/// we need a new layer of abstractions basically, not just statements and calls
/// 
/// 
/// so very simply, the projection will have one child, as will the datasource and predicate
/// only the join will have 2 children.
/// 
/// now to fin some way to extract this structure from our IR
/// 
/// very simply, we go through each statement in order, keep some variable map, and construct the final IR in a forward fashion
/// 
/// there is still a bunch of stuff missing from all of this, like CREATE, DELETE, etc (nee some seperate way of addressing those,) they should be ignored
/// here. create can maybe be kept under adta source but its ultimately useless. we'll see
/// 
/// 
/// 
/// data sources are root nodes, like variables, they must be defined first
/// the transformations already applied to them must be kept in a vec to be optimize later
/// join expressions can reference these variables, as new childdren.
/// this would mean the predicates are kept inside the data sources vec and the join only sees the source
/// these joins would have to be stored in variables also, so they can be referencedd by successive operations
/// 
/// anything not stored in a variable that doesnt access a variable gets optimize away
/// successive transformations of a variable shoul be taken note of? why woul we filter A after joining A an B? THINK ABOUT THIS
/// 
/// the projection operation should reference only one child, which could be a join
/// ultimately, this should be easier as we will only have data sources and joins, with the predicates stored within and easily shuffleable
/// 
/// 
/// but what use would the projection be? root noe/ tie it together?
/// 
///
/// this shoul be easy to work on.
/// 
/// 
/// things like aggregate expressions and nested subqueries should be considered. think i forgot about those
/// 
/// also, we might need to check the type validity of the program at this stage
/// even validating method ordering might be something to be one at this stage
/// 
/// 
/// our projection pushdown is going to be very weak anyways, unless the final output is going to be very restricted, because we already have projection
/// functions in our transforms



// struct Predicate {

// }

struct Transform {

    method: String, // preferrably an enum
    arguments: Vec<Expr> // convert it to a Vec of Value Types
}

struct Projection {

}

struct DataSource { 
    tableName: String,
    transforms: Vec<Transform>,
}

struct Join { 

}


struct ASTGen{
    lookupTable : Hashmap<String, Expr> 

}



impl ASTGen {

    fn pasrse(&mut self, statements: Vec<Stmt>){
        for statement in statements {
            match statement{
                Var(VarStmt) => {
                    //create some new variable and assign it to some 
                    
                    let name = statement.name;




                }
                Expression(ExprStmt) =>  {
                    //something to be evaluated on an existing variable
                }
                Print(PrintStmt) => _
            }
        }
    }
}