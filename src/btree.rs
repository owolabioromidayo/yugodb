// it is actually important that I use the pager, nice attempt at using traits though




//B+ Trees store values only in leaf nodes 

// what would be the dtype for keys nad values

//some progress made, search should be working
//but we need insertion to work before we can test it
//we have search and insert now, but weve not covered creating new nodes or keys

//and we've not covered the linking of new leaf nodes.

//we also need recursive serialization and deserialization


const M: u8 = 32;
//ahh, we should actually be using usize, which will take from the architecture

//the impls im seeing actualy dont store any of this in memory, they read and write from pages instead.


trait BPTreeNode {
    fn new() -> Result<Box<dyn BPTreeNode>, Err>;

    fn serialize(&self) -> Vec<u8>;
    fn deserialize() -> Result<Box<dyn BPTreeNode>, Err>;

    fn search(&self, key:u8) -> Result<(u8,u8), Err>;
    fn insert(&self, key:u8, page:u8, offset:u8) -> Result<(), Err>;

    fn is_leaf(&self) -> bool;
    fn is_root(&self) -> bool;
}

struct BPTreeInternalNode {
    keys : [u8; M],
    is_root: bool,
    // children: [Box <dyn BPTreeNode>; M+1],
    children: [Box <dyn BPTreeNode>; M+1],
}

struct BPTreeLeafNode {
    values: HashMap<u8, (u8,u8) >, 
    next_node: Optional<&BPTreeLeafNode> //  LList
    //do we want backwards links>
}


impl BTreeNode for BTreeLeafNode{

    fn new() -> Result<Box<dyn BPTreeNode>, Err>{
        Box::new(BTreeLeafNode {
        })
    }

    fn search(key: u8) -> Result<(u8, u8), Err>{

        if let Some(x) = self.values.get(key) {
            return Ok(*x); 
        }

        Err(Err::NotFound)
    }

    //doing this because I dont want it to affect serialization, might have to think of a better traeoff
    fn is_root() -> bool { false}
    fn is_leaf() -> bool { true}


    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    fn deserialize() -> Result<Box<dyn BPTreeNode>, Err>{
        unimplemented!()
    }

    fn insert(key:u8, page:u8, offset:u8) -> Result<(), Err> {
        //if it makes it all the way here, its fine
        self.values.insert(key, (page, offset));
        Ok()
    }
}


impl BPTreeNode for BPTreeInternalNode{

    fn new() -> Result<Box<dyn BPTreeNode>, Err>{
        Box::new(BTreeInternalNode{
            keys: [0 as u8; M],
            //is unsafe code the only way to initialize this
            // children: [Box::new(Default::default)]
        })
    }


    fn search(key: u8) -> Result<(u8, u8), Err>{

        for (id, k) in self.keys.iter().enumerate() {
            if *k > key {
                return self.children[id].search(key);
            }
        }
        //check last one 
        return self.children[M + 1].search(key);

    }

    fn insert(key:u8, page:u8, offset:u8) -> Result<(), Err> {

        for (id, k) in self.keys.iter().enumerate() {
            if *k > key {
                return self.children[id].insert(key, page, offset);
            }
        }
        //check last one 
        return self.children[M + 1].insert(key, page, offset);

    }

    fn is_root() -> bool { self.is_root }
    fn is_leaf() -> bool { false }

    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    fn deserialize() -> Result<Box<dyn BPTreeNode>, Err>{
        unimplemented!()
    }



}
impl BTreeLeafNode{

}

impl BTreeInternalNode {


    fn split() {}

    fn merge() {}
        
}

#[cfg(test)]

mod test {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn test_new_btree_node(){
        assert_eq!( TypeId::of::<BTreeNode>(), BTreenNode::new());
    }
}