// it is actually important that I use the pager, nice attempt at using traits though




//B+ Trees store values only in leaf nodes 

// what would be the dtype for keys nad values

//some progress made, search should be working
//but we need insertion to work before we can test it
//we have search and insert now, but weve not covered creating new nodes or keys

//and we've not covered the linking of new leaf nodes.

//we also need recursive serialization and deserialization


//TODO: change page and offset to usize


//limitations
//we only care about keys in increasing order
// if that is the case, why would a bptree be needed? just need some sort of heap index


//must ensure that the nodes fit in pages
// only the leaf nodes need to fit a whole page block
// since the internal nodes are so lightweight

use std::iter;
use std::collections::HashMap;
use crate::error::{Result, Error};

const M: u8 = 32;


enum BPTreeNodeEnum {
    Internal(BPTreeInternalNode),
    Leaf(BPTreeLeafNode),
}

trait BPTreeNode {
    // fn new() -> Result<BPTreeNodeEnum>;

    fn serialize(&self) -> Vec<u8>;
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>;

    fn search(&self, key:u8) -> Result<(u8,u8)>;
    fn insert(&mut self, key:u8, page:u8, offset:u8) -> Result<()>;

    fn is_leaf(&self) -> bool;
    fn is_root(&self) -> bool;

    fn split(&mut self) -> Result<BPTreeInternalNode>; 
}

struct BPTreeInternalNode {
    keys : Vec<u8>, // capacity M
    is_root: bool,
    children: Vec<Option<BPTreeNodeEnum>>, // capacity M+1
}

struct BPTreeLeafNode  {
    values: HashMap<u8, (u8,u8)>, 
    next_node: Option<Box<BPTreeLeafNode>>, //  LList
}

struct BPlusTree {
    root: BPTreeInternalNode
}


impl BPTreeNode for BPTreeLeafNode{


    fn search(&self, key: u8) -> Result<(u8, u8)>{

        if let Some(x) = self.values.get(&key) {
            return Ok(*x); 
        }

        Err(Error::AccessError)
    }

    //doing this because I dont want it to affect serialization, might have to think of a better traeoff
    fn is_root(&self) -> bool { false}
    fn is_leaf(&self) -> bool { true}


    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>{
    //     unimplemented!()
    // }

    fn insert(&mut self, key:u8, page:u8, offset:u8) -> Result<()> {
        //if it makes it all the way here, its fine
        self.values.insert(key, (page, offset));
        Ok(())
    }

    fn split(&mut self) -> Result<BPTreeInternalNode> {
        let mut keys : Vec<&u8> = self.values.keys().collect();
        keys.sort();
        let mid = keys.len() / 2;
        let middle_key = keys[mid];

        let mut left_map = HashMap::new();
        let mut right_map = HashMap::new();

        for (key, value) in self.values {
            if key < *middle_key {
                left_map.insert(key, value);
            } else {
                right_map.insert(key, value);
            }
        }

        let new_right_leaf_node = BPTreeLeafNode{
                values: right_map,
                next_node: None,
            }; 

        let new_left_leaf_node = BPTreeLeafNode{
                values: left_map,
                next_node: Some(Box::new(new_right_leaf_node)),
            }; 


        let new_node = BPTreeInternalNode::new();
        new_node.keys[0] = middle_key.clone();
        new_node.children[0] = Some(BPTreeNodeEnum::Leaf(new_left_leaf_node));
        new_node.children[1] = Some(BPTreeNodeEnum::Leaf(new_left_leaf_node)); 

        return Ok(new_node);


    }
}


impl BPTreeNode for BPTreeInternalNode{



    fn search(&self, key: u8) -> Result<(u8, u8)>{

        for (id, k) in self.keys.iter().enumerate() {
            if *k > key {
                if let Some(x) = self.children[id]{ 
                    match x {
                        BPTreeNodeEnum::Leaf(y) => return y.search(key),
                        BPTreeNodeEnum::Internal(y) => return y.search(key)
                    }
                }
            }
        }
        //check last one 
        if let Some(x) = self.children[M as usize + 1]{ 
            match x {
                BPTreeNodeEnum::Leaf(y) => return y.search(key),
                BPTreeNodeEnum::Internal(y) => return y.search(key)
            }
        }

        Err(Error::NotFound)
    }

    fn insert(&mut self, key:u8, page:u8, offset:u8) -> Result<()> {

        //top down is worse than splitting upwards it seems

        //okay, so we always insert at the end, that makes sense right

        //try inserting it here
        if self.children.len()  <  (M + 1).into() {
            //insert the key and corresponding children in sorted order
            // insert_sorted(&self.children, key);
            // let index = self.children.binary_search(&value).unwrap_or_else(|i| i);
            // self.children.insert(index, value);
            
            // create a new child?
            let new_child = BPTreeInternalNode::new();
            new_child.keys[0]= key;
            let new_child_leaf = BPTreeLeafNode::new();
            new_child_leaf.insert(key, page, offset);
            new_child.children[1] = Some(Box::new(new_child_leaf)); 

            self.children.push(Some(Box::new(new_child)));

            Ok(())

            //expand children vec
        } else {
            for (id, k) in self.keys.iter().enumerate() {
                if *k > key {
                    if let Some(curr) = self.children[id]{
                        if curr.is_leaf() {

                            

                            let curr2 = curr.downcast::<BPTreeLeafNode>()?;
                            if curr.values.len() <  M {
                                curr.insert(key, page, offset)
                            } else{
                                // we need to split the leaf node
                                //naive, create a new internal node which refs the 2 leaf nodes
                                self.children[id] = curr.split();
                                self.children[id].insert(key, page, offset);

                            }
                        } else {
                            // check if node is at capacity?
                            // we need to check if theyre actually populated

                            if curr.is_full() {
                                self.children[id] = curr.split();
                                self.children[id].insert(key, page, offset);
                                
                            } else{
                                self.children[id].insert(key, page, offset);
                            }
                        }
                    }
                }
            }
            //check last one 
            return self.children[self.children.len() -1].insert(key, page, offset);

        }

        //we might have to insert a new key here, which will lead to the splitting operation

    }

    fn is_root(&self) -> bool { self.is_root }
    fn is_leaf(&self) -> bool { false }

    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>{
    //     unimplemented!()
    // }

    fn split(&mut self) -> Result<BPTreeInternalNode> { 
        //TODO: important piece of the puzzle

        // do i need a reference to its head? s owe can try insert some things there

        //we'll just split into an internal node referencing 2 internal nodes, then we'll reconsolidate on the other end
        // like we have to know if we are updating the final value
        // its just like ensuring the tree is still split the right way

        let mut left_node = BPTreeInternalNode {
            keys: Vec::new(),
            is_root: false,
            children: Vec::new(),
        };

        let mut right_node = BPTreeInternalNode {
            keys: Vec::new(),
            is_root: false,
            children: Vec::new(),
        };

        let mid = M / 2;
        let split_key = self.keys[mid];

         // Move keys and children to the left node
        left_node.keys = self.keys[..mid].to_vec();
        left_node.children = self.children[..mid + 1].iter().map(|child| Box::new(child.clone())).collect(); // slow af it seems

        // Move keys and children to the right node
        right_node.keys = self.keys[mid + 1..].to_vec();
        right_node.children = self.children[mid + 1..].iter().map(|child| Box::new(child.clone())).collect();

        // Create a new parent node
        let mut parent_node = BPTreeInternalNode::new(); 
        parent_node.keys[0] = split_key;
        parent_node.children[0] = Box::new(left_node);
        parent_node.children[1] = Box::new(right_node);

        Ok(parent_node)    
    }


}
impl BPTreeLeafNode{
    fn new() -> BPTreeLeafNode{
        BPTreeLeafNode {
            values: HashMap::new(), // TODO: rename to map?
            next_node: None
        }
    }

}

impl BPTreeInternalNode {
    
    fn new() -> BPTreeInternalNode {
        BPTreeInternalNode{
            keys: [0 as u8; M],
            children: [Box::new(Default::default)]
        }
    }

    fn merge() {}

    fn is_full(&self) -> bool {
        let max = 0 ;
        for (id, k) in self.keys.iter().enumerate() {
            if k != 0 {
                max += 1;
            } 
        }
        return max == M
    }
        
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_insert_and_search() {
        let mut leaf_node = BPTreeLeafNode::new();
        leaf_node.insert(10, 1, 0);
        leaf_node.insert(20, 2, 0);
        leaf_node.insert(30, 3, 0);

        assert_eq!(leaf_node.search(10), Some((1, 0)));
        assert_eq!(leaf_node.search(20), Some((2, 0)));
        assert_eq!(leaf_node.search(30), Some((3, 0)));
        assert_eq!(leaf_node.search(40), None);
    }

    // #[test]
    // fn test_internal_node_insert_and_search() {
    //     let mut internal_node = BPTreeInternalNode::new();
    //     let mut leaf_node1 = BPTreeLeafNode::new();
    //     let mut leaf_node2 = BPTreeLeafNode::new();

    //     leaf_node1.insert(10, 1, 0);
    //     leaf_node1.insert(20, 2, 0);
    //     leaf_node2.insert(30, 3, 0);
    //     leaf_node2.insert(40, 4, 0);

    //     internal_node.keys[0] = 30;
    //     internal_node.children.push(Box::new(leaf_node1));
    //     internal_node.children.push(Box::new(leaf_node2));

    //     assert_eq!(internal_node.search(10), Some((1, 0)));
    //     assert_eq!(internal_node.search(20), Some((2, 0)));
    //     assert_eq!(internal_node.search(30), Some((3, 0)));
    //     assert_eq!(internal_node.search(40), Some((4, 0)));
    //     assert_eq!(internal_node.search(50), None);
    // }

    // #[test]
    // fn test_split_leaf_node() {
    //     let mut leaf_node = BPTreeLeafNode::new();
    //     leaf_node.insert(10, 1, 0);
    //     leaf_node.insert(20, 2, 0);
    //     leaf_node.insert(30, 3, 0);
    //     leaf_node.insert(40, 4, 0);
    //     leaf_node.insert(50, 5, 0);

    //     let new_node = leaf_node.split().unwrap();
    //     let internal_node = new_node.as_any().downcast_ref::<BPTreeInternalNode>().unwrap();

    //     assert_eq!(internal_node.keys[0], 30);
    //     assert_eq!(internal_node.children.len(), 2);

    //     let left_child = internal_node.children[0].as_any().downcast_ref::<BPTreeLeafNode>().unwrap();
    //     assert_eq!(left_child.values.len(), 2);
    //     assert_eq!(left_child.search(10), Some((1, 0)));
    //     assert_eq!(left_child.search(20), Some((2, 0)));

    //     let right_child = internal_node.children[1].as_any().downcast_ref::<BPTreeLeafNode>().unwrap();
    //     assert_eq!(right_child.values.len(), 3);
    //     assert_eq!(right_child.search(30), Some((3, 0)));
    //     assert_eq!(right_child.search(40), Some((4, 0)));
    //     assert_eq!(right_child.search(50), Some((5, 0)));
    // }
}