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

// if we are using the Enum, we dont need

// use std::iter;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

const M: usize = 32;

pub trait BKey: PartialEq + Ord + Hash + Debug + Clone + Default {}

impl BKey for usize {}
// impl BKey for &usize {}
impl BKey for String {}
// impl BKey for &String {}
// impl BKey for {}

#[derive(Clone, Debug)]
pub enum BPTreeNodeEnum<T: BKey, U: Debug + Clone> {
    Internal(BPTreeInternalNode<T, U>),
    Leaf(BPTreeLeafNode<T, U>),
}

pub trait BPTreeNode<T: BKey, U: Debug + Clone> {
    // fn new() -> Result<BPTreeNodeEnum>;

    fn serialize(&self) -> Vec<u8>;
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>;

    fn search(&self, key: &T) -> Option<&U>;
    fn insert(&mut self, key: T, value: U) -> Result<()>;

    fn is_leaf(&self) -> bool;
    fn is_root(&self) -> bool;

    fn split(&mut self) -> Result<BPTreeInternalNode<T, U>>;
}

#[derive(Clone, Debug)]
pub struct BPTreeInternalNode<T: BKey, U: Debug + Clone> {
    keys: Vec<T>, // capacity M
    is_root: bool,
    children: Vec<Option<BPTreeNodeEnum<T, U>>>, // capacity M+1
    child_count: usize,
}

#[derive(Clone, Debug)]
pub struct BPTreeLeafNode<T: PartialEq, U> {
    values: HashMap<T, U>,
    next_node: Option<Box<BPTreeLeafNode<T, U>>>, //  LList
}

// struct BPlusTree {
//     root: BPTreeInternalNode
// }

impl<T: BKey, U: Debug + Clone> BPTreeNode<T, U> for BPTreeLeafNode<T, U> {
    fn search(&self, key: &T) -> Option<&U> {
        if let Some(x) = self.values.get(key) {
            println!("{:?}", x);
            return Some(x);
        }
        None
    }

    //doing this because I dont want it to affect serialization, might have to think of a better traeoff
    fn is_root(&self) -> bool {
        false
    }
    fn is_leaf(&self) -> bool {
        true
    }

    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>{
    //     unimplemented!()
    // }

    fn insert(&mut self, key: T, val: U) -> Result<()> {
        //if it makes it all the way here, its fine
        self.values.insert(key, val);
        Ok(())
    }

    fn split(&mut self) -> Result<BPTreeInternalNode<T, U>> {
        let mut keys: Vec<&T> = self.values.keys().collect();
        keys.sort();
        let mid = keys.len() / 2;
        let middle_key = keys[mid];

        let mut left_map: HashMap<T, U> = HashMap::new();
        let mut right_map: HashMap<T, U> = HashMap::new();

        for (key, value) in &self.values {
            if key < middle_key {
                left_map.insert(key.clone(), value.clone());
            } else {
                right_map.insert(key.clone(), value.clone());
            }
        }

        let new_right_leaf_node: BPTreeLeafNode<T, U> = BPTreeLeafNode {
            values: right_map,
            next_node: None,
        };

        let new_left_leaf_node: BPTreeLeafNode<T, U> = BPTreeLeafNode {
            values: left_map,
            next_node: None,
            // next_node: Some(Box::new(new_right_leaf_node)), // this is so wrong ( why clone, I just moved it)
        };

        let mut new_node: BPTreeInternalNode<T, U> = BPTreeInternalNode::new();
        new_node.keys[0] = middle_key.clone();
        new_node.children[0] = Some(BPTreeNodeEnum::Leaf(new_left_leaf_node));
        new_node.children[1] = Some(BPTreeNodeEnum::Leaf(new_right_leaf_node));

        return Ok(new_node);
    }
}

impl<T: BKey, U: Debug + Clone> BPTreeNode<T, U> for BPTreeInternalNode<T, U> {
    fn search(&self, key: &T) -> Option<&U> {
        let mut ret = None;

        //check first one

        if *key > self.keys[self.child_count- 1] {
            // println!("What are we doing here");
            // we havent worked out this +1 thing yet
            if let Some(x) = &self.children[self.child_count + 1] {
                println!("Searching {:?}", &x);
                ret = match x {
                    BPTreeNodeEnum::Leaf(y) => y.search(key),
                    BPTreeNodeEnum::Internal(y) => y.search(key),
                };
            }
            return ret;
        } else {
            for (_id, k) in self.keys.iter().enumerate() {
                //wasteful fr, needs rewrite
                if key <= k {
                    //return on first occurence (this is so wrong)
                    if let Some(x) = &self.children[_id] {
                        println!("Searching {:?}", &x);
                        match x {
                            BPTreeNodeEnum::Leaf(y) => return y.search(key),
                            BPTreeNodeEnum::Internal(y) => return y.search(key),
                        };
                    }
                    break;
                }
            }
        }
        None
    }

    fn insert(&mut self, key: T, value: U) -> Result<()> {
        //top down is worse than splitting upwards it seems

        //okay, so we always insert at the end, that makes sense right

        //try inserting it here

        //TODO: notice that my inserts are not ordered

        //this doesnt work anymore, fixed capacity, lets keep a child count then
        // if self.children.len() < (M + 1).into() {
        if self.child_count < (M + 1) {
            //insert the key and corresponding children in sorted order
            // insert_sorted(&self.children, key);
            // let index = self.children.binary_search(&value).unwrap_or_else(|i| i);
            // self.children.insert(index, value);

            // create a new child?

            // this might be excessive but neccessary, at least when its full, new internal nodes wont be created this much,
            // right? wrong, need some sorta recursion depth
            // we'll test this at scale I guess, but yeah, seems wrong, but then again, my first argument carries recursively,
            // some leaf nodes will still be filled, we're just spacing out
            let mut new_child: BPTreeInternalNode<T, U> = BPTreeInternalNode::new();
            new_child.keys[0] = key.clone();
            let mut new_child_leaf = BPTreeLeafNode::new();
            new_child_leaf.insert(key.clone(), value)?;
            new_child.children[0] = Some(BPTreeNodeEnum::Leaf(new_child_leaf));
            new_child.child_count = 1;

            // self.children
            //     .push(Some(BPTreeNodeEnum::Internal(new_child)));
            self.children[self.child_count] = Some(BPTreeNodeEnum::Internal(new_child));
            self.keys[self.child_count] = key.clone(); // TODO: this is a violation of Btrees
            self.child_count += 1;

            Ok(())

            //expand children vec
        } else {
            for (id, k) in self.keys.iter().enumerate() {
                if *k > key {
                    if let Some(curr) = &mut self.children[id] {
                        match curr {
                            BPTreeNodeEnum::Leaf(x) => {
                                if x.values.len() < M.into() {
                                    x.insert(key.clone(), value.clone())?;
                                } else {
                                    // we need to split the leaf node
                                    //naive, create a new internal node which refs the 2 leaf nodes
                                    let mut new_internal_node = x.split()?;
                                    new_internal_node.insert(key.clone(), value.clone())?;
                                    *curr = BPTreeNodeEnum::Internal(new_internal_node);
                                }
                            }
                            BPTreeNodeEnum::Internal(x) => {
                                // check if node is at capacity?
                                // we need to check if theyre actually populated
                                if x.is_full() {
                                    let mut new_internal_node = x.split()?;
                                    new_internal_node.insert(key.clone(), value.clone())?;
                                    *curr = BPTreeNodeEnum::Internal(new_internal_node);
                                } else {
                                    x.insert(key.clone(), value.clone())?;
                                }
                            }
                        }
                    }
                }
            }
            //check last one
            let len = self.children.len();
            if let Some(curr) = &mut self.children[len - 1] {
                match curr {
                    BPTreeNodeEnum::Leaf(x) => {
                        if x.values.len() < M {
                            x.insert(key, value)?;
                        } else {
                            // we need to split the leaf node
                            //naive, create a new internal node which refs the 2 leaf nodes

                            let mut new_internal_node = x.split()?;
                            new_internal_node.insert(key, value)?;
                            *curr = BPTreeNodeEnum::Internal(new_internal_node);
                        }
                    }
                    BPTreeNodeEnum::Internal(x) => {
                        // check if node is at capacity?
                        // we need to check if theyre actually populated
                        if x.is_full() {
                            let mut new_internal_node = x.split()?;
                            new_internal_node.insert(key, value)?;
                            *curr = BPTreeNodeEnum::Internal(new_internal_node);
                        } else {
                            x.insert(key, value)?;
                        }
                    }
                }
            }
            Ok(())
        }

        //we might have to insert a new key here, which will lead to the splitting operation
    }

    fn is_root(&self) -> bool {
        self.is_root
    }
    fn is_leaf(&self) -> bool {
        false
    }

    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }
    // fn deserialize() -> Result<Box<dyn BPTreeNode>>{
    //     unimplemented!()
    // }

    fn split(&mut self) -> Result<BPTreeInternalNode<T, U>> {
        //TODO: important piece of the puzzle

        // do i need a reference to its head? s owe can try insert some things there

        //we'll just split into an internal node referencing 2 internal nodes, then we'll reconsolidate on the other end
        // like we have to know if we are updating the final value
        // its just like ensuring the tree is still split the right way

        let mut left_node = BPTreeInternalNode {
            keys: Vec::with_capacity(M),
            is_root: false,
            children: Vec::with_capacity(M + 1),
            child_count: 0,
        };

        let mut right_node = BPTreeInternalNode {
            keys: Vec::with_capacity(M),
            is_root: false,
            children: Vec::with_capacity(M + 1),
            child_count: 0,
        };

        //TODO: how do we resolve child count here then
        let mid = M / 2;
        let split_key = self.keys[mid].clone();

        // Move keys and children to the left node
        left_node.keys = self.keys[..mid].to_vec();
        // left_node.children = self.children[..mid + 1].to_vec();
        left_node.children = self.children.drain(..mid + 1).collect();

        // Move keys and children to the right node
        right_node.keys = self.keys[mid + 1..].to_vec();
        // right_node.children = self.children[mid + 1..]
        //     .iter()
        //     .map(|child| child.clone())
        //     .collect(); // slow af it seems

        right_node.children = self.children.drain(mid + 1..).collect();

        // Create a new parent node
        let mut parent_node = BPTreeInternalNode::new();
        parent_node.keys[0] = split_key;
        parent_node.children[0] = Some(BPTreeNodeEnum::Internal(left_node));
        parent_node.children[1] = Some(BPTreeNodeEnum::Internal(right_node));

        Ok(parent_node)
    }
}

impl<T: PartialEq, U> BPTreeLeafNode<T, U> {
    pub fn new() -> BPTreeLeafNode<T, U> {
        BPTreeLeafNode {
            values: HashMap::new(), // TODO: rename to map?
            next_node: None,
        }
    }
}

impl<T: BKey, U: Debug + Clone> BPTreeInternalNode<T, U> {
    pub fn new() -> Self {
        BPTreeInternalNode {
            //TODO : are the sizes secure enough?
            keys: vec![T::default(); M],
            children: vec![None; M + 1],
            is_root: false,
            child_count: 0,
        }
    }

    // fn merge() {}

    fn is_full(&self) -> bool {
        // let mut max = 0;
        // for (id, k) in self.keys.iter().enumerate() {
        //     if *k != 0 {
        //         max += 1;
        //     }
        // }
        // return max == M;
        let mut max = 0;
        for (id, k) in self.children.iter().enumerate() {
            if let Some(_) = k {
                max += 1;
            }
        }
        return max == M + 1;
    }
}

//TODO: need high volume tests

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_node_insert_and_search() {
        let mut leaf_node: BPTreeLeafNode<usize, (usize, u8)> = BPTreeLeafNode::new();
        leaf_node.insert(10, (1, 0)).unwrap();
        leaf_node.insert(20, (2, 0)).unwrap();
        leaf_node.insert(30, (3, 0)).unwrap();

        assert_eq!(leaf_node.search(&10), Some(&(1, 0)));
        assert_eq!(leaf_node.search(&20), Some(&(2, 0)));
        assert_eq!(leaf_node.search(&30), Some(&(3, 0)));
        assert_eq!(leaf_node.search(&40), None);
    }

    #[test]
    fn test_internal_node_search() {
        let mut internal_node = BPTreeInternalNode::new();
        let mut leaf_node1: BPTreeLeafNode<usize, (usize, u8)> = BPTreeLeafNode::new();
        let mut leaf_node2: BPTreeLeafNode<usize, (usize, u8)> = BPTreeLeafNode::new();

        leaf_node1.insert(10, (1, 0)).unwrap();
        leaf_node1.insert(20, (2, 0)).unwrap();

        leaf_node2.insert(30, (3, 0)).unwrap();
        leaf_node2.insert(40, (4, 0)).unwrap();

        internal_node.keys[0] = 30;
        // internal_node.keys.push(30);
        internal_node.children[0] = Some(BPTreeNodeEnum::Leaf((leaf_node1)));
        internal_node.children[1] = Some(BPTreeNodeEnum::Leaf((leaf_node2)));

        assert_eq!(internal_node.search(&10), Some(&(1, 0)));
        assert_eq!(internal_node.search(&20), Some(&(2, 0)));
        assert_eq!(internal_node.search(&30), Some(&(3, 0)));
        assert_eq!(internal_node.search(&40), Some(&(4, 0)));
        assert_eq!(internal_node.search(&50), None);
    }

    #[test]
    fn test_insert_internal_node() {
        let mut internal_node = BPTreeInternalNode::new();

        internal_node.insert(10, (1, 0)).unwrap();
        internal_node.insert(20, (2, 0)).unwrap();
        internal_node.insert(30, (3, 0)).unwrap();
        internal_node.insert(40, (4, 0)).unwrap();
        // internal_node.insert(25, (5, 0)).unwrap();

        println!("{:?}", internal_node);
        // println!("{:?}", internal_node.search(&10));

        assert_eq!(internal_node.search(&10), Some(&(1, 0)));
        assert_eq!(internal_node.search(&20), Some(&(2, 0)));
        // assert_eq!(internal_node.search(&25), Some(&(5, 0)));
        assert_eq!(internal_node.search(&30), Some(&(3, 0)));
        assert_eq!(internal_node.search(&40), Some(&(4, 0)));
    }

    #[test]
    fn test_split_leaf_node() {
        let mut leaf_node: BPTreeLeafNode<usize, (usize, u8)> = BPTreeLeafNode::new();
        leaf_node.insert(10, (1, 0)).unwrap();
        leaf_node.insert(20, (2, 0)).unwrap();
        leaf_node.insert(30, (3, 0)).unwrap();
        leaf_node.insert(40, (4, 0)).unwrap();
        leaf_node.insert(50, (5, 0)).unwrap();

        let new_node = leaf_node.split().unwrap();
        let internal_node = new_node;

        assert_eq!(internal_node.keys[0], 30);
        // assert_eq!(internal_node.children.len(), 2);

        let left_child = internal_node.children[0].as_ref().unwrap();
        match left_child {
            BPTreeNodeEnum::Leaf(left_child) => {
                // assert_eq!(left_child.values.len(), 2);
                assert_eq!(left_child.search(&10), Some(&(1, 0)));
                assert_eq!(left_child.search(&20), Some(&(2, 0)));
            }
            BPTreeNodeEnum::Internal(n) => (),
        }

        let right_child = internal_node.children[1].as_ref().unwrap();
        match right_child {
            BPTreeNodeEnum::Leaf(right_child) => {
                // assert_eq!(right_child.values.len(), 3);
                assert_eq!(right_child.search(&30), Some(&(3, 0)));
                assert_eq!(right_child.search(&40), Some(&(4, 0)));
                assert_eq!(right_child.search(&50), Some(&(5, 0)));
            }

            BPTreeNodeEnum::Internal(n) => (),
        }

        println!("Index after insertion: {:?}", internal_node);
    }
}
