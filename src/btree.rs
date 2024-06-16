use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

const M: usize = 32;

pub trait BKey: PartialEq + Ord + Hash + Debug + Clone + Default {}

impl BKey for usize {}
impl BKey for String {}

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

    // fn split(&mut self) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct BPTreeInternalNode<T: BKey, U: Debug + Clone> {
    keys: Vec<Option<T>>, // capacity M
    is_root: bool,
    children: Vec<Option<BPTreeNodeEnum<T, U>>>, // capacity M+1
    child_count: usize,
}

#[derive(Clone, Debug)]
pub struct BPTreeLeafNode<T: PartialEq, U> {
    values: HashMap<T, U>,
    next_node: Option<Box<BPTreeLeafNode<T, U>>>, //  LList
}

impl<T: BKey, U: Debug + Clone> BPTreeNode<T, U> for BPTreeLeafNode<T, U> {
    fn search(&self, key: &T) -> Option<&U> {
        if let Some(x) = self.values.get(key) {
            // println!("{:?}", x);
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
}

impl<T: BKey, U: Debug + Clone> BPTreeNode<T, U> for BPTreeInternalNode<T, U> {
    fn search(&self, key: &T) -> Option<&U> {
        let mut ret = None;

        if self.child_count == 0 {
            return None;
        }

        //check first key
        // = sign for 0
        if let Some(x) = &self.keys[0] {
            if key <= x {
                if let Some(x) = &self.children[0] {
                    ret = match x {
                        BPTreeNodeEnum::Leaf(y) => y.search(key),
                        BPTreeNodeEnum::Internal(y) => y.search(key),
                    };
                }
                return ret;
            } else {
                //backwards search
                for i in (0..self.keys.len()).rev() {
                    if let Some(k) = &self.keys[i] {
                        if key >= k {
                            println!("CHOSEN key: {:?}, idx : {} ", k, i);
                            if let Some(x) = &self.children[i] {
                                // println!("We are here arent we");
                                match x {
                                    BPTreeNodeEnum::Leaf(y) => return y.search(key),
                                    BPTreeNodeEnum::Internal(y) => return y.search(key),
                                };
                            }
                            return None;
                        }
                    }
                }
            }
        }
        None
    }

    fn insert(&mut self, key: T, value: U) -> Result<()> {
        let mut index = 0;

        if self.child_count == M {
            //keys are full , we split
            self.split()?;
            return self.insert(key, value);
        }

        while index < self.child_count {
            if self.keys[index].is_some() && key < self.keys[index].clone().unwrap() {
                index += 1;
                break;
            }
            index += 1;
        }

        if let Some(Some(child)) = self.children.get_mut(index) {
            match child {
                BPTreeNodeEnum::Leaf(leaf) => {
                    if leaf.values.len() < M {
                        leaf.insert(key, value)?;
                    } else {
                        leaf.split()?;
                        leaf.insert(key, value)?;
                    }
                }
                BPTreeNodeEnum::Internal(internal) => {
                    if internal.is_full() {
                        internal.split()?;
                        internal.insert(key, value)?;
                    } else {
                        internal.insert(key, value)?;
                    }
                }
            }
        } else {
            let mut new_leaf = BPTreeLeafNode::new();
            new_leaf.insert(key.clone(), value)?;
            self.children[self.child_count] = Some(BPTreeNodeEnum::Leaf(new_leaf));
            self.keys[self.child_count] = Some(key.clone());
            self.child_count += 1;
        }

        Ok(())
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
}

impl<T: BKey, U: Debug + Clone> BPTreeLeafNode<T, U> {
    pub fn new() -> BPTreeLeafNode<T, U> {
        BPTreeLeafNode {
            values: HashMap::new(), // TODO: rename to map?
            next_node: None,
        }
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
        //filler
        for i in 0..32 {
            new_node.keys.push(Some(keys[0].clone()));
        }
        new_node.children = vec![None; 32];
        new_node.keys[0] = Some(middle_key.clone());
        new_node.children[0] = Some(BPTreeNodeEnum::Leaf(new_left_leaf_node));
        new_node.children[1] = Some(BPTreeNodeEnum::Leaf(new_right_leaf_node));
        new_node.child_count = 2;

        return Ok(new_node);
    }
}

impl<T: BKey, U: Debug + Clone> BPTreeInternalNode<T, U> {
    pub fn new() -> Self {
        BPTreeInternalNode {
            //TODO : are the sizes secure enough?
            keys: vec![None; M],
            children: vec![None; M + 1],
            is_root: false,
            child_count: 0,
        }
    }

    fn is_full(&self) -> bool {
        let mut count = 0;
        for (id, k) in self.children.iter().enumerate() {
            if let Some(_) = k {
                count += 1;
            }
        }
        return count == M + 1;
    }

    fn split(&mut self) -> Result<()> {
        //TODO: important piece of the puzzle

        //TODO: how do we resolve child count here then
        let mid = M / 2;
        let split_key = self.keys[mid].clone();

        // Move keys and children to the left node
        let mut left_keys = self.keys[..mid].to_vec();
        left_keys.resize(M, None);

        let mut left_children: Vec<Option<BPTreeNodeEnum<T, U>>> =
            self.children.drain(..mid).collect();
        left_children.resize(M + 1, None);

        // Move keys and children to the right node
        let mut right_keys = self.keys[mid + 1..].to_vec();
        right_keys.resize(M, None);

        //take what is left
        let mut right_children: Vec<Option<BPTreeNodeEnum<T, U>>> =
            self.children.drain(0..self.children.len()).collect();
        right_children.resize(M + 1, None);

        let left_node = BPTreeInternalNode {
            keys: left_keys,
            is_root: false,
            children: left_children,
            child_count: 0,
        };

        let right_node = BPTreeInternalNode {
            keys: right_keys,
            is_root: false,
            children: right_children,
            child_count: 0,
        };

        self.keys = vec![Default::default(); M];
        self.keys[0] = split_key;
        self.children = vec![None; 32];

        self.children[0] = Some(BPTreeNodeEnum::Internal(left_node));
        self.children[1] = Some(BPTreeNodeEnum::Internal(right_node));

        self.child_count = 2;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    ///The only real test
    fn test_insert_and_search_1000_items() {
        let mut internal_node = BPTreeInternalNode::new();

        for i in 0..100 {
            internal_node.insert(i, (i, 0)).unwrap();
        }
        println!("{:?}", internal_node);

        for i in 0..100 {
            assert_eq!(internal_node.search(&i), Some(&(i, 0)));
        }
    }

    
}
