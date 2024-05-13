use std::collections::HashMap;

use crate::error::*;
use crate::pager::*;
use crate::record::*;
use crate::table::*;
use crate::types::*;

//TODO: lets finish up the table abstraction first
// there is get rows in range, so we just work on that

#[derive(Clone)]
pub struct RPredicate {
    pub offset: Option<usize>,
    pub limit: Option<usize>,

    //maybe we dont put this here again
    // pub filter: Fn, // handle using lambdas I guess, conversion would be finnicky though
    pub select: Option<Vec<String>>, // selected columns
    pub distinct: Option<bool>,

    // filter predicate?
    //vec<columns strings>  -> some fn that takes exactly those values (maybe in vec form too)
    

                             // TODO: cant really handle order here, that should be in projection. Another optimization
}

impl RPredicate {
    pub fn new() -> Self {
        Self {
            offset: None,
            limit: None,
            select: None,
            distinct: None,
        }
    }
}


#[derive(Clone)]
pub struct RecordIterator {
    // what special informaation is needed here?
    pub chunk_size: usize,

    //TODO: lifetime
    // this is needed? we cant just pass it in the func
    // passing it in the fun would work also, but might not be as clean

    // pub table: &Table, // ref needed because of potentially large index information, this wont scale though,
    // would have to decouple that information
    //we need like a query info
    // predicate, offset, range, that kind of thing
    pub predicate: RPredicate,
    pub progress: usize, // keeping track of current progress
}

impl RecordIterator {
    pub fn new(chunk_size: usize, predicate: RPredicate) -> Self {
        let mut n = RecordIterator {
            chunk_size: chunk_size,
            // table: table,
            predicate: predicate,
            progress: 0 as usize,
        };

        match n.predicate.offset {
            Some(x) => n.progress = x ,
            None => n.progress = 0,
        }
        n
    }

    //TODO: this means we should panic on each layer then?
    //TODO: we might need size constraints so the dataflow is synchronized
    // we might have to make those size constraints large to cater for the efficiency of columnar page storage
    pub fn get_next_chunk(&mut self, pager: &mut Pager, table: &mut Table) -> Option<Records> {
        //lets fetch by page number based on the offset in the index, and we need to keep track
        
        //URGENT TODO: these should not be 0 here right, need to rethink RPRed defaults
        if (self.progress >= self.predicate.offset.unwrap() + self.predicate.limit.unwrap()) {
            return None;
        }

        let ret = table
            .get_rows_in_range(pager, self.progress, self.progress + self.chunk_size)
            .unwrap();
        self.progress += self.chunk_size;

        Some(ret)
    }
}

//TODO: write some tests
