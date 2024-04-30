
use std::collections::HashMap;

use crate::record::*;
use crate::table::*;
use crate::types::*;
use crate::error::*;


//TODO: lets finish up the table abstraction first
// there is get rows in range, so we just work on that


#[derive(Clone)]
pub struct RPredicate {
    pub offset: usize,
    pub limit: usize,

    //maybe we dont put this here again
    // pub filter: Fn, // handle using lambdas I guess, conversion would be finnicky though
    pub select: Vec<String>, // selected columns

    // TODO: cant really handle order here, that should be in projection. Another optimization
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
    pub progress: usize // keeping track of current progress
}

impl RecordIterator {

    pub fn new(chunk_size:usize, predicate: RPredicate) -> Self {
        let mut n = RecordIterator {
            chunk_size: chunk_size,
            // table: table,
            predicate: predicate, 
            progress : 0 as usize
        }; 

        n.progress = n.predicate.offset; // initialize to start idx
        n 
    }    

    //TODO: this means we should panic on each layer then?
    pub fn get_next_chunk(&mut self, table: &mut Table) -> Option<Records> {
        //lets fetch by page number based on the offset in the index, and we need to keep track
        if (self.progress >= self.predicate.offset + self.predicate.limit) {
            return None
        }
    
        let ret = table.get_rows_in_range(self.progress, self.progress + self.chunk_size );
        self.progress += self.chunk_size;

        Some(ret)

    }
}


//TODO: write some tests