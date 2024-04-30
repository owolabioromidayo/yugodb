
use std::collections::HashMap;

use crate::record::*;
use crate::table::*;
use crate::types::*;
use crate::error::*;


//TODO: lets finish up the table abstraction first
// there is get rows in range, so we just work on that


pub struct RPredicate {
    pub offset: usize,
    pub limit: usize,
    pub filter: Fn, // handle using lambdas I guess, conversion would be finnicky though
    pub select: Vec<String>, // selected columns

    // TODO: cant really handle order here, that should be in projection. Another optimization
}




pub struct RecordIterator {
    // what special informaation is needed here?
    pub chunk_size: usize,
    pub table: &Table, // ref needed because of potentially large index information, this wont scale though,
                    // would have to decouple that information
    //we need like a query info
    // predicate, offset, range, that kind of thing
    pub predicate: RPredicate,
    pub progress: usize // keeping track of current progress
}

impl RecordIterator {

    pub fn new(chunk_size:usize, table: &Table, predicate: RPredicate) -> Self {
        let n = RecordIterator {
            chunk_size: chunk_size,
            table: table,
            predicate: predicate
        }; 

        self.progress = self.predicate.offset; // initialize to start idx
    }    

    //TODO: this means we should panic on each layer then?
    pub fn get_next_chunk() -> Option<Records> {
        //lets fetch by page number based on the offset in the index, and we need to keep track
        if (progress >= offset + limit) {
            return None
        }
    
        let ret = table.get_rows_in_range(progress, progress + chunk_size );
        progress += chunk_size;

        Some(ret)

    }
}