use std::collections::HashMap;
use std::cmp;

use crate::dbms::*;
use crate::error::*;
use crate::error::*;
use crate::pager::*;
use crate::record::*;
use crate::table::*;
use crate::types::*;

//TODO: lets finish up the table abstraction first
// there is get rows in range, so we just work on that

#[derive(Clone, Debug)]
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
        // Self {
        //     offset: None,
        //     limit: None,
        //     select: None,
        //     distinct: None,
        // }
        Self {
            offset: Some(0),
            limit: Some(usize::MAX),
            select: Some(vec![]),
            distinct: Some(false),
        }
    }

    pub fn add(mut self, other: &RPredicate) {
        if let Some(other_offset) = other.offset {
            self.offset = Some(cmp::max(self.offset.unwrap_or(0), other_offset));
        }

        if let Some(other_limit) = other.limit {
            self.limit = Some(self.limit.unwrap_or(usize::MAX).min(other_limit));
        }

        if let Some(other_select) = &other.select {
            if let Some(ref mut select) = self.select {
                select.extend(other_select.clone());
            } else {
                self.select = Some(other_select.clone());
            }
        }

        if let Some(other_distinct) = other.distinct {
            self.distinct = Some(self.distinct.unwrap_or(false) || other_distinct);
        }
    }

    //expensive  expensive expensive, damn
    // this wouldve caused horrible mutability issues with successive calls
    //stateless is better
    pub fn add_ret(&self, other: &RPredicate) -> Self {
        let mut npred = RPredicate::new();
        if let Some(other_offset) = other.offset {
            npred.offset = Some(self.offset.unwrap_or(0) + other_offset);
        }

        if let Some(other_limit) = other.limit {
            npred.limit = Some(self.limit.unwrap_or(usize::MAX).min(other_limit));
        }

        if let Some(other_select) = &other.select {
            if let Some(mut select) = self.select.clone() {
                select.extend(other_select.clone());
                npred.select = Some(select);
            } else {
                npred.select = Some(other_select.clone());
            }
        }

        if let Some(other_distinct) = other.distinct {
            npred.distinct = Some(self.distinct.unwrap_or(false) || other_distinct);
        }

        npred
    }
}

#[derive(Clone)]
pub struct RecordIterator {
    pub chunk_size: usize,
    pub db_name: String,
    pub table_name: String,
    // pub predicate: RPredicate,
    pub progress: usize, // keeping track of current progress
}

impl RecordIterator {
    pub fn new(
        chunk_size: usize,
        // predicate: RPredicate,
        db_name: String,
        table_name: String,
    ) -> Self {
        let mut n = RecordIterator {
            chunk_size: chunk_size,
            // predicate: predicate,
            progress: 0 as usize,
            db_name: db_name,
            table_name: table_name,
        };

        // match n.predicate.offset {
        //     Some(x) => n.progress = x,
        //     None => n.progress = 0,
        // }
        n
    }

    //TODO: this means we should panic on each layer then?
    //TODO: we might need size constraints so the dataflow is synchronized
    // we might have to make those size constraints large to cater for the efficiency of columnar page storage
    pub fn get_next_chunk(
        &mut self,
        dbms: &mut DBMS,
        predicate: &RPredicate,
    ) -> Result<Option<Records>> {
        //lets fetch by page number based on the offset in the index, and we need to keep track

        //URGENT TODO: these should not be 0 here right, need to rethink RPRed defaults
        println!(
            "prog: {} , offset: {}, limit: {} ",
            self.progress,
            predicate.offset.unwrap(),
            predicate.limit.unwrap()
        );
        if (self.progress >= predicate.limit.unwrap() - predicate.offset.unwrap()) {
            return Ok(None);
        }

        //TODO : do we really want something like this instead of using lifetimes?
        if let Some(db) = dbms.get_db_mut(&self.db_name) {
            println!(
                "Getting next chunk in range {} {}",
                self.progress, self.chunk_size
            );

            // this has to become table getrows in range
            let ret = db
                .get_rows_in_range(
                    &self.table_name,
                    self.progress + predicate.offset.unwrap(),
                    self.progress + self.chunk_size,
                )?;
            self.progress += self.chunk_size;

            return Ok(Some(ret));
        }
        return Err(Error::Unknown(format!(
            "Could not find database {} in riter.get_next_chunk",
            &self.db_name
        )));
    }
}

//TODO: write some tests
