
// need a page buffer pool
// need to be able to retrieve a page from memory, and access 

// file -> page -> offset?

// use 8k page size

//  some fn to decode or manage the 8k page

//page header


use std::collections::HashMap;
// lets do some struct mapping

const PAGE_SIZE_KB: usize = 4096;
const PAGE_SIZE_BYTES:usize = PAGE_SIZE_KB * 1024; 
const FILE_LIMIT_PAGES: usize = 4096; 
const INIT_PAGE_COUNT: usize = 24;


use std::fs::File;
use std::io::Write;

use crate::error::{Result, Error};


//from ext
// pub const PAGE_ID_1: PageId = NonZeroU32::MIN;
// /// The maximum page size is 65536.
// pub const MAX_PAGE_SIZE: usize = 65536;
// /// The maximum page id is 4294967294.
// const MAX_PAGE_ID: u32 = u32::MAX - 1;

// /// Page id starts from 1.
// pub type PageId = NonZeroU32;



//something else will keep track of usable pages, page numbers and what not. itll be a page also actually, so need to set 
//that up elsewhere

// the data pages need to be baked into the code, we call that the freelist?
//i mean, that why we are keeping file info after all, it needsd to know what is currently being stored.

struct FileInfo{
    page_count: usize,
    page_offsets: HashMap<usize, usize>, //map the page indices to offsets, so we know whats what
    free_space_size: usize,
    freelist: Vec<usize>
}

struct Pager {
    fname_prefix: String, 
    file_info: HashMap<String, FileInfo> ,
    cache: PageCache,
}

struct Page {

    index: usize,
    file: String, 
    bytes: [u8; PAGE_SIZE_BYTES],
    offset: usize

}

struct PageCache { //bufferpool

    limit: usize,
    loaded_pages: Vec<Page>,
}

impl PageCache{
    fn new() -> PageCache{
        PageCache {
            limit: 1000,
            loaded_pages: Vec::new()
        }
    }
}

impl FileInfo {
    fn new(page_count: usize, page_offsets: HashMap<usize, usize>, 
            free_space_size: usize, freelist: Vec<usize>) -> FileInfo {
        FileInfo {
            page_count,
            page_offsets,
            free_space_size,
            freelist,
        }
    }
}



impl Pager  {

    fn new(fname_prefix: String)-> Pager{
        Pager {
            fname_prefix: fname_prefix,
            file_info: HashMap::new(),
            cache: PageCache::new()
        }
    }

    fn create_new_page() -> Result<Page> {
        //how do we create a new page in a file
        unimplemented!()

        // check freelist and try to acquire a page there
        // if no space in freelist, extend file if num pages less than page count limit

        //otherwise, create a new file and create the page there
    }

    fn delete_page(page: Page) -> Result<()> {

        //just deallocate that page, it shouldnt exist in the cache or pager header
        unimplemented!()
    }

    fn create_new_file <'b>(&mut self, fname: String) -> Result<()> { 

        let mut file = File::create(&fname).unwrap();
        let data = vec![0 as u8; PAGE_SIZE_BYTES*INIT_PAGE_COUNT]; // initialize with 24 pages
        file.write_all(&data).unwrap();    


        self.file_info.insert(fname.as_str().to_string(), FileInfo::new(INIT_PAGE_COUNT, 
            HashMap::new(), INIT_PAGE_COUNT*PAGE_SIZE_BYTES, (0..10 as usize).collect() ));

        
        // let mut page_offsets = &self.file_info[fname.as_str()].page_offsets;
        for i in 0..10{
            //need to get this working
        //    self.file_info[&mut fname.as_str().to_string()].page_offsets.insert(i, PAGE_SIZE_BYTES*i);
        }

        Ok(())
    }

    fn flush_page(page: Page) -> Result<()> {

        unimplemented!()
    } 

    fn flush_all_pages() -> Result<()> {

        unimplemented!()
    }

    fn dump_info() -> Result<()> {
        unimplemented!()
    }


} 

impl PageCache {

    fn has_page(file: &str, page_number: usize) -> Result<bool> {

        unimplemented!()
    } 

    fn add_page(page: &Page) -> Result<()> { 
        //while vec_size > limit , evict pages
        unimplemented!()

    }

    fn evict_page() {
        //keep track using some algorithm like LRU
        //vec remove that page
        unimplemented!()
    } 
}


#[cfg(test)]

mod tests {

    use super::*;

    #[test]
    fn test_create_file(){
        let mut pager = Pager::new("helo".to_string()); 
        pager.create_new_file(format!("{}{}","pager.fname_prefix", "0.db")).unwrap(); 
        ()
    }

    #[test]
    fn test_add_pages_beyond_initialized () {
        unimplemented!()
    }

    #[test]
    fn test_update_page() {
        unimplemented!()
    }

    #[test]
    fn test_delete_page() {
        unimplemented!()
    }

    #[test]
    fn test_flush_pages() {
        unimplemented!()
    }

    #[test]
    fn test_retrieve_page_from_cache() {
        unimplemented!()
    }

    #[test]
    fn test_cache() {
        unimplemented!()
    }

}