use std::fs::File;
use std::io::{Read, Seek, Write};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{SeekFrom};
use crate::error::{Result, Error};


const PAGE_SIZE_KB: usize = 4096;
pub const PAGE_SIZE_BYTES:usize = PAGE_SIZE_KB * 1024; 
const FILE_LIMIT_PAGES: usize = 4096;
const FILE_LIMIT_KB:usize = FILE_LIMIT_PAGES * PAGE_SIZE_KB; 
const INIT_PAGE_COUNT: usize = 24;


//TODO : compaction would be nice, concurrency control, intermittent persistence, 

#[derive(Debug)]
struct FileInfo{
    freelist: Vec<usize>, //should contain offset positions
    size_kb: usize,
}


#[derive(Debug, Clone)]
pub struct Page {
    pub index: usize,
    pub bytes: Vec<u8>,
    pub dirty: bool,
}


#[derive(Debug)]
struct PageCache { 
    capacity: usize,
    loaded_pages: HashMap<usize, (Page, usize)>,
    counter: usize,
}


#[derive(Debug)]
pub struct Pager {
    fname_prefix: String, 
    file_map: HashMap<String, FileInfo>,
    cache: PageCache,
    page_index_map: HashMap<usize, (String, usize)>,// index -> file, page,
    page_count: usize,
}


impl FileInfo {
    fn new() -> FileInfo {
        FileInfo {
            freelist: Vec::new(),
            size_kb: 0 as usize
        }
    }
}

// TODO : serialize and deserialize pager information
// TODO: all communication should be with the pager directly, not with the pagecache
impl Pager  {

    pub fn new(fname_prefix: String)-> Pager{
        Pager {
            fname_prefix,
            file_map: HashMap::new(),
            cache: PageCache::new(),
            page_index_map: HashMap::new(),
            page_count: 0 as usize,
        } 
    }

    pub fn create_new_page(&mut self) -> Result<Page> {

        let file_with_free_page = self.file_map.iter_mut().find(|(_, info)| !info.freelist.is_empty());

        if let Some((file_name, file_info)) = file_with_free_page {

            let page_index = self.page_count;
            let page_offset = file_info.freelist.pop().unwrap();
            
            let page = Page {
                index: page_index,
                bytes: vec![0u8; PAGE_SIZE_BYTES],
                dirty: true,
            };

            self.page_index_map.insert(page_index, (file_name.clone(), page_offset)); 
            self.page_count += 1;

            //TODO: store page in page_cache?
            Ok(page)

        } else {
            let file_not_at_limit = self.file_map.iter_mut().find(|(_, info)| (FILE_LIMIT_KB - info.size_kb) > PAGE_SIZE_KB + 50);  // i got one more in me
            // No free pages, check if we can extend a file

            if let Some((file_name, file_info)) = file_not_at_limit {
                //seek to end of file, extend by 4kb, get the offset

                let mut file = OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(file_name)?;

                let offset = file.seek(SeekFrom::End(0))?;
                println!("Current offset: {}", offset);

                let available_page_count = (FILE_LIMIT_KB - file_info.size_kb) / PAGE_SIZE_KB ; 


                //optimal page allocation
                let extend_count = match available_page_count {
                    1..=2 => 1,
                    3..=4 => 2,
                    5..=10 => 4,
                    _ => 10,
                }; 
                //extend the file by the new pages
                file.set_len(offset + PAGE_SIZE_BYTES as u64*extend_count as u64 )?;
                //add the new pages to the freelist
                file_info.freelist.extend((0..extend_count).map( |i| (offset as usize + 1) + PAGE_SIZE_KB*i)); 

                let page = Page {
                    index: self.page_count,
                    bytes: vec![0u8; PAGE_SIZE_BYTES],
                    dirty: true,
                };

                self.page_index_map.insert(page.index, (file_name.clone(), offset as usize + 1)); // skip last bit in file 
                self.page_count += 1;

                Ok(page)

            } else {
                //All files full, create a new file
                let new_file_name = format!("{}-{}.ydb", self.fname_prefix, self.file_map.len());
                self.create_new_file(new_file_name.clone())?;

                let file_info = self.file_map.get_mut(&new_file_name).ok_or(Error::AccessError)?;
                let page_index = self.page_count;
                let page_offset = file_info.freelist.pop().unwrap();

                //so it must be getting no pop from here
                
                let page = Page {
                    index: page_index,
                    bytes: vec![0u8; PAGE_SIZE_BYTES],
                    dirty: true,
                };

                self.page_index_map.insert(page_index, (new_file_name.clone(), page_offset)); 
                self.page_count += 1;

                //store page in page_cache?
                Ok(page)
            }

        }

    }
    /// Get raw page from disk if not in cache
    fn fetch_page(&self, index: usize) -> Result<Page> {
        if let Some((filename, offset)) = self.page_index_map.get(&index) {
            if let Ok(mut file) = File::open(filename) {
                file.seek(std::io::SeekFrom::Start(*offset as u64))?;
                let mut buf = vec![0 as u8; PAGE_SIZE_BYTES]; 
                if let Ok(()) = file.read_exact(&mut buf) {
                    return Ok(Page {
                        index: index,
                        bytes: buf,
                        dirty: false
                    })
                }
                return Err(Error::Unknown("Could not read bytes from file".to_string()))

            }
            return Err(Error::FileNotFound)
        }
        return Err(Error::NotFound)

    }

    //get page should be manually requested before this
    /// If the page is not loaded, it loads it using Pager::fetch_page ; fails if it cannot fetch
    pub fn get_page_forced(&mut self, page_index:usize) -> Result<Page> {
        // unimplemented!()
        // if let Some(page ) =  self.cache.get_page(page_index) {
        //     return Ok(page)
        // }

        // otherwise
       let new_page = self.fetch_page(page_index)?;
       // WHY DID THIS SHIT JUST FIX ITSELF ( cannot borrow self as mut more than once) b
       // because i made the previous result a mut Page?
       // // great, it came back for no reason
       // so the issue was lifetime based then

        if let Ok(()) = self.cache.add_page(&new_page) {
            return self.cache.get_page_cloned(page_index)
                .ok_or_else(|| Error::Unknown("Failed to access new page from cache".to_string()))
            
        }
        return Err(Error::Unknown("Failed to add new page to cache".to_string())); 


    }

    fn delete_page(&mut self, page: Page) -> Result<()> {
        let (file_name, offset) = self.page_index_map.get(&page.index).ok_or(Error::AccessError)?;
        let file_info = self.file_map.get_mut(file_name).ok_or(Error::FileNotFound)?; 
        file_info.freelist.push(*offset);

        self.page_index_map.remove(&page.index);
        self.page_count -= 1;

        Ok(())

    }

    fn create_new_file (&mut self, fname: String) -> Result<()> { 

        let mut file = File::create(&fname).unwrap();
        let data = vec![0 as u8; PAGE_SIZE_BYTES*INIT_PAGE_COUNT]; // initialize with 24 pages
        file.write_all(&data).unwrap();    

        self.file_map.insert(fname.clone(), FileInfo {
            size_kb: INIT_PAGE_COUNT * PAGE_SIZE_KB,
            freelist: (0..INIT_PAGE_COUNT).rev().map( |i| PAGE_SIZE_KB*i).collect()
        } );
    
        Ok(())
    }

    pub fn flush_page(&self, page: &Page) -> Result<()> {
        let (fname, offset) = self.page_index_map.get(&page.index).ok_or(Error::AccessError)?;

        let file = OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(fname);

        match file{
            Ok(mut f) => {
                f.seek(std::io::SeekFrom::Start(*offset as u64))?;
                f.write_all(&page.bytes)?;
                Ok(())
            }
            Err(e) => Err(Error::IoError(e))
        }
    }


    fn flush_all_pages(&mut self) -> Result<()> {
        for (_, (page, _) ) in &self.cache.loaded_pages {
            self.flush_page(page)?;
        }
        Ok(())
    }


} 

impl PageCache {
    fn new() -> Self{
        Self {
            capacity: 500,
            loaded_pages: HashMap::new(),
            counter: 0
        }
    }

    fn has_page(&mut self, page_index: usize) -> bool {
        self.loaded_pages.contains_key(&page_index)
    } 

    // TODO: why are we cloning the page?
    fn add_page(&mut self, page: &Page) -> Result<()> { 

        if self.loaded_pages.contains_key(&page.index) {
            self.loaded_pages.remove(&page.index);
        } else if self.loaded_pages.len() == self.capacity {
            // remove page with minimum count
            let lru_key = self
                .loaded_pages
                .iter()
                .min_by_key(|(_, (_, counter))| *counter)
                .map(|(key, _)| key.clone())
                .unwrap();
            self.loaded_pages.remove(&lru_key);
        }

        self.loaded_pages.insert(page.index, (page.clone(), self.counter));
        self.counter += 1;        
        
        Ok(())
    }

    fn get_page(&mut self, page_index: usize) -> Option<&mut Page> {
        if let Some((page, counter)) =  self.loaded_pages.get_mut(&page_index) {
            *counter = self.counter;
            self.counter += 1;
            Some(page)
        } else {
            None
        }
    }

    fn get_page_cloned(&mut self, page_index:usize) -> Option<Page> { 
        if let Some((page, counter)) =  self.loaded_pages.get(&page_index) {
            Some(page.clone())
        } else {
            None
        }

    }


}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_create_and_delete_page() {
        let mut pager = Pager::new("testasdas".to_string());
        let page = pager.create_new_page().unwrap();
        assert_eq!(page.index, 0);
        assert_eq!(pager.page_count, 1);

        pager.delete_page(page).unwrap();
        assert_eq!(pager.page_count, 0);
    }

    // #[test]
    // fn test_create_new_file() {
    //     let mut pager = Pager::new("test".to_string());
    //     let file_name = "test-0.ydb".to_string();
    //     pager.create_new_file(file_name.clone()).unwrap();

    //     assert!(pager.file_map.contains_key(&file_name));
    //     let file_info = pager.file_map.get(&file_name).unwrap();
    //     assert_eq!(file_info.freelist.len(), INIT_PAGE_COUNT);
    //     assert_eq!(file_info.size_kb, INIT_PAGE_COUNT * PAGE_SIZE_KB);
    // }

    // #[test]
    // fn test_page_cache() {
    //     let mut cache = PageCache::new();
    //     let page1 = Page {
    //         index: 0,
    //         bytes: [0u8; PAGE_SIZE_BYTES],
    //         dirty: false,
    //     };
    //     let page2 = Page {
    //         index: 1,
    //         bytes: [0u8; PAGE_SIZE_BYTES],
    //         dirty: false,
    //     };

    //     cache.add_page(&page1).unwrap();
    //     cache.add_page(&page2).unwrap();

    //     assert!(cache.has_page(0));
    //     assert!(cache.has_page(1));

    //     let cached_page1 = cache.get_page(0).unwrap();
    //     assert_eq!(cached_page1.index, 0);
    // }


    // #[test]
    // fn test_add_pages_beyond_initialized() {
    //     let mut pager = Pager::new("test".to_string());
    //     let file_name = "test-0.ydb".to_string();
    //     pager.create_new_file(file_name.clone()).unwrap();

    //     for _ in 0..INIT_PAGE_COUNT + 10 {
    //         let page = pager.create_new_page().unwrap();
    //         pager.flush_page(&page).unwrap();
    //     }

    //     let file_info = pager.file_map.get(&file_name).unwrap();
    //     assert_eq!(file_info.freelist.len(), 0);
    //     assert_eq!(file_info.size_kb, (INIT_PAGE_COUNT + 10) * PAGE_SIZE_KB);

    //     fs::remove_file(file_name).unwrap();
    // }

    // #[test]
    // fn test_update_page() {
    //     let mut pager = Pager::new("test".to_string());
    //     let mut page = pager.create_new_page().unwrap();
    //     page.bytes[0] = 42;
    //     page.dirty = true;
    //     pager.flush_page(page).unwrap();

    //     let (file_name, offset) = pager.page_index_map.get(&0).unwrap();
    //     let mut file = File::open(file_name).unwrap();
    //     file.seek(std::io::SeekFrom::Start(*offset as u64)).unwrap();
    //     let mut buffer = [0u8; PAGE_SIZE_BYTES];
    //     file.read_exact(&mut buffer).unwrap();
    //     assert_eq!(buffer[0], 42);

    //     fs::remove_file(file_name).unwrap();
    // }

    // #[test]
    // fn test_delete_page() {
    //     let mut pager = Pager::new("test".to_string());
    //     let page = pager.create_new_page().unwrap();
    //     pager.delete_page(page).unwrap();

    //     assert_eq!(pager.page_count, 0);
    //     assert!(pager.page_index_map.is_empty());

    //     let file_name = format!("{}-0.ydb", pager.fname_prefix);
    //     let file_info = pager.file_map.get(&file_name).unwrap();
    //     assert_eq!(file_info.freelist.len(), INIT_PAGE_COUNT);

    //     fs::remove_file(file_name).unwrap();
    // }

    // #[test]
    // fn test_flush_pages() {
    //     let mut pager = Pager::new("test".to_string());
    //     let mut page1 = pager.create_new_page().unwrap();
    //     let mut page2 = pager.create_new_page().unwrap();
    //     page1.bytes[0] = 1;
    //     page2.bytes[0] = 2;
    //     page1.dirty = true;
    //     page2.dirty = true;
    //     pager.cache.add_page(&page1).unwrap();
    //     pager.cache.add_page(&page2).unwrap();

    //     pager.flush_all_pages().unwrap();

    //     let (file_name1, offset1) = pager.page_index_map.get(&page1.index).unwrap();
    //     let (file_name2, offset2) = pager.page_index_map.get(&page2.index).unwrap();
    //     let mut file1 = File::open(file_name1).unwrap();
    //     let mut file2 = File::open(file_name2).unwrap();
    //     file1.seek(std::io::SeekFrom::Start(*offset1 as u64)).unwrap();
    //     file2.seek(std::io::SeekFrom::Start(*offset2 as u64)).unwrap();
    //     let mut buffer1 = [0u8; PAGE_SIZE_BYTES];
    //     let mut buffer2 = [0u8; PAGE_SIZE_BYTES];
    //     file1.read_exact(&mut buffer1).unwrap();
    //     file2.read_exact(&mut buffer2).unwrap();
    //     assert_eq!(buffer1[0], 1);
    //     assert_eq!(buffer2[0], 2);

    //     fs::remove_file(file_name1).unwrap();
    //     fs::remove_file(file_name2).unwrap();
    // }

    // #[test]
    // fn test_retrieve_page_from_cache() {
    //     let mut pager = Pager::new("test".to_string());
    //     let page = pager.create_new_page().unwrap();
    //     pager.cache.add_page(&page).unwrap();

    //     let cached_page = pager.cache.get_page(page.index).unwrap();
    //     assert_eq!(cached_page.index, page.index);

    //     let file_name = format!("{}-0.ydb", pager.fname_prefix);
    //     fs::remove_file(file_name).unwrap();
    // }

    // #[test]
    // fn test_cache() {
    //     let mut cache = PageCache::new();
    //     let mut pages = Vec::new();
    //     for i in 0..cache.capacity + 10 {
    //         let page = Page {
    //             index: i,
    //             bytes: [0u8; PAGE_SIZE_BYTES],
    //             dirty: false,
    //         };
    //         cache.add_page(&page).unwrap();
    //         pages.push(page);
    //     }

    //     for i in 0..10 {
    //         assert!(!cache.has_page(i));
    //     }
    //     for i in 10..cache.capacity + 10 {
    //         assert!(cache.has_page(i));
    //     }
    // }

}