use crate::error::{Error, Result};
use parking_lot::RwLock;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::cell::RefMut;
use std::collections::HashMap;
// use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::{Read, Seek, Write};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

const PAGE_SIZE_KB: usize = 4096;
pub const PAGE_SIZE_BYTES: usize = PAGE_SIZE_KB * 1024;
const FILE_LIMIT_PAGES: usize = 4096;
const FILE_LIMIT_KB: usize = FILE_LIMIT_PAGES * PAGE_SIZE_KB;
const INIT_PAGE_COUNT: usize = 24;

//TODO: upgrade to Arc later?

// struct PageCacheRef(Rc<RefCell<PageCache>>);

// impl Deref for PageCacheRef {
//     type Target = PageCache;

//     fn deref(&self) -> &Self::Target {
//         &*self.0.borrow()
//     }
// }

// impl DerefMut for PageCacheRef {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut *self.0.borrow().borrow_mut()
//     }
// }

#[derive(Debug)]
struct FileInfo {
    freelist: Vec<usize>, //should contain offset positions
    size_kb: usize,
}

//good riddance to cloning!
#[derive(Debug)]
pub struct Page {
    pub index: usize,
    bytes: RwLock<Vec<u8>>,
    dirty: RwLock<bool>,
    taken: bool,
}

#[derive(Debug)]
struct PageCache {
    capacity: usize,
    loaded_pages: HashMap<usize, (Rc<RefCell<Page>>, usize)>,
    counter: usize,
}

#[derive(Debug)]
pub struct Pager {
    fname_prefix: String,
    file_map: HashMap<String, FileInfo>,
    cache: Rc<RefCell<PageCache>>,
    page_index_map: HashMap<usize, (String, usize)>, // index -> file, page,
    page_count: usize,
}

impl Page {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            bytes: RwLock::new(vec![0; PAGE_SIZE_BYTES]),
            dirty: RwLock::new(true),
            taken: false,
        }
    }

    pub fn read(&self, offset: usize, length: usize) -> Vec<u8> {
        let bytes = self.bytes.read();
        bytes[offset..offset + length].to_vec()
    }

    pub fn read_all(&self) -> Vec<u8> {
        let bytes = self.bytes.read();
        bytes.to_vec()
    }

    pub fn write(&self, offset: usize, data: &[u8]) {
        let mut bytes = self.bytes.write();
        let end = offset + data.len();
        bytes[offset..end].copy_from_slice(data);
        *self.dirty.write() = true;
    }

    pub fn clear(&mut self) {
        let mut bytes = self.bytes.write();
        bytes[0..PAGE_SIZE_BYTES].fill(0 as u8);
        *self.dirty.write() = true;
    }

    pub fn write_all(&self, mut data: Vec<u8>) {
        //bytes should maintain the lock till the end of this code, so bytes and dirty work together fine
        if data.len() < PAGE_SIZE_BYTES {
            data.resize(PAGE_SIZE_BYTES, 0);
        }

        *self.bytes.write() = data;
        *self.dirty.write() = true;
    }
}

impl FileInfo {
    fn new() -> FileInfo {
        FileInfo {
            freelist: Vec::new(),
            size_kb: 0 as usize,
        }
    }
}

// TODO : serialize and deserialize pager information
impl Pager {
    pub fn new(fname_prefix: String) -> Pager {
        let mut pager = Pager {
            fname_prefix,
            file_map: HashMap::new(),
            cache: Rc::new(RefCell::new(PageCache::new())),
            page_index_map: HashMap::new(),
            page_count: 0 as usize,
        };
        for _ in 0..2 {
            pager.create_new_page().unwrap();
        }
        pager
    }

    pub fn create_new_page(&mut self) -> Result<Page> {
        let file_with_free_page = self
            .file_map
            .iter_mut()
            .find(|(_, info)| !info.freelist.is_empty());

        if let Some((file_name, file_info)) = file_with_free_page {
            let page_index = self.page_count;
            let page_offset = file_info.freelist.pop().unwrap();

            let page = Page::new(page_index);

            self.page_index_map
                .insert(page_index, (file_name.clone(), page_offset));
            self.page_count += 1;

            //TODO: store page in page_cache?
            Ok(page)
        } else {
            let file_not_at_limit = self
                .file_map
                .iter_mut()
                .find(|(_, info)| (FILE_LIMIT_KB - info.size_kb) > PAGE_SIZE_KB + 50); // i got one more in me
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

                let available_page_count = (FILE_LIMIT_KB - file_info.size_kb) / PAGE_SIZE_KB;

                //optimal page allocation
                let extend_count = match available_page_count {
                    1..=2 => 1,
                    3..=4 => 2,
                    5..=10 => 4,
                    _ => 10,
                };
                //extend the file by the new pages
                file.set_len(offset + PAGE_SIZE_BYTES as u64 * extend_count as u64)?;
                //add the new pages to the freelist
                file_info
                    .freelist
                    .extend((0..extend_count).map(|i| (offset as usize + 1) + PAGE_SIZE_KB * i));

                let page = Page::new(self.page_count);

                self.page_index_map
                    .insert(page.index, (file_name.clone(), offset as usize + 1)); // skip last bit in file
                self.page_count += 1;

                Ok(page)
            } else {
                //All files full, create a new file
                let new_file_name = format!("{}-{}.ydb", self.fname_prefix, self.file_map.len());
                self.create_new_file(new_file_name.clone())?;

                let file_info = self
                    .file_map
                    .get_mut(&new_file_name)
                    .ok_or(Error::AccessError)?;
                let page_index = self.page_count;
                let page_offset = file_info.freelist.pop().unwrap();

                //so it must be getting no pop from here
                let page = Page::new(page_index);

                self.page_index_map
                    .insert(page_index, (new_file_name.clone(), page_offset));
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
                        bytes: RwLock::new(buf),
                        dirty: RwLock::new(false),
                        taken: false,
                    });
                }
                return Err(Error::Unknown("Could not read bytes from file".to_string()));
            }
            return Err(Error::FileNotFound);
        }
        return Err(Error::NotFound(format!(
            "Pager could not fetch page with index {}",
            index
        )));
    }

    //TODO: make fetching from page cache more viable than this

    fn get_page_forced(
        &self,
        page_index: usize,
        mut cache: RefMut<PageCache>,
    ) -> Result<Rc<RefCell<Page>>> {
        let new_page = self.fetch_page(page_index)?;

        // let cache = Rc::clone(&self.cache);
        // let mut cache_mut = (*cache).borrow_mut();

        // match Rc::clone(&self.cache).try_borrow_mut() {
        //     Ok(mut cache) => {
        if let Ok(()) = cache.add_page(new_page) {
            if let Some(page) = cache.get_page(page_index) {
                Ok(Rc::clone(page))
            } else {
                Err(Error::Unknown(
                    "Failed to access new page from cache".to_string(),
                ))
            }
        } else {
            Err(Error::Unknown(
                "Failed to add new page to cache".to_string(),
            ))
        }
        //     }
        //     Err(e) => panic!("{:?}", e.to_string()),
        // }

        // if let Ok(()) = cache_mut.add_page(new_page) {
        //     return match cache_mut.get_page(page_index) {
        //         Some(k) => Ok(Rc::clone(k)),
        //         None => Err(Error::Unknown(
        //             "Failed to access new page from cache".to_string(),
        //         )),
        //     };
        // }
        // return Err(Error::Unknown(
        //     "Failed to add new page to cache".to_string(),
        // ));
    }

    pub fn get_page_or_force(&self, page_index: usize) -> Result<Rc<RefCell<Page>>> {
        match Rc::clone(&self.cache).try_borrow_mut() {
            Ok(mut cache_mut) => match cache_mut.get_page(page_index) {
                Some(k) => Ok(Rc::clone(k)),
                None => self.get_page_forced(page_index, cache_mut),
            },
            Err(_) => Err(Error::Unknown(
                "Failed to borrow cache mutably from here".to_string(),
            )),
        }
    }

    pub fn get_page_or_fail(&self, page_index: usize) -> Result<Rc<RefCell<Page>>> {
        match Rc::clone(&self.cache).try_borrow_mut() {
            Ok(mut cache_mut) => match cache_mut.get_page(page_index) {
                Some(k) => Ok(Rc::clone(k)),
                None => Err(Error::NotFound(
                    "Page not found in cache, use get_page_forced as a fallback".to_string(),
                )),
            },
            Err(_) => Err(Error::Unknown("Failed to borrow cache mutably".to_string())),
        }
    }

    fn delete_page(&mut self, page: Page) -> Result<()> {
        let (file_name, offset) = self
            .page_index_map
            .get(&page.index)
            .ok_or(Error::AccessError)?;
        let file_info = self
            .file_map
            .get_mut(file_name)
            .ok_or(Error::FileNotFound)?;
        file_info.freelist.push(*offset);

        self.page_index_map.remove(&page.index);
        self.page_count -= 1;

        Ok(())
    }

    fn create_new_file(&mut self, fname: String) -> Result<()> {
        let mut file = File::create(&fname).unwrap();
        let data = vec![0 as u8; PAGE_SIZE_BYTES * INIT_PAGE_COUNT]; // initialize with 24 pages
        file.write_all(&data).unwrap();

        self.file_map.insert(
            fname.clone(),
            FileInfo {
                size_kb: INIT_PAGE_COUNT * PAGE_SIZE_KB,
                freelist: (0..INIT_PAGE_COUNT)
                    .rev()
                    .map(|i| PAGE_SIZE_KB * i)
                    .collect(),
            },
        );

        Ok(())
    }

    pub fn flush_page(&self, page: &Page) -> Result<()> {
        let (fname, offset) = self
            .page_index_map
            .get(&page.index)
            .ok_or(Error::AccessError)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(fname);

        match file {
            Ok(mut f) => {
                f.seek(std::io::SeekFrom::Start(*offset as u64))?;
                f.write_all(&page.read_all())?;
                *page.dirty.write() = false;
                Ok(())
            }
            Err(e) => Err(Error::IoError(e)),
        }
    }

    // is there a more efficient way to do this?
    pub fn flush_all_pages(&mut self) -> Result<()> {
        for (_, (page, _)) in &self.cache.borrow().loaded_pages {
            self.flush_page(page.borrow().borrow_mut())?;
        }
        Ok(())
    }
}

impl PageCache {
    fn new() -> Self {
        Self {
            capacity: 500,
            loaded_pages: HashMap::new(),
            counter: 0,
        }
    }

    fn has_page(&mut self, page_index: usize) -> bool {
        self.loaded_pages.contains_key(&page_index)
    }

    fn add_page(&mut self, page: Page) -> Result<()> {
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

        self.loaded_pages
            .insert(page.index, (Rc::new(RefCell::new(page)), self.counter));
        self.counter += 1;

        Ok(())
    }

    fn get_page(&mut self, page_index: usize) -> Option<&Rc<RefCell<Page>>> {
        if let Some((page, ref mut counter)) = self.loaded_pages.get_mut(&page_index) {
            *counter = self.counter;
            self.counter += 1;
            Some(page)
        } else {
            None
        }
    }

    // fn get_page_cloned(&mut self, page_index:usize) -> Option<Page> {
    //     if let Some((page, counter)) =  self.loaded_pages.get(&page_index) {
    //         Some(page.clone())
    //     } else {
    //         None
    //     }

    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_delete_page() {
        let mut pager = Pager::new("testasdas".to_string());
        let page = pager.create_new_page().unwrap();
        assert_eq!(page.index, 0);
        assert_eq!(pager.page_count, 1);

        pager.delete_page(page).unwrap();
        assert_eq!(pager.page_count, 0);
    }
}
