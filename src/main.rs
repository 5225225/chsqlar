use std::collections::HashMap;
use crypto::sha3::Sha3;
use crypto::digest::Digest;

trait Archive {
    fn chunk_data(&self, data: Vec<u8>) -> Vec<Vec<u8>>;
    fn get_chunk(&self, hash: &str) -> Vec<u8>;
    fn put_chunk(&mut self, data: Vec<u8>) -> String;
    fn get_file(&self, name: &str) -> File;
    fn put_file(&mut self, file: File);
    fn list_files(&self) -> Vec<String>;

    fn put_file_data(&mut self, name: &str, data: Vec<u8>) {
        let mut f = self.get_file(name);

        let mut chunks = Vec::new();

        for chunk in self.chunk_data(data) {
            let hash = self.put_chunk(chunk);
            chunks.push(hash);
        }

        f.chunks = chunks;

        self.put_file(f);
    }

    fn get_file_data(&mut self, name: &str) -> Vec<u8> {
        let f = self.get_file(name);

        f.chunks.iter().flat_map(|hash| self.get_chunk(hash)).collect()
    }
}

#[derive(Debug, Clone)]
struct File {
    name: String,
    mode: u32,
    mtime: u64,
    size: u64,
    chunks: Vec<String>,
}

struct MemoryDatabase {
    files: HashMap<String, File>,
    chunks: HashMap<String, Vec<u8>>,
}

impl MemoryDatabase {
    fn new() -> Self {
        MemoryDatabase {
            files: HashMap::new(),
            chunks: HashMap::new(),
        }
    }
}

impl Archive for MemoryDatabase {
    fn chunk_data(&self, data: Vec<u8>) -> Vec<Vec<u8>> {
        let mut chunk = Vec::new();
        chunk.push(data);
        chunk
    }

    fn get_chunk(&self, hash: &str) -> Vec<u8> {
        self.chunks[hash].clone()
    }

    fn put_chunk(&mut self, data: Vec<u8>) -> String {
        let mut hasher = Sha3::sha3_512();

        hasher.input(&data);

        let hash = hasher.result_str();

        self.chunks.insert(hash.clone(), data);

        hash
    }

    fn get_file(&self, name: &str) -> File {
        self.files[name].clone()
    }

    fn put_file(&mut self, file: File) {
        self.files.insert(file.name.clone(), file);
    }

    fn list_files(&self) -> Vec<String> {
        let mut result = Vec::new();

        for key in self.files.keys() {
            result.push(key.clone());
        }

        result
    }
}

fn main() {
    let mut db = MemoryDatabase::new();

    let f = File {
        chunks: Vec::new(),
        mode: 0o755,
        mtime: 0,
        name: "Empty.txt".to_string(),
        size: 0,
    };

    db.put_file(f);

    db.put_file_data("Empty.txt", vec![1,3,3,7]);

    let f = db.get_file("Empty.txt");

    println!("{:?}", f);

}
