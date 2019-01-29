use cdchunking::{Chunker, ZPAQ};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use rusqlite::{Connection, NO_PARAMS};
use std::collections::HashMap;
use rusqlite::types::ToSql;
use std::env;
use std::fs;
use std::io::{Read, Write};
use zstd::{encode_all, decode_all};
use structopt::StructOpt;
use std::path::{Path, PathBuf};


#[derive(StructOpt, Debug)]
struct Opt {
    #[structopt(flatten)]
    opt: CommonOpt,

    #[structopt(subcommand)]
    cmd: OptCommand,
}

#[derive(StructOpt, Debug)]
struct CommonOpt {
    database: String,
    #[structopt(short = "v", parse(from_occurrences))]
    verbosity: u8,
}

#[derive(StructOpt, Debug)]
enum OptCommand {
    Add {
        files: Vec<String>
    },
    List,
}

trait Archive {
    fn get_chunk(&self, hash: &str) -> Vec<u8>;
    fn put_chunk(&mut self, hash: String, data: Vec<u8>);
    fn get_file(&self, name: &str) -> File;
    fn put_file(&mut self, file: File);
    fn list_files(&self) -> Vec<String>;

    fn put_file_data(&mut self, name: &str, data: Vec<u8>) {
        let mut f = self.get_file(name);

        let mut chunks = Vec::new();

        for chunk in self.chunk_data(data) {
            let hash = self.put_hash_chunk(chunk);
            chunks.push(hash);
        }

        f.chunks = chunks;

        self.put_file(f);
    }

    fn get_file_data(&mut self, name: &str) -> Vec<u8> {
        let f = self.get_file(name);

        f.chunks
            .iter()
            .flat_map(|hash| self.get_chunk(hash))
            .collect()
    }

    fn chunk_data(&self, data: Vec<u8>) -> Vec<Vec<u8>> {
        let chunker = Chunker::new(ZPAQ::new(13));

        let mut chunks = Vec::new();

        for chunk in chunker.slices(&data) {
            chunks.push(chunk.to_owned());
        }

        chunks
    }

    fn hash_chunk(&self, data: &[u8]) -> String {
        let mut hasher = Sha3::sha3_512();

        hasher.input(&data);

        hasher.result_str()
    }

    fn put_hash_chunk(&mut self, data: Vec<u8>) -> String {
        let hash = self.hash_chunk(&data);

        self.put_chunk(hash.clone(), data);

        hash
    }
}

#[derive(Debug, Clone)]
struct File {
    name: String,
    size: i64,
    chunks: Vec<String>,
}

struct SqliteDatabase {
    connection: Connection,
}

impl SqliteDatabase {
    fn new(fname: &str) -> Self {
        let connection = Connection::open(fname).unwrap();

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS
            files (
                name TEXT PRIMARY KEY,
                size INT,
                chunks BLOB
            );
        ",
                NO_PARAMS,
            )
            .unwrap();

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS
            chunks (
                hash BLOB PRIMARY KEY,
                data BLOB
            );
        ",
                NO_PARAMS,
            )
            .unwrap();

        SqliteDatabase { connection }
    }
}

impl Archive for SqliteDatabase {
    fn get_chunk(&self, hash: &str) -> Vec<u8> {
        let data: Vec<u8> = self.connection.query_row("SELECT data FROM chunks WHERE hash=?", &[&hash], |row| row.get(0)).unwrap();

        decode_all(&*data).unwrap()
    }
    fn put_chunk(&mut self, hash: String, data: Vec<u8>) {
        let compressed = encode_all(&*data, 0).unwrap();
        self.connection.execute("INSERT OR IGNORE INTO chunks VALUES (?,?)", &[&hash, &compressed as &ToSql]).unwrap();
    }
    fn get_file(&self, name: &str) -> File {
        let size: i64;
        let chunks: String;

        let result = self.connection.query_row(
        "SELECT size, chunks FROM files WHERE name=?", 
        &[&name], 
        |row| (row.get(0), row.get(1)))
        .unwrap();

        size = result.0;
        chunks = result.1;

        let chunks_vec = chunks.split(";").map(|s| s.to_string()).collect();

        File {
            name: name.to_string(),
            size,
            chunks: chunks_vec,
        }
    }
    fn put_file(&mut self, file: File) {
        let chunks = file.chunks.join(";");

        self.connection.execute("INSERT OR REPLACE INTO files VALUES (?,?,?)", &[
            &file.name as &ToSql,
            &file.size,
            &chunks,
        ]).unwrap();
    }

    fn list_files(&self) -> Vec<String> {
        let mut stmt = self.connection.prepare("SELECT name FROM files").unwrap();
        let mut results = Vec::new();
        for name in stmt.query_map(NO_PARAMS, |row| row.get(0)).unwrap() {
            results.push(name.unwrap());
        }
        results
    }
}

fn list_cmd(db: &Archive) {
    let files = db.list_files();

    for file in files {
        println!("{}", file);
    }
}

fn add_file(db: &mut Archive, fname: String) {
    let mut buf = Vec::new();
    fs::File::open(&fname).unwrap().read_to_end(&mut buf).unwrap();
    let metadata = fs::metadata(&fname).unwrap();

    let f = File {
        name: fname.clone(),
        size: metadata.len() as i64,
        chunks: Vec::new(),
    };

    db.put_file(f);

    db.put_file_data(&fname, buf);
}

fn resolve_files(file: String) -> Vec<String> {
    let mut result = Vec::new();

    let meta = fs::metadata(&file).unwrap();

    if meta.is_file() {
        result.push(file);
    } else if meta.is_dir() {
        let files: Vec<_> = fs::read_dir(&file)
            .unwrap()
            .map(|x| x.unwrap().path())
            .collect();

        for f in files {
            let mut pathbuf = PathBuf::new();
            pathbuf.push(&file);
            pathbuf.push(f);
            let resolved_files = resolve_files(pathbuf.to_str().unwrap().to_string());
            for resolved in resolved_files {
                result.push(resolved);
            }
        }
    } else {
        unimplemented!("Unknown file type");
    }

    result
}

fn add_files_cmd(db: &mut Archive, files: Vec<String>) {
    for file in files.into_iter().flat_map(resolve_files) {
        add_file(db, file);
    }
}

fn main() {
    let app = Opt::from_args();

    let mut db = SqliteDatabase::new(&app.opt.database);

    match app.cmd {
        OptCommand::List => {
            list_cmd(&db);
        },
        OptCommand::Add{files} => {
            add_files_cmd(&mut db, files);
        },
    }
}
