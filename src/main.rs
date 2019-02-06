use cdchunking::{Chunker, ZPAQ};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use rusqlite::{Connection, NO_PARAMS};
use rusqlite::types::ToSql;
use std::fs;
use std::io::Read;
use zstd::{encode_all, decode_all};
use structopt::StructOpt;
use std::path::{Path, PathBuf};
use failure::Error;


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
        files: Vec<PathBuf>
    },
    List,
}

trait Archive {
    fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, Error>;
    fn put_chunk(&mut self, hash: String, data: Vec<u8>) -> Result<(), Error>;
    fn get_file(&self, name: PathBuf) -> Result<File, Error>;
    fn put_file(&mut self, file: File) -> Result<(), Error>;
    fn list_files(&self) -> Result<Vec<String>, Error>;

    fn put_file_data(&mut self, name: PathBuf, data: Vec<u8>) -> Result<(), Error> {
        let mut f = self.get_file(name)?;

        let mut chunks = Vec::new();

        for chunk in self.chunk_data(data) {
            let hash = self.put_hash_chunk(chunk)?;
            chunks.push(hash);
        }

        f.chunks = chunks;

        self.put_file(f)?;

        Ok(())
    }

    fn get_file_data(&mut self, name: PathBuf) -> Result<Vec<u8>, Error> {
        let f = self.get_file(name)?;
        
        let mut result = Vec::new();

        for hash in f.chunks {
            let chunk = self.get_chunk(&hash)?;
            result.extend_from_slice(&chunk);
        }

        Ok(result)
    }

    fn chunk_data(&self, data: Vec<u8>) -> Vec<Vec<u8>> {
        let chunker = Chunker::new(ZPAQ::new(20));

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

    fn put_hash_chunk(&mut self, data: Vec<u8>) -> Result<String, Error> {
        let hash = self.hash_chunk(&data);

        self.put_chunk(hash.clone(), data)?;

        Ok(hash)
    }
}

#[derive(Debug, Clone)]
struct File {
    name: PathBuf,
    size: i64,
    chunks: Vec<String>,
}

struct SqliteDatabase {
    connection: Connection,
}

impl SqliteDatabase {
    fn new(fname: &str) -> Result<Self, Error> {
        let connection = Connection::open(fname)?;

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
            )?
            ;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS
            chunks (
                hash BLOB PRIMARY KEY,
                data BLOB
            );
        ",
                NO_PARAMS,
            )?;
            

        Ok(SqliteDatabase { connection })
    }
}

impl Archive for SqliteDatabase {
    fn get_chunk(&self, hash: &str) -> Result<Vec<u8>, Error> {
        let data: Vec<u8> = self.connection.query_row("SELECT data FROM chunks WHERE hash=?", &[&hash], |row| row.get(0))?;

        let decoded = decode_all(&*data)?;
        Ok(decoded)
    }
    fn put_chunk(&mut self, hash: String, data: Vec<u8>) -> Result<(), Error> {
        let compressed = encode_all(&*data, 0)?;
        self.connection.execute("INSERT OR IGNORE INTO chunks VALUES (?,?)", &[&hash, &compressed as &ToSql])?;
        Ok(())
    }
    fn get_file(&self, name: PathBuf) -> Result<File, Error> {
        let size: i64;
        let chunks: String;

        let result = self.connection.query_row(
        "SELECT size, chunks FROM files WHERE name=?", 
        &[&name.to_str().unwrap()], 
        |row| (row.get(0), row.get(1)))?;

        size = result.0;
        chunks = result.1;

        let chunks_vec = chunks.split(";").map(|s| s.to_string()).collect();

        Ok(File {
            name,
            size,
            chunks: chunks_vec,
        })
    }
    fn put_file(&mut self, file: File) -> Result<(), Error> {
        let chunks = file.chunks.join(";");

        self.connection.execute("INSERT OR REPLACE INTO files VALUES (?,?,?)", &[
            &file.name.to_str().unwrap() as &ToSql,
            &file.size,
            &chunks,
        ])?;

        Ok(())
    }

    fn list_files(&self) -> Result<Vec<String>, Error> {
        let mut stmt = self.connection.prepare("SELECT name FROM files")?;
        let mut results = Vec::new();
        for name in stmt.query_map(NO_PARAMS, |row| row.get(0))? {
            results.push(name?);
        }
        Ok(results)
    }
}

fn list_cmd(db: &Archive) -> Result<(), Error> {
    let files = db.list_files()?;

    for file in files {
        println!("{}", file);
    }

    Ok(())
}

fn add_file(db: &mut Archive, fname: PathBuf) -> Result<(), Error> {
    let mut buf = Vec::new();
    fs::File::open(&fname)?.read_to_end(&mut buf)?;
    let metadata = fs::metadata(&fname)?;

    let f = File {
        name: fname.clone(),
        size: metadata.len() as i64,
        chunks: Vec::new(),
    };

    db.put_file(f)?;

    db.put_file_data(fname, buf)?;

    Ok(())
}

fn resolve_files(file: PathBuf) -> Result<Vec<PathBuf>, Error> {
    let mut result = Vec::new();

    let file = fs::canonicalize(file)?;

    let meta = fs::metadata(&file)?;

    if meta.is_file() {
        result.push(file);
    } else if meta.is_dir() {
        let files: Vec<_> = fs::read_dir(&file)?
            .map(|x| x.unwrap().path())
            .collect();

        for f in files {
            let mut pathbuf = PathBuf::new();
            pathbuf.push(&file);
            pathbuf.push(f);
            let resolved_files = resolve_files(pathbuf)?;
            for resolved in resolved_files {
                result.push(resolved);
            }
        }
    } else {
        unimplemented!("Unknown file type");
    }

    Ok(result)
}

fn add_files_cmd(db: &mut Archive, files: Vec<PathBuf>) -> Result<(), Error> {
    for file in files.into_iter() {
        let resolved = resolve_files(file)?;
        for f in resolved {
            add_file(db, f)?;
        }
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let app = Opt::from_args();

    let mut db = SqliteDatabase::new(&app.opt.database)?;

    match app.cmd {
        OptCommand::List => {
            list_cmd(&db)?;
        },
        OptCommand::Add{files} => {
            add_files_cmd(&mut db, files)?;
        },
    }

    Ok(())
}
