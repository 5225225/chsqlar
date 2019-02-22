use cdchunking::{Chunker, ZPAQ};
use crypto::digest::Digest;
use crypto::sha3::Sha3;
use failure::Error;
use rusqlite::types::ToSql;
use rusqlite::DropBehavior;
use rusqlite::Transaction;
use rusqlite::{Connection, NO_PARAMS};
use std::env::current_dir;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use zstd::{decode_all, encode_all};

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
    Add { files: Vec<PathBuf> },
    List,
    Extract { files: Vec<PathBuf> },
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

        connection.execute(
            "CREATE TABLE IF NOT EXISTS
            files (
                name TEXT PRIMARY KEY,
                size INT,
                chunks BLOB
            );
        ",
            NO_PARAMS,
        )?;

        connection.execute(
            "CREATE TABLE IF NOT EXISTS
            chunks (
                hash BLOB PRIMARY KEY,
                data BLOB
            );
        ",
            NO_PARAMS,
        )?;

        connection.execute("PRAGMA journal_mode=WAL;", NO_PARAMS);

        Ok(SqliteDatabase { connection })
    }
}

fn get_file_data(trans: &mut Transaction, name: PathBuf) -> Result<Vec<u8>, Error> {
    let f = get_file(trans, name)?;

    let mut result = Vec::new();

    for hash in f.chunks {
        let chunk = get_chunk(trans, &hash)?;
        result.extend_from_slice(&chunk);
    }

    Ok(result)
}

fn put_hash_chunk(trans: &mut Transaction, data: Vec<u8>) -> Result<String, Error> {
    let hash = hash_chunk(&data);

    put_chunk(trans, hash.clone(), data)?;

    Ok(hash)
}

fn get_chunk(trans: &mut Transaction, hash: &str) -> Result<Vec<u8>, Error> {
    let data: Vec<u8> =
        trans.query_row("SELECT data FROM chunks WHERE hash=?", &[&hash], |row| {
            row.get(0)
        })?;

    let decoded = decode_all(&*data)?;
    Ok(decoded)
}

fn put_chunk(trans: &mut Transaction, hash: String, data: Vec<u8>) -> Result<(), Error> {
    let compressed = encode_all(&*data, 0)?;
    trans.execute(
        "INSERT OR IGNORE INTO chunks VALUES (?,?)",
        &[&hash, &compressed as &ToSql],
    )?;
    Ok(())
}

fn put_file(trans: &mut Transaction, file: File) -> Result<(), Error> {
    let chunks = file.chunks.join(";");

    trans.execute(
        "INSERT OR REPLACE INTO files VALUES (?,?,?)",
        &[&file.name.to_str().unwrap() as &ToSql, &file.size, &chunks],
    )?;

    Ok(())
}

fn list_files(trans: &mut Transaction) -> Result<Vec<PathBuf>, Error> {
    let mut stmt = trans.prepare("SELECT name FROM files")?;
    let mut results = Vec::<String>::new();
    for name in stmt.query_map(NO_PARAMS, |row| row.get(0))? {
        results.push(name?);
    }

    Ok(results.iter().map(PathBuf::from).collect())
}

fn chunk_data(data: Vec<u8>) -> Vec<Vec<u8>> {
    let chunker = Chunker::new(ZPAQ::new(20));

    let mut chunks = Vec::new();

    for chunk in chunker.slices(&data) {
        chunks.push(chunk.to_owned());
    }

    chunks
}

fn hash_chunk(data: &[u8]) -> String {
    let mut hasher = Sha3::sha3_512();

    hasher.input(&data);

    hasher.result_str()
}

fn get_file(trans: &mut Transaction, name: PathBuf) -> Result<File, Error> {
    let size: i64;
    let chunks: String;

    let result = trans.query_row(
        "SELECT size, chunks FROM files WHERE name=?",
        &[&name.to_str().unwrap()],
        |row| (row.get(0), row.get(1)),
    )?;

    size = result.0;
    chunks = result.1;

    let chunks_vec = chunks.split(";").map(|s| s.to_string()).collect();

    Ok(File {
        name,
        size,
        chunks: chunks_vec,
    })
}

fn put_file_data(trans: &mut Transaction, name: PathBuf, data: Vec<u8>) -> Result<(), Error> {
    let mut f = get_file(trans, name)?;

    let mut chunks = Vec::new();

    for chunk in chunk_data(data) {
        let hash = put_hash_chunk(trans, chunk)?;
        chunks.push(hash);
    }

    f.chunks = chunks;

    put_file(trans, f)?;

    Ok(())
}

fn list_cmd(db: &mut SqliteDatabase) -> Result<(), Error> {
    let mut trans = db.connection.transaction()?;

    let files = list_files(&mut trans)?;

    for file in files {
        println!("{}", file.to_str().unwrap());
    }

    Ok(())
}

fn add_file(trans: &mut Transaction, fpath: PathBuf, fname: PathBuf) -> Result<(), Error> {
    let mut buf = Vec::new();
    fs::File::open(&fpath)?.read_to_end(&mut buf)?;
    let metadata = fs::metadata(&fpath)?;

    let f = File {
        name: fname.clone(),
        size: metadata.len() as i64,
        chunks: Vec::new(),
    };

    put_file(trans, f)?;

    put_file_data(trans, fname, buf)?;

    Ok(())
}

fn normalise_path<'a>(cwd: &'a Path, p: &'a Path) -> &'a Path {
    cwd.ancestors()
        .map(|x| p.strip_prefix(x))
        .filter(|x| x.is_ok())
        .next()
        .unwrap()
        .unwrap()
}

fn resolve_files(file: PathBuf) -> Result<Vec<PathBuf>, Error> {
    let mut result = Vec::new();

    let file = fs::canonicalize(file)?;

    let meta = fs::metadata(&file)?;

    if meta.is_file() {
        result.push(file);
    } else if meta.is_dir() {
        let files: Vec<_> = fs::read_dir(&file)?.map(|x| x.unwrap().path()).collect();

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

fn add_files_cmd(db: &mut SqliteDatabase, files: Vec<PathBuf>) -> Result<(), Error> {
    let mut trans = db.connection.transaction()?;

    let cwd = current_dir()?;
    for file in files.into_iter() {
        let resolved = resolve_files(file)?;
        for f in resolved {
            let normalised = normalise_path(&cwd, &f).to_path_buf();
            add_file(&mut trans, f, normalised)?;
        }
    }

    Ok(())
}

fn write_file_data_safe(fname: &Path, data: &[u8]) -> Result<(), Error> {
    fs::create_dir_all(fname.parent().unwrap())?;
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(fname)?;
    f.write_all(&data)?;
    Ok(())
}

fn extract_file(trans: &mut Transaction, file: PathBuf, ex_to: PathBuf) -> Result<(), Error> {
    let db_data = get_file_data(trans, file.clone())?;

    let par = ex_to.parent().unwrap();

    let common = file.strip_prefix(par).unwrap();

    write_file_data_safe(common, &db_data)?;

    Ok(())
}

fn extract_path(trans: &mut Transaction, file: PathBuf) -> Result<(), Error> {
    let db_files = list_files(trans)?;

    let files: Vec<_> = db_files
        .iter()
        .filter(|x| Path::new(x).starts_with(&file))
        .collect();

    for f in files {
        extract_file(trans, f.to_path_buf(), file.clone())?;
    }

    Ok(())
}

fn extract_files_cmd(db: &mut SqliteDatabase, files: Vec<PathBuf>) -> Result<(), Error> {
    let mut trans = db.connection.transaction()?;
    for file in files {
        extract_path(&mut trans, file)?;
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let app = Opt::from_args();

    let mut db = SqliteDatabase::new(&app.opt.database)?;

    match app.cmd {
        OptCommand::List => {
            list_cmd(&mut db)?;
        }
        OptCommand::Add { files } => {
            add_files_cmd(&mut db, files)?;
        }
        OptCommand::Extract { files } => {
            extract_files_cmd(&mut db, files)?;
        }
    }

    Ok(())
}
