use clap::{ArgMatches, Parser};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

use crate::{Backend, CmdExecutor, ReplContext, ReplMsg};

use super::ReplResult;

#[derive(Debug, Clone)]
pub enum DatasetConn {
    Postgres(String),
    Csv(FileOpts),
    Parquet(String),
    NdJson(FileOpts),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FileOpts {
    pub filename: String,
    pub ext: String,
    pub compression: FileCompressionType,
}

impl FileOpts {
    pub fn new(
        filename: impl Into<String>,
        ext: impl Into<String>,
        compression: FileCompressionType,
    ) -> Self {
        Self {
            filename: filename.into(),
            ext: ext.into(),
            compression,
        }
    }
}

#[derive(Parser, Debug)]
pub struct ConnectOpts {
    #[arg(value_parser=verify_conn_str, help = "Connection string to the dataset, could be postgres of local file (support: csv, parquet, json)")]
    pub conn: DatasetConn,
    #[arg(short, long, help = "If database, the name of the table")]
    pub table: Option<String>,
    #[arg(short, long, help = "The name of the dataset")]
    pub name: String,
}

pub fn connect(args: ArgMatches, ctx: &mut ReplContext) -> ReplResult {
    let conn = args
        .get_one::<DatasetConn>("conn")
        .expect("expect conn_str")
        .to_owned();

    let table = args.get_one::<String>("table").map(|s| s.to_string());
    let name = args
        .get_one::<String>("name")
        .expect("expect name")
        .to_string();

    let (msg, rx) = ReplMsg::new(ConnectOpts::new(conn, table, name));

    Ok(ctx.send(msg, rx))
}

impl ConnectOpts {
    pub fn new(conn: DatasetConn, table: Option<String>, name: String) -> Self {
        Self { conn, table, name }
    }
}

impl CmdExecutor for ConnectOpts {
    async fn execute<T: Backend>(self, backend: &mut T) -> anyhow::Result<String> {
        backend.connect(&self).await?;
        Ok(format!("Connected to database: {}", self.name))
    }
}

fn verify_conn_str(s: &str) -> Result<DatasetConn, String> {
    if s.starts_with("postgres://") {
        return Ok(DatasetConn::Postgres(s.to_owned()));
    }
    let opt = get_file_opt(s).ok_or(format!("invalid connection string: {}", s))?;
    match opt.ext.as_str() {
        "csv" => Ok(DatasetConn::Csv(opt)),
        "json" | "jsonl" | "ndjson" => Ok(DatasetConn::NdJson(opt)),
        "parquet" => Ok(DatasetConn::Parquet(s.to_string())),
        v => Err(format!("unsupported file extension: {}", v)),
    }
}

fn get_file_opt(conn_str: &str) -> Option<FileOpts> {
    let mut exts = conn_str.rsplitn(3, '.');
    let ext = exts.next()?;
    let compression = match ext {
        "gz" => FileCompressionType::GZIP,
        "bz2" => FileCompressionType::BZIP2,
        "xz" => FileCompressionType::XZ,
        "zstd" => FileCompressionType::ZSTD,
        v => {
            let filetype = v;
            let _ = exts.next()?;
            return Some(FileOpts::new(
                conn_str,
                filetype,
                FileCompressionType::UNCOMPRESSED,
            ));
        }
    };
    let filetype = exts.next()?;
    let _ = exts.next()?;

    Some(FileOpts::new(conn_str, filetype, compression))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_conn_str() {
        let opt = get_file_opt("foo.csv.bz2").unwrap();
        assert_eq!(
            opt,
            FileOpts::new("foo.csv.bz2", "csv", FileCompressionType::BZIP2)
        );
        let opt = get_file_opt("foo.csv").unwrap();
        assert_eq!(
            opt,
            FileOpts::new("foo.csv", "csv", FileCompressionType::UNCOMPRESSED)
        );
        let opt = get_file_opt("foo.bar").unwrap();
        assert_eq!(
            opt,
            FileOpts::new("foo.bar", "bar", FileCompressionType::UNCOMPRESSED)
        );
        let opt = get_file_opt("foobar");
        assert!(opt.is_none());
    }
}
