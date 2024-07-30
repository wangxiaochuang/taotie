use std::fs::File;

use anyhow::Result;
use arrow::array::AsArray;
use datafusion::prelude::SessionContext;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use polars::{prelude::*, sql::SQLContext};

const PQ_FILE: &str = "assets/sample.parquet";

#[tokio::main]
async fn main() -> Result<()> {
    // read_with_parquet(PQ_FILE)?;
    // read_with_datafusion(PQ_FILE).await?;
    read_with_polars(PQ_FILE)?;
    Ok(())
}

fn read_with_polars(file: &str) -> Result<()> {
    // let df = ParquetReader::new(File::open(file).unwrap()).finish()?;
    let lf = LazyFrame::scan_parquet(file, Default::default())?;
    let mut ctx = SQLContext::new();
    ctx.register("stat", lf);
    let df = ctx
        .execute("select email, name from stat limit 3")?
        .collect()?;
    println!("{:?}", df);
    Ok(())
}

#[allow(dead_code)]
fn read_with_parquet(file: &str) -> Result<()> {
    let file = File::open(file)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(3)
        // .with_limit(3)
        .build()?;

    for record_batch in reader {
        let record_batch = record_batch?;
        let emails = record_batch.column(0).as_string::<i32>();
        println!("begin........");
        println!("{:?}", emails)
    }
    Ok(())
}

#[allow(dead_code)]
async fn read_with_datafusion(file: &str) -> Result<()> {
    let ctx = SessionContext::new();
    // ===== register
    ctx.register_parquet("stat", file, Default::default())
        .await?;
    let df = ctx.sql("select email, name from stat limit 3").await?;
    // ===== read parequet file directly
    // let df = ctx.read_parquet(file, Default::default()).await?;

    // ===== print Batch
    // let res = df.collect().await?;
    // println!("{:?}", res[0].column_by_name("email"));
    // println!("{:?}", res[0].columns());
    // println!("{:?}", res[0].column(0));
    // ===== just show
    // let a = df.collect().await?;
    // let res = a[0].column(0).as_string::<i32>();
    // println!("{:?}", res);
    df.show().await?;
    Ok(())
}
