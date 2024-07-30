use anyhow::Result;
use chrono::NaiveDateTime;

fn main() -> Result<()> {
    let s = "2019-12-28T05:35:42.666Z";
    let from: NaiveDateTime = s.parse()?;
    println!("{:?}", from);

    Ok(())
}
