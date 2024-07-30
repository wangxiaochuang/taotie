use clap::Parser;
pub use connect::ConnectOpts;
pub use describe::DescribeOpts;
use enum_dispatch::enum_dispatch;
pub use exit::ExitOpts;
pub use head::HeadOpts;
pub use list::ListOpts;
pub use schema::SchemaOpts;
pub use sql::SqlOpts;

pub use connect::{connect, DatasetConn};
pub use describe::describe;
pub use exit::exit;
pub use head::head;
pub use list::list;
pub use schema::schema;
pub use sql::sql;

mod connect;
mod describe;
mod exit;
mod head;
mod list;
mod schema;
mod sql;

type ReplResult = Result<Option<String>, reedline_repl_rs::Error>;

#[derive(Parser, Debug)]
#[enum_dispatch(CmdExecutor)]
pub enum ReplCommand {
    #[command(
        name = "connect",
        about = "connect to a dataset and register it to Taotie"
    )]
    Connect(ConnectOpts),
    #[command(name = "list", about = "List all registered datasets")]
    List(ListOpts),
    #[command(name = "schema", about = "Describe the schema of a dataset")]
    Schema(SchemaOpts),
    #[command(name = "describe", about = "Describe a dataset")]
    Describe(DescribeOpts),
    #[command(about = "Show first few rows of a dataset")]
    Head(HeadOpts),
    #[command(about = "Query a dataset using given SQL")]
    Sql(SqlOpts),
    #[command(name = "exit", about = "exit")]
    Exit(ExitOpts),
}
