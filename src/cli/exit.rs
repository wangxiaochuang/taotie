use super::ReplResult;
use crate::{Backend, CmdExecutor, ReplContext};
use clap::{ArgMatches, Parser};

#[derive(Parser, Debug)]
pub struct ExitOpts;

pub fn exit(_args: ArgMatches, _ctx: &mut ReplContext) -> ReplResult {
    std::process::exit(0);
}

impl CmdExecutor for ExitOpts {
    async fn execute<T: Backend>(self, _backend: &mut T) -> anyhow::Result<String> {
        std::process::exit(0);
    }
}
