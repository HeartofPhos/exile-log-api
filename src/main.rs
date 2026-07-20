use std::{io::Error, path::PathBuf, time::Duration};

use clap::Parser;
use env_logger::{Builder, Env};
use log::info;
use regex::RegexSet;
use sysinfo::System;
use tokio_tungstenite::tungstenite::Message;

mod log_reader;
mod server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = { "127.0.0.1:6754".to_string() })]
    address: String,
    #[arg(long, default_value_t = 5)]
    client_timeout: u64,
    #[arg(long, default_value_t = 1)]
    heart_beat: u64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let env = Env::default().filter_or("RUST_LOG", log::LevelFilter::Info.as_str());
    Builder::from_env(env).init();

    let args = Args::parse();

    info!("Waiting for PathOfExile client...");
    let exe_folder: PathBuf = async {
        let mut sys = System::new_all();
        let mut interval = tokio::time::interval(Duration::from_secs(args.heart_beat));
        loop {
            sys.refresh_all();

            for process in sys.processes_by_name("PathOfExile".as_ref()) {
                if let Some(path) = process.exe()
                    && let Some(parent) = path.parent()
                {
                    return parent.into();
                }
            }

            interval.tick().await;
        }
    }
    .await;

    let log_path = exe_folder.join("logs").join("LatestClient.txt");
    info!("Log file: {}", log_path.display());

    let (tx, rx) = tokio::sync::broadcast::channel(1);

    let listen = server::listen(args.address, rx, Duration::from_secs(args.client_timeout));
    let log = async move {
        let mut reader = log_reader::build(
            log_path,
            RegexSet::new([r"Generating level \d+ area"]).unwrap(),
        );

        let mut interval = tokio::time::interval(Duration::from_secs(args.heart_beat));
        loop {
            let mut messages = vec![Message::Ping(Default::default())];
            let _ = reader.read_latest(|line| messages.push(line.into()));
            let _ = tx.send(messages);
            interval.tick().await;
        }
    };

    tokio::select! {
        _ = listen => (),
        _ = log => (),
    }

    Ok(())
}
