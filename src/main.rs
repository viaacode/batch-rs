use std::io;
use clap::{Parser, Subcommand};
use postgres::{Client, NoTls};
use amiquip::{Connection, Exchange, Publish, Result};

use batch_rs::*;

// CLI parsing
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Batch ID (formerly "name"): eg, 'QAS-BD-OR-123abc-2022-01-01-00-00-00-000'
    /// There should be one and only one batch found via its ID.
    #[clap(short, long)]
    batch_id: String,
    //~ #[clap(subcommand)]
    //~ cmd: SubCommand,
}

// TODO: implement subcommands
#[derive(Subcommand, Debug)]
enum SubCommand {
    /// Start a batch
    Start {
        #[clap(short, long)]
        /// some var
        batch_id: String,
    },
    /// List all the batches
    List {
        #[clap(short, long)]
        /// some var
        start_path: String,
    },
    /// Check if batch is present and good to start
    Check {
        #[clap(short, long)]
        /// some var
        package_name: String,
    },
    /// Report on a running batch
    Report {
        #[clap(short, long)]
        /// some var
        package_name: String,
    },
}

fn main() -> Result<(), anyhow::Error> {
    //
    env_logger::init();
    // Get our configuration from the environment
    // The necessary environment variables can be found in the `.env` file
    let config = match envy::from_env::<Config>() {
       Ok(config) => config,
       Err(error) => panic!("{:#?}", error)
    };
    // Args
    let args = Args::parse();
    // Postgres
    log::info!("Connecting to database {} on {}", &config.postgres_database, &config.postgres_host);
    let connection_string = format_postgres_connection_string(&config);
    //~ let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;
    let mut client = Client::connect(&connection_string, NoTls)?;

    // AMQP
    // Open AMQP-connection.
    let connection_string = format_amqp_connection_string(&config);
    log::info!("Connecting to AMQP on {}", &config.amqp_host);
    log::debug!("Will publish to AMQP q {}", &config.amqp_out_queue);
    let mut connection = Connection::insecure_open(&connection_string)?;
    // Open a channel - None says let the library choose the channel ID.
    let channel = connection.open_channel(None)?;
    // Get a handle to the direct exchange on our channel.
    let exchange = Exchange::direct(&channel);

    // Get batch: there should be one and only one batch found via it's ID.
    let batch_row = client.query_one("SELECT * FROM batchin_batches where batch_id = $1;", &[&args.batch_id])?;
    log::trace!("Found row: {:#?}", batch_row);
    let batch = Batch::from_postgres_row(&batch_row);
    log::debug!("Found batch: {:#?}", batch);

    let rows = client.query("SELECT * FROM batchin_batch_records where batch_row_id = $1;", &[&batch.row_id])?;
    log::info!("Found batch: '{}' with {} records", &batch.batch_id, rows.len());

    println!("Proceed? [y/n]");

    let mut proceed = String::new();

    io::stdin()
        .read_line(&mut proceed)
        .expect("Failed to read line");

    // A newline is added to the input when pressing enter: trim it.
    let proceed = proceed.trim();

    if proceed == "y" {
    
        for row in &rows {
            log::trace!("Found row: {:#?}", row);
            let batch_record = BatchRecord::from_postgres_row(&row);
            log::debug!("Found batch_record: {:#?}", batch_record);
            let watchfolder_msg = WatchfolderMsg::new(&batch, &batch_record);
            log::debug!("WatchfolderMsg: {:#?}", watchfolder_msg);
            let watchfolder_json = watchfolder_msg.to_json();
            log::debug!("WatchfolderMsgJson: {:?}", watchfolder_json);
            // Publish
            log::info!("Publishing message for essence '{}' to {}/{}", &batch_record.filename, &config.amqp_host, &config.amqp_out_queue);
            exchange.publish(Publish::new(watchfolder_json.as_bytes(), &config.amqp_out_queue))?;
        }

    } else {
        println!("Exiting");
    }

    // Remember to close the AMQP-connection
    connection.close();

    Ok(())
}
