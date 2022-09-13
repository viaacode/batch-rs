use std::io;
use std::env;

use clap::{Parser, Subcommand};
use postgres::{Client, NoTls};
use amiquip::{Connection, Exchange, Publish, Result};
use cli_table::{format::Justify, print_stdout, Cell, Style, Table};
use duct::cmd;
use ftp::FtpStream;

use batch_rs::*;

// CLI parsing
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)] // Read from `Cargo.toml`
struct Args {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    /// TODO: Display or set the batch-variables
    Vars {
        #[clap(short = 'b', long, value_parser)]
        /// Set or display the Batch ID
        batch_id: Option<String>,
        #[clap(short, long, value_parser)]
        /// Set or display the ftp_path
        ftp_path: Option<String>,
        #[clap(short = 'd', long, value_parser)]
        /// Set or display the batch_description
        batch_description: Option<String>,
    },
    /// List all batches in the database
    List {
    },
    /// TODO: Transform metadata for a batch
    Transform {
        #[clap(required = true, value_parser)]
        /// Batch ID
        batch_id: String,
    },
    /// TODO: Upload a batch.
    /// This adds the batch and it's records to the database and uploads the sidecars to the FTP-server.
    Upload {
        #[clap(required = true, value_parser)]
        /// Batch ID (formerly "name"): eg, 'QAS-BD-OR-123abc-2022-01-01-00-00-00-000'
        /// There should be one and only one batch found via its ID.
        batch_id: String,
    },
    /// Start a batch.
    /// This will send out a so called "watchfolder-message" for every pair in the batch.
    /// If an - optional - local_id is provided, then only this item will be started.
    Start {
        #[clap(required = true, value_parser)]
        /// Batch ID (formerly "name"): eg, 'QAS-BD-OR-123abc-2022-01-01-00-00-00-000'
        /// There should be one and only one batch found via its ID.
        batch_id: String,
        #[clap(required = false, short, long, value_parser)]
        /// Local ID: provide a local ID of the item that should be ingested.
        local_id: Option<String>,
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

    let batch_config = BatchConfig {
        local_path: get_current_working_dir(),
        ftp_path: "/TODO".to_string(),
        batch_description: "Een beschrijving".to_string(),
    };

    // Args
    let args = Args::parse();

    match args.cmd {

        SubCommand::Vars {batch_id, ftp_path, batch_description} => {
            println!("Vars set: {}", "batch_id".to_string());
            println!("Inferred:\n\t- TODO");
        }

        SubCommand::List { } => {
            println!("Listing batches on {}...\n", &config.postgres_host);
            // Postgres
            log::info!("Connecting to database {} on {}", &config.postgres_database, &config.postgres_host);
            let connection_string = format_postgres_connection_string(&config);
            //~ let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await?;
            let mut client = Client::connect(&connection_string, NoTls)?;

            // Select some fields from every batch and include the recordcount
            // For now, non-text fields are cast to text by the DB as a
            // convenience in lieu of casting the pg-timestamp to String in Rust.
            let batch_rows = client.query("SELECT
                    bb.batch_id,
                    bb.description,
                    bb.path,
                    bb.status,
                    bb.created_at::text,
                    c.nr_or_records::text
                FROM batchin_batches bb
                JOIN (
                    SELECT bbr.batch_row_id , count(bbr.row_id) AS nr_or_records
                    FROM batchin_batch_records bbr
                    GROUP BY bbr.batch_row_id
                ) AS c
                on bb.row_id = c.batch_row_id
                ORDER BY bb.created_at ASC;", &[])?;
            log::debug!("Found batches: {:#?}", batch_rows);

            let mut table: Vec<Vec<String>> = Vec::new();

            for row in batch_rows {
                let batch_id: String = row.get("batch_id");
                let description: String = row.get("description");
                let path: String = row.get("path");
                let status: String = row.get("status");
                let created_at: String = row.get("created_at");
                let nr_or_records: String = row.get("nr_or_records");
                table.push(vec![
                    batch_id,
                    description,
                    path,
                    status,
                    created_at,
                    nr_or_records,
                ]);
            }

            let cli_table = table.table()
            .title(vec![
                "batch_id".cell().bold(true),
                "description".cell().bold(true),
                "path".cell().bold(true),
                "status".cell().bold(true),
                "created_at".cell().bold(true),
                "nr_or_records".cell().bold(true),
            ])
            .bold(true);
            assert!(print_stdout(cli_table).is_ok());
        }

        SubCommand::Transform { batch_id } => {
            println!("Transforming batch records in batch {}...\n", &batch_id);
            let java_cmd = "java".to_string();
            let saxon_jar = "/opt/saxonica/saxon11/saxon-he-11.4.jar".to_string();
            let saxon_transform = "net.sf.saxon.Transform".to_string();
            let saxon_query = "net.sf.saxon.Query".to_string();
            let mut arg_vec: Vec<String> = vec![
                "-classpath".to_string(),
                saxon_jar,
                saxon_query,
                "-t".to_string(),
                "-qs:current-date()".to_string(),
            ];
            let stdout = cmd(java_cmd, &arg_vec).read()?;
            println!("Output for \"{}\":\n\n{}", batch_id, stdout);
        }

        SubCommand::Upload { batch_id } => {
            println!("Uploading sidecars for \"{}\" to \"{}\"", batch_id, &config.ftp_host);

            // Local dir
            println!("Current dir: {}", &batch_config.local_path);

            // We need to specify the ftp-port as part of the host
            let ftp_host = format!("{}:{}", &config.ftp_host, "21");

            // Create a connection to an FTP server and authenticate to it.
            let mut ftp_stream = FtpStream::connect(&ftp_host).unwrap();
            let _ = ftp_stream.login(&config.ftp_user, &config.ftp_passwd).unwrap();

            // Get the current directory that the client will be reading from and writing to.
            println!("Current directory: {}", ftp_stream.pwd().unwrap());

            // Change into a new directory, relative to the one we are currently in.
            println!("Changing directory to: {}", &batch_config.ftp_path);
            let _ = ftp_stream.cwd(&batch_config.ftp_path).unwrap();

            // Get the current directory that the client will be reading from and writing to.
            println!("Current directory: {}", ftp_stream.pwd().unwrap());

            // Get listing: None is the working dir
            let list = ftp_stream.list(None).unwrap();
            for line in list {
                println!("{}", line)
            }

            //~ // Retrieve (GET) a file from the FTP server in the current working directory.
            //~ let remote_file = ftp_stream.simple_retr("ftpext-charter.txt").unwrap();
            //~ println!("Read file with contents\n{}\n", str::from_utf8(&remote_file.into_inner()).unwrap());

            //~ // Store (PUT) a file from the client to the current working directory of the server.
            //~ let mut reader = Cursor::new("Hello from the Rust \"ftp\" crate!".as_bytes());
            //~ let _ = ftp_stream.put("greeting.txt", &mut reader);
            //~ println!("Successfully wrote greeting.txt");

            // Terminate the connection to the server.
            let _ = ftp_stream.quit();

        }

        SubCommand::Start { batch_id, local_id } => {
            println!("Starting batch {}...\n", &batch_id);
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
            let batch_row = client.query_one("SELECT * FROM batchin_batches where batch_id = $1;", &[&batch_id])?;

            log::trace!("Found row: {:#?}", batch_row);
            let batch = Batch::from_postgres_row(&batch_row);
            log::debug!("Found batch: {:#?}", batch);

            let rows = match local_id {
                None => {
                    let rows = client.query("SELECT * FROM batchin_batch_records WHERE batch_row_id = $1;", &[&batch.row_id])?;
                    log::info!("Found batch: '{}' with {} records", &batch.batch_id, rows.len());
                    rows
                },
                Some(local_id) => {
                    let rows = client.query("SELECT * FROM batchin_batch_records
                    WHERE batch_row_id = $1 AND dc_identifier_localid = $2;", &[&batch.row_id, &local_id])?;
                    log::info!("Found batch: '{}' with {} records for local_id '{}'", &batch.batch_id, rows.len(), &local_id);
                    rows
                }
            };

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

        }

    }

    Ok(())
}
