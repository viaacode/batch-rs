use std::str;
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};
use serde_json;
use chrono::{NaiveDate, DateTime, Utc};
use postgres::row::Row;


#[derive(Deserialize, Debug)]
pub struct Config {
    // Postgres
    #[serde(default="default_user_pass")]
    pub postgres_user: String,
    #[serde(default="default_user_pass")]
    pub postgres_passwd: String,
    #[serde(default="default_host")]
    pub postgres_host: String,
    #[serde(default="default_database")]
    pub postgres_database: String,
    // AMQP
    #[serde(default="default_user_pass")]
    pub amqp_user: String,
    #[serde(default="default_user_pass")]
    pub amqp_passwd: String,
    #[serde(default="default_host")]
    pub amqp_host: String,
    #[serde(default="default_port")]
    pub amqp_port: String,
    #[serde(default="default_empty")]
    pub amqp_vhost: String,
    #[serde(default="default_empty")]
    pub amqp_out_queue: String,
    #[serde(default="default_amqp_prefetch_count")]
    pub amqp_prefetch_count: u16,
}

fn default_user_pass() -> String  {
  String::from("admin")
}

fn default_host() -> String  {
  String::from("localhost")
}

fn default_port() -> String  {
  String::from("5672")
}

fn default_database() -> String  {
  String::from("postgres")
}

fn default_empty() -> String  {
  String::from("")
}

fn default_amqp_prefetch_count() -> u16  {
  100
}

// These 2 conn string fn's can become methods on their respective configs
pub fn format_postgres_connection_string(config: &Config) -> String {
    format!("postgresql://{}:{}@{}/{}",
        config.postgres_user,
        config.postgres_passwd,
        config.postgres_host,
        config.postgres_database
    )
}

pub fn format_amqp_connection_string(config: &Config) -> String {
    format!("amqp://{}:{}@{}:{}/{}",
        config.amqp_user,
        config.amqp_passwd,
        config.amqp_host,
        config.amqp_port,
        config.amqp_vhost)
}

pub trait FromPostgresRow {
    fn from_postgres_row(row: &Row) -> Self;
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Batch {
    pub row_id: i32,
    pub batch_id: String,
    pub description: String,
    pub cp_id: String,
    pub status: String,
    pub host: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub last_modified_at: DateTime<Utc>,
}

impl FromPostgresRow for Batch {
    fn from_postgres_row(row: &Row) -> Self {
        let default = "default".to_string();
        Self {
            row_id: row.get("row_id"),
            batch_id: row.get("batch_id"),
            description: row.try_get("description").unwrap_or(default.clone()),
            cp_id: row.get("cp_id"),
            status: row.try_get("status").unwrap_or(default.clone()),
            host: row.try_get("host").unwrap_or(default.clone()),
            path: row.try_get("path").unwrap_or(default.clone()),
            created_at: row.get("created_at"), // Utc::now(), // TODO
            last_modified_at: row.get("created_at"), // Utc::now(), // TODO
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchRecord {
    pub row_id: i32,
    pub batch_row_id: i32,
    pub dc_identifier_localid: String,
    pub dc_title: String,
    pub filename: String,
    pub filesize: i64,
    pub md5_hash: String,
    //~ #[serde(skip)]          // Skip serde for XML-field for now until we know how to serde this Postgres-type
    //~ pub xml: String,
    pub created_at: DateTime<Utc>,
    pub last_modified_at: DateTime<Utc>,
}

impl FromPostgresRow for BatchRecord {
    fn from_postgres_row(row: &Row) -> Self {
        Self {
            row_id: row.get("row_id"),
            batch_row_id: row.get("batch_row_id"),
            dc_identifier_localid: row.get("dc_identifier_localid"),
            dc_title: row.get("dc_title"),
            filename: row.get("filename"),
            filesize: row.get("filesize"),
            md5_hash: row.get("md5_hash"),
            //~ xml: row.get("xml"),
            created_at: row.get("created_at"),
            last_modified_at: row.get("last_modified_at"),
        }
    }
}

// See: https://hermanradtke.com/2015/05/03/string-vs-str-in-rust-functions.html
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WatchfolderMsg {
    pub cp_name: String,
    pub flow_id: String,
    pub server: String,
    pub username: String,
    pub password: String,
    pub timestamp: DateTime<Utc>,
    pub sip_package: Vec<SipPackage>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SipPackage {
    pub file_name: String,
    pub file_path: String,
    pub file_type: String,
    pub md5: String,
    pub timestamp: DateTime<Utc>,
}

// TODO:
//fn filename_ext_to_xml<S>(filename: S) -> Option<String>
//where S: AsRef<Path>
fn filename_ext_to_xml(filename: String) -> Option<String>
{
    if filename == "" {
        None
    } else {
        let path = Path::new(&filename);
        let file_stem = path.file_stem();
        let mut path2 = PathBuf::from(file_stem.unwrap().to_os_string());
        path2.set_extension("xml");
        Some(path2.into_os_string().into_string().unwrap())
    }
}

impl WatchfolderMsg {
    pub fn new(batch: &Batch, batch_record: &BatchRecord) -> Self {
        let essence_file_name = batch_record.filename.to_string();
        Self {
            cp_name: "CP_NAME:TODO".to_string(), // TODO
            flow_id: batch.cp_id.to_string(),
            server: batch.host.to_string(),
            username: "".to_string(),
            password: "".to_string(),
            timestamp: Utc::now(),  // TODO
            sip_package: vec![
                SipPackage {
                    file_name: essence_file_name.to_string(),
                    file_path: batch.path.to_string(),
                    file_type: "essence".to_string(),
                    md5: batch_record.md5_hash.to_string(),
                    timestamp: Utc::now(),  // TODO
                },
                SipPackage {
                    file_name: filename_ext_to_xml(essence_file_name).unwrap(),
                    file_path: batch.path.to_string(),
                    file_type: "sidecar".to_string(),
                    md5: "".to_string(),
                    timestamp: Utc::now(),  // TODO
                },
            ]
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}
