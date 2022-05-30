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
    pub droid_id: i32,
    pub name: String,
    pub description: String,
    pub cp_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub host: String,
    pub path: String,
}

impl FromPostgresRow for Batch {
    fn from_postgres_row(row: &Row) -> Self {
        let default = "default".to_string();
        Self {
            row_id: row.get("row_id"),
            droid_id: row.get("droid_id"),
            cp_id: row.get("cp_id"),
            name: row.get("name"),
            description: row.try_get("description").unwrap_or(default.clone()),
            status: row.try_get("status").unwrap_or(default.clone()),
            created_at: row.get("created_at"), // Utc::now(), // TODO
            host: row.try_get("host").unwrap_or(default.clone()),
            path: row.try_get("path").unwrap_or(default.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DroidRecord {
    pub row_id: i32,
    pub droid_id: i32,
    pub id: String,
    pub parent_id: String,
    pub uri: String,
    pub file_path: String,
    pub file_name: String,
    pub method: String,
    pub status: String,
    pub size: i64,
    #[serde(rename = "type")]
    pub type_field: String,
    pub ext: String,
    pub last_modified: NaiveDate,
    pub extension_mismatch: String,
    pub md5_hash: String,
    pub format_count: i32,
    pub puid: String,
    pub mime_type: String,
    pub format_name: String,
    pub format_version: String,
}

impl FromPostgresRow for DroidRecord {
    fn from_postgres_row(row: &Row) -> Self {
        Self {
            row_id: row.get("row_id"),
            droid_id: row.get("droid_id"),
            id: row.get("id"),
            parent_id: row.get("parent_id"),
            uri: row.get("uri"),
            file_path: row.get("file_path"),
            file_name: row.get("file_name"),
            method: row.get("method"),
            status: row.get("status"),
            size: row.get("size"),
            type_field: row.get("type"),
            ext: row.get("ext"),
            last_modified: row.get("last_modified"),
            extension_mismatch: row.get("extension_mismatch"),
            md5_hash: row.get("md5_hash"),
            format_count: row.get("format_count"),
            puid: row.get("puid"),
            mime_type: row.get("mime_type"),
            format_name: row.get("format_name"),
            format_version: row.get("format_version"),
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
    pub fn new(batch: &Batch, droid_record: &DroidRecord) -> Self {
        let essence_file_name = droid_record.file_name.to_string();
        Self {
            cp_name: batch.cp_id.to_string(),
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
                    md5: droid_record.md5_hash.to_string(),
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
