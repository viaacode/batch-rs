# batch-rs

Simple CLI-tool for batch-intake: list batches, start a batch, etc.

## For users

1. Download the latest version for your architecture from "[Releases](https://github.com/viaacode/batch-rs/releases)".
2. Fill in the required values in the `.env` file (example in `.env.example`) and export the variables:

  ```bash
  $ export $(grep -v '^#' .env | xargs)
  ```

3. Check out the possibilities with:

  ```bash
  $ batch-rs -h
  ```

  ```
  batch-rs 0.1.0

  USAGE:
      batch-rs <SUBCOMMAND>

  OPTIONS:
      -h, --help       Print help information
      -V, --version    Print version information

  SUBCOMMANDS:
      help         Print this message or the help of the given subcommand(s)
      list         List all batches in the database
      start        Start a batch. This will send out a so called "watchfolder-message" for every
                       pair in the batch. If an - optional - local_id is provided, then only this item
                       will be started
      transform    TODO: Transform metadata for a batch
      upload       TODO: Upload a batch. This adds the batch and it's records to the database and
                       uploads the sidecars to the FTP-server
      vars         TODO: Display or set the batch-variables
  ```

### Help

Help for any of the subcommands can be invoked as such: `$ batch-rs help <SUBCOMMAND>`. For instance:

```bash
$ batch-rs help start
```

Output:

```
batch-rs-start
Start a batch. This will send out a so called "watchfolder-message" for every pair in the batch. If
an - optional - local_id is provided, then only this item will be started

USAGE:
    batch-rs start [OPTIONS] <BATCH_ID>

ARGS:
    <BATCH_ID>    Batch ID (formerly "name"): eg, 'QAS-BD-OR-123abc-2022-01-01-00-00-00-000'
                  There should be one and only one batch found via its ID

OPTIONS:
    -h, --help                   Print help information
    -l, --local-id <LOCAL_ID>    Local ID: provide a local ID of the item that should be ingested

```

### Subcommands

#### List batches

```bash
$ batch-rs list
```

#### Start batches

```bash
$ batch-rs start [OPTIONS] <BATCH_ID>
```

## For developers

### Prerequisites

- Rust toolchain: see [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).
- Cargo (should be installed along with the Rust toolchain)

### Build

- Clone this repository.
- Build with `cargo build`.
