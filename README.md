# batch-rs

Simple CLI-tool to start batch-intake.

## Prerequisites

- Rust toolchain: see [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install).
- Cargo (should be installed along with the Rust toolchain)

## Usage

- Clone this repository.
- Fill in and source an `.env` file (Example in `.env.example`):
  ```bash
  $ export $(cat .env | xargs)
  ```
- Run with `cargo run`.
