## Prerequisites

*   Rust toolchain (Cargo)
*   `mkcert` for generating local development certificates. ([Installation Guide](https://github.com/FiloSottile/mkcert#installation))
*   `just` command runner ([Installation Guide](https://github.com/casey/just#installation))

## Setup

1.  **Install `mkcert` and `just`** if you haven't already (see links above).
2.  **Generate Certificates:** Run the following command in the project root to generate the necessary certificates in the `cert/` directory:
    ```bash
    just cert
    ```
    This command uses `mkcert` to create and install a local Certificate Authority (CA) if needed, and then generates `cert.pem`, `key.pem`, `cert.der`, and `key.der`.

## Running Examples

This repository uses `just` for convenient command execution. You can list all available commands with `just --list`.

The examples are located in the `wt-rs/examples` directory.

### Echo Server & Client

1.  **Start the Echo Server:**
    ```bash
    just run echo-server
    ```
    This will start the WebTransport echo server, listening on the address specified in the example code (likely localhost).

2.  **Run the Echo Client:**
    In a separate terminal, run:
    ```bash
    just run echo-client
    ```
    This will connect to the echo server, send a message, and print the echoed response.

## Development

*   **Check Code:**
    ```bash
    just check
    ```
    This runs `cargo clippy` and `cargo check`.
