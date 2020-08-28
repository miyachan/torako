# Torako

> Torako: An 4chan imageboard scraper

Torako is imageboard scraper, designed to support multiple backends, but currently
only supports [Asagi](https://github.com/eksopl/asagi/) as a drop-in replacement.

Torako is written Rust with Tokio. The async design allows scraping multiple
boards with minimal overhead and still remain performant.

Report any issues or requests using GitHub Issues.

*Requires `Rust 1.46.0`*

## Getting Started

Builds are generated for Windows, Linux and macOS and are available on the
GitHub releases page. Once downloaded, grab and modify the sample configuration
and place it in the same directory as `Torako.toml` (You can also use the
`-c` flag to specify a custom configuration path).

```sh
$ ./torako -c ./Torako.toml
```

## Configuration

The `Torako.sample.toml` sample file is annotated to include the various
configuration options. The only required configuration parameters are providing
MySQL credentials.

## Building

Building Torako only requires Rust v1.46.0. You can install Rust with [rustup](https://rustup.rs/). Simply clone the repo and run:

```sh
$ cargo build --release
```

## Asagi Compatibility

Torako is designed to be a drop-in replacement for Asagi, so any deviation from
Asagi's outputs should be considered a bug. However, Torako does not communicate
with MySQL the same way Torako does, and Torako tries to be more performant.

## API Server

Torako supports a very basic http endpoint (port 2377 by default) that will show
metrics about the amount of posts downloaded and uptime in JSON format.

## Performance

TODO
