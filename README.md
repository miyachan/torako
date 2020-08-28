# Torako

> Torako: A 4chan imageboard scraper

Torako is an imageboard scraper, designed to support multiple backends, with support for an [Asagi](https://github.com/eksopl/asagi/) style backend as a drop-in replacement for it.

Torako is written in Rust with with async-features using the Tokio reactor. The async design allows scraping multiple boards with minimal overhead and still remain performant.

Report any issues or features requests using GitHub Issues.

## Getting Started

Builds are generated for Windows, Linux and macOS and are available on the GitHub releases page. Once downloaded, grab and modify the sample configuration and place it in the same directory as `Torako.toml` (You can also use the `-c` flag to specify a custom configuration path).

```sh
$ ./torako -c ./Torako.toml
```

## Configuration

The `Torako.sample.toml` sample file can be used as a base to start running Torako. The sample configuration file also documents all the various options that can be used to configure and customize Torako. In order to start Torako you must provide at least one board to scape and configure a storage backend.

## Building

Building Torako only requires Rust v1.46.0+. You can install Rust with [rustup](https://rustup.rs/). Simply clone the repo and run:

```sh
$ cargo build --release
```
> *Note*: On certain platforms you may need to ensure you have C/C++ compiler, `pkg-config` and libssl-dev installed to build the openssl bindings.

## Asagi Compatibility

Torako is designed to be a drop-in replacement for Asagi, so any deviation from Asagi's outputs should be considered a bug. However, Torako does not communicate with MySQL the same way Torako does, and Torako tries to be more performant. Torako's default configuration should be compatiable with an existing deployment with no changes.

### MySQL Triggers

Traditionally, Asagi uses MySQL triggers to keep tables like `board_images` and `board_daily` in sync. Triggers can work well, but when mass inserting data, triggers may not be the most optimal solution. Instead Torako has an option to compute the required database changes internally for a set of posts and make one batch update to every table.

Enabling this mode requires setting `use_triggers = false` in the configuration.

> **WARNING**: Enabling this option will drop all triggers on your database (except the delete triggers) when Torako starts. You can recreate them by restarting Torako with `use_triggers = true`.

### Stats

By default, the stats table are not computed, unless you already have the proper triggers in place. If Torako is running with `use_triggers = false`, then `compute_stats = true` can also be set to generate the stats for the `board_daily` and `board_users` table.

## Architecture

At a high level, Torako organizes data flow from `BoardStream`s which are implemented as a `Stream<Vec<Post>>` to storage sinks which are are `Sink`s of the same type. Using these base types, it's natural to introduce backpressure in the system that can termporarily pause archiving if the backlog is too great or if memory usage is too high.

### BoardStream

`BoardStream` is a `futures::stream::Stream` that will output all new posts. A post may get restransmitted if the content of the post is edited or if the post is deleted (resent posts will be sent with the `is_retransmission` flag).

### Storage Backend

#### Asagi

The Asagi backend is designed to deliver the same outputs as the [Asagi](https://github.com/eksopl/asagi/) imageboard scraper. Using the configuration it is possible to tune the concurrency parameters to optomize for throughput and memory usage. In general, allowing more requests in flight will increase memory usage as the Torako will keep those posts buffered in memory before they are finally flushed to the database. The storage backend is also designed to keep MySQL reads at a minimum and optimizations are in place to reduce superflous reads.

## API

Torako includes an API endpoint (`http://127.0.0.1:2377` by default) for interacting
and inspecting Torako.

### API Documentation

#### `GET /`
> Content-Type: application/json

Returns a JSON object with metrics information for every board and all storage backends.

## Performance

Setup:

* torako: GCP e2-standard-4 (4 vCPUs, 16 GB memory)
* MySQL 5.7 (empty):  db-n1-standard-2 (2 vCPUs, 7.5 GB memory)
* `use_triggers = false`
* `compute_stats = true`
* `concurrent_downloads = 1024`
* `inflight_posts = unlimited`
* `media_backpressure = false`
* Boards: All except /f/, media downloads disabled for /wsg/ and /gif/

When Torako first starts, it will attempt to download all posts and any missing images. Downloading all posts is relatively fast, and most of the CPU usage then goes to downloading media until it is fully caught up. It took approximately 25 minutes to download all the content from every board on a gigabit connection.

Resource information:
* **CPU**: Torako will use nearly all cores if there is a large backlog of media to download
* **Memory**: Memory usage is tunable and depends on your settings for `concurrent_downloads` and `inflight_posts`, and `media_backpressure`. In this test Torako spikes up to 3 GiB of memory, slowly settled to 500MiB of usage as the backlog cleared
* **Network**: The instance's bandwidth was completely maxed out at an average of 128MiB/s
