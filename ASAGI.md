# Asagi Modifications

This document notes any changes to the basic Asagi install that were done. See [https://archive.wakarimasen.co/_/articles/about/](https://archive.wakarimasen.co/_/articles/about/).

## Schema Modifications

### New Columns

The original Asagi implementation had a `timestamp` column that was the seconds since UNIX Epoch, but did the calculation from the timezone America/New_York. Torako continues this, but this timestamp has issues where around DST clock changes you lose proper ordering of the posts. Torako, by default on creation, and if it detects a `unix_timestamp` field (of type `TIMESTAMP`) will also write the time of the post in seconds after UNIX Epoch in UTC.

### Dropped Indexes

It's safe to drop some indexes; having a lot of indexes can cause memory and disk usage to blow up, and slow down inserts. They are a waste if they are never used. FoolFuuka doesn't seem to use these indexes (or in some other cases the indexes are worthless because other WHERE conditions make the result set relatively small, or the seatch is better served by the search index). The size column represents a table with more than 100M rows.

| Table | Index Name | Size (MiB) |
| -- | -- | -- |
| %%BOARD%% | subnum_index | 1462.00 |
| %%BOARD%% | op_index | 1015.00 |
| %%BOARD%% | media_hash_index | 2770.69 |
| %%BOARD%% | media_orig_index | 2379.94 |
| %%BOARD%% | name_trip_index | 2512.94 |
| %%BOARD%% | trip_index | 1309.91 |
| %%BOARD%% | email_index | 1208.00 |
| %%BOARD%% | timestamp_index | 2821.95 |