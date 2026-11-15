//! The commands behind each subcommand.

use std::io::{self, Write};

use fl_client::{Batch, Client, FetchOptions, NodeStatus, StartAt};
use fl_types::{Offset, Record, MAX_BATCH_RECORDS};

use crate::args::{Command, Start};

/// The error type for a failed command.
pub type CliError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Lines of input for `produce`, normally standard input.
pub type Lines<'a> = &'a mut dyn Iterator<Item = io::Result<String>>;

/// Runs one command against `client`, reading from `input` and printing to
/// `out`.
pub async fn run(
    command: Command,
    client: &Client,
    input: Lines<'_>,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    match command {
        Command::Topics => topics(client, out).await,
        Command::CreateTopic { name } => create_topic(client, &name, out).await,
        Command::Produce {
            topic,
            values,
            key,
            batch,
        } => produce(client, &topic, values, key, batch, input, out).await,
        Command::Consume {
            topic,
            from,
            follow,
            max,
        } => consume(client, &topic, from, follow, max, out).await,
        Command::Status => status(client, out).await,
    }
}

async fn topics(client: &Client, out: &mut dyn Write) -> Result<(), CliError> {
    let topics = client.list_topics().await?;
    if topics.is_empty() {
        writeln!(out, "no topics")?;
        return Ok(());
    }
    let rows = topics
        .into_iter()
        .map(|topic| {
            vec![
                topic.id.get().to_string(),
                topic.name,
                topic.high_watermark.get().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    table(out, &["ID", "NAME", "RECORDS"], &rows)?;
    Ok(())
}

async fn create_topic(client: &Client, name: &str, out: &mut dyn Write) -> Result<(), CliError> {
    let created = client.create_topic(name).await?;
    let id = created.id.get();
    if created.created {
        writeln!(out, "created topic {name} (id {id})")?;
    } else {
        writeln!(out, "topic {name} already exists (id {id})")?;
    }
    Ok(())
}

async fn produce(
    client: &Client,
    topic: &str,
    values: Vec<String>,
    key: Option<String>,
    batch: usize,
    input: Lines<'_>,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let batch = batch.clamp(1, MAX_BATCH_RECORDS);
    let record = |value: String| {
        let record = Record::new(value);
        match &key {
            Some(key) => record.with_key(key.as_str()),
            None => record,
        }
    };

    // Arguments if there are any, otherwise lines of input.
    let mut from_args = values.into_iter().map(Ok);
    let source: Lines<'_> = if from_args.len() > 0 {
        &mut from_args
    } else {
        input
    };

    let mut first: Option<Offset> = None;
    let mut written = 0u64;
    let mut pending = Vec::with_capacity(batch);
    loop {
        let line = source.next().transpose()?;
        let finished = line.is_none();
        if let Some(value) = line.filter(|value| !value.is_empty()) {
            pending.push(record(value));
        }
        if pending.len() == batch || (finished && !pending.is_empty()) {
            let produced = client.produce(topic, std::mem::take(&mut pending)).await?;
            first.get_or_insert(produced.base_offset);
            written += u64::from(produced.count);
        }
        if finished {
            break;
        }
    }

    match first {
        None => Err("nothing to write: no records were given".into()),
        Some(first) => {
            let noun = if written == 1 { "record" } else { "records" };
            let last = first.advance(written - 1);
            writeln!(
                out,
                "wrote {written} {noun} to {topic}, offsets {} to {}",
                first.get(),
                last.get()
            )?;
            Ok(())
        }
    }
}

async fn consume(
    client: &Client,
    topic: &str,
    from: Start,
    follow: bool,
    max: Option<u64>,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    let mut remaining = max.unwrap_or(u64::MAX);
    if remaining == 0 {
        return Ok(());
    }

    if follow {
        let start = match from {
            Start::Earliest => StartAt::Earliest,
            Start::Latest => StartAt::Latest,
            Start::Offset(offset) => StartAt::Offset(Offset::new(offset)),
        };
        let mut subscription = client.subscribe(topic, start).await?;
        while remaining > 0 {
            let batch = subscription.next().await?;
            remaining -= print_records(out, &batch, remaining)?;
            // Someone is watching, so do not sit on buffered output.
            out.flush()?;
        }
        return Ok(());
    }

    // Without --follow: read up to the end as it stands, then stop.
    let options = FetchOptions::default();
    let mut offset = match from {
        Start::Earliest => Offset::ZERO,
        Start::Offset(offset) => Offset::new(offset),
        // A fetch far past the end returns no records and the watermark.
        Start::Latest => {
            client
                .fetch(topic, Offset::new(u64::MAX), options)
                .await?
                .high_watermark
        }
    };
    while remaining > 0 {
        let batch = client.fetch(topic, offset, options).await?;
        let Some(next) = batch.next_offset() else {
            break;
        };
        remaining -= print_records(out, &batch, remaining)?;
        offset = next;
    }
    Ok(())
}

/// Prints up to `limit` records, one per line, as offset, key, and value
/// separated by tabs. Returns how many were printed.
fn print_records(out: &mut dyn Write, batch: &Batch, limit: u64) -> io::Result<u64> {
    let mut printed = 0;
    for consumed in batch
        .records
        .iter()
        .take(usize::try_from(limit).unwrap_or(usize::MAX))
    {
        let key = match &consumed.record.key {
            Some(key) => String::from_utf8_lossy(key),
            None => "-".into(),
        };
        let value = String::from_utf8_lossy(&consumed.record.value);
        writeln!(out, "{}\t{key}\t{value}", consumed.offset.get())?;
        printed += 1;
    }
    Ok(printed)
}

async fn status(client: &Client, out: &mut dyn Write) -> Result<(), CliError> {
    let mut rows = Vec::new();
    let mut failures = Vec::new();
    for endpoint in client.endpoints() {
        match client.status_of(&endpoint).await {
            Ok(status) => rows.push(status_row(&status)),
            Err(e) => failures.push(format!("{endpoint}: unreachable: {e}")),
        }
    }

    if !rows.is_empty() {
        let header = [
            "ENDPOINT", "NODE", "ROLE", "TERM", "LEADER", "LAST", "COMMIT", "APPLIED", "SEGMENTS",
            "SIZE",
        ];
        table(out, &header, &rows)?;
    }
    for failure in &failures {
        writeln!(out, "{failure}")?;
    }
    if rows.is_empty() {
        return Err("no node answered".into());
    }
    Ok(())
}

fn status_row(status: &NodeStatus) -> Vec<String> {
    let leader = match status.leader_id {
        Some(id) => id.get().to_string(),
        None => "-".into(),
    };
    vec![
        status.endpoint.clone(),
        status.node_id.get().to_string(),
        status.role.to_string(),
        status.term.get().to_string(),
        leader,
        status.last_log.index.get().to_string(),
        status.commit_index.get().to_string(),
        status.applied_index.get().to_string(),
        status.segment_count.to_string(),
        human_bytes(status.log_bytes),
    ]
}

/// Prints rows under a header with every column padded to its widest cell.
fn table(out: &mut dyn Write, header: &[&str], rows: &[Vec<String>]) -> io::Result<()> {
    let mut widths: Vec<usize> = header.iter().map(|cell| cell.len()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.len());
        }
    }
    let mut print = |cells: &mut dyn Iterator<Item = &str>| -> io::Result<()> {
        let mut line = String::new();
        for (cell, width) in cells.zip(&widths) {
            line.push_str(&format!("{cell:<width$}  "));
        }
        writeln!(out, "{}", line.trim_end())
    };
    print(&mut header.iter().copied())?;
    for row in rows {
        print(&mut row.iter().map(String::as_str))?;
    }
    Ok(())
}

/// Formats a byte count with a binary unit, such as `1.5 MiB`.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_counts_are_readable() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(64 << 20), "64.0 MiB");
        assert_eq!(human_bytes(3 << 30), "3.0 GiB");
        assert_eq!(human_bytes(u64::MAX), "16777216.0 TiB");
    }

    #[test]
    fn table_columns_line_up() {
        let rows = vec![
            vec!["1".to_owned(), "quotes".to_owned(), "12".to_owned()],
            vec!["10".to_owned(), "ust.trades".to_owned(), "3".to_owned()],
        ];
        let mut out = Vec::new();
        table(&mut out, &["ID", "NAME", "RECORDS"], &rows).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "ID  NAME        RECORDS\n\
             1   quotes      12\n\
             10  ust.trades  3\n"
        );
    }
}
