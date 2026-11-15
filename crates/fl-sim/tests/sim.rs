//! Whole runs of the simulator.

use fl_sim::{run, Bug, Report, SimConfig, Violation, ViolationKind};

/// Runs a range of seeds and fails on the first violation.
fn all_pass(seeds: std::ops::Range<u64>, make: impl Fn(u64) -> SimConfig) -> Vec<Report> {
    seeds
        .map(|seed| run(make(seed)).unwrap_or_else(|violation| panic!("{violation}")))
        .collect()
}

/// Runs a range of seeds and returns the violations found.
fn violations(seeds: std::ops::Range<u64>, make: impl Fn(u64) -> SimConfig) -> Vec<Violation> {
    seeds.filter_map(|seed| run(make(seed)).err()).collect()
}

#[test]
fn a_calm_cluster_commits_what_it_accepts() {
    for report in all_pass(0..10, SimConfig::calm) {
        // One election at the start and none after it.
        assert_eq!(report.leaders_elected, 1, "{report:?}");
        assert_eq!(report.crashes, 0);
        assert_eq!(report.messages_lost, 0);
        // Every proposal the leader accepted was committed and
        // acknowledged. The committed log also holds the leader's no-op.
        assert!(report.accepted > 100, "{report:?}");
        assert_eq!(report.acknowledged, report.accepted, "{report:?}");
        assert_eq!(report.committed, report.accepted + 1, "{report:?}");
    }
}

#[test]
fn the_same_seed_gives_the_same_run() {
    let first = run(SimConfig::new(42)).unwrap();
    let second = run(SimConfig::new(42)).unwrap();
    assert_eq!(first, second);

    let other = run(SimConfig::new(43)).unwrap();
    assert_ne!(first.fingerprint, other.fingerprint);

    // Tracing only records. It must not change what happens.
    let mut traced = SimConfig::new(42);
    traced.trace = true;
    assert_eq!(run(traced).unwrap(), first);
}

#[test]
fn three_nodes_survive_moderate_faults() {
    let reports = all_pass(0..150, SimConfig::new);
    // The faults really happened, and work still got done.
    assert!(reports.iter().map(|r| r.crashes).sum::<u64>() > 500);
    assert!(reports.iter().map(|r| r.partitions).sum::<u64>() > 500);
    assert!(reports.iter().map(|r| r.messages_duplicated).sum::<u64>() > 500);
    assert!(reports.iter().all(|r| r.leaders_elected >= 1));
    assert!(reports.iter().map(|r| r.acknowledged).sum::<u64>() > 10_000);
}

#[test]
fn three_nodes_survive_harsh_faults() {
    let reports = all_pass(0..150, SimConfig::harsh);
    assert!(reports.iter().map(|r| r.crashes).sum::<u64>() > 5_000);
    assert!(reports.iter().map(|r| r.max_term).max().unwrap() > 50);
}

#[test]
fn five_nodes_survive_faults() {
    all_pass(0..40, |seed| SimConfig::new(seed).with_nodes(5));
    all_pass(0..40, |seed| SimConfig::harsh(seed).with_nodes(5));
}

#[test]
fn odd_cluster_sizes_work() {
    // One node is its own majority. Two nodes need both.
    all_pass(0..20, |seed| SimConfig::new(seed).with_nodes(1));
    all_pass(0..20, |seed| SimConfig::harsh(seed).with_nodes(2));
    all_pass(0..10, |seed| SimConfig::harsh(seed).with_nodes(7));
}

#[test]
fn a_run_can_be_shortened_or_lengthened() {
    let mut short = SimConfig::new(5);
    short.ticks = 200;
    let mut long = SimConfig::new(5);
    long.ticks = 6_000;
    let short = run(short).unwrap();
    let long = run(long).unwrap();
    assert!(long.committed > short.committed);
    assert!(long.ticks > short.ticks);
}

#[test]
fn skipping_the_log_sync_is_caught() {
    let found = violations(0..20, |seed| {
        SimConfig::new(seed).with_bug(Bug::SkipLogSync)
    });
    assert!(
        found.len() >= 15,
        "only {} of 20 seeds caught it",
        found.len()
    );
    // A node that acknowledged entries and then lost them lets a leader
    // without those entries win, or leaves a hole under a commit index.
    for violation in &found {
        assert!(
            matches!(
                violation.kind,
                ViolationKind::CommittedEntryChanged
                    | ViolationKind::LeaderMissingCommitted
                    | ViolationKind::AckedButLost
                    | ViolationKind::LogMismatch
                    | ViolationKind::NodePanicked
            ),
            "{violation}"
        );
        assert!(violation.tick > 0);
    }
}

#[test]
fn forgetting_the_vote_is_caught() {
    let found = violations(0..60, |seed| {
        SimConfig::harsh(seed).with_bug(Bug::SkipHardStateWrite)
    });
    assert!(!found.is_empty(), "no seed caught the forgotten vote");
}

#[test]
fn a_violation_carries_its_seed_and_trace() {
    let mut config = SimConfig::new(3).with_bug(Bug::SkipLogSync);
    config.trace = true;
    let violation = run(config).unwrap_err();

    assert_eq!(violation.seed, 3);
    assert!(!violation.trace.is_empty());
    assert!(violation.trace.len() <= 400);
    assert!(violation.to_string().contains("seed 3"));
}

#[test]
#[should_panic(expected = "drop_rate must be between 0 and 1")]
fn a_nonsense_rate_is_rejected() {
    let mut config = SimConfig::new(0);
    config.drop_rate = 1.5;
    let _ = run(config);
}
