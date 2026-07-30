======== NEG-22 end-to-end bus bench (soak) ========
machine       : Mac OS X aarch64, 12 cores, java 21.0.9
heap max      : 2,048 MB
redis         : 7.4.9
started       : 2026-07-29T19:02:39.232853Z
instruments   : 20 (40 streams, BENCH-01.ITEST ..)
rate          : 10,000 events/s aggregate (80% quote / 20% trade)
groups        : 3 (bench-strategy-1..3)
publishers    : 1
warmup        : PT30S (unmeasured)
duration      : PT30M (18,000,000 scheduled events)
memory sampling: on (15 s)
target        : >= 10,000 events/s sustained, per-group p99 < 50 ms
=================================================
-------- generator --------
scheduled     : 18,000,000 events over PT30M
published     : 18,000,000
failed        : 0
wall          : 1800.001 s
throughput    : 10,000 events/s achieved (100.00% of target)
=================================================
-------- consumer groups --------
group         : bench-strategy-1
  delivered   : 18,000,000
  redeliveries: 0
  throughput  : 10,000 events/s
  latency     : p50 1 ms, p90 2 ms, p99 6 ms, max 662 ms
  lag         : max 2,002, final 0
group         : bench-strategy-2
  delivered   : 18,000,000
  redeliveries: 0
  throughput  : 10,000 events/s
  latency     : p50 1 ms, p90 2 ms, p99 6 ms, max 641 ms
  lag         : max 189, final 0
group         : bench-strategy-3
  delivered   : 18,000,000
  redeliveries: 0
  throughput  : 10,000 events/s
  latency     : p50 1 ms, p90 2 ms, p99 6 ms, max 664 ms
  lag         : max 946, final 0
=================================================
-------- memory (soak) --------
sampled       : every 15 s; the first third is the fill phase and is excluded from the verdict
redis used_memory (120 samples)
  first third : min 107 MB, median 1,114 MB, max 1,275 MB (40 samples)
  middle third: min 1,110 MB, median 1,167 MB, max 1,273 MB (40 samples)
  final third : min 1,106 MB, median 1,163 MB, max 1,269 MB (40 samples)
  verdict     : FLAT (final median 0.997x middle median, allowance 1.100x)
jvm heap after gc (120 samples)
  first third : min 430 MB, median 461 MB, max 494 MB (40 samples)
  middle third: min 492 MB, median 556 MB, max 558 MB (40 samples)
  final third : min 555 MB, median 557 MB, max 685 MB (40 samples)
  verdict     : FLAT (final median 1.002x middle median, allowance 1.100x)
stream retention
  md.tick.quote.* : window PT5M, expected ~120,000 entries, observed min 130,641 / median 130,641 / max 130,641 over 20 streams
  md.tick.trade.* : window PT5M, expected ~30,000 entries, observed min 32,651 / median 32,656 / max 32,661 over 20 streams
=================================================
-------- verdict --------
result        : PASS
=================================================
