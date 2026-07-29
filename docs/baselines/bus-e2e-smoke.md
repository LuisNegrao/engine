======== NEG-22 end-to-end bus bench (smoke) ========
machine       : Mac OS X aarch64, 12 cores, java 21.0.9
heap max      : 2,048 MB
redis         : 7.4.9
started       : 2026-07-29T15:16:28.198395Z
instruments   : 20 (40 streams, BENCH-01.ITEST ..)
rate          : 10,000 events/s aggregate (80% quote / 20% trade)
groups        : 3 (bench-strategy-1..3)
publishers    : 1
warmup        : PT30S (unmeasured)
duration      : PT2M (1,200,000 scheduled events)
memory sampling: off
target        : >= 10,000 events/s sustained, per-group p99 < 50 ms
=================================================
-------- generator --------
scheduled     : 1,200,000 events over PT2M
published     : 1,200,000
failed        : 0
wall          : 120.000 s
throughput    : 10,000 events/s achieved (100.00% of target)
=================================================
-------- consumer groups --------
group         : bench-strategy-1
  delivered   : 1,200,000
  redeliveries: 0
  throughput  : 9,995 events/s
  latency     : p50 1 ms, p90 2 ms, p99 5 ms, max 39 ms
  lag         : max 90, final 0
group         : bench-strategy-2
  delivered   : 1,200,000
  redeliveries: 0
  throughput  : 9,995 events/s
  latency     : p50 1 ms, p90 2 ms, p99 5 ms, max 42 ms
  lag         : max 139, final 0
group         : bench-strategy-3
  delivered   : 1,200,000
  redeliveries: 0
  throughput  : 9,995 events/s
  latency     : p50 1 ms, p90 2 ms, p99 5 ms, max 36 ms
  lag         : max 81, final 0
=================================================
-------- verdict --------
result        : PASS
=================================================
