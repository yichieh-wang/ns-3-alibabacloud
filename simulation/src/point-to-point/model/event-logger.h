#ifndef EVENT_LOGGER_H
#define EVENT_LOGGER_H
#include <ns3/rdma-queue-pair.h>
#include <ns3/ptr.h>
#include <cstdint>
#include <map>
#include <vector>
#include <string>

namespace ns3 {

// Lightweight per-event dump observed by rdma-hw via cheap notify hooks. This is
// design-neutral instrumentation: it only OBSERVES the running RDMA leaf and
// prints one structured line per event so we can see what events constitute a
// leaf subnet's behavior. It does NOT change the simulation: every hook only READS
// state and prints, so an instrumented run's trace is byte-identical to a plain one.
//
// Gated by the NS3_EVENTS environment variable (read once on first Get()):
// when it is unset or empty every hook is a no-op, so a default run is
// byte-identical to vanilla ns-3 (mirrors deja's "no-op when disabled").
//
// When enabled, each hook emits one line to std::cout in the house key=value
// style, e.g.:
//   EVENT qp_add t_ns=... sip=... ... pg=... active_n=<#active flows now>
//   EVENT rate_update t_ns=... flow=10.0.0.1:10000->10.0.0.2:100 rate_gbps=...
//   EVENT qp_complete t_ns=... flow=... fct_ns=... size_bytes=... active_n=...
// A "key event" (qp_add / qp_complete) CHANGES the set of active flows; right
// after each one, one "EVENT active" line per still-active flow is emitted
// (sorted by 5-tuple) carrying its progress + rate -- the snapshot the cache
// keys on:
//   EVENT active t_ns=... flow=... sent_bytes=... acked_bytes=... remaining_bytes=... rate_gbps=...
class EventLogger {
public:
  static EventLogger& Get();
  bool Enabled() const { return m_enabled; }

  // Hooks called from rdma-hw. Every one returns immediately when disabled.
  void OnQpAdded(Ptr<RdmaQueuePair> qp);
  // `why` is a short tag for the CC site that changed the rate (e.g.
  // "change_rate", "mlx_cnp", "mlx_dec", "mlx_fastrec", "mlx_actinc",
  // "mlx_hyperinc", "timely", "dctcp_dec", "dctcp_inc").
  void OnRateUpdate(Ptr<RdmaQueuePair> qp, const char* why);
  void OnQpComplete(Ptr<RdmaQueuePair> qp);

  // SWITCH-SIDE hook (design point (d)): called from SwitchNode::SendToDev once a
  // valid egress device index is known, for DATA packets only. Reports a flow's
  // RAW traversal per (switch, egress port) so post-processing can reason about
  // transit flows + reroute bubbles -- something the host-side hooks (which only
  // see the QP at its endpoints) cannot. Primitives only (no CustomHeader) so the
  // logger needs no extra ns-3 coupling; src/dst IPs are raw uint32 IPv4 and are
  // formatted via Ipv4Address(...) to the dotted-quad flow-id style.
  // First sighting of a (switch,port,5-tuple) key emits one "EVENT switch_enter";
  // per-key first_ns/last_ns/bytes accumulate and flush as "EVENT switch_flow" at
  // logger destruction (program exit). Never per-packet spam.
  void OnSwitchForward(uint32_t switch_id, uint32_t out_port, uint64_t bytes,
                       uint32_t src_ip, uint32_t dst_ip,
                       uint16_t sport, uint16_t dport);

  // TOPOLOGY hook: a switch DECLARES its structural signature (e.g. "leaf"/"spine") into
  // the event stream as one "EVENT switch_info switch=<id> sig=<sig>", so the cache can map
  // node id -> signature FROM THE STREAM rather than hardcoding it. Emit once per switch at
  // setup. Gated like every other event.
  void OnSwitchInfo(uint32_t switch_id, const std::string& sig);

  // PFC hook: a QbbNetDevice's TX on (port, queue) PAUSES (it received a PFC PAUSE frame)
  // or RESUMES. Emitted ONLY on a real state TRANSITION -- no spam on the periodic
  // PAUSE-refresh frames -- as one
  //   EVENT pfc t_ns=<now> node=<id> port=<ifIndex> q=<qIndex> paused=<0|1>
  // per transition. PFC is the dominant flow-control mechanism in the PFC-bound regime
  // (cache-design §2.4), so these paused intervals ARE the forwarder's boundary
  // flow-control state. Gated like every other event; off => no-op => byte-identical.
  void OnPfc(uint32_t node_id, uint32_t port, uint32_t q_index, bool paused);

  // DROP hook: a switch MMU dropped a packet because the ingress headroom for (port, queue) is
  // full -- i.e. PFC FAILED to be lossless. One
  //   EVENT drop t_ns=<now> node=<id> port=<port> pg=<qIndex> bytes=<psize>
  // per dropped packet. In a correctly-provisioned lossless fabric this NEVER fires (drop count == 0);
  // a non-zero count means PFC could not pause in time -- a bug or an under-provisioned config (too
  // little headroom / buffer), NOT a normal regime. So it is an ALARM signal, distinct from the cache
  // guardrail (which skips the congested-but-lossless regime). Gated like every other event.
  void OnDrop(uint32_t node_id, uint32_t port, uint32_t q_index, uint32_t bytes);

  // INTERACTIVE-mode accessor: on a harness QUERY, serialise the CURRENT state -- one "EVENT active" per
  // active QP (all subnets) + one "EVENT queue" per non-zero egress backlog -- at Simulator::Now(). Same
  // key=value format the streaming hooks use, so the Rust decoder reads it unchanged. Used by control.h.
  void DumpState();

  // Number of currently-active flows (QPs) across all subnets -- control.h uses it to tell a PAUSED
  // RUN_UNTIL (active > 0, the workload continues) from an ENDED one (active == 0, the workload drained).
  uint32_t ActiveFlowCount() const;

private:
  EventLogger();
  ~EventLogger();
  bool m_enabled = false;
  // Active flows (QPs) grouped BY SUBNET, maintained by OnQpAdded / OnQpComplete.
  // The cache keys on a SUBNET's active set, not a global one (cache-design §7.5),
  // so we partition. Subnet key = the source IP's /16 prefix (our 11.<leaf>.<host>.1
  // scheme groups by leaf). This is an IP-prefix PROXY: exact for intra-subnet flows
  // whose IP scheme encodes the subnet (our fabric does); transit/inter-subnet flows
  // and topology-aware grouping are a consumer concern (need switch-side hooks).
  std::map<uint32_t, std::vector<Ptr<RdmaQueuePair>>> m_activeBySubnet;
  // Emit one "EVENT active ..." line per active QP in ONE subnet (the affected one),
  // sorted by 5-tuple, carrying sent/acked/remaining bytes + current rate.
  void DumpActiveSet(int64_t t_ns, uint32_t subnetKey);

  // Emit one "EVENT queue ..." line per (switch, egress port, priority-group) that has a NON-ZERO
  // egress backlog (an absent one == 0), for every SwitchNode (no subnet filter) -- the in-fabric /
  // queue half of a mid-flight state. The Rust decoder/example picks the ports that matter. Called at
  // each completion, and (only when NS3_QUEUE_PROBE is set) by the recurring mid-flight probe below.
  void DumpAllQueues(int64_t t_ns);

  // Recurring mid-flight queue sample (every 5us from the first qp_add, stops when no flow is
  // active) -- the egress backlog only exists BETWEEN events, so completion-only sampling reads ~0.
  void PeriodicQueueDump();
  bool m_queueProbe = false;        // opt-in gate (NS3_QUEUE_PROBE): the 5us probe is heavy, off by default
  bool m_queueProbeStarted = false; // latched while a burst's probe loop is live; reset when it goes idle

  // Per (switch, egress port, 5-tuple) traversal state for OnSwitchForward. Key is
  // ordered (switch, port, src_ip, dst_ip, sport, dport) so the destructor flush is
  // deterministic. first_ns set on first sight, last_ns updated each call, bytes summed.
  struct SwitchFlowKey {
    uint32_t switch_id;
    uint32_t out_port;
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t sport;
    uint16_t dport;
    bool operator<(const SwitchFlowKey& o) const {
      if (switch_id != o.switch_id) return switch_id < o.switch_id;
      if (out_port  != o.out_port)  return out_port  < o.out_port;
      if (src_ip    != o.src_ip)    return src_ip    < o.src_ip;
      if (dst_ip    != o.dst_ip)    return dst_ip    < o.dst_ip;
      if (sport     != o.sport)     return sport     < o.sport;
      return dport < o.dport;
    }
  };
  struct SwitchFlowStat {
    int64_t  first_ns;
    int64_t  last_ns;
    uint64_t bytes;
  };
  std::map<SwitchFlowKey, SwitchFlowStat> m_switchFlows;
};

} // namespace ns3
#endif // EVENT_LOGGER_H
