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
  // First sighting of a (switch,flow) opens a segment ("EVENT switch_enter"); a
  // change of egress is a reroute (close old segment + open new); the segment
  // closes as "EVENT switch_leave" at qp_complete. Never per-packet spam.
  void OnSwitchForward(uint32_t switch_id, uint32_t out_port, uint64_t bytes,
                       uint32_t src_ip, uint32_t dst_ip,
                       uint16_t sport, uint16_t dport);

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

  // Emit EVERY node's port wiring as a block (NodeList walked in node-id order): each node's "EVENT
  // node_port" lines (RAW TypeId class names for the node + its peer), so a decoder rebuilds the REAL
  // topology (incl. UNUSED ports) for hosts AND switches. Each setup path calls this ONCE, right AFTER
  // all wiring: it reports the current (fully-wired) state, no sim-event dependency.
  void DumpAllNodeInfo();

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

  // Emit one "EVENT node_port ..." per CONNECTED port of a node declaring that port's peer (peer node,
  // peer device index) with RAW TypeId class names for BOTH ends, so a decoder rebuilds the REAL
  // topology incl. UNUSED ports. Called by DumpAllNodeInfo for each node.
  void EmitNodePorts(uint32_t node_id);

  // Recurring mid-flight queue sample (every 5us from the first qp_add, stops when no flow is
  // active) -- the egress backlog only exists BETWEEN events, so completion-only sampling reads ~0.
  void PeriodicQueueDump();
  bool m_queueProbe = false;        // opt-in gate (NS3_QUEUE_PROBE): the 5us probe is heavy, off by default
  bool m_queueProbeStarted = false; // latched while a burst's probe loop is live; reset when it goes idle

  // Per (switch, flow) traversal state for OnSwitchForward. Key is the (switch,
  // 5-tuple) with NO egress port, so a flow that leaves an egress and later
  // returns to it (an A->B->A reroute) does NOT merge with its earlier A-stint.
  // Ordered so the destructor flush is deterministic.
  struct SwitchFlowKey {
    uint32_t switch_id;
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t sport;
    uint16_t dport;
    bool operator<(const SwitchFlowKey& o) const {
      if (switch_id != o.switch_id) return switch_id < o.switch_id;
      if (src_ip    != o.src_ip)    return src_ip    < o.src_ip;
      if (dst_ip    != o.dst_ip)    return dst_ip    < o.dst_ip;
      if (sport     != o.sport)     return sport     < o.sport;
      return dport < o.dport;
    }
  };
  // The flow's CURRENT segment at this switch: the egress it uses now + that
  // segment's first/last sighting + bytes. A change in out_port is a REROUTE --
  // close the old segment (switch_leave) and open a new one (switch_enter).
  struct SwitchFlowStat {
    uint32_t out_port;
    int64_t  first_ns;
    int64_t  last_ns;
    uint64_t bytes;
  };
  std::map<SwitchFlowKey, SwitchFlowStat> m_switchFlows;
};

} // namespace ns3
#endif // EVENT_LOGGER_H
