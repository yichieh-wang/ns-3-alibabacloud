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
  // First sighting of a (switch,flow) opens a segment ("EVENT switch_enter",
  // carrying both the ingress NetDevice in_port and the egress out_port); a
  // change of egress is a reroute (close old segment + open new); the segment
  // closes as "EVENT switch_leave" at qp_complete. Never per-packet spam.
  // Carries BOTH byte counts of this forwarded packet: `bytes` = ON-WIRE size
  // (p->GetSize(); feeds switch_leave's bytes=, unchanged) and `payload` = the L4
  // PAYLOAD (p->GetSize() - ch.GetSerializedSize(), the receiver's identity in
  // RdmaHw::ReceiveUdp; feeds flow_cut's in_bytes= == the cut_in count).
  void OnSwitchForward(uint32_t switch_id, uint32_t in_port, uint32_t out_port,
                       uint64_t bytes, uint64_t payload, uint32_t src_ip, uint32_t dst_ip,
                       uint16_t sport, uint16_t dport);

  // SWITCH-EGRESS-TRANSMIT hook: called from SwitchNode::SwitchNotifyDequeue when a DATA packet is
  // dequeued from the BEgressQueue and handed to TransmitStart -- i.e. it TRULY LEAVES this switch
  // on the wire (the routed-vs-actually-transmitted distinction: OnSwitchForward counts bytes that
  // ENTERED + were routed; this counts bytes that DEPARTED). Adds the packet's PAYLOAD to the
  // matching (switch, 5-tuple) OPEN segment's cut_out counter (flow_cut's out_bytes=). No-op if the
  // segment is already closed (nothing open to attribute to). Observe-only; off => no-op.
  void OnSwitchTransmit(uint32_t switch_id, uint32_t out_port, uint64_t payload,
                        uint32_t src_ip, uint32_t dst_ip, uint16_t sport, uint16_t dport);

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
  // key=value format the streaming hooks use, so the Rust parser reads it unchanged. Used by control.h.
  void DumpState();

  // Number of currently-active flows (QPs) across all subnets -- control.h uses it to tell a PAUSED
  // RUN_UNTIL (active > 0, the workload continues) from an ENDED one (active == 0, the workload drained).
  uint32_t ActiveFlowCount() const;

  // Emit the fabric as a block (NodeList walked in node-id order): one "EVENT node" per node (RAW TypeId
  // class; a HOST also carries its cca + ip) then one "EVENT link" per link (bw + delay, printed once
  // from its lower-id endpoint). A parser rebuilds the REAL topology (incl. UNUSED ports) into a
  // Netlist. Each setup path calls this ONCE after wiring: it reports the current fully-wired state.
  void DumpAllNodeInfo();

  // Emit one "EVENT flow_cut ..." line per currently-OPEN (switch, flow) segment, carrying that flow's
  // TWO cumulative PAYLOAD counters through the switch so far -- each grouped next to the port it belongs
  // to: ingress_port + in_bytes (cut_in = bytes that entered + were routed, SwitchFlowStat.in_port /
  // .in_bytes) then egress_port + out_bytes (cut_out = bytes actually transmitted out the egress,
  // SwitchFlowStat.out_port / .out_bytes) -- the boundary-observable ("cut") analog of the sender-side
  // "EVENT active" line (whose sent_bytes is L4 payload = snd_nxt). PUBLIC so control.h can snapshot a
  // MID-RUN state on demand at a harness RUN_UNTIL pause, not only at the internal cut transitions below.
  // Iterates m_switchFlows in its (switch, 5-tuple) key order, so output is deterministic. Observe-only.
  // NS3_CUT_PROBE-gated. Snapshotted at every cut TRANSITION: switch_enter / switch_leave (in
  // OnSwitchForward), qp_add, qp_complete, and each rate_update CC event -- plus on demand (control.h).
  void DumpFlowCut(int64_t t_ns);

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
  // queue half of a mid-flight state. The Rust parser/example picks the ports that matter. Called at
  // each completion, and (only when NS3_QUEUE_PROBE is set) by the recurring mid-flight probe below.
  void DumpAllQueues(int64_t t_ns);

  // "EVENT node id=<id> class=<TypeId>" for one node; a HOST (non-switch) also carries " cca=<..> ip=<..>"
  // (its RdmaHw CcMode + Ipv4). Called by DumpAllNodeInfo per node.
  void EmitNode(uint32_t node_id);
  // "EVENT link a_node=.. a_port=.. b_node=.. b_port=.. bw=.. delay_ns=.." for each CONNECTED port of a
  // node, emitted ONCE per link (only when this node id < the peer's, so the pair isn't printed twice).
  void EmitNodeLinks(uint32_t node_id);

  // Recurring mid-flight queue sample (every 5us from the first qp_add, stops when no flow is
  // active) -- the egress backlog only exists BETWEEN events, so completion-only sampling reads ~0.
  void PeriodicQueueDump();
  bool m_queueProbe = false;        // opt-in gate (NS3_QUEUE_PROBE): the 5us probe is heavy, off by default
  bool m_queueProbeStarted = false; // latched while a burst's probe loop is live; reset when it goes idle
  bool m_cutProbe = false;          // opt-in gate (NS3_CUT_PROBE): the flow_cut byte-level dump, off by default (goldens stay clean)

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
  // The flow's CURRENT segment at this switch: the ingress it arrived on + the egress it
  // uses now + that segment's first/last sighting + byte counters. A change in out_port is a
  // REROUTE -- close the old segment (switch_leave) and open a new one (switch_enter).
  //   in_port   = ingress NetDevice the segment arrived on (-> flow_cut ingress_port=; the
  //               same value switch_enter prints as ingress_port=)
  //   out_port  = egress the segment uses now              (-> flow_cut egress_port=)
  //   bytes     = ON-WIRE bytes routed here (feeds switch_leave bytes=; kept as-is)
  //   in_bytes  = PAYLOAD bytes routed here     (cut_in  -> flow_cut in_bytes=)
  //   out_bytes = PAYLOAD bytes transmitted out (cut_out -> flow_cut out_bytes=)
  struct SwitchFlowStat {
    uint32_t out_port;
    uint32_t in_port;
    int64_t  first_ns;
    int64_t  last_ns;
    uint64_t bytes;
    uint64_t in_bytes;
    uint64_t out_bytes;
    bool     inject_done; // its last byte has entered (flow_inject_done fired): dedups the emit.
    bool     eject_done; // its last byte has left the egress (flow_eject_done fired): kept (not erased) so a
                       // lossy retransmit's forward folds in instead of re-opening a spurious segment;
                       // skipped by flow_cut; erased at qp_complete.
  };
  std::map<SwitchFlowKey, SwitchFlowStat> m_switchFlows;

  // Flow message size (payload = m_size), recorded at qp_add, looked up per egress packet in
  // OnSwitchTransmit: when a segment's cut_out reaches it, that flow's LAST byte just left this
  // egress -> emit switch_leave at the REAL exit (symmetric to switch_enter at the real entry),
  // not deferred to qp_complete. Keyed by the flow 5-tuple (size is per-flow, not per-switch).
  struct FlowSizeKey {
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t sport;
    uint16_t dport;
    bool operator<(const FlowSizeKey& o) const {
      if (src_ip != o.src_ip) return src_ip < o.src_ip;
      if (dst_ip != o.dst_ip) return dst_ip < o.dst_ip;
      if (sport  != o.sport)  return sport  < o.sport;
      return dport < o.dport;
    }
  };
  std::map<FlowSizeKey, uint64_t> m_flowPayload;
};

} // namespace ns3
#endif // EVENT_LOGGER_H
