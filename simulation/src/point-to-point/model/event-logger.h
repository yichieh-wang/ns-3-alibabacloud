#ifndef EVENT_LOGGER_H
#define EVENT_LOGGER_H
#include <ns3/rdma-queue-pair.h>
#include <ns3/ptr.h>
#include <cstdint>
#include <vector>

namespace ns3 {

// Lightweight per-event dump observed by rdma-hw via cheap notify hooks. This is
// design-neutral instrumentation: it only OBSERVES the running RDMA leaf and
// prints one structured line per event so we can see what events constitute a
// leaf subnet's behavior. It does NOT change the simulation.
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

private:
  EventLogger();
  bool m_enabled = false;
  // Flows (QPs) currently active, maintained by OnQpAdded / OnQpComplete. A "key
  // event" (a flow starting or finishing) changes this set; after each change we
  // dump a snapshot -- the cache wants, at every active-set change, WHO is
  // contending + each flow's progress + rate.
  std::vector<Ptr<RdmaQueuePair>> m_active;
  // Emit one "EVENT active ..." line per currently-active QP, sorted by 5-tuple
  // (deterministic order), carrying sent/acked/remaining bytes + current rate.
  void DumpActiveSet(int64_t t_ns);
};

} // namespace ns3
#endif // EVENT_LOGGER_H
