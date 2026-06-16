#ifndef EVENT_LOGGER_H
#define EVENT_LOGGER_H
#include <ns3/rdma-queue-pair.h>
#include <ns3/ptr.h>

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
//   EVENT qp_add t_ns=... sip=... sport=... dip=... dport=... size_bytes=... pg=...
//   EVENT rate_update t_ns=... flow=10.0.0.1:10000->10.0.0.2:100 rate_gbps=...
//   EVENT qp_complete t_ns=... flow=... fct_ns=... size_bytes=...
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
};

} // namespace ns3
#endif // EVENT_LOGGER_H
