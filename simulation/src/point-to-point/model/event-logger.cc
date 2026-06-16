#include "event-logger.h"
#include <ns3/simulator.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace ns3 {

EventLogger& EventLogger::Get() {
  static EventLogger e;
  return e;
}

// Read the gate once at construction so the hot path is a single bool test.
EventLogger::EventLogger() {
  const char* v = std::getenv("NS3_EVENTS");
  m_enabled = (v != nullptr && std::strlen(v) > 0);
}

namespace {
// "sip:sport->dip:dport" -- the flow identity used by rate_update/qp_complete.
// Ipv4Address streams as a dotted-quad, matching the FLOW_COMPLETE house style.
void EmitFlow(std::ostream& os, const Ptr<RdmaQueuePair>& qp) {
  os << qp->sip << ":" << qp->sport << "->" << qp->dip << ":" << qp->dport;
}
} // namespace

void EventLogger::OnQpAdded(Ptr<RdmaQueuePair> qp) {
  if (!m_enabled) return;
  m_active.push_back(qp); // the new flow is now active
  int64_t t = Simulator::Now().GetNanoSeconds();
  std::cout << "EVENT qp_add"
            << " t_ns=" << t
            << " sip=" << qp->sip
            << " sport=" << qp->sport
            << " dip=" << qp->dip
            << " dport=" << qp->dport
            << " size_bytes=" << qp->m_size
            << " pg=" << qp->m_pg
            << " active_n=" << m_active.size()
            << std::endl;
  DumpActiveSet(t);
}

void EventLogger::OnRateUpdate(Ptr<RdmaQueuePair> qp, const char* why) {
  if (!m_enabled) return;
  std::cout << "EVENT rate_update"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " flow=";
  EmitFlow(std::cout, qp);
  std::cout << " why=" << why
            << " rate_gbps=" << (qp->m_rate.GetBitRate() * 1e-9)
            << std::endl;
}

void EventLogger::OnQpComplete(Ptr<RdmaQueuePair> qp) {
  if (!m_enabled) return;
  int64_t t = Simulator::Now().GetNanoSeconds();
  uint64_t fct_ns = (Simulator::Now() - qp->startTime).GetNanoSeconds();
  // Remove the finishing flow BEFORE the snapshot, so the dumped active set is
  // the one that governs the next segment (until the next key event).
  for (auto it = m_active.begin(); it != m_active.end(); ++it) {
    if (*it == qp) { m_active.erase(it); break; }
  }
  std::cout << "EVENT qp_complete"
            << " t_ns=" << t
            << " flow=";
  EmitFlow(std::cout, qp);
  std::cout << " fct_ns=" << fct_ns
            << " size_bytes=" << qp->m_size
            << " active_n=" << m_active.size()
            << std::endl;
  DumpActiveSet(t);
}

// One "EVENT active" line per currently-active QP, sorted by 5-tuple so the
// snapshot order is deterministic + canonical. sent=snd_nxt (bytes put on wire),
// acked=snd_una (bytes acknowledged), remaining=size-sent.
void EventLogger::DumpActiveSet(int64_t t_ns) {
  std::vector<Ptr<RdmaQueuePair>> v = m_active;
  std::sort(v.begin(), v.end(),
            [](const Ptr<RdmaQueuePair>& a, const Ptr<RdmaQueuePair>& b) {
    if (a->sip.Get() != b->sip.Get()) return a->sip.Get() < b->sip.Get();
    if (a->sport != b->sport) return a->sport < b->sport;
    if (a->dip.Get() != b->dip.Get()) return a->dip.Get() < b->dip.Get();
    return a->dport < b->dport;
  });
  for (const Ptr<RdmaQueuePair>& qp : v) {
    uint64_t remaining = (qp->m_size > qp->snd_nxt) ? (qp->m_size - qp->snd_nxt) : 0;
    std::cout << "EVENT active"
              << " t_ns=" << t_ns
              << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << " sent_bytes=" << qp->snd_nxt
              << " acked_bytes=" << qp->snd_una
              << " remaining_bytes=" << remaining
              << " rate_gbps=" << (qp->m_rate.GetBitRate() * 1e-9)
              << std::endl;
  }
}

} // namespace ns3
