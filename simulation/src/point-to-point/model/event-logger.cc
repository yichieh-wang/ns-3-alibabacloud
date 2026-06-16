#include "event-logger.h"
#include <ns3/simulator.h>
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
  std::cout << "EVENT qp_add"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " sip=" << qp->sip
            << " sport=" << qp->sport
            << " dip=" << qp->dip
            << " dport=" << qp->dport
            << " size_bytes=" << qp->m_size
            << " pg=" << qp->m_pg
            << std::endl;
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
  uint64_t fct_ns = (Simulator::Now() - qp->startTime).GetNanoSeconds();
  std::cout << "EVENT qp_complete"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " flow=";
  EmitFlow(std::cout, qp);
  std::cout << " fct_ns=" << fct_ns
            << " size_bytes=" << qp->m_size
            << std::endl;
}

} // namespace ns3
