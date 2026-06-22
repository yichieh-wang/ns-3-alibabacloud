#include "event-logger.h"
#include <ns3/simulator.h>
#include <ns3/ipv4-address.h>
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

// Runs at program exit (after Simulator::Destroy), so it is the right place to
// flush the per-(switch,port,flow) traversal summary. One "EVENT switch_flow"
// line per key; the map is ordered so output is deterministic. No-op when off
// (the map is never populated when disabled).
EventLogger::~EventLogger() {
  if (!m_enabled) return;
  for (const auto& kv : m_switchFlows) {
    const SwitchFlowKey& k = kv.first;
    const SwitchFlowStat& s = kv.second;
    std::cout << "EVENT switch_flow"
              << " switch=" << k.switch_id
              << " port=" << k.out_port
              << " flow=" << Ipv4Address(k.src_ip) << ":" << k.sport
              << "->" << Ipv4Address(k.dst_ip) << ":" << k.dport
              << " first_ns=" << s.first_ns
              << " last_ns=" << s.last_ns
              << " bytes=" << s.bytes
              << std::endl;
  }
}

namespace {
// "sip:sport->dip:dport" -- the flow identity used by rate_update/qp_complete.
// Ipv4Address streams as a dotted-quad, matching the FLOW_COMPLETE house style.
void EmitFlow(std::ostream& os, const Ptr<RdmaQueuePair>& qp) {
  os << qp->sip << ":" << qp->sport << "->" << qp->dip << ":" << qp->dport;
}
// Subnet key = the source IP's /16 prefix. For our 11.<leaf>.<host>.1 scheme this
// groups flows by leaf (the cache atom). See the m_activeBySubnet comment.
uint32_t SubnetKey(const Ptr<RdmaQueuePair>& qp) {
  return qp->sip.Get() & 0xFFFF0000u;
}
// Print a /16 subnet key as its first two octets, e.g. "11.2".
void EmitSubnet(std::ostream& os, uint32_t key) {
  os << ((key >> 24) & 0xFFu) << "." << ((key >> 16) & 0xFFu);
}
} // namespace

void EventLogger::OnQpAdded(Ptr<RdmaQueuePair> qp) {
  if (!m_enabled) return;
  uint32_t sub = SubnetKey(qp);
  std::vector<Ptr<RdmaQueuePair>>& group = m_activeBySubnet[sub];
  group.push_back(qp); // the new flow joins its subnet's active set
  int64_t t = Simulator::Now().GetNanoSeconds();
  std::cout << "EVENT qp_add"
            << " t_ns=" << t
            << " sip=" << qp->sip
            << " sport=" << qp->sport
            << " dip=" << qp->dip
            << " dport=" << qp->dport
            << " size_bytes=" << qp->m_size
            << " pg=" << qp->m_pg
            << " subnet=";
  EmitSubnet(std::cout, sub);
  std::cout << " active_n=" << group.size() << std::endl;
  DumpActiveSet(t, sub);
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
  // Remove the finishing flow from its subnet's set BEFORE the snapshot, so the
  // dumped set is the one that governs the next segment (until the next key event).
  uint32_t sub = SubnetKey(qp);
  std::vector<Ptr<RdmaQueuePair>>& group = m_activeBySubnet[sub];
  for (auto it = group.begin(); it != group.end(); ++it) {
    if (*it == qp) { group.erase(it); break; }
  }
  std::cout << "EVENT qp_complete"
            << " t_ns=" << t
            << " flow=";
  EmitFlow(std::cout, qp);
  std::cout << " fct_ns=" << fct_ns
            << " size_bytes=" << qp->m_size
            << " subnet=";
  EmitSubnet(std::cout, sub);
  std::cout << " active_n=" << group.size() << std::endl;
  DumpActiveSet(t, sub);
}

// SWITCH-SIDE traversal hook. Called per data packet from SwitchNode::SendToDev,
// but only EMITS on the FIRST sighting of a (switch, port, 5-tuple) key -- this is
// the RAW "flow entered this switch via this egress port" event. Per-key timing
// (first_ns/last_ns) + byte counts accumulate silently and flush at destruction.
void EventLogger::OnSwitchForward(uint32_t switch_id, uint32_t out_port,
                                  uint64_t bytes, uint32_t src_ip, uint32_t dst_ip,
                                  uint16_t sport, uint16_t dport) {
  if (!m_enabled) return;
  int64_t t = Simulator::Now().GetNanoSeconds();
  SwitchFlowKey key{switch_id, out_port, src_ip, dst_ip, sport, dport};
  auto it = m_switchFlows.find(key);
  if (it == m_switchFlows.end()) {
    // First time this flow is seen at this switch+port: emit the enter event.
    m_switchFlows.emplace(key, SwitchFlowStat{t, t, bytes});
    std::cout << "EVENT switch_enter"
              << " t_ns=" << t
              << " switch=" << switch_id
              << " port=" << out_port
              << " flow=" << Ipv4Address(src_ip) << ":" << sport
              << "->" << Ipv4Address(dst_ip) << ":" << dport
              << std::endl;
  } else {
    // Already seen: just fold in this packet's timing + bytes (no per-packet line).
    it->second.last_ns = t;
    it->second.bytes += bytes;
  }
}

// TOPOLOGY hook: a switch declares its structural signature so the cache can map
// node id -> signature from the stream instead of hardcoding it. Once per switch at setup.
void EventLogger::OnSwitchInfo(uint32_t switch_id, const std::string& sig) {
  if (!m_enabled) return;
  std::cout << "EVENT switch_info"
            << " switch=" << switch_id
            << " sig=" << sig
            << std::endl;
}

// PFC hook: one line per pause/resume STATE TRANSITION on a (node, port, queue), emitted
// immediately (transitions are low-frequency thanks to the call-site guard). A paused
// interval is the [paused=1 ... paused=0] pair for that (node, port, q).
void EventLogger::OnPfc(uint32_t node_id, uint32_t port, uint32_t q_index, bool paused) {
  if (!m_enabled) return;
  std::cout << "EVENT pfc"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " node=" << node_id
            << " port=" << port
            << " q=" << q_index
            << " paused=" << (paused ? 1 : 0)
            << std::endl;
}

// One "EVENT active" line per active QP IN ONE SUBNET (the affected one), sorted by
// 5-tuple so the snapshot order is deterministic + canonical. sent=snd_nxt (bytes put
// on wire), acked=snd_una (bytes acknowledged), remaining=size-sent.
void EventLogger::DumpActiveSet(int64_t t_ns, uint32_t subnetKey) {
  auto it = m_activeBySubnet.find(subnetKey);
  if (it == m_activeBySubnet.end()) return;
  std::vector<Ptr<RdmaQueuePair>> v = it->second;
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
              << " subnet=";
    EmitSubnet(std::cout, subnetKey);
    std::cout << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << " sent_bytes=" << qp->snd_nxt
              << " acked_bytes=" << qp->snd_una
              << " remaining_bytes=" << remaining
              << " rate_gbps=" << (qp->m_rate.GetBitRate() * 1e-9)
              << std::endl;
  }
}

} // namespace ns3
