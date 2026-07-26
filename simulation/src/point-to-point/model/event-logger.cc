#include "event-logger.h"
#include <mutex>
#include "switch-node.h"
#include "point-to-point-net-device.h"
#include "qbb-channel.h"
#include "rdma-driver.h"
#include <ns3/simulator.h>
#include <ns3/node-list.h>
#include <ns3/net-device.h>
#include <ns3/channel.h>
#include <ns3/ipv4.h>
#include <ns3/ipv4-address.h>
#include <ns3/data-rate.h>
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
// UNISON (mtp) runs these emitters from MANY threads: the ONE process-wide lock (shared via
// EmitLock() with every other stdout emitter, e.g. a scratch program's FLOW_COMPLETE) makes
// each logical trace line AND its shared cut-state mutation (m_hostSendMark / m_switchFlows /
// ...) atomic. Recursive because emitters call DumpFlowCut/DumpActiveSet inline. Uncontended
// cost under the sequential kernel is nanoseconds.
std::recursive_mutex& EventLogger::EmitLock() {
  static std::recursive_mutex m;
  return m;
}
#define EMIT_LOCK std::lock_guard<std::recursive_mutex> emit_guard(EventLogger::EmitLock())

EventLogger::EventLogger() {
  const char* v = std::getenv("NS3_EVENTS");
  m_enabled = (v != nullptr && std::strlen(v) > 0);
  const char* q = std::getenv("NS3_QUEUE_PROBE");
  m_queueProbe = (q != nullptr && std::strlen(q) > 0);
  const char* c = std::getenv("NS3_CUT_PROBE");
  m_cutProbe = (c != nullptr && std::strlen(c) > 0);
  const char* hp = std::getenv("NS3_HOST_PORT_EVENTS");
  m_hostPortEvents = (hp != nullptr && std::strlen(hp) > 0);
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
// One "EVENT flow_inject" -- a flow opened a segment on (switch, egress port),
// arriving on ingress NetDevice in_port (the boundary cut a parser anchors entry_port on).
void EmitSwitchEnter(int64_t t, uint32_t sw, uint32_t in_port, uint32_t port, uint8_t pg, uint64_t size,
                     uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport) {
  // HEADS-UP: byte-level queue snapshot (per-pg backlog at entry) attaches here later (§4.4 byte-level phase).
  std::cout << "EVENT flow_inject"
            << " t_ns=" << t
            << " switch=" << sw
            << " ingress_port=" << in_port
            << " egress_port=" << port
            << " pg=" << (uint32_t)pg;
  if (size) std::cout << " size_bytes=" << size;
  std::cout << " flow=" << Ipv4Address(sip) << ":" << sport
            << "->" << Ipv4Address(dip) << ":" << dport
            << std::endl;
}
// One "EVENT flow_eject_done" -- a flow closed a segment on (switch, egress port),
// by reroute, qp_complete, or the exit flush.
void EmitSwitchLeave(uint32_t sw, uint32_t port, uint64_t size,
                     uint32_t sip, uint32_t dip, uint16_t sport, uint16_t dport,
                     int64_t leave_ns, uint64_t bytes) {
  // HEADS-UP: byte-level queue snapshot (per-pg backlog at leave) attaches here later (§4.4 byte-level phase).
  std::cout << "EVENT flow_eject_done"
            << " switch=" << sw
            << " egress_port=" << port
            << " flow=" << Ipv4Address(sip) << ":" << sport
            << "->" << Ipv4Address(dip) << ":" << dport
            << " leave_ns=" << leave_ns
            << " bytes=" << bytes;
  if (size) std::cout << " size_bytes=" << size;
  std::cout << std::endl;
}
// The cca string for an RdmaHw CcMode (the config's cca<->CcMode mapping, DCQCN == 1).
const char* CcaName(uint32_t ccMode) {
  return ccMode == 1 ? "dcqcn" : "unknown";
}
} // namespace

// Runs at program exit (after Simulator::Destroy). Any entry still here is a
// leftover segment for a flow that never reached qp_complete (uncompleted-flow
// fallback); flush it as a final switch_leave. The map is ordered so output is
// deterministic. No-op when off (the map is never populated when disabled).
EventLogger::~EventLogger() {
  EMIT_LOCK;
  if (!m_enabled) return;
  for (const auto& kv : m_switchFlows) {
    const SwitchFlowKey& k = kv.first;
    const SwitchFlowStat& s = kv.second;
    auto sz = m_flowPayload.find(FlowSizeKey{k.src_ip, k.dst_ip, k.sport, k.dport});
    EmitSwitchLeave(k.switch_id, s.out_port, (sz == m_flowPayload.end()) ? 0 : sz->second,
                    k.src_ip, k.dst_ip, k.sport, k.dport, s.last_ns, s.bytes);
  }
}

void EventLogger::OnQpAdded(Ptr<RdmaQueuePair> qp) {
  EMIT_LOCK;
  if (!m_enabled) return;
  uint32_t sub = SubnetKey(qp);
  std::vector<Ptr<RdmaQueuePair>>& group = m_activeBySubnet[sub];
  group.push_back(qp); // the new flow joins its subnet's active set
  // Record the flow's total payload so each switch can detect its own last EGRESS byte (cut_out == size).
  m_flowPayload[FlowSizeKey{qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport}] = qp->m_size;
  int64_t t = Simulator::Now().GetNanoSeconds();
  std::cout << "EVENT qp_add"
            << " t_ns=" << t
            << " sip=" << qp->sip
            << " sport=" << qp->sport
            << " dip=" << qp->dip
            << " dport=" << qp->dport
            << " size_bytes=" << qp->m_size
            << " pg=" << qp->m_pg
            << " active_n=" << group.size() << std::endl;
  DumpActiveSet(t, sub);
  DumpFlowCut(t); // cut-side analog: per-(switch,flow) in/out payload for each open segment
  if (m_queueProbe && !m_queueProbeStarted) { // opt-in (NS3_QUEUE_PROBE): start the mid-flight queue sample
    m_queueProbeStarted = true;
    Simulator::Schedule(NanoSeconds(0), &EventLogger::PeriodicQueueDump, this);
  }
}

void EventLogger::OnRateUpdate(Ptr<RdmaQueuePair> qp, const char* why) {
  EMIT_LOCK;
  if (!m_enabled) return;
  int64_t t = Simulator::Now().GetNanoSeconds();
  std::cout << "EVENT rate_update"
            << " t_ns=" << t
            << " flow=";
  EmitFlow(std::cout, qp);
  std::cout << " why=" << why
            << " rate_gbps=" << (qp->m_rate.GetBitRate() * 1e-9)
            << std::endl;
  DumpFlowCut(t); // a CC rate change is a cut transition: snapshot every open segment's in/out payload
}

void EventLogger::OnHostSend(uint32_t node_id, uint32_t port, Ptr<RdmaQueuePair> qp) {
  EMIT_LOCK;
  if (!m_enabled) return;
  uint8_t& mark = m_hostSendMark[FlowSizeKey{qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport}];
  uint64_t sent = qp->snd_nxt; // cumulative payload handed to the NIC (rewinds on go-back-N: marks dedup)
  int64_t t = Simulator::Now().GetNanoSeconds();
  if (!(mark & 1) && sent > 0) {
    mark |= 1;
    std::cout << "EVENT host_send t_ns=" << t << " node=" << node_id
              << " pg=" << (uint32_t)qp->m_pg << " size_bytes=" << qp->m_size << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << std::endl;
  }
  if (!(mark & 2) && sent >= qp->m_size) {
    mark |= 2;
    std::cout << "EVENT host_send_done t_ns=" << t << " node=" << node_id
              << " size_bytes=" << qp->m_size << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << std::endl;
  }
  if (!m_hostPortEvents) return;
  // HOST-PORT quadrant (NS3_HOST_PORT_EVENTS), NoC convention: a terminal SENDING is INJECTING
  // into the network -- flow_inject/_done at the source NIC, cumulative progress (snd_nxt) in
  // in_bytes. (Terminals speak the network frame; switches keep their per-node segment frame.)
  // The segment closes at its last byte; a TRUNCATED sender's leftover still falls to the
  // destructor's eject_done flush -- the wrong family for a source, a known truncated-trace wart.
  SwitchFlowKey key{node_id, qp->sip.Get(), qp->dip.Get(), qp->sport, qp->dport};
  auto it = m_switchFlows.find(key);
  if (it == m_switchFlows.end()) {
    if (sent == 0) return;
    it = m_switchFlows.emplace(key, SwitchFlowStat{port, port, t, t, 0, 0, 0, (uint8_t)qp->m_pg, false, false}).first;
    std::cout << "EVENT flow_inject t_ns=" << t << " switch=" << node_id << " ingress_port=" << port
              << " egress_port=" << port << " pg=" << (uint32_t)qp->m_pg << " size_bytes=" << qp->m_size << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << std::endl;
    DumpFlowCut(t);
  }
  it->second.last_ns = t;
  it->second.in_bytes = sent;
  if (sent >= qp->m_size) {
    std::cout << "EVENT flow_inject_done t_ns=" << t << " switch=" << node_id
              << " ingress_port=" << port << " size_bytes=" << qp->m_size << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << std::endl;
    m_switchFlows.erase(it);
    DumpFlowCut(t);
  }
}

void EventLogger::OnHostRecv(uint32_t node_id, uint32_t port, uint8_t pg, uint32_t sip, uint32_t dip, uint16_t sport,
                             uint16_t dport, uint64_t received) {
  EMIT_LOCK;
  if (!m_enabled) return;
  FlowSizeKey key{sip, dip, sport, dport};
  uint8_t& mark = m_hostRecvMark[key];
  int64_t t = Simulator::Now().GetNanoSeconds();
  auto sz = m_flowPayload.find(key);
  uint64_t size = (sz == m_flowPayload.end()) ? 0 : sz->second; // 0 = unknown -> field omitted
  if (!(mark & 1) && received > 0) {
    mark |= 1;
    std::cout << "EVENT host_recv t_ns=" << t << " node=" << node_id << " pg=" << (uint32_t)pg;
    if (size) std::cout << " size_bytes=" << size;
    std::cout << " flow=" << Ipv4Address(sip) << ":" << sport
              << "->" << Ipv4Address(dip) << ":" << dport << std::endl;
  }
  if (!(mark & 2) && size && received >= size) {
    mark |= 2;
    std::cout << "EVENT host_recv_done t_ns=" << t << " node=" << node_id
              << " size_bytes=" << size
              << " flow=" << Ipv4Address(sip) << ":" << sport
              << "->" << Ipv4Address(dip) << ":" << dport << std::endl;
  }
  if (!m_hostPortEvents) return;
  // HOST-PORT quadrant (NS3_HOST_PORT_EVENTS), NoC convention: a terminal RECEIVING is EJECTING
  // from the network (bytes leave the fabric into the host's memory) -- flow_eject/_done at the
  // sink NIC, cumulative progress in out_bytes. A truncated receiver's leftover falls to the
  // destructor's eject_done flush, which for a sink is the RIGHT family.
  SwitchFlowKey skey{node_id, sip, dip, sport, dport};
  auto it = m_switchFlows.find(skey);
  if (it == m_switchFlows.end()) {
    if (received == 0) return;
    it = m_switchFlows.emplace(skey, SwitchFlowStat{port, port, t, t, 0, 0, 0, pg, false, false}).first;
    std::cout << "EVENT flow_eject t_ns=" << t << " switch=" << node_id << " egress_port=" << port
              << " pg=" << (uint32_t)pg;
    if (size) std::cout << " size_bytes=" << size;
    std::cout << " flow=" << Ipv4Address(sip) << ":" << sport
              << "->" << Ipv4Address(dip) << ":" << dport << std::endl;
    DumpFlowCut(t);
  }
  it->second.last_ns = t;
  it->second.out_bytes = received;
  if (size && received >= size) {
    EmitSwitchLeave(node_id, port, size, sip, dip, sport, dport, t, received);
    m_switchFlows.erase(it);
    DumpFlowCut(t);
  }
}

void EventLogger::OnQpComplete(Ptr<RdmaQueuePair> qp) {
  EMIT_LOCK;
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
            << " active_n=" << group.size() << std::endl;
  DumpActiveSet(t, sub);
  DumpFlowCut(t); // cut-side analog: per-(switch,flow) in/out payload for each still-open segment
  DumpAllQueues(t); // the in-fabric / queue half of the post-completion (next) state
  // FALLBACK: most segments already closed at their real egress-exit (OnSwitchTransmit, cut_out==size).
  // Any segment still open here never reached its size (e.g. lossy retransmit accounting) -- close it now
  // at qp_complete with its last-forward time, then drop the entries so a recycled 4-tuple starts fresh.
  uint32_t sip = qp->sip.Get(), dip = qp->dip.Get();
  for (auto it = m_switchFlows.begin(); it != m_switchFlows.end(); ) {
    const SwitchFlowKey& k = it->first;
    if (k.src_ip == sip && k.dst_ip == dip && k.sport == qp->sport && k.dport == qp->dport) {
      const SwitchFlowStat& s = it->second;
      if (!s.eject_done) // never hit its egress-exit (lossy/straggler): fall back to the last-forward time
        EmitSwitchLeave(k.switch_id, s.out_port, qp->m_size, k.src_ip, k.dst_ip, k.sport, k.dport,
                        s.last_ns, s.bytes);
      it = m_switchFlows.erase(it);
    } else {
      ++it;
    }
  }
  FlowSizeKey fkey{sip, dip, qp->sport, qp->dport};
  m_flowPayload.erase(fkey); // the flow is done: drop its size
  m_hostSendMark.erase(fkey);
  m_hostRecvMark.erase(fkey);
}

// SWITCH-SIDE traversal hook. Called per data packet from SwitchNode::SendToDev.
// Keyed by (switch, flow): the first sighting opens a segment (switch_enter);
// same-egress packets accumulate silently; a CHANGE of egress is a REROUTE --
// close the old segment (switch_leave) and open a fresh one (switch_enter).
void EventLogger::OnSwitchForward(uint32_t switch_id, uint32_t in_port, uint32_t out_port, uint8_t pg,
                                  uint64_t bytes, uint64_t payload, uint32_t src_ip, uint32_t dst_ip,
                                  uint16_t sport, uint16_t dport) {
  EMIT_LOCK;
  if (!m_enabled) return;
  int64_t t = Simulator::Now().GetNanoSeconds();
  SwitchFlowKey key{switch_id, src_ip, dst_ip, sport, dport};
  auto sz = m_flowPayload.find(FlowSizeKey{src_ip, dst_ip, sport, dport});
  uint64_t size = (sz == m_flowPayload.end()) ? 0 : sz->second; // 0 = unknown -> field omitted
  // Entry-cut crossing-END: the flow's LAST byte just entered this switch (cut_in reached its total
  // payload) -- the fourth quarter of the cut-event symmetry (flow_inject = first byte in,
  // flow_eject/_done = first/last byte out). The parser stamps `inflow_end_age` here.
  auto check_inject_done = [&](SwitchFlowStat& s) {
    if (s.inject_done) return;
    if (sz == m_flowPayload.end() || s.in_bytes < sz->second) return;
    s.inject_done = true;
    std::cout << "EVENT flow_inject_done"
              << " t_ns=" << t
              << " switch=" << switch_id
              << " ingress_port=" << s.in_port
              << " size_bytes=" << sz->second
              << " flow=" << Ipv4Address(src_ip) << ":" << sport
              << "->" << Ipv4Address(dst_ip) << ":" << dport
              << std::endl;
    DumpFlowCut(t); // a cut transition (the entry-side inflow closed): snapshot all open segments
  };
  auto it = m_switchFlows.find(key);
  if (it == m_switchFlows.end()) {
    // First sighting: open the flow's segment (cut_in seeded, cut_out starts at 0 -- it accrues
    // later when packets are actually transmitted out the egress via OnSwitchTransmit).
    it = m_switchFlows.emplace(key, SwitchFlowStat{out_port, in_port, t, t, bytes, payload, 0, pg, false, false}).first;
    EmitSwitchEnter(t, switch_id, in_port, out_port, pg, size, src_ip, dst_ip, sport, dport);
    DumpFlowCut(t); // flow_inject is a cut transition: snapshot all open segments
    check_inject_done(it->second); // a single-packet flow injects fully at first sighting
  } else if (it->second.out_port == out_port) {
    // Same egress: fold in this packet's timing + on-wire bytes + payload (no per-packet line).
    it->second.last_ns = t;
    it->second.bytes += bytes;
    it->second.in_bytes += payload;
    check_inject_done(it->second);
  } else {
    // REROUTE: egress changed -- close the old segment, open a new one.
    EmitSwitchLeave(switch_id, it->second.out_port, size, src_ip, dst_ip, sport, dport,
                    it->second.last_ns, it->second.bytes);
    it->second = SwitchFlowStat{out_port, in_port, t, t, bytes, payload, 0, pg, false, false};
    EmitSwitchEnter(t, switch_id, in_port, out_port, pg, size, src_ip, dst_ip, sport, dport);
    DumpFlowCut(t); // flow_eject_done+flow_inject is a cut transition: snapshot all open segments
    check_inject_done(it->second);
  }
}

// SWITCH-EGRESS-TRANSMIT hook. Called per DATA packet from SwitchNode::SwitchNotifyDequeue, at the
// point a packet is dequeued from the BEgressQueue and about to TransmitStart -- i.e. it truly LEAVES
// the switch. Adds the packet's PAYLOAD to the matching (switch, flow) OPEN segment's cut_out counter.
// The segment MUST already exist (routing/OnSwitchForward opened it before the packet could be
// enqueued+dequeued); if it was already closed (e.g. a straggler dequeued after qp_complete drained
// the flow) there is no open cut to attribute to, so this is a no-op. Observe-only; no output.
void EventLogger::OnSwitchTransmit(uint32_t switch_id, uint32_t out_port, uint64_t payload,
                                   uint32_t src_ip, uint32_t dst_ip, uint16_t sport, uint16_t dport) {
  EMIT_LOCK;
  if (!m_enabled) return;
  SwitchFlowKey key{switch_id, src_ip, dst_ip, sport, dport};
  auto it = m_switchFlows.find(key);
  if (it == m_switchFlows.end()) return; // no open segment (closed/drained): nothing to attribute
  auto sz = m_flowPayload.find(FlowSizeKey{src_ip, dst_ip, sport, dport});
  uint64_t size = (sz == m_flowPayload.end()) ? 0 : sz->second; // 0 = unknown -> field omitted
  bool first_out = (it->second.out_bytes == 0);
  it->second.out_bytes += payload;
  if (first_out) {
    // Exit-cut crossing-START: the flow's FIRST byte just left this egress -- the missing quarter of
    // the cut-event symmetry (switch_enter = first byte in, switch_leave = last byte out). The parser
    // reveals the flow's exit port HERE (observed, never predicted) and emits the Start crossing. A
    // reroute reopens the segment with out_bytes = 0, so the new egress re-reveals -- correct.
    int64_t t = Simulator::Now().GetNanoSeconds();
    std::cout << "EVENT flow_eject"
              << " t_ns=" << t
              << " switch=" << switch_id
              << " egress_port=" << out_port
              << " pg=" << (uint32_t)it->second.pg;
    if (size) std::cout << " size_bytes=" << size;
    std::cout << " flow=" << Ipv4Address(src_ip) << ":" << sport
              << "->" << Ipv4Address(dst_ip) << ":" << dport
              << std::endl;
    DumpFlowCut(t); // a cut transition: snapshot so the reveal-instant state reads fresh byte counters
  }

  // Real EGRESS-EXIT: once cut_out has reached the flow's total payload, this flow's LAST byte has just
  // left this egress -- it has physically crossed the exit cut. Close the segment HERE (leave_ns = now),
  // symmetric to switch_enter at the real entry, instead of deferring to qp_complete (the sender ACK,
  // ~RTT later). Drop the segment BEFORE the snapshot so flow_cut shows the SURVIVORS at the real exit.
  if (sz != m_flowPayload.end() && !it->second.eject_done && it->second.out_bytes >= sz->second) {
    int64_t t = Simulator::Now().GetNanoSeconds();
    EmitSwitchLeave(switch_id, it->second.out_port, size, src_ip, dst_ip, sport, dport, t, it->second.bytes);
    it->second.eject_done = true; // mark eject_done but KEEP the segment: a lossy retransmit's later forward
                                // then folds in via find() instead of re-opening a spurious segment.
    DumpFlowCut(t);             // snapshot the SURVIVORS at the real exit (eject_done segments are skipped)
  }
}

// Emit the whole fabric: one "EVENT node" per node, then one "EVENT link" per link (each printed once).
// NodeList in id order; each setup path calls this ONCE after wiring (report-current-state).
void EventLogger::DumpAllNodeInfo() {
  EMIT_LOCK;
  if (!m_enabled) return;
  for (uint32_t i = 0; i < NodeList::GetNNodes(); i++) EmitNode(NodeList::GetNode(i)->GetId());
  for (uint32_t i = 0; i < NodeList::GetNNodes(); i++) EmitNodeLinks(NodeList::GetNode(i)->GetId());
}

// One "EVENT node" for a node: its id + RAW TypeId class. A HOST (non-switch) also carries the
// build-only params the structure can't show -- its RdmaHw cca + Ipv4 address (each guarded so a host
// without a full RDMA/IP stack still emits id + class).
void EventLogger::EmitNode(uint32_t node_id) {
  EMIT_LOCK;
  Ptr<Node> node = NodeList::GetNode(node_id);
  if (!node) return;
  std::cout << "EVENT node id=" << node_id << " class=" << node->GetInstanceTypeId().GetName();
  if (!DynamicCast<SwitchNode>(node)) {
    Ptr<RdmaDriver> drv = node->GetObject<RdmaDriver>();
    if (drv && drv->m_rdma) std::cout << " cca=" << CcaName(drv->m_rdma->m_cc_mode);
    Ptr<Ipv4> ipv4 = node->GetObject<Ipv4>();
    if (ipv4 && ipv4->GetNInterfaces() > 1) std::cout << " ip=" << ipv4->GetAddress(1, 0).GetLocal();
  }
  std::cout << std::endl;
}

// One "EVENT link" per CONNECTED port of a node, emitted ONCE per link: only from the lower-id endpoint
// (node_id < peer node id), so each QbbChannel prints a single line. Carries the link's bw (the port's
// DataRate) + delay (the channel delay) -- the media params a parser needs to rebuild the Netlist.
void EventLogger::EmitNodeLinks(uint32_t node_id) {
  EMIT_LOCK;
  Ptr<Node> node = NodeList::GetNode(node_id);
  if (!node) return;
  for (uint32_t p = 0; p < node->GetNDevices(); p++) {
    Ptr<NetDevice> dev = node->GetDevice(p);
    Ptr<Channel> ch = dev ? dev->GetChannel() : nullptr;
    if (!ch || ch->GetNDevices() < 2) continue; // loopback / unconnected port: skip, don't crash
    Ptr<NetDevice> peer = (ch->GetDevice(0) == dev) ? ch->GetDevice(1) : ch->GetDevice(0);
    Ptr<Node> peerNode = peer->GetNode();
    if (node_id >= peerNode->GetId()) continue; // DEDUP: print the link once, from its lower-id end
    uint32_t peerPort = 0; // peer's device index for this link (loop to find it)
    for (uint32_t i = 0; i < peerNode->GetNDevices(); i++) {
      if (peerNode->GetDevice(i) == peer) { peerPort = i; break; }
    }
    std::cout << "EVENT link"
              << " a_node=" << node_id << " a_port=" << p
              << " b_node=" << peerNode->GetId() << " b_port=" << peerPort;
    Ptr<PointToPointNetDevice> p2p = DynamicCast<PointToPointNetDevice>(dev);
    Ptr<QbbChannel> qbbCh = DynamicCast<QbbChannel>(ch); // QbbChannel re-exposes GetDelay publicly
    if (p2p) std::cout << " bw=" << p2p->GetDataRate();
    if (qbbCh) std::cout << " delay_ns=" << qbbCh->GetDelay().GetNanoSeconds();
    std::cout << std::endl;
  }
}

// PFC hook: one line per pause/resume STATE TRANSITION on a (node, port, queue), emitted
// immediately (transitions are low-frequency thanks to the call-site guard). A paused
// interval is the [paused=1 ... paused=0] pair for that (node, port, q).
void EventLogger::OnPfc(uint32_t node_id, uint32_t port, uint32_t q_index, bool paused) {
  EMIT_LOCK;
  if (!m_enabled) return;
  std::cout << "EVENT pfc"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " node=" << node_id
            << " port=" << port
            << " q=" << q_index
            << " paused=" << (paused ? 1 : 0)
            << std::endl;
}

// One "EVENT drop ..." line per packet the switch MMU drops on a full ingress headroom (PFC failed
// to be lossless). Structured replacement for the old debug printf. Should be ZERO in a sound run.
void EventLogger::OnDrop(uint32_t node_id, uint32_t port, uint32_t q_index, uint32_t bytes) {
  EMIT_LOCK;
  if (!m_enabled) return;
  std::cout << "EVENT drop"
            << " t_ns=" << Simulator::Now().GetNanoSeconds()
            << " node=" << node_id
            << " port=" << port
            << " pg=" << q_index
            << " bytes=" << bytes
            << std::endl;
}

// One "EVENT active" line per active QP IN ONE SUBNET (the affected one), sorted by
// 5-tuple so the snapshot order is deterministic + canonical. sent=snd_nxt (bytes put
// on wire), acked=snd_una (bytes acknowledged), remaining=size-sent.
void EventLogger::DumpActiveSet(int64_t t_ns, uint32_t subnetKey) {
  EMIT_LOCK;
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
              << " flow=";
    EmitFlow(std::cout, qp);
    std::cout << " sent_bytes=" << qp->snd_nxt
              << " acked_bytes=" << qp->snd_una
              << " remaining_bytes=" << remaining
              << " rate_gbps=" << (qp->m_rate.GetBitRate() * 1e-9)
              << std::endl;
  }
}

// One "EVENT flow_cut" line per currently-OPEN (switch, flow) segment, carrying that flow's TWO
// cumulative PAYLOAD counters through the switch so far -- in_bytes (cut_in: bytes that entered + were
// routed) and out_bytes (cut_out: bytes actually transmitted out the egress) -- the boundary-observable
// ("cut") analog of the sender-side "EVENT active" line (whose sent_bytes is L4 payload = snd_nxt).
// Each counter sits next to the port it belongs to: ingress_port + in_bytes (the arrival side), then
// egress_port + out_bytes (the departure side). m_switchFlows holds only OPEN segments (erased at
// segment close) and is ordered
// by (switch, 5-tuple), so a plain iteration is already deterministic. Observe-only: reads the map,
// mutates nothing. Snapshotted at every cut transition (switch_enter/leave, qp_add, qp_complete,
// rate_update). flow= is printed exactly like EmitSwitchEnter/EmitSwitchLeave.
void EventLogger::DumpFlowCut(int64_t t_ns) {
  EMIT_LOCK;
  if (!m_cutProbe) return; // opt-in gate (NS3_CUT_PROBE): off by default so goldens stay flow_cut-free
  for (const auto& kv : m_switchFlows) {
    const SwitchFlowKey& k = kv.first;
    const SwitchFlowStat& s = kv.second;
    if (s.eject_done) continue; // already crossed the exit cut (switch_leave fired): not in the cut now
    std::cout << "EVENT flow_cut"
              << " t_ns=" << t_ns
              << " switch=" << k.switch_id
              << " flow=" << Ipv4Address(k.src_ip) << ":" << k.sport
              << "->" << Ipv4Address(k.dst_ip) << ":" << k.dport
              << " ingress_port=" << s.in_port
              << " in_bytes=" << s.in_bytes
              << " egress_port=" << s.out_port
              << " out_bytes=" << s.out_bytes
              << std::endl;
  }
}

// One "EVENT queue ..." line per (switch, egress port, priority-group) carrying the REAL egress
// backlog -- the QbbNetDevice's BEgressQueue (dev->GetQueue()->GetNBytes(pg)), the same queue HPCC
// reads for its qlen (the BEgressQueue's per-pg occupancy, not SwitchMmu::egress_bytes; both exclude
// the currently-transmitting packet). Every SwitchNode (NVSwitch GPU domains are a separate node type
// and not covered), no subnet filter; only (port, pg) with a NON-ZERO backlog is emitted (an absent
// one == 0). Fired at each completion so it pairs with that instant's active snapshot.
void EventLogger::DumpAllQueues(int64_t t_ns) {
  EMIT_LOCK;
  for (auto it = NodeList::Begin(); it != NodeList::End(); ++it) {
    Ptr<SwitchNode> sw = DynamicCast<SwitchNode>(*it);
    if (!sw) continue;
    uint32_t nDev = sw->GetNDevices();
    for (uint32_t port = 0; port < nDev; ++port) { // start at 0: a SwitchNode has no loopback, so
      Ptr<QbbNetDevice> dev = DynamicCast<QbbNetDevice>(sw->GetDevice(port)); // device 0 is a real
      if (!dev) continue;                          // egress port (the incast bottleneck is here);
                                                   // a non-Qbb device (e.g. host loopback) casts to null and is skipped.
      if (!dev->GetQueue()) continue;              // a Qbb device may exist before its egress queue is wired
      for (uint32_t pg = 0; pg < 8; ++pg) {        // 8 priority groups
        uint32_t qbytes = dev->GetQueue()->GetNBytes(pg);
        if (qbytes == 0) continue;                 // emit only a real backlog; absent == 0, so this
                                                   // keeps lean (rate-paced) traces and their goldens noise-free.
        std::cout << "EVENT queue"
                  << " t_ns=" << t_ns
                  << " switch=" << sw->GetId()
                  << " egress_port=" << port
                  << " pg=" << pg
                  << " bytes=" << qbytes
                  << std::endl;
      }
    }
  }
}

// Recurring mid-flight queue sample. BOUNDED: stops rescheduling once no subnet has an active flow,
// so it can't keep the simulator alive past the workload (an unbounded reschedule ran the sim to its
// 10 s stop and emitted ~100M lines). The egress backlog is transient between events, so this is
// where a non-zero queue shows up -- completion-only sampling reads ~0.
void EventLogger::PeriodicQueueDump() {
  EMIT_LOCK;
  if (!m_enabled) return;
  bool anyActive = false;
  for (const auto& kv : m_activeBySubnet) {
    if (!kv.second.empty()) { anyActive = true; break; }
  }
  if (!anyActive) { m_queueProbeStarted = false; return; } // idle -> stop, and re-arm for a later burst
  DumpAllQueues(Simulator::Now().GetNanoSeconds());
  Simulator::Schedule(NanoSeconds(5000), &EventLogger::PeriodicQueueDump, this);
}

// INTERACTIVE QUERY: serialise the CURRENT state -- every active QP (all subnets) + every non-zero
// egress backlog -- at Now(), in the same format the streaming hooks emit. Called by control.h on a
// harness QUERY; unlike the automatic hooks this fires on demand, at an arbitrary paused instant.
void EventLogger::DumpState() {
  EMIT_LOCK;
  int64_t t = Simulator::Now().GetNanoSeconds();
  for (const auto& kv : m_activeBySubnet) {
    if (!kv.second.empty()) DumpActiveSet(t, kv.first);
  }
  DumpAllQueues(t);
}

// Active flow (QP) count across all subnets -- the "is the workload still running?" signal for control.h.
uint32_t EventLogger::ActiveFlowCount() const {
  uint32_t n = 0;
  for (const auto& kv : m_activeBySubnet) n += (uint32_t)kv.second.size();
  return n;
}

} // namespace ns3
