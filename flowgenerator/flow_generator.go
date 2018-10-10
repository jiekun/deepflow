package flowgenerator

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/google/gopacket/layers"
	"github.com/op/go-logging"
	. "gitlab.x.lan/yunshan/droplet-libs/datatype"
	. "gitlab.x.lan/yunshan/droplet-libs/queue"
	. "gitlab.x.lan/yunshan/droplet-libs/stats"
)

var log = logging.MustGetLogger("flowgenerator")

func (f *FlowGenerator) genFlowKey(meta *MetaPacket) *FlowKey {
	flowKey := f.innerFlowKey
	flowKey.Exporter = meta.Exporter
	flowKey.MACSrc = meta.MacSrc
	flowKey.MACDst = meta.MacDst
	flowKey.IPSrc = meta.IpSrc
	flowKey.IPDst = meta.IpDst
	flowKey.Proto = meta.Protocol
	flowKey.PortSrc = meta.PortSrc
	flowKey.PortDst = meta.PortDst
	flowKey.InPort = meta.InPort
	if tunnel := meta.Tunnel; tunnel != nil {
		flowKey.TunnelInfo = *tunnel
	} else {
		flowKey.TunnelInfo.Id = 0
		flowKey.TunnelInfo.Type = 0
		flowKey.TunnelInfo.Src = 0
		flowKey.TunnelInfo.Dst = 0
	}
	return flowKey
}

// hash of the key L3, symmetric
func getKeyL3Hash(flowKey *FlowKey, basis uint32) uint64 {
	return uint64(hashFinish(hashAdd(basis, flowKey.IPSrc^flowKey.IPDst)))
}

// hash of the key L4, symmetric
func getKeyL4Hash(flowKey *FlowKey, basis uint32) uint64 {
	portSrc := uint32(flowKey.PortSrc)
	portDst := uint32(flowKey.PortDst)
	if portSrc >= portDst {
		return uint64(hashFinish(hashAdd(basis, (portSrc<<16)|portDst)))
	}
	return uint64(hashFinish(hashAdd(basis, (portDst<<16)|portSrc)))
}

func (f *FlowGenerator) getQuinTupleHash(flowKey *FlowKey) uint64 {
	return getKeyL3Hash(flowKey, f.hashBasis) ^ ((uint64(flowKey.InPort) << 32) | getKeyL4Hash(flowKey, f.hashBasis))
}

func isFromTor(inPort uint32) bool {
	return inPort&PACKET_SOURCE_TOR == PACKET_SOURCE_TOR
}

func (f *FlowExtra) MacEquals(meta *MetaPacket) bool {
	taggedFlow := f.taggedFlow
	flowMacSrc, flowMacDst := taggedFlow.MACSrc, taggedFlow.MACDst
	if flowMacSrc == meta.MacSrc && flowMacDst == meta.MacDst {
		return true
	}
	if flowMacSrc == meta.MacDst && flowMacDst == meta.MacSrc {
		return true
	}
	return false
}

func (f *FlowExtra) TunnelMatch(key *FlowKey) bool {
	taggedFlow := f.taggedFlow
	if taggedFlow.TunnelInfo.Id == 0 && key.TunnelInfo.Id == 0 {
		return true
	}
	if taggedFlow.TunnelInfo.Type != key.TunnelInfo.Type || taggedFlow.TunnelInfo.Id != key.TunnelInfo.Id {
		return false
	}
	return taggedFlow.TunnelInfo.Src^key.TunnelInfo.Src^taggedFlow.TunnelInfo.Dst^key.TunnelInfo.Dst == 0
}

func (f *FlowCache) keyMatch(meta *MetaPacket, key *FlowKey) (*FlowExtra, bool) {
	for e := f.flowList.Front(); e != nil; e = e.Next() {
		flowExtra := e.Value
		taggedFlow := flowExtra.taggedFlow
		if taggedFlow.Exporter != key.Exporter || (isFromTor(key.InPort) && !flowExtra.MacEquals(meta)) {
			continue
		}
		if taggedFlow.Proto != key.Proto || !flowExtra.TunnelMatch(key) {
			continue
		}
		if taggedFlow.IPSrc == key.IPSrc && taggedFlow.IPDst == key.IPDst && taggedFlow.PortSrc == key.PortSrc && taggedFlow.PortDst == key.PortDst {
			f.flowList.MoveToFront(e)
			return flowExtra, false
		} else if taggedFlow.IPSrc == key.IPDst && taggedFlow.IPDst == key.IPSrc && taggedFlow.PortSrc == key.PortDst && taggedFlow.PortDst == key.PortSrc {
			f.flowList.MoveToFront(e)
			return flowExtra, true
		}
	}
	return nil, false
}

func (f *FlowGenerator) initFlowCache() bool {
	if f.hashMap == nil {
		log.Error("flow cache init failed: FlowGenerator.hashMap is nil")
		return false
	}
	for i := range f.hashMap {
		f.hashMap[i] = &FlowCache{capacity: FLOW_CACHE_CAP, flowList: NewListFlowExtra()}
	}
	return true
}

func (f *FlowGenerator) addFlow(flowCache *FlowCache, flowExtra *FlowExtra) *FlowExtra {
	return flowCache.flowList.PushFront(flowExtra).Value
}

func (f *FlowGenerator) genFlowId(timestamp uint64, inPort uint64) uint64 {
	return ((inPort & IN_PORT_FLOW_ID_MASK) << 32) | ((timestamp & TIMER_FLOW_ID_MASK) << 32) | (f.stats.TotalNumFlows & TOTAL_FLOWS_ID_MASK)
}

func (f *FlowGenerator) initFlow(meta *MetaPacket, key *FlowKey, now time.Duration) *FlowExtra {
	taggedFlow := f.taggedFlowHandler.alloc()
	taggedFlow.FlowKey = *key
	taggedFlow.FlowID = f.genFlowId(uint64(now), uint64(key.InPort))
	taggedFlow.TimeBitmap = 1
	taggedFlow.StartTime = now
	taggedFlow.EndTime = now
	taggedFlow.CurStartTime = now
	taggedFlow.VLAN = meta.Vlan
	taggedFlow.EthType = meta.EthType
	taggedFlow.CloseType = CloseTypeUnknown
	taggedFlow.PolicyData = meta.PolicyData

	flowExtra := f.flowExtraHandler.alloc()
	flowExtra.taggedFlow = taggedFlow
	flowExtra.flowState = FLOW_STATE_RAW
	flowExtra.recentTime = now
	flowExtra.reversed = false

	return flowExtra
}

func (f *FlowGenerator) updateFlowStateMachine(flowExtra *FlowExtra, flags uint8, reply, invalid bool) bool {
	var timeout time.Duration
	var flowState FlowState
	closed := false
	taggedFlow := flowExtra.taggedFlow
	if reply {
		taggedFlow.FlowMetricsPeerDst.TCPFlags |= flags
	} else {
		taggedFlow.FlowMetricsPeerSrc.TCPFlags |= flags
	}
	if isExceptionFlags(flags, reply) || invalid {
		flowExtra.timeout = f.TimeoutConfig.Exception
		flowExtra.flowState = FLOW_STATE_EXCEPTION
		return false
	}
	if stateValue, ok := f.stateMachineMaster[flowExtra.flowState][flags&TCP_FLAG_MASK]; ok {
		timeout = stateValue.timeout
		flowState = stateValue.flowState
		closed = stateValue.closed
	} else {
		timeout = f.TimeoutConfig.Exception
		flowState = FLOW_STATE_EXCEPTION
		closed = false
	}
	if reply {
		if stateValue, ok := f.stateMachineSlave[flowExtra.flowState][flags&TCP_FLAG_MASK]; ok {
			timeout = stateValue.timeout
			flowState = stateValue.flowState
			closed = stateValue.closed
		}
	}
	flowExtra.timeout = timeout
	flowExtra.flowState = flowState
	if taggedFlow.FlowMetricsPeerSrc.TotalPacketCount == 0 || taggedFlow.FlowMetricsPeerDst.TotalPacketCount == 0 {
		flowExtra.timeout = f.SingleDirection
	}
	return closed
}

func (f *FlowExtra) updatePlatformData(meta *MetaPacket, reply bool) {
	endpointData := meta.EndpointData
	var srcInfo, dstInfo *EndpointInfo
	if endpointData == nil {
		return
	}
	taggedFlow := f.taggedFlow
	if reply {
		srcInfo = endpointData.DstInfo
		dstInfo = endpointData.SrcInfo
	} else {
		srcInfo = endpointData.SrcInfo
		dstInfo = endpointData.DstInfo
	}
	if srcInfo != nil {
		taggedFlow.FlowMetricsPeerSrc.EpcID = srcInfo.L2EpcId
		taggedFlow.FlowMetricsPeerSrc.DeviceType = DeviceType(srcInfo.L2DeviceType)
		taggedFlow.FlowMetricsPeerSrc.DeviceID = srcInfo.L2DeviceId
		taggedFlow.FlowMetricsPeerSrc.IsL2End = srcInfo.L2End
		taggedFlow.FlowMetricsPeerSrc.IsL3End = srcInfo.L3End
		taggedFlow.FlowMetricsPeerSrc.L3EpcID = srcInfo.L3EpcId
		taggedFlow.FlowMetricsPeerSrc.L3DeviceType = DeviceType(srcInfo.L3DeviceType)
		taggedFlow.FlowMetricsPeerSrc.L3DeviceID = srcInfo.L3DeviceId
		taggedFlow.FlowMetricsPeerSrc.Host = srcInfo.HostIp
		taggedFlow.FlowMetricsPeerSrc.SubnetID = srcInfo.SubnetId
		taggedFlow.GroupIDs0 = srcInfo.GroupIds
	}
	if dstInfo != nil {
		taggedFlow.FlowMetricsPeerDst.EpcID = dstInfo.L2EpcId
		taggedFlow.FlowMetricsPeerDst.DeviceType = DeviceType(dstInfo.L2DeviceType)
		taggedFlow.FlowMetricsPeerDst.DeviceID = dstInfo.L2DeviceId
		taggedFlow.FlowMetricsPeerDst.IsL2End = dstInfo.L2End
		taggedFlow.FlowMetricsPeerDst.IsL3End = dstInfo.L3End
		taggedFlow.FlowMetricsPeerDst.L3EpcID = dstInfo.L3EpcId
		taggedFlow.FlowMetricsPeerDst.L3DeviceType = DeviceType(dstInfo.L3DeviceType)
		taggedFlow.FlowMetricsPeerDst.L3DeviceID = dstInfo.L3DeviceId
		taggedFlow.FlowMetricsPeerDst.Host = dstInfo.HostIp
		taggedFlow.FlowMetricsPeerDst.SubnetID = dstInfo.SubnetId
		taggedFlow.GroupIDs1 = dstInfo.GroupIds
	}
}

func (f *FlowExtra) reversePolicyData() {
	if f.taggedFlow.PolicyData == nil {
		return
	}
	for i, aclAction := range f.taggedFlow.PolicyData.AclActions {
		f.taggedFlow.PolicyData.AclActions[i] = aclAction.ReverseDirection()
	}
}

func (f *FlowExtra) reverseFlow() {
	taggedFlow := f.taggedFlow
	taggedFlow.TunnelInfo.Src, taggedFlow.TunnelInfo.Dst = taggedFlow.TunnelInfo.Dst, taggedFlow.TunnelInfo.Src
	taggedFlow.MACSrc, taggedFlow.MACDst = taggedFlow.MACDst, taggedFlow.MACSrc
	taggedFlow.IPSrc, taggedFlow.IPDst = taggedFlow.IPDst, taggedFlow.IPSrc
	taggedFlow.PortSrc, taggedFlow.PortDst = taggedFlow.PortDst, taggedFlow.PortSrc
	taggedFlow.FlowMetricsPeerSrc, taggedFlow.FlowMetricsPeerDst = FlowMetricsPeerSrc(taggedFlow.FlowMetricsPeerDst), FlowMetricsPeerDst(taggedFlow.FlowMetricsPeerSrc)
	taggedFlow.GroupIDs0, taggedFlow.GroupIDs1 = taggedFlow.GroupIDs1, taggedFlow.GroupIDs0
	f.reversePolicyData()
}

func (f *FlowGenerator) tryReverseFlow(flowExtra *FlowExtra, meta *MetaPacket, reply bool) bool {
	taggedFlow := flowExtra.taggedFlow
	if flagContain(taggedFlow.FlowMetricsPeerSrc.TCPFlags|taggedFlow.FlowMetricsPeerDst.TCPFlags, TCP_SYN) || meta.TcpData == nil {
		return false
	}
	// if meta.Invalid is false, TcpData will not be nil
	if flagEqual(meta.TcpData.Flags&TCP_FLAG_MASK, TCP_SYN) && reply {
		flowExtra.reverseFlow()
		flowExtra.reversed = !flowExtra.reversed
		return true
	} else if flagEqual(meta.TcpData.Flags&TCP_FLAG_MASK, TCP_SYN|TCP_ACK) && !reply {
		flowExtra.reverseFlow()
		flowExtra.reversed = !flowExtra.reversed
		return true
	}
	return false
}

func (f *FlowGenerator) updateFlow(flowExtra *FlowExtra, meta *MetaPacket, reply bool) {
	taggedFlow := flowExtra.taggedFlow
	bytes := uint64(meta.PacketLen)
	packetTimestamp := meta.Timestamp
	maxArrTime := timeMax(taggedFlow.FlowMetricsPeerSrc.ArrTimeLast, taggedFlow.FlowMetricsPeerDst.ArrTimeLast)
	if taggedFlow.FlowMetricsPeerSrc.PacketCount == 0 && taggedFlow.FlowMetricsPeerDst.PacketCount == 0 {
		taggedFlow.CurStartTime = packetTimestamp
		taggedFlow.PolicyData = meta.PolicyData
		if flowExtra.reversed {
			flowExtra.reversePolicyData()
		}
		flowExtra.updatePlatformData(meta, reply)
	}
	if reply {
		if taggedFlow.FlowMetricsPeerDst.TotalPacketCount == 0 {
			taggedFlow.FlowMetricsPeerDst.ArrTime0 = packetTimestamp
		}
		if maxArrTime < packetTimestamp {
			taggedFlow.FlowMetricsPeerDst.ArrTimeLast = packetTimestamp
		} else {
			packetTimestamp = maxArrTime
		}
		taggedFlow.FlowMetricsPeerDst.PacketCount++
		taggedFlow.FlowMetricsPeerDst.TotalPacketCount++
		taggedFlow.FlowMetricsPeerDst.ByteCount += bytes
		taggedFlow.FlowMetricsPeerDst.TotalByteCount += bytes
	} else {
		if taggedFlow.FlowMetricsPeerSrc.TotalPacketCount == 0 {
			taggedFlow.FlowMetricsPeerSrc.ArrTime0 = packetTimestamp
		}
		if maxArrTime < packetTimestamp {
			taggedFlow.FlowMetricsPeerSrc.ArrTimeLast = packetTimestamp
		} else {
			packetTimestamp = maxArrTime
		}
		taggedFlow.FlowMetricsPeerSrc.PacketCount++
		taggedFlow.FlowMetricsPeerSrc.TotalPacketCount++
		taggedFlow.FlowMetricsPeerSrc.ByteCount += bytes
		taggedFlow.FlowMetricsPeerSrc.TotalByteCount += bytes
	}
	flowExtra.recentTime = packetTimestamp
	// a flow will report every minute and StartTime will be reset, so the value could not be overflow
	taggedFlow.TimeBitmap |= 1 << uint64((flowExtra.recentTime-taggedFlow.StartTime)/time.Second)
}

func (f *FlowExtra) setCurFlowInfo(now time.Duration, desireInterval time.Duration) {
	taggedFlow := f.taggedFlow
	// desireInterval should not be too small
	if now-taggedFlow.StartTime > desireInterval+REPORT_TOLERANCE {
		taggedFlow.EndTime = now - REPORT_TOLERANCE
	} else {
		taggedFlow.EndTime = now
	}
	minArrTime := timeMin(taggedFlow.FlowMetricsPeerSrc.ArrTime0, taggedFlow.FlowMetricsPeerDst.ArrTime0)
	if minArrTime == 0 {
		minArrTime = timeMax(taggedFlow.FlowMetricsPeerSrc.ArrTime0, taggedFlow.FlowMetricsPeerDst.ArrTime0)
	}
	taggedFlow.Duration = timeMax(taggedFlow.FlowMetricsPeerSrc.ArrTimeLast, taggedFlow.FlowMetricsPeerDst.ArrTimeLast) - minArrTime
}

func (f *FlowExtra) resetCurFlowInfo(now time.Duration) {
	taggedFlow := f.taggedFlow
	taggedFlow.TimeBitmap = 0
	taggedFlow.StartTime = now
	taggedFlow.EndTime = now
	taggedFlow.CurStartTime = now
	taggedFlow.FlowMetricsPeerSrc.PacketCount = 0
	taggedFlow.FlowMetricsPeerDst.PacketCount = 0
	taggedFlow.FlowMetricsPeerSrc.ByteCount = 0
	taggedFlow.FlowMetricsPeerDst.ByteCount = 0
	taggedFlow.TcpPerfStats = nil
}

func (f *FlowExtra) calcCloseType(force bool) {
	if force {
		f.taggedFlow.CloseType = CloseTypeForcedReport
		return
	}
	switch f.flowState {
	case FLOW_STATE_EXCEPTION:
		f.taggedFlow.CloseType = CloseTypeUnknown
	case FLOW_STATE_OPENING_1:
		f.taggedFlow.CloseType = CloseTypeServerHalfOpen
	case FLOW_STATE_OPENING_2:
		f.taggedFlow.CloseType = CloseTypeClientHalfOpen
	case FLOW_STATE_ESTABLISHED:
		f.taggedFlow.CloseType = CloseTypeTimeout
	case FLOW_STATE_CLOSING_TX1:
		f.taggedFlow.CloseType = CloseTypeServerHalfClose
	case FLOW_STATE_CLOSING_RX1:
		f.taggedFlow.CloseType = CloseTypeClientHalfClose
	case FLOW_STATE_CLOSING_TX2:
		fallthrough
	case FLOW_STATE_CLOSING_RX2:
		fallthrough
	case FLOW_STATE_CLOSED:
		f.taggedFlow.CloseType = CloseTypeTCPFin
	case FLOW_STATE_RESET:
		if flagContain(f.taggedFlow.FlowMetricsPeerDst.TCPFlags, TCP_RST) {
			f.taggedFlow.CloseType = CloseTypeTCPServerRst
		} else {
			f.taggedFlow.CloseType = CloseTypeTCPClientRst
		}
	default:
		log.Warningf("unexcepted 'unknown' close type, flow id is %d", f.taggedFlow.FlowID)
		f.taggedFlow.CloseType = CloseTypeUnknown
	}
}

func (f *FlowGenerator) processPackets(processBuffer []interface{}) {
	for _, e := range processBuffer {
		meta := e.(*MetaPacket)
		if meta.EthType != layers.EthernetTypeIPv4 {
			f.processNonIpPacket(meta)
		} else if meta.Protocol == layers.IPProtocolTCP {
			f.processTcpPacket(meta)
		} else if meta.Protocol == layers.IPProtocolUDP {
			f.processUdpPacket(meta)
		} else {
			f.processOtherIpPacket(meta)
		}
	}
	f.packetHandler.Done()
}

func (f *FlowGenerator) handlePackets() {
	metaPacketInQueue := f.metaPacketHeaderInQueue
	packetHandler := f.packetHandler
	recvBuffer := packetHandler.recvBuffer
	processBuffer := packetHandler.processBuffer
	gotSize := 0
	hashKey := HashKey(f.index)
loop:
	if !f.handleRunning {
		log.Info("flow fenerator packet handler exit")
		return
	}
	packetHandler.Add(1)
	go f.processPackets(processBuffer[:gotSize])
	gotSize = metaPacketInQueue.Gets(hashKey, recvBuffer)
	packetHandler.Wait()
	processBuffer, recvBuffer = recvBuffer, processBuffer
	goto loop
}

func (f *FlowGenerator) cleanHashMapByForce(hashMap []*FlowCache, start, end uint64, now time.Duration) {
	flowOutQueue := f.flowOutQueue
	forceReportInterval := f.forceReportInterval
	for _, flowCache := range hashMap[start:end] {
		if flowCache == nil {
			continue
		}
		flowCache.Lock()
		for e := flowCache.flowList.Front(); e != nil; {
			flowExtra := e.Value
			f.stats.CurrNumFlows--
			flowExtra.taggedFlow.TcpPerfStats = Report(flowExtra.metaFlowPerf, false, &f.perfCounter)
			flowExtra.setCurFlowInfo(now, forceReportInterval)
			flowExtra.calcCloseType(false)
			flowOutQueue.Put(flowExtra.taggedFlow)
			if flowExtra.metaFlowPerf != nil {
				f.ReleaseMetaFlowPerf(flowExtra.metaFlowPerf)
			}
			e = e.Next()
		}
		flowCache.flowList.Init()
		flowCache.Unlock()
	}
}

func (f *FlowGenerator) cleanTimeoutHashMap(hashMap []*FlowCache, start, end, index uint64) {
	flowOutQueue := f.flowOutQueue
	forceReportInterval := f.forceReportInterval
	sleepDuration := f.minLoopInterval
	f.cleanWaitGroup.Add(1)

loop:
	time.Sleep(sleepDuration)
	flowOutBuffer := [FLOW_OUT_BUFFER_CAP]interface{}{}
	flowOutNum := 0
	now := time.Duration(time.Now().UnixNano())
	cleanRange := now - f.minLoopInterval
	maxFlowCacheLen := 0
	nonEmptyFlowCacheNum := 0
	for _, flowCache := range hashMap[start:end] {
		flowList := flowCache.flowList
		len := flowList.Len()
		if len > 0 {
			nonEmptyFlowCacheNum++
		} else {
			continue
		}
		if maxFlowCacheLen <= len {
			maxFlowCacheLen = len
		}
		flowCache.Lock()
		e := flowList.Back()
		for e != nil {
			flowExtra := e.Value
			if flowExtra.recentTime < cleanRange && flowExtra.recentTime+flowExtra.timeout <= now {
				taggedFlow := flowExtra.taggedFlow
				f.stats.CurrNumFlows--
				flowExtra.setCurFlowInfo(now, forceReportInterval)
				if f.servicePortDescriptor.judgeServiceDirection(taggedFlow.PortSrc, taggedFlow.PortDst) {
					flowExtra.reverseFlow()
					flowExtra.reversed = !flowExtra.reversed
				}
				flowExtra.calcCloseType(false)
				taggedFlow.TcpPerfStats = Report(flowExtra.metaFlowPerf, flowExtra.reversed, &f.perfCounter)
				if flowExtra.metaFlowPerf != nil {
					f.ReleaseMetaFlowPerf(flowExtra.metaFlowPerf)
				}
				flowExtra.reset()
				flowOutBuffer[flowOutNum] = taggedFlow
				flowOutNum++
				if flowOutNum >= FLOW_OUT_BUFFER_CAP {
					flowOutQueue.Put(flowOutBuffer[:flowOutNum]...)
					flowOutNum = 0
				}
				del := e
				e = e.Prev()
				flowList.Remove(del)
				continue
			} else if flowExtra.taggedFlow.StartTime+forceReportInterval < now {
				taggedFlow := flowExtra.taggedFlow
				flowExtra.setCurFlowInfo(now, forceReportInterval)
				if f.servicePortDescriptor.judgeServiceDirection(taggedFlow.PortSrc, taggedFlow.PortDst) {
					flowExtra.reverseFlow()
					flowExtra.reversed = !flowExtra.reversed
				}
				flowExtra.calcCloseType(true)
				taggedFlow.TcpPerfStats = Report(flowExtra.metaFlowPerf, flowExtra.reversed, &f.perfCounter)
				putFlow := *taggedFlow
				flowOutBuffer[flowOutNum] = &putFlow
				flowOutNum++
				if flowOutNum >= FLOW_OUT_BUFFER_CAP {
					flowOutQueue.Put(flowOutBuffer[:flowOutNum]...)
					flowOutNum = 0
				}
				flowExtra.resetCurFlowInfo(now)
			}
			e = e.Prev()
		}
		flowCache.Unlock()
	}
	if flowOutNum > 0 {
		flowOutQueue.Put(flowOutBuffer[:flowOutNum]...)
		flowOutNum = 0
	}
	f.stats.cleanRoutineFlowCacheNums[index] = nonEmptyFlowCacheNum
	f.stats.cleanRoutineMaxFlowCacheLens[index] = maxFlowCacheLen
	if f.cleanRunning {
		goto loop
	}
	f.cleanHashMapByForce(hashMap, start, end, now)
	f.cleanWaitGroup.Done()
}

func (f *FlowGenerator) timeoutReport() {
	var flowCacheNum uint64
	f.cleanRunning = true
	if f.mapSize%f.timeoutParallelNum != 0 {
		flowCacheNum = f.mapSize/f.timeoutParallelNum + 1
	} else {
		flowCacheNum = f.mapSize / f.timeoutParallelNum
	}
	for i := uint64(0); i < f.timeoutParallelNum; i++ {
		start := i * flowCacheNum
		end := start + flowCacheNum
		if end <= f.mapSize {
			go f.cleanTimeoutHashMap(f.hashMap, start, end, i)
			log.Infof("clean goroutine %d (hashmap range %d to %d) created", i, start, end)
		} else {
			go f.cleanTimeoutHashMap(f.hashMap, start, f.mapSize, i)
			log.Infof("clean goroutine %d (hashmap range %d to %d) created", i, start, f.mapSize)
			break
		}
	}
}

func (f *FlowGenerator) run() {
	if !f.cleanRunning {
		f.cleanRunning = true
		f.timeoutReport()
	}
	if !f.handleRunning {
		f.handleRunning = true
		go f.handlePackets()
	}
}

// we need these goroutines are thread safe
func (f *FlowGenerator) Start() {
	f.run()
	log.Infof("flow generator %d started", f.index)
}

func (f *FlowGenerator) Stop() {
	if f.handleRunning {
		f.handleRunning = false
	}
	if f.cleanRunning {
		f.cleanRunning = false
		f.cleanWaitGroup.Wait()
	}
	log.Infof("flow generator %d stopped", f.index)
}

// create a new flow generator
func New(metaPacketHeaderInQueue MultiQueueReader, flowOutQueue QueueWriter, cfg FlowGeneratorConfig, index int) *FlowGenerator {
	flowGenerator := &FlowGenerator{
		TimeoutConfig:           defaultTimeoutConfig,
		FastPath:                FastPath{FlowCacheHashMap: FlowCacheHashMap{make([]*FlowCache, HASH_MAP_SIZE), rand.Uint32(), HASH_MAP_SIZE, TIMOUT_PARALLEL_NUM}},
		metaPacketHeaderInQueue: metaPacketHeaderInQueue,
		flowOutQueue:            flowOutQueue,
		stats:                   FlowGeneratorStats{cleanRoutineFlowCacheNums: make([]int, TIMOUT_PARALLEL_NUM), cleanRoutineMaxFlowCacheLens: make([]int, TIMOUT_PARALLEL_NUM)},
		stateMachineMaster:      make([]map[uint8]*StateValue, FLOW_STATE_EXCEPTION+1),
		stateMachineSlave:       make([]map[uint8]*StateValue, FLOW_STATE_EXCEPTION+1),
		innerFlowKey:            &FlowKey{},
		packetHandler:           &PacketHandler{recvBuffer: make([]interface{}, cfg.BufferSize), processBuffer: make([]interface{}, cfg.BufferSize)},
		servicePortDescriptor:   getServiceDescriptorWithIANA(),
		forceReportInterval:     cfg.ForceReportInterval,
		minLoopInterval:         defaultTimeoutConfig.minTimeout(),
		flowLimitNum:            cfg.FlowLimitNum,
		handleRunning:           false,
		cleanRunning:            false,
		index:                   index,
		perfCounter:             NewFlowPerfCounter(),
	}
	if !flowGenerator.initFlowCache() {
		return nil
	}
	flowGenerator.taggedFlowHandler.Init()
	flowGenerator.flowExtraHandler.Init()
	flowGenerator.initStateMachineMaster()
	flowGenerator.initStateMachineSlave()
	flowGenerator.initMetaFlowPerfPool()
	tags := OptionStatTags{"index": strconv.Itoa(index)}
	RegisterCountable("flow_generator", flowGenerator, tags)
	RegisterCountable(FP_NAME, &flowGenerator.perfCounter, tags)
	log.Infof("flow generator %d created", index)
	return flowGenerator
}
