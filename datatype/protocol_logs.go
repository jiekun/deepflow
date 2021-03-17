package datatype

import (
	"fmt"
	"net"
	"time"

	"gitlab.x.lan/yunshan/droplet-libs/codec"
	"gitlab.x.lan/yunshan/droplet-libs/pool"
	"gitlab.x.lan/yunshan/droplet-libs/utils"
)

type LogProtoType uint8

const (
	PROTO_UNKOWN LogProtoType = iota
	PROTO_HTTP
	PROTO_DNS
)

func (t *LogProtoType) String() string {
	formatted := ""
	switch *t {
	case PROTO_HTTP:
		formatted = "HTTP"
	case PROTO_DNS:
		formatted = "DNS"
	default:
		formatted = "UNKOWN"
	}

	return formatted
}

type LogMessageType uint8

// 仅针对HTTP,DNS
const (
	MSG_T_REQUEST LogMessageType = iota
	MSG_T_RESPONSE
)

func (t *LogMessageType) String() string {
	formatted := ""
	if *t == MSG_T_REQUEST {
		formatted = "REQUEST"
	} else {
		formatted = "RESPONSE"
	}

	return formatted
}

type AppProtoHead struct {
	Proto   LogProtoType
	MsgType LogMessageType // HTTP，DNS: request/response
	Code    uint16         // HTTP状态码: 1xx-5xx, DNS状态码: 0-7
	RRT     time.Duration  // HTTP，DNS时延: response-request
}

type AppProtoLogsBaseInfo struct {
	Timestamp time.Duration // packet时间戳
	FlowId    uint64        // 对应flow的ID
	VtapId    uint16
	TapType   uint16
	AppProtoHead

	/* L3 */
	IPSrc IPv4Int
	IPDst IPv4Int
	/* L3 IPv6 */
	IP6Src [net.IPv6len]byte
	IP6Dst [net.IPv6len]byte
	/* L4 */
	PortSrc uint16
	PortDst uint16
	/* L3EpcID */
	L3EpcIDSrc int32
	L3EpcIDDst int32
}

func (i *AppProtoLogsBaseInfo) String() string {
	formatted := ""
	formatted += fmt.Sprintf("Timestamp: %v ", i.Timestamp)
	formatted += fmt.Sprintf("FlowId: %v ", i.FlowId)
	formatted += fmt.Sprintf("VtapId: %v ", i.VtapId)
	formatted += fmt.Sprintf("TapType: %v ", i.TapType)
	formatted += fmt.Sprintf("Proto: %s ", i.Proto.String())
	formatted += fmt.Sprintf("MsgType: %s ", i.MsgType.String())
	formatted += fmt.Sprintf("Code: %v ", i.Code)
	formatted += fmt.Sprintf("RRT: %v ", i.RRT)

	if len(i.IP6Src) > 0 {
		formatted += fmt.Sprintf("IP6Src: %s ", i.IP6Src)
		formatted += fmt.Sprintf("IP6Dst: %s ", i.IP6Dst)

	} else {
		formatted += fmt.Sprintf("IPSrc: %s ", utils.IpFromUint32(i.IPSrc))
		formatted += fmt.Sprintf("IPDst: %s ", utils.IpFromUint32(i.IPDst))
	}
	formatted += fmt.Sprintf("PortSrc: %v ", i.PortSrc)
	formatted += fmt.Sprintf("PortDst: %v ", i.PortDst)
	formatted += fmt.Sprintf("L3EpcIDSrc: %v ", i.L3EpcIDSrc)
	formatted += fmt.Sprintf("L3EpcIDDst: %v", i.L3EpcIDDst)

	return formatted
}

type AppProtoLogsData struct {
	AppProtoLogsBaseInfo
	Detail ProtoSpecialInfo

	pool.ReferenceCount
}

var httpInfoPool = pool.NewLockFreePool(func() interface{} {
	return new(HTTPInfo)
})

func AcquireHTTPInfo() *HTTPInfo {
	return httpInfoPool.Get().(*HTTPInfo)
}

func ReleaseHTTPInfo(h *HTTPInfo) {
	*h = HTTPInfo{}
	httpInfoPool.Put(h)
}

var dnsInfoPool = pool.NewLockFreePool(func() interface{} {
	return new(DNSInfo)
})

func AcquireDNSInfo() *DNSInfo {
	return dnsInfoPool.Get().(*DNSInfo)
}

func ReleaseDNSInfo(d *DNSInfo) {
	*d = DNSInfo{}
	dnsInfoPool.Put(d)
}

var appProtoLogsDataPool = pool.NewLockFreePool(func() interface{} {
	return new(AppProtoLogsData)
})
var zeroAppProtoLogsData = AppProtoLogsData{}

func AcquireAppProtoLogsData() *AppProtoLogsData {
	d := appProtoLogsDataPool.Get().(*AppProtoLogsData)
	d.Reset()
	return d
}

func ReleaseAppProtoLogsData(d *AppProtoLogsData) {
	if d.SubReferenceCount() {
		return
	}

	if d.Proto == PROTO_HTTP {
		ReleaseHTTPInfo(d.Detail.(*HTTPInfo))
	} else if d.Proto == PROTO_DNS {
		ReleaseDNSInfo(d.Detail.(*DNSInfo))
	}

	*d = zeroAppProtoLogsData
	appProtoLogsDataPool.Put(d)
}

func CloneAppProtoLogsData(d *AppProtoLogsData) *AppProtoLogsData {
	newAppProtoLogsData := AcquireAppProtoLogsData()
	*newAppProtoLogsData = *d
	newAppProtoLogsData.Reset()
	return newAppProtoLogsData
}

func (l *AppProtoLogsData) String() string {
	return fmt.Sprintf("base info: %s, Detail info: %s",
		l.AppProtoLogsBaseInfo.String(), l.Detail.String())
}

func (l *AppProtoLogsData) Release() {
	ReleaseAppProtoLogsData(l)
}

func (l *AppProtoLogsData) Encode(encoder *codec.SimpleEncoder) error {
	encoder.WriteU64(uint64(l.Timestamp))
	encoder.WriteU64(l.FlowId)
	encoder.WriteU16(l.VtapId)
	encoder.WriteU16(l.TapType)
	encoder.WriteU8(byte(l.Proto))
	encoder.WriteU8(byte(l.MsgType))
	encoder.WriteU16(l.Code)
	encoder.WriteU64(uint64(l.RRT))

	if len(l.IP6Src) > 0 {
		encoder.WriteBool(true) // 额外encode bool, decode时需要根据该bool, 来判断是否decode ipv6
		encoder.WriteIPv6(l.IP6Src[:])
		encoder.WriteIPv6(l.IP6Dst[:])
	} else {
		encoder.WriteBool(false)
		encoder.WriteU32(uint32(l.IPSrc))
		encoder.WriteU32(uint32(l.IPDst))
	}
	encoder.WriteU16(l.PortSrc)
	encoder.WriteU16(l.PortDst)
	encoder.WriteU32(uint32(l.L3EpcIDSrc))
	encoder.WriteU32(uint32(l.L3EpcIDDst))

	l.Detail.Encode(encoder, l.MsgType, l.Code)
	return nil
}

func (l *AppProtoLogsData) Decode(decoder *codec.SimpleDecoder) error {
	l.Timestamp = time.Duration(decoder.ReadU64())
	l.FlowId = decoder.ReadU64()
	l.VtapId = decoder.ReadU16()
	l.TapType = decoder.ReadU16()
	l.Proto = LogProtoType(decoder.ReadU8())
	l.MsgType = LogMessageType(decoder.ReadU8())
	l.Code = decoder.ReadU16()
	l.RRT = time.Duration(decoder.ReadU64())

	if decoder.ReadBool() {
		decoder.ReadIPv6(l.IP6Src[:])
		decoder.ReadIPv6(l.IP6Dst[:])
	} else {
		l.IPSrc = decoder.ReadU32()
		l.IPDst = decoder.ReadU32()
	}
	l.PortSrc = decoder.ReadU16()
	l.PortDst = decoder.ReadU16()
	l.L3EpcIDSrc = int32(decoder.ReadU32())
	l.L3EpcIDDst = int32(decoder.ReadU32())

	if l.Proto == PROTO_HTTP {
		httpInfo := AcquireHTTPInfo()
		httpInfo.Decode(decoder, l.MsgType, l.Code)
		l.Detail = httpInfo
	} else if l.Proto == PROTO_DNS {
		dnsInfo := AcquireDNSInfo()
		dnsInfo.Decode(decoder, l.MsgType, l.Code)
		l.Detail = dnsInfo
	}

	return nil
}

type ProtoSpecialInfo interface {
	Encode(encoder *codec.SimpleEncoder, msgType LogMessageType, code uint16)
	Decode(decoder *codec.SimpleDecoder, msgType LogMessageType, code uint16)
	String() string
}

// HTTPv2根据需要添加
type HTTPInfo struct {
	StreamID      uint32 // HTTPv2
	ContentLength int64
	Version       string
	Method        string
	Path          string
	Host          string
	ClientIP      string
	TraceID       string
}

func (h *HTTPInfo) Encode(encoder *codec.SimpleEncoder, msgType LogMessageType, code uint16) {
	encoder.WriteU32(h.StreamID)
	encoder.WriteU64(uint64(h.ContentLength))
	encoder.WriteString255(h.Version)
	if msgType == MSG_T_REQUEST {
		encoder.WriteString255(h.Method)
		encoder.WriteString255(h.Path)
		encoder.WriteString255(h.Host)
		encoder.WriteString255(h.ClientIP)
	}
	encoder.WriteString255(h.TraceID)
}

func (h *HTTPInfo) Decode(decoder *codec.SimpleDecoder, msgType LogMessageType, code uint16) {
	h.StreamID = decoder.ReadU32()
	h.ContentLength = int64(decoder.ReadU64())
	h.Version = decoder.ReadString255()
	if msgType == MSG_T_REQUEST {
		h.Method = decoder.ReadString255()
		h.Path = decoder.ReadString255()
		h.Host = decoder.ReadString255()
		h.ClientIP = decoder.ReadString255()
	}
	h.TraceID = decoder.ReadString255()
}

func (h *HTTPInfo) String() string {
	return fmt.Sprintf("%#v", h)
}

// | type | 查询类型 | 说明|
// | ---- | -------- | --- |
// | 1	  | A	     |由域名获得IPv4地址|
// | 2	  | NS	     |查询域名服务器|
// | 5	  | CNAME    |查询规范名称|
// | 6	  | SOA	     |开始授权|
// | 11	  | WKS	     |熟知服务|
// | 12	  | PTR	     |把IP地址转换成域名|
// | 13	  | HINFO	 |主机信息|
// | 15	  | MX	     |邮件交换|
// | 28	  | AAAA	 |由域名获得IPv6地址|
// | 252  | AXFR	 |传送整个区的请求|
// | 255  | ANY      |对所有记录的请求|
type DNSInfo struct {
	TransID   uint16
	QueryType uint16
	QueryName string
	// 根据查询类型的不同而不同，如：
	// A: ipv4/ipv6地址
	// NS: name server
	// SOA: primary name server
	Answers string
}

func (d *DNSInfo) Encode(encoder *codec.SimpleEncoder, msgType LogMessageType, code uint16) {
	encoder.WriteU16(d.TransID)
	encoder.WriteU16(d.QueryType)
	if msgType == MSG_T_REQUEST {
		encoder.WriteString255(d.QueryName)
	} else {
		encoder.WriteString255(d.Answers)
	}
}

func (d *DNSInfo) Decode(decoder *codec.SimpleDecoder, msgType LogMessageType, code uint16) {
	d.TransID = decoder.ReadU16()
	d.QueryType = decoder.ReadU16()
	if msgType == MSG_T_REQUEST {
		d.QueryName = decoder.ReadString255()
	} else {
		d.Answers = decoder.ReadString255()
	}
}

func (d *DNSInfo) String() string {
	return fmt.Sprintf("%#v", d)
}
