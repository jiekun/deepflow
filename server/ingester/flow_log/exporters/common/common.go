package common

import (
	"encoding/binary"
	"github.com/deepflowio/deepflow/server/libs/datatype"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"strconv"
	"strings"
)

// Return the first part after 'key' from the 'parts' array.
// Returns an empty string if 'key' does not exist or has no next part.
func getFirstPartAfterKey(key string, parts []string) string {
	for i := range parts {
		if strings.ToUpper(parts[i]) == key && len(parts) > i+1 {
			return parts[i+1]
		}
	}
	return ""
}

// Extract the database, table, and command from the SQL statement to form SpanName("${comman} ${db}.${table}")
// Returns "unknown","" if it cannot be fetched.
func GetSQLSpanNameAndOperation(sql string) (string, string) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "unknow", ""
	}
	parts := strings.Split(sql, " ")
	if len(parts) <= 2 {
		return parts[0], parts[0]
	}

	var command, dbTable string
	command = parts[0]
	parts = parts[1:]
	switch strings.ToUpper(command) {
	case "SELECT", "DELETE":
		dbTable = getFirstPartAfterKey("FROM", parts)
	case "INSERT":
		dbTable = getFirstPartAfterKey("INTO", parts)
	case "UPDATE":
		dbTable = parts[0]
	case "CREATE", "DROP":
		createType := strings.ToUpper(parts[0])
		if createType == "DATABASE" || createType == "TABLE" {
			// ignore 'if not exists' or 'if exists'
			if strings.ToUpper(parts[1]) == "IF" {
				dbTable = getFirstPartAfterKey("EXISTS", parts)
			} else {
				dbTable = parts[1]
			}
		}
	case "ALTER":
		dbTable = getFirstPartAfterKey("TABLE", parts)
	}

	if dbTable == "" {
		return command, command
	}
	if i := strings.Index(dbTable, "("); i > 0 {
		dbTable = dbTable[:i]
	} else {
		dbTable = strings.TrimRight(dbTable, ";")
	}
	return strings.Join([]string{command, dbTable}, " "), command
}

func ResponseStatusToSpanStatus(status uint8) ptrace.StatusCode {
	switch datatype.LogMessageStatus(status) {
	case datatype.STATUS_OK:
		return ptrace.StatusCodeOk
	case datatype.STATUS_CLIENT_ERROR, datatype.STATUS_SERVER_ERROR, datatype.STATUS_ERROR:
		return ptrace.StatusCodeError
	default:
		return ptrace.StatusCodeUnset
	}
}

func TapSideToSpanKind(tapSide string) ptrace.SpanKind {
	if IsClientSide(tapSide) {
		return ptrace.SpanKindClient
	} else if IsServerSide(tapSide) {
		return ptrace.SpanKindServer
	}
	return ptrace.SpanKindUnspecified
}

func IsClientSide(tapSide string) bool {
	return strings.HasPrefix(tapSide, "c")
}

func IsServerSide(tapSide string) bool {
	return strings.HasPrefix(tapSide, "s")
}

func Uint64ToSpanID(id uint64) pcommon.SpanID {
	b := [8]byte{0}
	binary.BigEndian.PutUint64(b[:], uint64(id))
	return pcommon.SpanID(b)
}

func GenTraceID(id int) pcommon.TraceID {
	b := [16]byte{0}
	binary.BigEndian.PutUint64(b[:], uint64(id))
	return pcommon.TraceID(b)
}

func RapPortTypeToString(tapPortType uint8) string {
	switch tapPortType {
	case 0:
		return "Local NIC"
	case 1:
		return "NFV Gateway NIC"
	case 2:
		return "ERSPAN"
	case 3:
		return "ERSPAN (IPv6)"
	case 4:
		return "Traffic Mirror"
	case 5:
		return "NetFlow"
	case 6:
		return "sFlow"
	case 7:
		return "eBPF"
	case 8:
		return "OTel"
	}
	return strconv.Itoa(int(tapPortType))
}

func RapSideToName(tapSide string) string {
	switch tapSide {
	case "c":
		return "Client NIC"
	case "c-nd":
		return "Client K8s Node"
	case "c-hv":
		return "Client VM Hypervisor"
	case "c-gw-hv":
		return "Client-side Gateway Hypervisor"
	case "c-gw":
		return "Client-side Gateway"
	case "local":
		return "Local NIC"
	case "rest":
		return "Other NIC"
	case "s-gw":
		return "Server-side Gateway"
	case "s-gw-hv":
		return "Server-side Gateway Hypervisor"
	case "s-hv":
		return "Server VM Hypervisor"
	case "s-nd":
		return "Server K8s Node"
	case "s":
		return "Server NIC"
	case "c-p":
		return "Client Process"
	case "s-p":
		return "Server Process"
	case "c-app":
		return "Client Application"
	case "s-app":
		return "Server Application"
	case "app":
		return "Application"

	}
	return tapSide
}

type ExportItem interface {
	Release()
}
