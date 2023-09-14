package prometheus_exporter

import (
	"net"
	"net/url"
	"strings"
)

func GetMySQLEndpoint(ipv4 net.IP, sql string) (string, string) {
	targetIP := ""
	if len(ipv4) > 0 {
		targetIP = ipv4.String() + " "
	}
	// e.g.: SELECT / SELECT my_tab
	stmtType, tableName := GetStmtTypeAndTableName(sql)
	return targetIP + stmtType, targetIP + stmtType + " " + tableName
}

func GetRedisEndpoint(ipv4 net.IP, cmd string) (string, string) {
	targetIP := ""
	if len(ipv4) > 0 {
		targetIP = ipv4.String() + " "
	}
	if readCommandMap[strings.ToLower(cmd)] {
		return targetIP + "read", cmd
	} else if writeCommandMap[strings.ToLower(cmd)] {
		return targetIP + "write", cmd
	}
	return targetIP + "unknown", cmd
}

func GetKafkaEndpoint(ipv4 net.IP, requestDomain string) (string, string) {
	targetIP := ""
	if len(ipv4) > 0 {
		targetIP = ipv4.String() + " "
	}
	return targetIP + requestDomain, targetIP + requestDomain
}

func GetMQTTEndpoint(ipv4 net.IP, requestDomain string) (string, string) {
	targetIP := ""
	if len(ipv4) > 0 {
		targetIP = ipv4.String() + " "
	}
	return targetIP + requestDomain, targetIP + requestDomain
}

func GetGRPCEndpoint(rpcPath string) (string, string) {
	// "/Account.Account/GetUsersByUids"
	// split the first part as summary, use the full path as detail

	pathSlice := strings.SplitN(strings.TrimPrefix(rpcPath, "/"), "/", 2)
	if len(pathSlice) >= 2 {
		return pathSlice[0], rpcPath
	}
	return rpcPath, rpcPath
}

func GetHTTPEndpoint(requestDomain, requestResource string) (string, string) {
	var summaryEndpoint, detailEndpoint string
	if strings.HasPrefix(strings.ToLower(requestResource), "http") {
		detailEndpoint = requestResource
	} else {
		detailEndpoint = requestDomain + requestResource
	}

	// try to remove query params
	log.Debugf("HTTP endpoint: %s", detailEndpoint)
	tmpEndpoint := detailEndpoint
	if !strings.HasPrefix(detailEndpoint, "http") {
		tmpEndpoint = "https://" + detailEndpoint
	}
	if u, err := url.ParseRequestURI(tmpEndpoint); err == nil {
		summaryEndpoint = u.Host
		detailEndpoint = u.Host + u.Path
		log.Debugf("HTTP new endpoint: %s", detailEndpoint)
	}
	return summaryEndpoint, detailEndpoint
}
