package prometheus_exporter

import (
	"net/url"
	"strings"
)

func GetMySQLEndpoint(sql string) (string, string) {
	// e.g.: SELECT / SELECT my_tab
	stmtType, tableName := GetStmtTypeAndTableName(sql)
	return stmtType, stmtType + " " + tableName
}

func GetRedisEndpoint(cmd string) (string, string) {
	if readCommandMap[strings.ToLower(cmd)] {
		return "read", cmd
	} else if writeCommandMap[strings.ToLower(cmd)] {
		return "write", cmd
	}
	return "unknown", cmd
}

func GetKafkaEndpoint(requestDomain string) (string, string) {
	return requestDomain, requestDomain
}

func GetMQTTEndpoint(requestDomain string) (string, string) {
	return requestDomain, requestDomain
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
