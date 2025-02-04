/**
 * Copyright (c) 2023 Yunshan Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package generator

import (
	"golang.org/x/exp/slices"

	"github.com/deepflowio/deepflow/server/controller/http/service/resource/common"
	"github.com/deepflowio/deepflow/server/controller/http/service/resource/filter"
)

type PeerConnection struct {
	FilterGeneratorComponent
}

func NewPeerConnection() *PeerConnection {
	g := new(PeerConnection)
	g.SetConditionConvertor(g)
	return g
}

func (p *PeerConnection) conditionsMapToStruct(fcs common.FilterConditions) filter.Condition {
	c := filter.NewAND()
	c.InitSkippedFields = []string{"EPC_ID"}
	c.Init(fcs)
	if vpcIDs, ok := fcs["EPC_ID"]; ok {
		c.TryAppendIntFieldCondition(NewVPCIDCondition("EPC_ID", vpcIDs))
	}
	return c
}

func (p *PeerConnection) userPermittedResourceToConditions(upr *UserPermittedResource) (common.FilterConditions, bool) {
	return nil, false
}

type VPCIDCondition struct {
	filter.FieldConditionBase[float64]
}

func NewVPCIDCondition(key string, value interface{}) *VPCIDCondition {
	return &VPCIDCondition{filter.FieldConditionBase[float64]{Key: key, Value: filter.ConvertValueToSlice[float64](value)}}
}

func (p *VPCIDCondition) Keep(v common.ResponseElem) bool {
	if slices.Contains(p.Value, float64(v["REMOTE_EPC_ID"].(int))) || slices.Contains(p.Value, float64(v["LOCAL_EPC_ID"].(int))) {
		return true
	}
	return false
}
