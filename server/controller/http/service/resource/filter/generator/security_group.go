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

type SecurityGroup struct {
	FilterGeneratorComponent
}

func NewSecurityGroup() *SecurityGroup {
	g := new(SecurityGroup)
	g.SetConditionConvertor(g)
	return g
}

func (p *SecurityGroup) conditionsMapToStruct(fcs common.FilterConditions) filter.Condition {
	log.Info(fcs)
	c := filter.NewAND()
	c.InitSkippedFields = []string{"VM_ID"}
	c.Init(fcs)
	if vmIDs, ok := fcs["VM_ID"]; ok {
		c.TryAppendIntFieldCondition(NewVMIDCondition("VM_ID", vmIDs))
	}
	return c
}

func (p *SecurityGroup) userPermittedResourceToConditions(upr *UserPermittedResource) (common.FilterConditions, bool) {
	return nil, false
}

type VMIDCondition struct {
	filter.FieldConditionBase[float64]
}

func NewVMIDCondition(key string, value interface{}) *VMIDCondition {
	return &VMIDCondition{filter.FieldConditionBase[float64]{Key: key, Value: filter.ConvertValueToSlice[float64](value)}}
}

func (p *VMIDCondition) Keep(v common.ResponseElem) bool {
	log.Info(p.Value)
	vms := v["VMS"].([]map[string]interface{})
	for _, item := range vms {
		if slices.Contains(p.Value, float64(item["ID"].(int))) {
			return true
		}
	}
	return false
}
