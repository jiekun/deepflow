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

package appender

import (
	"github.com/deepflowio/deepflow/server/controller/config"
	"github.com/deepflowio/deepflow/server/controller/http/common/registrant"
	"github.com/deepflowio/deepflow/server/controller/http/router/configuration"
	"github.com/deepflowio/deepflow/server/controller/http/router/resource"
)

func GetRegistrants(cfg *config.ControllerConfig) []registrant.Registrant {
	return []registrant.Registrant{
		configuration.NewConfiguration(), // TODO delete

		resource.NewVPC(cfg.HTTPCfg, cfg.RedisCfg, cfg.FPermit),
	}
}
