// Copyright 2025 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps

import (
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
)

type TunnelSender interface {
	Init() error
	Send(logGroup *protocol.LogGroup, log *protocol.Log) error
	Flush() error
}

func NewTunnelSender(context pipeline.Context, project, schema, table, partitionConfig string, timeRange, extraLevel int, tunnel *tunnel.Tunnel) *TunnelSenderImpl {
	cache := NewSessionCache(context, project, schema, table, tunnel)
	cache.recordBuilder = NewRecordBuilder(extraLevel)
	return &TunnelSenderImpl{
		context:         context,
		projectName:     project,
		schemaName:      schema,
		tableName:       table,
		partitionConfig: partitionConfig,
		timeRange:       timeRange,
		tunnelIns:       tunnel,
		partition:       NewPartitionHelper(),
		sessionCache:    cache,
	}
}

type TunnelSenderImpl struct {
	context         pipeline.Context
	projectName     string
	schemaName      string
	tableName       string
	partitionConfig string
	timeRange       int
	tunnelIns       *tunnel.Tunnel
	partition       *PartitionHelper
	sessionCache    *SessionCache
}

func (ts *TunnelSenderImpl) Init() error {
	ts.sessionCache.Init()

	if err := ts.partition.Init(ts.partitionConfig, ts.timeRange); err != nil {
		return err
	}

	logger.Infof(ts.context.GetRuntimeContext(), "Init odps tunnel sender success, config:[%s] columns:%v", ts.partitionConfig, ts.partition.columns)
	return nil
}
func (ts *TunnelSenderImpl) Send(logGroup *protocol.LogGroup, log *protocol.Log) error {
	partitionStr, err := ts.partition.GenPartition(log)
	if err != nil {
		if ts.sessionCache.discarded != nil {
			ts.sessionCache.discarded.Add(1)
		}
		return err
	}
	return ts.sessionCache.WriteLog(partitionStr, logGroup, log)
}

func (ts *TunnelSenderImpl) Flush() error { return ts.sessionCache.Flush() }
