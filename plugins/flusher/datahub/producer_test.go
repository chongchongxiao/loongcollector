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

package datahub

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/plugins/test/mock"
)

type mockDataHubAPI struct {
	datahub.DataHubApi
}

func (m *mockDataHubAPI) ListShard(projectName string, topicName string) (*datahub.ListShardResult, error) {
	return &datahub.ListShardResult{
		Shards: []datahub.ShardEntry{
			{ShardId: "0", State: "CLOSED"},
			{ShardId: "1", State: "ACTIVE"},
			{ShardId: "2", State: "ACTIVE"},
			{ShardId: "3", State: "ACTIVE"},
		},
		IntervalMs: 1000,
		CommonResponseResult: datahub.CommonResponseResult{
			StatusCode: 200,
			RequestId:  "requestId",
		},
	}, nil
}

func newMockDataHubAPI() *mockDataHubAPI {
	return &mockDataHubAPI{}
}

func TestFreshShardIds(t *testing.T) {
	s := &ProducerImpl{
		projectName: "test_project",
		topicName:   "test_topic",
		client:      newMockDataHubAPI(),
	}

	err := s.freshShardIDs(true)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(s.shardInfo.shardIDs))

	m := []string{"1", "2", "3"}
	for idx, shardID := range m {
		assert.Equal(t, shardID, s.shardInfo.shardIDs[idx])
	}
}

func TestGetShardId(t *testing.T) {
	s := &ProducerImpl{
		projectName: "test_project",
		topicName:   "test_topic",
		client:      newMockDataHubAPI(),
	}

	_, err := s.nextShardID()
	assert.Nil(t, err)
	s.shardInfo.index = 0
	shard, err := s.nextShardID()
	assert.Nil(t, err)
	assert.Equal(t, "2", shard)
	shard, err = s.nextShardID()
	assert.Nil(t, err)
	assert.Equal(t, "3", shard)
	shard, err = s.nextShardID()
	assert.Nil(t, err)
	assert.Equal(t, "1", shard)
	shard, err = s.nextShardID()
	assert.Nil(t, err)
	assert.Equal(t, "2", shard)
}

type sendingAPI struct {
	mockDataHubAPI
	failures int
	calls    int
	sealed   bool
}

func (a *sendingAPI) PutRecordsByShard(project, topic, shard string, records []datahub.IRecord) (*datahub.PutRecordsByShardResult, error) {
	a.calls++
	if a.sealed && a.calls == 1 {
		return nil, fmt.Errorf("wrapped: %w", &datahub.ShardSealedError{})
	}
	if a.calls <= a.failures {
		return nil, errors.New("temporary failure")
	}
	return &datahub.PutRecordsByShardResult{}, nil
}
func TestSendRetriesAndReturnsFinalFailure(t *testing.T) {
	for _, failures := range []int{1, 10} {
		t.Run(fmt.Sprint(failures), func(t *testing.T) {
			api := &sendingAPI{failures: failures}
			p := NewProducer("p", "t", api, mock.NewEmptyContext("p", "l", "c")).(*ProducerImpl)
			var waits []time.Duration
			p.sleep = func(d time.Duration) { waits = append(waits, d) }
			require.NoError(t, p.Init())
			err := p.Send([]datahub.IRecord{datahub.NewBlobRecord([]byte("value"))})
			if failures == 1 {
				require.NoError(t, err)
				require.Equal(t, 2, api.calls)
				require.Equal(t, []time.Duration{500 * time.Millisecond}, waits)
			} else {
				require.Error(t, err)
				require.Equal(t, 3, api.calls)
				require.Equal(t, []time.Duration{500 * time.Millisecond, time.Second}, waits)
			}
		})
	}
}
func TestSendRefreshesSealedShardWithoutOuterDelay(t *testing.T) {
	api := &sendingAPI{sealed: true}
	p := NewProducer("p", "t", api, mock.NewEmptyContext("p", "l", "c")).(*ProducerImpl)
	p.sleep = func(time.Duration) { t.Error("sealed shard should recover within DoSend") }
	require.NoError(t, p.Init())
	require.NoError(t, p.Send([]datahub.IRecord{datahub.NewBlobRecord([]byte("value"))}))
	require.Equal(t, 2, api.calls)
}

func TestSendSuccessLogsAtInfo(t *testing.T) {
	logger.ClearMemoryLog()
	p := NewProducer("p", "t", &sendingAPI{}, mock.NewEmptyContext("p", "l", "c"))
	require.NoError(t, p.Init())
	require.NoError(t, p.Send([]datahub.IRecord{datahub.NewBlobRecord([]byte("private-payload"))}))
	logger.Flush()
	found := false
	for i := 1; i <= logger.GetMemoryLogCount(); i++ {
		line, _ := logger.ReadMemoryLog(i)
		require.NotContains(t, line, "private-payload")
		if strings.Contains(line, "Flush datahub(p/t/") && strings.Contains(line, "success") {
			require.Contains(t, line, "[INF]")
			found = true
		}
	}
	require.True(t, found, "successful sends must be visible at INFO")
}
