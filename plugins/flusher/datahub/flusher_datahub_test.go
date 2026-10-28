// Copyright 2026 iLogtail Authors
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
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/plugins/test/mock"
)

type testBuilder struct{}

func (*testBuilder) Init() error { return nil }
func (*testBuilder) Log2Record(_ *protocol.LogGroup, log *protocol.Log) (datahub.IRecord, error) {
	if log.Time == 2 {
		return nil, errors.New("invalid number")
	}
	return datahub.NewBlobRecord([]byte{byte(log.Time)}), nil
}

type captureProducer struct{ records []datahub.IRecord }

func (*captureProducer) Init() error { return nil }
func (p *captureProducer) Send(records []datahub.IRecord) error {
	p.records = append(p.records, records...)
	return nil
}

func TestFlushIsolatesDirtyRecords(t *testing.T) {
	p := &captureProducer{}
	f := &FlusherDatahub{recordBuilder: &testBuilder{}, producer: p, context: mock.NewEmptyContext("p", "l", "c")}
	err := f.Flush("", "", "", []*protocol.LogGroup{
		{Logs: []*protocol.Log{{Time: 1}, {Time: 2}, {Time: 3}}},
		{Logs: []*protocol.Log{{Time: 4}}},
	})
	require.NoError(t, err)
	require.Len(t, p.records, 3)
}

func TestMain(m *testing.M) {
	logger.InitTestLogger(logger.OptionDebugLevel, logger.OptionOpenMemoryReceiver, logger.OptionOpenConsole)
	code := m.Run()
	logger.Flush()
	os.Exit(code)
}
func TestInitWithSDKDoesNotLogCredentials(t *testing.T) {
	logger.ClearMemoryLog()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/shards") {
			_, _ = w.Write([]byte(`{"Shards":[{"ShardId":"0","State":"ACTIVE"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"RecordType":"BLOB","ShardCount":1}`))
	}))
	defer server.Close()
	f := pipeline.Flushers["flusher_aliyun_datahub"]().(*FlusherDatahub)
	f.Endpoint = server.URL
	f.AccessKeyID = "test-access-id"
	f.AccessKeySecret = "test-private-secret"
	f.SecurityToken = "test-session-token"
	f.ProjectName = "test_project"
	f.TopicName = "test_topic"
	require.NoError(t, f.Init(mock.NewEmptyContext("p", "l", "c")))
	require.True(t, f.IsReady("", "", 0))
	require.NoError(t, f.Stop())
	logger.Flush()
	require.Positive(t, logger.GetMemoryLogCount())
	for i := 1; i <= logger.GetMemoryLogCount(); i++ {
		line, _ := logger.ReadMemoryLog(i)
		require.NotContains(t, line, f.AccessKeySecret)
		require.NotContains(t, line, f.SecurityToken)
	}
}
func TestInitRejectsInvalidConfig(t *testing.T) {
	f := NewFlusherDatahub()
	require.Error(t, f.validate())
	f.AccessKeyID = "test"
	f.AccessKeySecret = "test"
	f.ProjectName = "p"
	f.TopicName = "t"
	f.Endpoint = "https://example.invalid"
	require.NoError(t, f.validate())
	f.Endpoint = "file:///tmp/test"
	require.Error(t, f.validate())
}
