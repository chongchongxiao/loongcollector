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

package odps

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
	"github.com/alibaba/ilogtail/plugins/test/mock"
)

// A failed SDK writer rejects further appends until replaced.
type testWriter struct {
	records    []data.Record
	failAppend bool
	failFlush  bool
	flushing   bool
	delivered  *[]data.Record
}

func (w *testWriter) Append(r data.Record) error {
	if w.failAppend || w.flushing {
		return errors.New("writer unavailable")
	}
	w.records = append(w.records, r)
	return nil
}
func (w *testWriter) Flush(_ ...time.Duration) (string, int64, int64, error) {
	w.flushing = true
	if w.failFlush {
		return "", 0, 0, errors.New("temporary outage")
	}
	*w.delivered = append(*w.delivered, w.records...)
	n := int64(len(w.records))
	w.records = nil
	w.flushing = false
	return "trace", n, n, nil
}
func (w *testWriter) DataSize() int64 { return int64(len(w.records)) }

func newTestCache() *SessionCache {
	return &SessionCache{context: mock.NewEmptyContext("p", "l", "c"), sessionMap: map[string]*SessionItem{}, expireTime: time.Minute, maxPartitions: 2, sleep: func(time.Duration) {}}
}

func TestFlushReplaysBatchWithFreshWriter(t *testing.T) {
	for _, appendFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "append"}[appendFailure], func(t *testing.T) {
			var delivered []data.Record
			attempts := 0
			item := &SessionItem{records: []data.Record{{data.String("one")}, {data.String("two")}}, newWriter: func() recordWriter {
				attempts++
				return &testWriter{failAppend: appendFailure && attempts == 1, failFlush: !appendFailure && attempts == 1, delivered: &delivered}
			}}
			sc := newTestCache()
			sc.sessionMap["p"] = item
			require.NoError(t, sc.Flush())
			require.Equal(t, []data.Record{{data.String("one")}, {data.String("two")}}, delivered)
			require.Empty(t, item.records)
		})
	}
}

func TestFlushExhaustionDoesNotBlockOtherPartitionsOrNextBatch(t *testing.T) {
	sc := newTestCache()
	var delivered []data.Record
	attempts := 0
	failed := &SessionItem{records: []data.Record{{data.String("lost")}}, newWriter: func() recordWriter {
		attempts++
		return &testWriter{failFlush: true, delivered: &delivered}
	}}
	sc.sessionMap["failed"] = failed
	sc.sessionMap["healthy"] = &SessionItem{records: []data.Record{{data.String("kept")}}, newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}
	require.Error(t, sc.Flush())
	require.Equal(t, 3, attempts)
	require.Equal(t, []data.Record{{data.String("kept")}}, delivered)
	require.Empty(t, failed.records)
	require.NotContains(t, sc.sessionMap, "failed")
	// A subsequent batch for the same partition gets a fresh session/writer.
	sc.sessionMap["failed"] = &SessionItem{records: []data.Record{{data.String("next")}}, newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}
	require.NoError(t, sc.Flush())
	require.Equal(t, []data.Record{{data.String("kept")}, {data.String("next")}}, delivered)
}

func TestCleanupFlushesExpiredBufferedRecords(t *testing.T) {
	sc := newTestCache()
	var delivered []data.Record
	sc.sessionMap["expired"] = &SessionItem{accessTime: time.Now().Add(-2 * time.Minute), records: []data.Record{{data.String("pending")}}, newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}
	sc.cleanup()
	require.Equal(t, []data.Record{{data.String("pending")}}, delivered)
	require.Empty(t, sc.sessionMap)
}

func TestCapacityFlushesOldestBeforeCreatingSession(t *testing.T) {
	sc := newTestCache()
	sc.maxPartitions = 1
	var delivered []data.Record
	sc.sessionMap["old"] = &SessionItem{accessTime: time.Now(), records: []data.Record{{data.String("old")}}, newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}
	sc.openSession = func(partition string) (*SessionItem, error) {
		return &SessionItem{partition: partition, accessTime: time.Now(), newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}, nil
	}
	_, err := sc.getSessionItem("new")
	require.NoError(t, err)
	require.Len(t, sc.sessionMap, 1)
	require.Contains(t, sc.sessionMap, "new")
	require.Equal(t, []data.Record{{data.String("old")}}, delivered)
}

func TestSessionCreationFailureIsBoundedPerBatch(t *testing.T) {
	sc := newTestCache()
	calls := 0
	sc.openSession = func(string) (*SessionItem, error) { calls++; return nil, errors.New("service down") }
	for range 3 {
		_, err := sc.getSessionItem("p")
		require.Error(t, err)
	}
	require.Equal(t, 1, calls)
	require.NoError(t, sc.Flush())
	_, err := sc.getSessionItem("p")
	require.Error(t, err)
	require.Equal(t, 2, calls)
}

func TestSessionLifecycleAndSendLogs(t *testing.T) {
	logger.InitTestLogger(logger.OptionInfoLevel, logger.OptionOpenMemoryReceiver, logger.OptionOpenConsole)
	logger.ClearMemoryLog()
	sc := newTestCache()
	var delivered []data.Record
	sc.openSession = func(partition string) (*SessionItem, error) {
		return &SessionItem{partition: partition, accessTime: time.Now(), newWriter: func() recordWriter { return &testWriter{delivered: &delivered} }}, nil
	}
	item, err := sc.getSessionItem("ds=20261028")
	require.NoError(t, err)
	item.records = []data.Record{{data.String("private-payload")}}
	require.NoError(t, sc.Flush())
	item.accessTime = time.Now().Add(-2 * time.Minute)
	sc.cleanup()
	logger.Flush()
	var logs strings.Builder
	for i := 1; i <= logger.GetMemoryLogCount(); i++ {
		line, _ := logger.ReadMemoryLog(i)
		require.NotContains(t, line, "private-payload")
		logs.WriteString(line)
		if strings.Contains(line, "ODPS session created") || strings.Contains(line, "ODPS cached session removed") {
			require.Contains(t, line, "[INF]")
		}
		if strings.Contains(line, "ODPS send succeeded") {
			require.Contains(t, line, "[INF]")
			require.Contains(t, line, "trace")
		}
	}
	require.Contains(t, logs.String(), "ODPS session created")
	require.Contains(t, logs.String(), "ODPS cached session removed")
	require.Contains(t, logs.String(), "expired")
	require.Contains(t, logs.String(), "ODPS send succeeded")
}

func TestSessionFailuresVisibleAtInfoLevel(t *testing.T) {
	logger.InitTestLogger(logger.OptionInfoLevel, logger.OptionOpenMemoryReceiver, logger.OptionOpenConsole)
	logger.ClearMemoryLog()
	sc := newTestCache()
	sc.openSession = func(string) (*SessionItem, error) { return nil, errors.New("service unavailable") }
	for range 2 {
		_, err := sc.getSessionItem("missing")
		require.Error(t, err)
	}
	var delivered []data.Record
	sc.sessionMap["failed"] = &SessionItem{partition: "failed", records: []data.Record{{data.String("private-payload")}}, newWriter: func() recordWriter { return &testWriter{failFlush: true, delivered: &delivered} }}
	require.Error(t, sc.Flush())
	logger.Flush()
	creates, sends := 0, 0
	for i := 1; i <= logger.GetMemoryLogCount(); i++ {
		line, _ := logger.ReadMemoryLog(i)
		require.NotContains(t, line, "private-payload")
		if strings.Contains(line, "ODPS session creation failed") {
			creates++
			require.Contains(t, line, "[ERR]")
			require.Contains(t, line, "service unavailable")
		}
		if strings.Contains(line, "ODPS send failed") {
			sends++
			require.Contains(t, line, "[ERR]")
			require.Contains(t, line, "temporary outage")
		}
	}
	require.Equal(t, 1, creates, "cached creation failures should not repeat within the batch")
	require.Equal(t, 3, sends, "each failed send attempt must be logged")
}

func TestPartitionFailuresCountDiscardedRecords(t *testing.T) {
	sc := newTestCache()
	sc.discarded = selfmonitor.NewCounterMetric(selfmonitor.MetricPluginDiscardedEventsTotal)
	ph := NewPartitionHelper()
	require.NoError(t, ph.Init("ds={{date}}", 15))
	sender := &TunnelSenderImpl{partition: ph, sessionCache: sc}
	flusher := &FlusherOdps{sender: sender}
	logs := []*protocol.Log{{}, {Contents: []*protocol.Log_Content{{Key: "date", Value: ""}}}}
	err := flusher.Flush("p", "l", "c", []*protocol.LogGroup{{Logs: logs}})
	require.ErrorContains(t, err, "missing partition field date")
	require.Equal(t, float64(2), sc.discarded.Collect().Value)
	require.Empty(t, sc.sessionMap)
	require.NoError(t, sender.Flush())
	require.Equal(t, float64(0), sc.discarded.Collect().Value)
}
