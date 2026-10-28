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
	"errors"
	"fmt"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
)

const DefaultPartition = "default"
const DefaultExpireTime = 5 * time.Minute
const defaultMaxCachedPartitions = 128
const maxSendAttempts = 3
const requestTimeout = 30 * time.Second

// recordWriter isolates the SDK writer lifecycle from batch ownership.
type recordWriter interface {
	Append(data.Record) error
	Flush(...time.Duration) (string, int64, int64, error)
	DataSize() int64
}

// SessionItem keeps batch records outside the writer so failed writes can be replayed.
// The v1 runner invokes Flush serially; these buffers are not shared across goroutines.
type SessionItem struct {
	partition  string
	schema     *tableschema.TableSchema
	newWriter  func() recordWriter
	records    []data.Record
	accessTime time.Time
}

type SessionCache struct {
	creationErrors                     map[string]error
	openSession                        func(string) (*SessionItem, error)
	context                            pipeline.Context
	projectName, schemaName, tableName string
	tunnelIns                          *tunnel.Tunnel
	sessionMap                         map[string]*SessionItem
	expireTime                         time.Duration
	maxPartitions                      int
	recordBuilder                      RecordBuilder
	sleep                              func(time.Duration)
	sent, discarded, failed            selfmonitor.CounterMetric
}

func NewSessionCache(context pipeline.Context, project, schema, table string, tunnelIns *tunnel.Tunnel) *SessionCache {
	sc := &SessionCache{
		context: context, projectName: project, schemaName: schema, tableName: table, tunnelIns: tunnelIns,
		sessionMap: make(map[string]*SessionItem), expireTime: DefaultExpireTime, maxPartitions: defaultMaxCachedPartitions,
		recordBuilder: NewRecordBuilder(1), sleep: time.Sleep,
	}
	sc.openSession = sc.createSessionItem
	return sc
}

func (sc *SessionCache) Init() {
	sc.recordBuilder.Init()
	mr := sc.context.GetMetricRecord()
	sc.sent = selfmonitor.NewCounterMetricAndRegister(mr, selfmonitor.MetricPluginOutSuccessfulEventsTotal)
	sc.discarded = selfmonitor.NewCounterMetricAndRegister(mr, selfmonitor.MetricPluginDiscardedEventsTotal)
	sc.failed = selfmonitor.NewCounterMetricAndRegister(mr, selfmonitor.MetricPluginOutFailedEventsTotal)
}

func (sc *SessionCache) createSession(partition string) (*tunnel.StreamUploadSession, error) {
	opts := []tunnel.Option{tunnel.SessionCfg.WithDefaultDeflateCompressor()}
	if partition != DefaultPartition {
		opts = append(opts, tunnel.SessionCfg.WithPartitionKey(partition), tunnel.SessionCfg.WithCreatePartition())
	}
	if sc.schemaName != "" {
		opts = append(opts, tunnel.SessionCfg.WithSchemaName(sc.schemaName))
	}
	return sc.tunnelIns.CreateStreamUploadSession(sc.projectName, sc.tableName, opts...)
}

func (sc *SessionCache) createSessionItem(partition string) (*SessionItem, error) {
	session, err := sc.createSession(partition)
	if err != nil {
		return nil, fmt.Errorf("create upload session: %w", err)
	}
	return &SessionItem{partition: partition, schema: session.Schema(), newWriter: func() recordWriter { return session.OpenRecordPackWriter() }, accessTime: time.Now()}, nil
}

// evict handles pending records before removing the local session, including on failure.
func (sc *SessionCache) evict(key, reason string) {
	if err := sc.flushOneWriter(sc.sessionMap[key]); err != nil {
		logger.Error(sc.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "evict session failed", key, "error", err)
	}
	sc.removeSession(key, reason)
}

// removeSession only removes the local cache; it does not delete the remote partition.
func (sc *SessionCache) removeSession(key, reason string) {
	delete(sc.sessionMap, key)
	logger.Infof(sc.context.GetRuntimeContext(), "ODPS cached session removed, project:%s, schema:%s, table:%s, partition:%s, reason:%s, cachedSessions:%d",
		sc.projectName, sc.schemaName, sc.tableName, key, reason, len(sc.sessionMap))
}

func (sc *SessionCache) cleanup() {
	now := time.Now()
	for key, item := range sc.sessionMap {
		if now.Sub(item.accessTime) > sc.expireTime {
			sc.evict(key, "expired")
		}
	}
}

func (sc *SessionCache) getSessionItem(partition string) (*SessionItem, error) {
	if err := sc.creationErrors[partition]; err != nil {
		return nil, err
	}
	sc.cleanup()
	if item, ok := sc.sessionMap[partition]; ok {
		item.accessTime = time.Now()
		return item, nil
	}
	if len(sc.sessionMap) >= sc.maxPartitions {
		var oldest string
		var oldestTime time.Time
		for key, item := range sc.sessionMap {
			if oldestTime.IsZero() || item.accessTime.Before(oldestTime) {
				oldest, oldestTime = key, item.accessTime
			}
		}
		sc.evict(oldest, "capacity")
	}
	start := time.Now()
	item, err := sc.openSession(partition)
	if err != nil {
		if sc.creationErrors == nil {
			sc.creationErrors = make(map[string]error)
		}
		sc.creationErrors[partition] = err
		logger.Errorf(sc.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "ODPS session creation failed, project:%s, schema:%s, table:%s, partition:%s, cost:%v, error:%v",
			sc.projectName, sc.schemaName, sc.tableName, partition, time.Since(start), err)
		return nil, err
	}
	sc.sessionMap[partition] = item
	logger.Infof(sc.context.GetRuntimeContext(), "ODPS session created, project:%s, schema:%s, table:%s, partition:%s, cost:%v, cachedSessions:%d",
		sc.projectName, sc.schemaName, sc.tableName, partition, time.Since(start), len(sc.sessionMap))
	return item, nil
}

func (sc *SessionCache) WriteLog(partition string, group *protocol.LogGroup, log *protocol.Log) error {
	item, err := sc.getSessionItem(partition)
	if err != nil {
		if sc.discarded != nil {
			sc.discarded.Add(1)
		}
		if sc.failed != nil {
			sc.failed.Add(1)
		}
		return err
	}
	record, err := sc.recordBuilder.Log2Record(group, log, item.schema)
	if err != nil {
		if sc.discarded != nil {
			sc.discarded.Add(1)
		}
		// Do not include raw log contents or credentials in diagnostics.
		logger.Warning(sc.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "discard invalid ODPS record", "schema conversion failed", "error", err)
		return nil
	}
	item.records = append(item.records, record)
	return nil
}

func (sc *SessionCache) flushOneWriter(item *SessionItem) error {
	if len(item.records) == 0 {
		return nil
	}
	count := int64(len(item.records))
	// Batch-local replay only: exhausted batches must not poison the next batch.
	defer func() { item.records = nil }()
	var lastErr error
	for attempt := 0; attempt < maxSendAttempts; attempt++ {
		start := time.Now()
		var traceID string
		var sentRecords, sentBytes int64
		writer := item.newWriter()
		lastErr = nil
		for _, record := range item.records {
			if lastErr = writer.Append(record); lastErr != nil {
				break
			}
		}
		if lastErr == nil {
			traceID, sentRecords, sentBytes, lastErr = writer.Flush(requestTimeout)
		}
		if lastErr == nil {
			if sc.sent != nil {
				sc.sent.Add(count)
			}
			logger.Infof(sc.context.GetRuntimeContext(), "ODPS send succeeded, project:%s, schema:%s, table:%s, partition:%s, traceId:%s, records:%d, bytes:%d, attempt:%d, cost:%v",
				sc.projectName, sc.schemaName, sc.tableName, item.partition, traceID, sentRecords, sentBytes, attempt+1, time.Since(start))
			return nil
		}
		logger.Errorf(sc.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "ODPS send failed, project:%s, schema:%s, table:%s, partition:%s, records:%d, attempt:%d/%d, cost:%v, error:%v",
			sc.projectName, sc.schemaName, sc.tableName, item.partition, count, attempt+1, maxSendAttempts, time.Since(start), lastErr)
		if attempt+1 < maxSendAttempts {
			sc.sleep(500 * time.Millisecond * time.Duration(1<<attempt))
		}
	}
	if sc.failed != nil {
		sc.failed.Add(count)
	}
	if sc.discarded != nil {
		sc.discarded.Add(count)
	}
	logger.Critical(sc.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "discard ODPS records after retries", count, "partition", item.partition)
	return fmt.Errorf("send ODPS partition %s after %d attempts: %w", item.partition, maxSendAttempts, lastErr)
}

func (sc *SessionCache) Flush() error {
	defer func() { sc.creationErrors = nil }()
	var errs []error
	for key, item := range sc.sessionMap {
		if err := sc.flushOneWriter(item); err != nil {
			errs = append(errs, err)
			sc.removeSession(key, "send_failed")
		}
	}
	return errors.Join(errs...)
}
