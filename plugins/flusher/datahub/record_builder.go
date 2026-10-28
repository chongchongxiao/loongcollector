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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/shopspring/decimal"

	"github.com/alibaba/ilogtail/pkg/config"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
)

const (
	schemaFreshInterval = 3 * time.Minute
	blobContentColumn   = "content"
)

const (
	HostIPKey      = "host_ip"
	CollectPathKey = "path"
	HostnameKey    = "hostname"
	CollectTimeKey = "collect_time"
	FlushTimeKey   = "flush_time"
)

const (
	LogtailPath  = "__path__"
	LogtailTopic = "__topic__"
)

type RecordBuilder interface {
	Init() error
	Log2Record(logGroup *protocol.LogGroup, log *protocol.Log) (datahub.IRecord, error)
}

func NewRecordBuilder(projectName string, topicName string, extraLevel int, client datahub.DataHubApi, ctx pipeline.Context) RecordBuilder {
	return &RecordBuilderImpl{
		projectName:   projectName,
		topicName:     topicName,
		extraLevel:    extraLevel,
		client:        client,
		context:       ctx,
		nextFreshTime: time.Now(),
	}
}

type RecordBuilderImpl struct {
	projectName   string
	topicName     string
	extraLevel    int
	client        datahub.DataHubApi
	schema        *datahub.RecordSchema
	context       pipeline.Context
	nextFreshTime time.Time

	hostIP   string
	hostname string
}

func (rb *RecordBuilderImpl) Init() error {
	rb.hostIP = config.LoongcollectorGlobalConfig.HostIP
	rb.hostname = config.LoongcollectorGlobalConfig.Hostname

	return rb.doFreshRecordSchema()
}

func (rb *RecordBuilderImpl) doFreshRecordSchema() error {
	gt, err := rb.client.GetTopic(rb.projectName, rb.topicName)
	if err != nil {
		return err
	}

	if gt.RecordType == datahub.TUPLE {
		rb.schema = gt.RecordSchema
		rb.nextFreshTime = time.Now().Add(schemaFreshInterval)
	} else {
		// Set a large value for BLOB topic
		rb.nextFreshTime = time.Now().Add(1e6 * time.Hour)
	}

	return nil
}

func (rb *RecordBuilderImpl) freshRecordSchema() error {
	if time.Now().Before(rb.nextFreshTime) {
		return nil
	}
	if err := rb.doFreshRecordSchema(); err != nil {
		// Keep the last known schema and rate-limit metadata requests during outages.
		// Init still fails if no schema could be loaded initially.
		if rb.schema == nil {
			return err
		}
		rb.nextFreshTime = time.Now().Add(schemaFreshInterval)
		logger.Warning(rb.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "DataHub schema refresh failed; using cached schema", err)
	}
	return nil
}

func (rb *RecordBuilderImpl) Log2Record(logGroup *protocol.LogGroup, log *protocol.Log) (datahub.IRecord, error) {
	if err := rb.freshRecordSchema(); err != nil {
		return nil, err
	}

	var record datahub.IRecord
	var err error
	if rb.schema == nil {
		record, err = rb.log2BlobRecord(log)
	} else {
		record, err = rb.log2TupleRecord(log)
	}

	if err != nil {
		return nil, err
	}

	rb.addExtraInfo(logGroup, log, record)
	return record, nil
}

func (rb *RecordBuilderImpl) addExtraInfo(group *protocol.LogGroup, log *protocol.Log, record datahub.IRecord) {
	if rb.extraLevel <= 0 {
		return
	}
	record.SetAttribute(HostIPKey, rb.hostIP)
	if group != nil {
		for _, tag := range group.LogTags {
			if tag != nil && tag.Key == LogtailPath {
				record.SetAttribute(CollectPathKey, tag.Value)
				break
			}
		}
		if group.Topic != "" {
			record.SetAttribute(LogtailTopic, group.Topic)
		}
	}
	if rb.extraLevel >= 2 {
		record.SetAttribute(HostnameKey, rb.hostname)
		record.SetAttribute(CollectTimeKey, strconv.FormatUint(uint64(log.Time), 10))
		record.SetAttribute(FlushTimeKey, strconv.FormatInt(time.Now().UnixMilli(), 10))
	}
}

// conversionError deliberately excludes the raw field value and SDK error text.
type conversionError struct{ column, targetType, reason string }

func (e *conversionError) Error() string {
	return fmt.Sprintf("column:%q, type:%s, reason:%s", e.column, e.targetType, e.reason)
}

func (rb *RecordBuilderImpl) log2TupleRecord(log *protocol.Log) (datahub.IRecord, error) {
	record := datahub.NewTupleRecord(rb.schema)
	for _, c := range log.Contents {
		filed, err := rb.schema.GetFieldByName(c.Key)
		if err != nil {
			continue
		}

		if c.Value == "" && filed.Type != datahub.STRING {
			if !filed.AllowNull {
				return nil, &conversionError{c.Key, string(filed.Type), "null_not_allowed"}
			}
			continue
		}

		val, err := validateFieldValue(filed.Type, c.Value)
		if err != nil {
			return nil, &conversionError{c.Key, string(filed.Type), "invalid_value"}
		}
		if err := record.SetValueByName(c.Key, val); err != nil {
			return nil, &conversionError{c.Key, string(filed.Type), "type_mismatch"}
		}
	}
	for idx, field := range rb.schema.Fields {
		if record.Values[idx] == nil && !field.AllowNull {
			return nil, &conversionError{field.Name, string(field.Type), "required_field_missing"}
		}
	}
	return record, nil
}

func (rb *RecordBuilderImpl) log2BlobRecord(log *protocol.Log) (datahub.IRecord, error) {
	if len(log.Contents) == 0 {
		return nil, fmt.Errorf("log is empty")
	}

	if len(log.Contents) == 1 && log.Contents[0].Key == blobContentColumn {
		return datahub.NewBlobRecord([]byte(log.Contents[0].Value)), nil
	}

	m := make(map[string]string)
	for _, c := range log.Contents {
		m[c.Key] = c.Value
	}

	buf, _ := json.Marshal(m)
	record := datahub.NewBlobRecord(buf)
	return record, nil
}

func validateFieldValue(fieldType datahub.FieldType, value string) (interface{}, error) {
	switch fieldType {
	case datahub.STRING:
		return value, nil
	case datahub.BIGINT:
		return strconv.ParseInt(value, 10, 64)
	case datahub.INTEGER:
		return strconv.ParseInt(value, 10, 32)
	case datahub.SMALLINT:
		return strconv.ParseInt(value, 10, 16)
	case datahub.TINYINT:
		return strconv.ParseInt(value, 10, 8)
	case datahub.FLOAT:
		tmp, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, err
		}
		return float32(tmp), nil
	case datahub.DOUBLE:
		return strconv.ParseFloat(value, 64)
	case datahub.TIMESTAMP:
		return strconv.ParseUint(value, 10, 64)
	case datahub.BOOLEAN:
		return strconv.ParseBool(value)
	case datahub.DECIMAL:
		return decimal.NewFromString(value)
	default:
		return nil, fmt.Errorf("unsupported field type %s", fieldType)
	}
}
