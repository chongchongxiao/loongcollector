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
	"testing"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/stretchr/testify/assert"

	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/plugins/test/mock"
)

func TestValidateFieldValue(t *testing.T) {
	val, err := validateFieldValue(datahub.STRING, "test")
	assert.Nil(t, err)
	assert.Equal(t, "test", val)

	val, err = validateFieldValue(datahub.STRING, "1")
	assert.Nil(t, err)
	assert.Equal(t, "1", val)

	val, err = validateFieldValue(datahub.INTEGER, "1")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), val)

	_, err = validateFieldValue(datahub.INTEGER, "test")
	assert.Error(t, err)

	_, err = validateFieldValue(datahub.FLOAT, "1.1")
	assert.Nil(t, err)

	_, err = validateFieldValue(datahub.FLOAT, "test")
	assert.Error(t, err)

	val, err = validateFieldValue(datahub.BOOLEAN, "true")
	assert.Nil(t, err)
	assert.Equal(t, true, val)

	_, err = validateFieldValue(datahub.BOOLEAN, "test")
	assert.Error(t, err)

	val, err = validateFieldValue(datahub.TIMESTAMP, "1")
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), val)

	_, err = validateFieldValue(datahub.TIMESTAMP, "test")
	assert.Error(t, err)

	_, err = validateFieldValue("unknown", "test")
	assert.Error(t, err)
}

func TestGenBlobRecord(t *testing.T) {
	builder := RecordBuilderImpl{}

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "content",
			Value: "value",
		},
	}
	record, err := builder.log2BlobRecord(log)
	assert.Nil(t, err)
	bRecord, ok := record.(*datahub.BlobRecord)
	assert.True(t, ok)
	assert.Equal(t, "value", string(bRecord.RawData))

	log.Contents = []*protocol.Log_Content{
		{
			Key:   "key1",
			Value: "val1",
		},
		{
			Key:   "key2",
			Value: "val2",
		},
		{
			Key:   "content",
			Value: "val3",
		},
	}
	record, err = builder.log2BlobRecord(log)
	assert.Nil(t, err)
	bRecord, ok = record.(*datahub.BlobRecord)
	assert.True(t, ok)
	assert.Equal(t, "{\"content\":\"val3\",\"key1\":\"val1\",\"key2\":\"val2\"}", string(bRecord.RawData))

	log.Contents = []*protocol.Log_Content{
		{
			Key:   "key1",
			Value: "val1",
		},
	}
	record, err = builder.log2BlobRecord(log)
	assert.Nil(t, err)
	bRecord, ok = record.(*datahub.BlobRecord)
	assert.True(t, ok)
	assert.Equal(t, "{\"key1\":\"val1\"}", string(bRecord.RawData))

	log.Contents = []*protocol.Log_Content{}
	_, err = builder.log2BlobRecord(log)
	assert.NotNil(t, err)
}

func TestGenTupleRecord(t *testing.T) {
	builder := RecordBuilderImpl{
		schema: datahub.NewRecordSchema(),
	}

	builder.schema.AddField(datahub.Field{Name: "f1", Type: datahub.BOOLEAN})
	builder.schema.AddField(datahub.Field{Name: "f2", Type: datahub.TINYINT})
	builder.schema.AddField(datahub.Field{Name: "f3", Type: datahub.SMALLINT})
	builder.schema.AddField(datahub.Field{Name: "f4", Type: datahub.INTEGER})
	builder.schema.AddField(datahub.Field{Name: "f5", Type: datahub.BIGINT})
	builder.schema.AddField(datahub.Field{Name: "f6", Type: datahub.TIMESTAMP})
	builder.schema.AddField(datahub.Field{Name: "f7", Type: datahub.FLOAT})
	builder.schema.AddField(datahub.Field{Name: "f8", Type: datahub.DOUBLE})
	builder.schema.AddField(datahub.Field{Name: "f9", Type: datahub.DECIMAL})
	builder.schema.AddField(datahub.Field{Name: "f10", Type: datahub.STRING})

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "f1",
			Value: "true",
		},
		{
			Key:   "f2",
			Value: "1",
		},
		{
			Key:   "f3",
			Value: "123",
		},
		{
			Key:   "f4",
			Value: "123456",
		},
		{
			Key:   "f5",
			Value: "123456789",
		},
		{
			Key:   "f6",
			Value: "1733205626",
		},
		{
			Key:   "f7",
			Value: "3.14",
		},
		{
			Key:   "f8",
			Value: "123.456",
		},
		{
			Key:   "f9",
			Value: "1234.56789",
		},
		{
			Key:   "f10",
			Value: "aaaabbbbcccc",
		},
	}

	record, err := builder.log2TupleRecord(log)
	assert.Nil(t, err)
	tRecord, ok := record.(*datahub.TupleRecord)
	assert.True(t, ok)
	val, err := tRecord.GetValueByName("f1")
	assert.NoError(t, err)
	bVal, ok := val.(datahub.Boolean)
	assert.True(t, ok)
	assert.True(t, bool(bVal))
	val, err = tRecord.GetValueByName("f2")
	assert.NoError(t, err)
	tVal, ok := val.(datahub.Tinyint)
	assert.True(t, ok)
	assert.Equal(t, int8(1), int8(tVal))
	val, err = tRecord.GetValueByName("f3")
	assert.NoError(t, err)
	sVal, ok := val.(datahub.Smallint)
	assert.True(t, ok)
	assert.Equal(t, int16(123), int16(sVal))
	val, err = tRecord.GetValueByName("f4")
	assert.NoError(t, err)
	iVal, ok := val.(datahub.Integer)
	assert.True(t, ok)
	assert.Equal(t, 123456, int(iVal))
	val, err = tRecord.GetValueByName("f5")
	assert.NoError(t, err)
	gVal, ok := val.(datahub.Bigint)
	assert.True(t, ok)
	assert.Equal(t, int64(123456789), int64(gVal))
	val, err = tRecord.GetValueByName("f6")
	assert.NoError(t, err)
	pVal, ok := val.(datahub.Timestamp)
	assert.True(t, ok)
	assert.Equal(t, uint64(pVal), uint64(pVal))
	val, err = tRecord.GetValueByName("f7")
	assert.NoError(t, err)
	fVal, ok := val.(datahub.Float)
	assert.True(t, ok)
	assert.Equal(t, float32(3.14), float32(fVal))
	val, err = tRecord.GetValueByName("f8")
	assert.NoError(t, err)
	dVal, ok := val.(datahub.Double)
	assert.True(t, ok)
	assert.Equal(t, 123.456, float64(dVal))
	val, err = tRecord.GetValueByName("f9")
	assert.NoError(t, err)
	cVal, ok := val.(datahub.Decimal)
	assert.True(t, ok)
	assert.Equal(t, "1234.56789", cVal.String())
	val, err = tRecord.GetValueByName("f10")
	assert.NoError(t, err)
	rVal, ok := val.(datahub.String)
	assert.True(t, ok)
	assert.Equal(t, "aaaabbbbcccc", string(rVal))
}

func TestGenTupleRecordWithNull(t *testing.T) {
	builder := RecordBuilderImpl{
		schema: datahub.NewRecordSchema(),
	}

	builder.schema.AddField(datahub.Field{Name: "f1", Type: datahub.BOOLEAN, AllowNull: true})
	builder.schema.AddField(datahub.Field{Name: "f2", Type: datahub.TINYINT, AllowNull: true})

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "f1",
			Value: "true",
		},
		{
			Key:   "f2",
			Value: "",
		},
	}

	record, err := builder.log2TupleRecord(log)
	assert.Nil(t, err)
	tRecord, ok := record.(*datahub.TupleRecord)
	assert.True(t, ok)
	val, err := tRecord.GetValueByName("f1")
	assert.NoError(t, err)
	bVal, ok := val.(datahub.Boolean)
	assert.True(t, ok)
	assert.True(t, bool(bVal))
	val, err = tRecord.GetValueByName("f2")
	assert.NoError(t, err)
	assert.Nil(t, val)

	builder.schema = datahub.NewRecordSchema()
	builder.schema.AddField(datahub.Field{Name: "f1", Type: datahub.BOOLEAN, AllowNull: true})
	builder.schema.AddField(datahub.Field{Name: "f2", Type: datahub.TINYINT, AllowNull: false})

	_, err = builder.log2TupleRecord(log)
	assert.NotNil(t, err)
}

func TestGenTupleRecordWithTypeMissMatch(t *testing.T) {
	builder := RecordBuilderImpl{
		schema: datahub.NewRecordSchema(),
	}

	builder.schema.AddField(datahub.Field{Name: "f1", Type: datahub.BOOLEAN, AllowNull: true})
	builder.schema.AddField(datahub.Field{Name: "f2", Type: datahub.TINYINT, AllowNull: true})

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "f1",
			Value: "123",
		},
		{
			Key:   "f2",
			Value: "2",
		},
	}

	_, err := builder.log2TupleRecord(log)
	assert.NotNil(t, err)
}

func TestTupleRejectsMissingRequiredField(t *testing.T) {
	rb := &RecordBuilderImpl{schema: datahub.NewRecordSchema()}
	rb.schema.AddField(datahub.Field{Name: "required", Type: datahub.STRING, AllowNull: false})
	_, err := rb.log2TupleRecord(&protocol.Log{})
	assert.Error(t, err)
}
func TestTuplePreservesEmptyString(t *testing.T) {
	rb := &RecordBuilderImpl{schema: datahub.NewRecordSchema()}
	rb.schema.AddField(datahub.Field{Name: "value", Type: datahub.STRING, AllowNull: false})
	record, err := rb.log2TupleRecord(&protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: ""}}})
	if assert.NoError(t, err) {
		v, err := record.(*datahub.TupleRecord).GetValueByName("value")
		assert.NoError(t, err)
		assert.Equal(t, datahub.String(""), v)
	}
}

type unavailableSchemaAPI struct {
	datahub.DataHubApi
	calls int
}

func (a *unavailableSchemaAPI) GetTopic(string, string) (*datahub.GetTopicResult, error) {
	a.calls++
	return nil, errors.New("unavailable")
}
func TestSchemaRefreshFailureUsesCachedSchemaWithBackoff(t *testing.T) {
	api := &unavailableSchemaAPI{}
	rb := &RecordBuilderImpl{schema: datahub.NewRecordSchema(), client: api, context: mock.NewEmptyContext("p", "l", "c"), nextFreshTime: time.Now().Add(-time.Second)}
	rb.schema.AddField(datahub.Field{Name: "value", Type: datahub.STRING, AllowNull: false})
	for range 3 {
		_, err := rb.Log2Record(&protocol.LogGroup{}, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: "ok"}}})
		assert.NoError(t, err)
	}
	assert.Equal(t, 1, api.calls)
}

func TestTupleExtraFieldsAndAttributes(t *testing.T) {
	for _, tuple := range []bool{false, true} {
		rb := &RecordBuilderImpl{extraLevel: 1, hostIP: "127.0.0.1", nextFreshTime: time.Now().Add(time.Hour)}
		if tuple {
			rb.schema = datahub.NewRecordSchema()
			rb.schema.AddField(datahub.Field{Name: "content", Type: datahub.STRING})
		}
		group := &protocol.LogGroup{Topic: "source", LogTags: []*protocol.LogTag{{Key: "__path__", Value: "/tmp/input.log"}}}
		log := &protocol.Log{Contents: []*protocol.Log_Content{{Key: "content", Value: "hello"}, {Key: "extra", Value: "ignored"}}}
		record, err := rb.Log2Record(group, log)
		if !assert.NoError(t, err) {
			continue
		}
		assert.Equal(t, map[string]string{"host_ip": "127.0.0.1", "path": "/tmp/input.log", "__topic__": "source"}, record.GetAttributes())
	}
}

func TestConversionDiagnosticDoesNotExposeValue(t *testing.T) {
	rb := &RecordBuilderImpl{schema: datahub.NewRecordSchema()}
	rb.schema.AddField(datahub.Field{Name: "count", Type: datahub.BIGINT})
	_, err := rb.log2TupleRecord(&protocol.Log{Contents: []*protocol.Log_Content{{Key: "count", Value: "private-payload"}}})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "count")
		assert.Contains(t, err.Error(), "BIGINT")
		assert.NotContains(t, err.Error(), "private-payload")
	}
}

func TestExtraAttributesLevels(t *testing.T) {
	rb := &RecordBuilderImpl{extraLevel: 2, hostIP: "127.0.0.1", hostname: "agent"}
	record := datahub.NewBlobRecord(nil)
	rb.addExtraInfo(nil, &protocol.Log{Time: 123}, record)
	assert.Equal(t, "agent", record.GetAttributes()[HostnameKey])
	assert.Equal(t, "123", record.GetAttributes()[CollectTimeKey])
	assert.NotEmpty(t, record.GetAttributes()[FlushTimeKey])
	rb.extraLevel = 0
	record = datahub.NewBlobRecord(nil)
	rb.addExtraInfo(nil, &protocol.Log{}, record)
	assert.Empty(t, record.GetAttributes())
}
