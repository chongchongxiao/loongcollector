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
	"regexp"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/protocol"
)

func TestGenRecordWithoutParse(t *testing.T) {
	builder := RecordBuilderImpl{
		hostIP:     "127.0.0.1",
		hostname:   "hostname",
		extraLevel: 1,
	}

	logGroup := &protocol.LogGroup{
		LogTags: []*protocol.LogTag{
			{
				Key:   "__path__",
				Value: "/tmp/test.log",
			},
		},
	}

	log := &protocol.Log{
		Time: 1733205626,
	}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "content",
			Value: "value",
		},
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Column(tableschema.Column{Name: "content", Type: datatype.StringType})
	schema := schemaBuilder.Build()

	record, err := builder.Log2Record(logGroup, log, &schema)
	assert.Nil(t, err)
	assert.Equal(t, 1, record.Len())
	assert.Equal(t, "[value]", record.String())
}

func TestGenRecordWithParse(t *testing.T) {
	builder := RecordBuilderImpl{
		hostIP:   "127.0.0.1",
		hostname: "hostname",
	}

	logGroup := &protocol.LogGroup{
		LogTags: []*protocol.LogTag{
			{
				Key:   "__path__",
				Value: "/tmp/test.log",
			},
		},
	}

	log := &protocol.Log{
		Time: 1733205626,
	}
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
			Value: "3.14",
		},
		{
			Key:   "f7",
			Value: "123.456",
		},
		{
			Key:   "f8",
			Value: "2024-12-04",
		},
		{
			Key:   "f9",
			Value: "2024-12-04 10:00:00",
		},
		{
			Key:   "f10",
			Value: "2024-12-04 10:00:00.000",
		},
		{
			Key:   "f11",
			Value: "2024-12-04 10:00:00.000",
		},
		{
			Key:   "f12",
			Value: "aaaa",
		},
		{
			Key:   "f13",
			Value: "bbbb",
		},
		{
			Key:   "f14",
			Value: "cccc",
		},
		{
			Key:   "f15",
			Value: "dddd",
		},
		{
			Key:   "f16",
			Value: `"xxxx"`,
		},
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Column(tableschema.Column{Name: "f1", Type: datatype.BooleanType})
	schemaBuilder.Column(tableschema.Column{Name: "f2", Type: datatype.TinyIntType})
	schemaBuilder.Column(tableschema.Column{Name: "f3", Type: datatype.SmallIntType})
	schemaBuilder.Column(tableschema.Column{Name: "f4", Type: datatype.IntType})
	schemaBuilder.Column(tableschema.Column{Name: "f5", Type: datatype.BigIntType})
	schemaBuilder.Column(tableschema.Column{Name: "f6", Type: datatype.FloatType})
	schemaBuilder.Column(tableschema.Column{Name: "f7", Type: datatype.DoubleType})
	schemaBuilder.Column(tableschema.Column{Name: "f8", Type: datatype.DateType})
	schemaBuilder.Column(tableschema.Column{Name: "f9", Type: datatype.DateTimeType})
	schemaBuilder.Column(tableschema.Column{Name: "f10", Type: datatype.TimestampType})
	schemaBuilder.Column(tableschema.Column{Name: "f11", Type: datatype.TimestampNtzType})
	schemaBuilder.Column(tableschema.Column{Name: "f12", Type: datatype.StringType})
	schemaBuilder.Column(tableschema.Column{Name: "f13", Type: datatype.NewCharType(100)})
	schemaBuilder.Column(tableschema.Column{Name: "f14", Type: datatype.NewVarcharType(100)})
	schemaBuilder.Column(tableschema.Column{Name: "f15", Type: datatype.BinaryType})
	schemaBuilder.Column(tableschema.Column{Name: "f16", Type: datatype.NewJsonType()})
	schema := schemaBuilder.Build()

	record, err := builder.Log2Record(logGroup, log, &schema)
	assert.Nil(t, err)
	assert.Equal(t, 16, record.Len())
	expect := "[true, 1, 123, 123456, 123456789, 3.140000E+00, 1.234560E+02, 2024-12-04, 2024-12-04 10:00:00, 2024-12-04 10:00:00.000, 2024-12-04 10:00:00.000, aaaa, bbbb, cccc, unhex('64646464'), \"xxxx\"]"
	assert.Equal(t, expect, record.String())
}

func TestUnsupportType(t *testing.T) {
	builder := RecordBuilderImpl{}

	logGroup := &protocol.LogGroup{}

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "f1",
			Value: "value",
		},
		{
			Key:   "f2",
			Value: "value",
		},
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Column(tableschema.Column{Name: "f1", Type: datatype.StringType})
	schemaBuilder.Column(tableschema.Column{Name: "f2", Type: datatype.NewArrayType(datatype.StringType)})
	schema := schemaBuilder.Build()

	_, err := builder.Log2Record(logGroup, log, &schema)
	assert.NotNil(t, err)
}

func TestTypeMissMatch(t *testing.T) {
	builder := RecordBuilderImpl{}

	logGroup := &protocol.LogGroup{}

	log := &protocol.Log{}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "f1",
			Value: "value",
		},
		{
			Key:   "f2",
			Value: "value",
		},
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Column(tableschema.Column{Name: "f1", Type: datatype.StringType})
	schemaBuilder.Column(tableschema.Column{Name: "f2", Type: datatype.BigIntType})
	schema := schemaBuilder.Build()

	_, err := builder.Log2Record(logGroup, log, &schema)
	assert.NotNil(t, err)
}

func TestGenRecordWithExtraInfo(t *testing.T) {
	builder := RecordBuilderImpl{
		hostIP:     "127.0.0.1",
		hostname:   "hostname",
		extraLevel: 1,
	}

	logGroup := &protocol.LogGroup{
		LogTags: []*protocol.LogTag{
			{
				Key:   "__path__",
				Value: "/tmp/test.log",
			},
		},
	}

	log := &protocol.Log{
		Time: 1733205626,
	}
	log.Contents = []*protocol.Log_Content{
		{
			Key:   "content",
			Value: "value",
		},
	}

	schemaBuilder := tableschema.NewSchemaBuilder()
	schemaBuilder.Column(tableschema.Column{Name: "content", Type: datatype.StringType})
	schemaBuilder.Column(tableschema.Column{Name: "__extra_info__", Type: datatype.NewJsonType()})
	schema := schemaBuilder.Build()

	record, err := builder.Log2Record(logGroup, log, &schema)
	assert.Nil(t, err)
	assert.Equal(t, 2, record.Len())
	assert.Equal(t, "[value, {\"host_ip\":\"127.0.0.1\"}]", record.String())

	builder.extraLevel = 2
	record, err = builder.Log2Record(logGroup, log, &schema)
	assert.Nil(t, err)
	assert.Equal(t, 2, record.Len())
	re := regexp.MustCompile(`\[value, \{"host_ip":"127\.0\.0\.1","hostname":"hostname","collect_time":"1733205626","flush_time":"\d+"\}\]`)
	assert.Truef(t, re.MatchString(record.String()), "regexp match failed, result:%s", record.String())
}

func TestDoublePreservesPrecisionAndRange(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  float64
	}{{"16777217", 16777217}, {"1e100", 1e100}} {
		t.Run(tc.input, func(t *testing.T) {
			sb := tableschema.NewSchemaBuilder()
			sb.Column(tableschema.Column{Name: "value", Type: datatype.DoubleType})
			schema := sb.Build()
			rb := &RecordBuilderImpl{}
			record, err := rb.Log2Record(&protocol.LogGroup{}, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: tc.input}}}, &schema)
			require.NoError(t, err)
			require.Equal(t, data.Double(tc.want), record[0])
		})
	}
}

func TestIntegerOverflowIsRejected(t *testing.T) {
	for _, tc := range []struct {
		typ   datatype.DataType
		value string
	}{{datatype.TinyIntType, "128"}, {datatype.SmallIntType, "32768"}, {datatype.IntType, "2147483648"}, {datatype.TinyIntType, "-129"}} {
		sb := tableschema.NewSchemaBuilder()
		sb.Column(tableschema.Column{Name: "value", Type: tc.typ})
		schema := sb.Build()
		_, err := (&RecordBuilderImpl{}).Log2Record(&protocol.LogGroup{}, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: tc.value}}}, &schema)
		require.Error(t, err)
	}
}
func TestJSONColumnPreservesJSONValue(t *testing.T) {
	sb := tableschema.NewSchemaBuilder()
	sb.Column(tableschema.Column{Name: "value", Type: datatype.JsonType{}})
	schema := sb.Build()
	for _, value := range []string{`{"a":1}`, `[1,true]`, `null`, `"text"`, `123`} {
		record, err := (&RecordBuilderImpl{}).Log2Record(&protocol.LogGroup{}, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: value}}}, &schema)
		require.NoError(t, err)
		require.JSONEq(t, value, record[0].String())
	}
	_, err := (&RecordBuilderImpl{}).Log2Record(&protocol.LogGroup{}, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: "invalid"}}}, &schema)
	require.Error(t, err)
}

func TestDeclaredStringLength(t *testing.T) {
	for _, typ := range []datatype.DataType{datatype.NewCharType(3), datatype.NewVarcharType(3)} {
		sb := tableschema.NewSchemaBuilder()
		sb.Column(tableschema.Column{Name: "value", Type: typ})
		schema := sb.Build()
		rb := &RecordBuilderImpl{}
		for _, value := range []string{"abc", "中"} {
			record, err := rb.Log2Record(nil, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: value}}}, &schema)
			require.NoError(t, err)
			require.Equal(t, typ.Name(), record[0].Type().Name())
		}
		for _, value := range []string{"abcd", "中文"} {
			_, err := rb.Log2Record(nil, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "value", Value: value}}}, &schema)
			require.Error(t, err)
			require.Contains(t, err.Error(), "value")
			require.NotContains(t, err.Error(), value)
		}
	}
}

func TestExtraInfoColumnReservation(t *testing.T) {
	rb := &RecordBuilderImpl{extraLevel: 1, hostIP: "127.0.0.1"}
	for _, name := range []string{"__extra_info__", "__dwarf_info__"} {
		sb := tableschema.NewSchemaBuilder()
		sb.Column(tableschema.Column{Name: name, Type: datatype.NewJsonType()})
		schema := sb.Build()
		record, err := rb.Log2Record(nil, &protocol.Log{Contents: []*protocol.Log_Content{{Key: name, Value: `{"business":true}`}}}, &schema)
		require.NoError(t, err)
		if name == "__extra_info__" {
			require.Contains(t, record.String(), "host_ip")
			require.NotContains(t, record.String(), "business")
		} else {
			require.Contains(t, record.String(), "business")
		}
	}
	sb := tableschema.NewSchemaBuilder()
	sb.Column(tableschema.Column{Name: "__extra_info__", Type: datatype.StringType})
	schema := sb.Build()
	record, err := rb.Log2Record(nil, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "__extra_info__", Value: "business"}}}, &schema)
	require.NoError(t, err)
	require.Equal(t, "[business]", record.String())
}

func TestConversionDiagnosticDoesNotExposeValue(t *testing.T) {
	sb := tableschema.NewSchemaBuilder()
	sb.Column(tableschema.Column{Name: "count", Type: datatype.BigIntType})
	schema := sb.Build()
	rb := &RecordBuilderImpl{}
	_, err := rb.Log2Record(nil, &protocol.Log{Contents: []*protocol.Log_Content{{Key: "count", Value: "private-payload"}}}, &schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "count")
	require.Contains(t, err.Error(), "BIGINT")
	require.NotContains(t, err.Error(), "private-payload")
}
