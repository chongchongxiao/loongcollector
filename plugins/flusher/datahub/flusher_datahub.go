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
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"

	"github.com/alibaba/ilogtail/pkg/config"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
)

const (
	compressTypeDefault = datahub.ZSTD
	extraLevelDefault   = 1
)

type FlusherDatahub struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	Endpoint        string
	ProjectName     string
	TopicName       string
	recordBuilder   RecordBuilder
	producer        Producer
	context         pipeline.Context
	httpClient      *http.Client
	discarded       selfmonitor.CounterMetric
}

func NewFlusherDatahub() *FlusherDatahub {
	return &FlusherDatahub{
		AccessKeyID:     "",
		AccessKeySecret: "",
		SecurityToken:   "",
		Endpoint:        "",
		ProjectName:     "",
		TopicName:       "",
	}
}

func (d *FlusherDatahub) Init(context pipeline.Context) (initErr error) {
	defer func() {
		if initErr != nil && d.httpClient != nil {
			d.httpClient.CloseIdleConnections()
		}
	}()
	d.context = context
	if err := d.validate(); err != nil {
		return err
	}
	d.discarded = selfmonitor.NewCounterMetricAndRegister(context.GetMetricRecord(), selfmonitor.MetricPluginDiscardedEventsTotal)
	logger.Infof(d.context.GetRuntimeContext(), "Initialize DataHub flusher, project:%s, topic:%s", d.ProjectName, d.TopicName)
	d.httpClient = datahub.DefaultHttpClient()
	d.httpClient.Timeout = 30 * time.Second

	var account datahub.Account
	if len(d.SecurityToken) > 0 {
		account = datahub.NewStsCredential(d.AccessKeyID, d.AccessKeySecret, d.SecurityToken)
	} else {
		account = datahub.NewAliyunAccount(d.AccessKeyID, d.AccessKeySecret)
	}

	config := &datahub.Config{
		UserAgent:      fmt.Sprintf("loongcollector/%s-%s", config.BaseVersion, config.LoongcollectorGlobalConfig.HostIP),
		CompressorType: compressTypeDefault,
		Protocol:       datahub.Batch,
		HttpClient:     d.httpClient,
	}

	client := datahub.NewClientWithConfig(d.Endpoint, config, account)

	d.recordBuilder = NewRecordBuilder(d.ProjectName, d.TopicName, extraLevelDefault, client, d.context)
	err := d.recordBuilder.Init()
	if err != nil {
		logger.Errorf(d.context.GetRuntimeContext(), selfmonitor.FlusherInitAlarm, "Init datahub(%s/%s) record builder failed, error:%v", d.ProjectName, d.TopicName, err)
		return err
	}
	logger.Infof(d.context.GetRuntimeContext(), "Init datahub(%s/%s) record builder success", d.ProjectName, d.TopicName)

	d.producer = NewProducer(d.ProjectName, d.TopicName, client, d.context)
	err = d.producer.Init()
	if err != nil {
		logger.Errorf(d.context.GetRuntimeContext(), selfmonitor.FlusherInitAlarm, "Init datahub(%s/%s) producer failed, error:%v", d.ProjectName, d.TopicName, err)
		return err
	}
	logger.Infof(d.context.GetRuntimeContext(), "Init datahub(%s/%s) producer success", d.ProjectName, d.TopicName)

	return nil
}

func (d *FlusherDatahub) Description() string {
	return "datahub flusher"
}

func (d *FlusherDatahub) Flush(projectName string, logstoreName string, configName string, logGroupList []*protocol.LogGroup) error {
	var errs []error
	var dirty int64
	var firstConversionError *conversionError
	for _, group := range logGroupList {
		if group == nil {
			continue
		}
		records := make([]datahub.IRecord, 0, len(group.Logs))
		for _, log := range group.Logs {
			if log == nil {
				dirty++
				continue
			}
			record, err := d.recordBuilder.Log2Record(group, log)
			if err != nil {
				dirty++
				if firstConversionError == nil {
					errors.As(err, &firstConversionError)
				}
				continue
			}
			records = append(records, record)
		}
		if len(records) > 0 {
			if err := d.producer.Send(records); err != nil {
				errs = append(errs, err)
				if d.discarded != nil {
					d.discarded.Add(int64(len(records)))
				}
			}
		}
	}
	if dirty > 0 {
		if d.discarded != nil {
			d.discarded.Add(dirty)
		}
		logger.Critical(d.context.GetRuntimeContext(), selfmonitor.FlusherFlushAlarm, "discard invalid DataHub records", dirty, "first_conversion_error", firstConversionError)
	}
	return errors.Join(errs...)
}

func (d *FlusherDatahub) validate() error {
	for _, field := range []struct{ name, value string }{{"AccessKeyID", d.AccessKeyID}, {"AccessKeySecret", d.AccessKeySecret}, {"ProjectName", d.ProjectName}, {"TopicName", d.TopicName}} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	u, err := url.Parse(d.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil {
		return fmt.Errorf("Endpoint must be an HTTP(S) URL without user info")
	}
	return nil
}

func (d *FlusherDatahub) IsReady(projectName string, logstoreName string, logstoreKey int64) bool {
	return d.producer != nil && d.recordBuilder != nil
}

func (d *FlusherDatahub) SetUrgent(flag bool) {
}

func (d *FlusherDatahub) Stop() error {
	if d.httpClient != nil {
		d.httpClient.CloseIdleConnections()
	}
	return nil
}

func init() {
	pipeline.Flushers["flusher_aliyun_datahub"] = func() pipeline.Flusher {
		f := NewFlusherDatahub()
		return f
	}
}
