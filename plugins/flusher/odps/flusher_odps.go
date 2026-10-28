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
	"net/url"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"

	"github.com/alibaba/ilogtail/pkg/config"
	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/pkg/selfmonitor"
)

const extraLevelDefault = 1

type FlusherOdps struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	Endpoint        string
	ProjectName     string
	SchemaName      string
	TableName       string
	PartitionConfig string
	TimeRange       int
	sender          TunnelSender
	context         pipeline.Context
}

func NewFlusherOdps() *FlusherOdps {
	return &FlusherOdps{
		AccessKeyID:     "",
		AccessKeySecret: "",
		SecurityToken:   "",
		Endpoint:        "",
		ProjectName:     "",
		TableName:       "",
		PartitionConfig: "",
		TimeRange:       15,
	}
}

func (o *FlusherOdps) Init(context pipeline.Context) error {
	o.context = context
	if err := o.validate(); err != nil {
		return err
	}

	tunnelIns, err := o.CreateTunnel()
	if err != nil {
		logger.Errorf(o.context.GetRuntimeContext(), selfmonitor.FlusherInitAlarm, "Create (%s/%s/%s) tunnel failed, error:%v",
			o.ProjectName, o.SchemaName, o.TableName, err)
		return err
	}

	sender := NewTunnelSender(context, o.ProjectName, o.SchemaName, o.TableName, o.PartitionConfig, o.TimeRange, extraLevelDefault, tunnelIns)
	o.sender = sender
	if err := o.sender.Init(); err != nil {
		logger.Errorf(o.context.GetRuntimeContext(), selfmonitor.FlusherInitAlarm, "Init (%s/%s/%s) tunnel sender failed, error:%v",
			o.ProjectName, o.SchemaName, o.TableName, err)
		return err
	}

	logger.Infof(o.context.GetRuntimeContext(), "Init odps (%s/%s/%s) flusher success, partition:%s/%d",
		o.ProjectName, o.SchemaName, o.TableName, o.PartitionConfig, o.TimeRange)
	return nil
}

func (o *FlusherOdps) CreateTunnel() (*tunnel.Tunnel, error) {
	var aliAccount account.Account
	if len(o.SecurityToken) > 0 {
		aliAccount = account.NewStsAccount(o.AccessKeyID, o.AccessKeySecret, o.SecurityToken)
	} else {
		aliAccount = account.NewAliyunAccount(o.AccessKeyID, o.AccessKeySecret)
	}

	odpsIns := odps.NewOdps(aliAccount, o.Endpoint)
	odpsIns.SetUserAgent(fmt.Sprintf("loongcollector/%s-%s", config.BaseVersion, config.LoongcollectorGlobalConfig.HostIP))
	odpsIns.SetDefaultProjectName(o.ProjectName)
	odpsIns.SetHttpTimeout(requestTimeout)
	odpsIns.SetTcpConnectTimeout(requestTimeout)
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return nil, err
	}
	logger.Infof(o.context.GetRuntimeContext(), "Get (%s/%s) tunnel endpoint success, endpoint:%s", o.ProjectName, o.TableName, tunnelEndpoint)
	return tunnel.NewTunnel(odpsIns, tunnelEndpoint), nil
}

func (o *FlusherOdps) Description() string {
	return "odps flusher"
}

func (o *FlusherOdps) Flush(projectName string, logstoreName string, configName string, groups []*protocol.LogGroup) error {
	var errs []error
	for _, group := range groups {
		if group == nil {
			continue
		}
		for _, log := range group.Logs {
			if log == nil {
				continue
			}
			if err := o.sender.Send(group, log); err != nil {
				if len(errs) == 0 {
					errs = append(errs, err)
				}
			}
		}
	}
	// Always drain all partitions, even when another record/session could not be created.
	if err := o.sender.Flush(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (o *FlusherOdps) validate() error {
	for _, field := range []struct{ name, value string }{{"AccessKeyID", o.AccessKeyID}, {"AccessKeySecret", o.AccessKeySecret}, {"ProjectName", o.ProjectName}, {"TableName", o.TableName}} {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	u, err := url.Parse(o.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("Endpoint must be an HTTP(S) URL without user info")
	}
	return NewPartitionHelper().Init(o.PartitionConfig, o.TimeRange)
}

func (o *FlusherOdps) IsReady(projectName string, logstoreName string, logstoreKey int64) bool {
	return o.sender != nil
}

func (o *FlusherOdps) SetUrgent(flag bool) {
	// do nothing
}

func (o *FlusherOdps) Stop() error {
	if o.sender == nil {
		return nil
	}
	return o.sender.Flush()
}

func init() {
	pipeline.Flushers["flusher_aliyun_odps"] = func() pipeline.Flusher {
		f := NewFlusherOdps()
		return f
	}
}
