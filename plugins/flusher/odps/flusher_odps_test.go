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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/protocol"
)

func TestFlusherOdps(t *testing.T) {
	d := NewFlusherOdps()
	require.NotNil(t, d)
}

type failingSender struct {
	sent    int
	flushed bool
}

func (*failingSender) Init() error { return nil }
func (s *failingSender) Send(_ *protocol.LogGroup, _ *protocol.Log) error {
	s.sent++
	if s.sent == 1 {
		return errors.New("session unavailable")
	}
	return nil
}
func (s *failingSender) Flush() error { s.flushed = true; return nil }
func TestFlushDrainsAfterSendFailure(t *testing.T) {
	s := &failingSender{}
	f := &FlusherOdps{sender: s}
	err := f.Flush("", "", "", []*protocol.LogGroup{{Logs: []*protocol.Log{{Time: 1}, {Time: 2}}}})
	require.Error(t, err)
	require.Equal(t, 2, s.sent)
	require.True(t, s.flushed)
}
func TestInitRejectsInvalidConfigBeforeNetwork(t *testing.T) {
	f := NewFlusherOdps()
	require.Error(t, f.validate())
	f.AccessKeyID = "placeholder"
	f.AccessKeySecret = "placeholder"
	f.Endpoint = "https://example.invalid"
	f.ProjectName = "p"
	f.TableName = "t"
	require.NoError(t, f.validate())
	f.PartitionConfig = "ds={{%Y%m%d}}"
	f.TimeRange = 1
	require.Error(t, f.validate())
}
