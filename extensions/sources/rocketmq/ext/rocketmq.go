// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rocketmq

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"math/rand"
	"strings"
	"time"
)

type RocketMqSource struct {
	consumer rocketmq.PushConsumer
	sc       *rockerMqSourceConf
	topic    string
}

type rockerMqSourceConf struct {
	GroupName  string `json:"groupName"`
	NameServer string `json:"nameServer"`
	Topic      string `json:"topic"`
}

func (s *RocketMqSource) Ping(d string, props map[string]interface{}) error {
	return nil
}

func (c *rockerMqSourceConf) validate() error {
	if len(strings.Split(c.NameServer, ",")) == 0 {
		return fmt.Errorf("NameServer can not be empty")
	}
	return nil
}

func getSourceConf(props map[string]interface{}) (*rockerMqSourceConf, error) {
	c := &rockerMqSourceConf{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	return c, nil
}

func (s *RocketMqSource) Configure(topic string, props map[string]interface{}) error {
	rConf, err := getSourceConf(props)
	if err != nil {
		conf.Log.Errorf("rocketMq source config error: %v", err)
		return err
	}
	if err := rConf.validate(); err != nil {
		return err
	}
	mqConsumer, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(rConf.GroupName+"-"+generateRandomString(5)),
		consumer.WithNameServer(strings.Split(rConf.NameServer, ",")),
	)
	if err != nil {
		conf.Log.Infof("rocket mq connect error,%v", err)
	}

	s.consumer = mqConsumer
	s.sc = rConf
	s.topic = topic
	conf.Log.Infof("rocketMq source got configured.")
	return nil
}

func (s *RocketMqSource) Open(ctx api.StreamContext, chans chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	for {
		select {
		case <-ctx.Done():
			logger.Infof("flow end quit rocket mq")
			return
		default:
		}
		meta := make(map[string]interface{})
		meta["topic"] = s.topic
		_ = s.consumer.Subscribe(s.topic, consumer.MessageSelector{}, func(_ context.Context,
			msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			tuples := make([]api.SourceTuple, 0, len(msgs))
			for _, msg := range msgs {
				logger.Infof("rocket mq message is %+v ", msg)
				data, _ := ctx.Decode(msg.Body)
				rcvTime := conf.GetNow()
				tuples = append(tuples, api.NewDefaultSourceTupleWithTime(data, meta, rcvTime))
				logger.Infof("subscribe callback: %+v \n", msg)
			}
			io.ReceiveTuples(ctx, chans, tuples)
			// 消费成功，进行ack确认
			return consumer.ConsumeSuccess, nil
		})
		err := s.consumer.Start()
		if err != nil {
			logger.Errorln("rocket mq consumer error %+v", err)
			return
		}

	}
}

func (s *RocketMqSource) Close(_ api.StreamContext) error {
	return nil
}

func GetSource() api.Source {
	return &RocketMqSource{}
}

func generateRandomString(length int) string {
	// 定义可选的字符集合
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// 创建随机数生成器，并设置种子为当前时间的纳秒数
	rand.Seed(time.Now().UnixNano())

	// 创建一个切片用于存储生成的字符
	result := make([]byte, length)

	// 生成随机字符
	for i := 0; i < length; i++ {
		// 随机选择一个索引
		index := rand.Intn(len(charset))
		// 从字符集合中取出对应索引的字符
		result[i] = charset[index]
		// 从字符集合中移除已选择的字符，以确保不会生成重复的字符
		charset = charset[:index] + charset[index+1:]
	}

	// 将切片转换为字符串并返回
	return string(result)
}
