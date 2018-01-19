/*
Copyright 2017 Heptio Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sinks

import (
	"errors"

	"github.com/golang/glog"
	"github.com/spf13/viper"

	"k8s.io/api/core/v1"
)

// EventSinkInterface is the interface used to shunt events
type EventSinkInterface interface {
	UpdateEvents(eNew *v1.Event, eOld *v1.Event)
}

// ManufactureSink will manufacture a sink according to viper configs
// TODO: Determine if it should return an array of sinks
func ManufactureSink() (e EventSinkInterface) {
	s := viper.GetString("sink")
	glog.Infof("Sink is [%v]", s)
	switch s {
	case "glog":
		e = NewGlogSink()
	case "stdout":
		e = NewStdoutSink()
	case "http", "containership_http":
		url := viper.GetString("httpSinkUrl")
		if url == "" {
			panic("http sync specified but no httpSinkUrl")
		}

		// By default we buffer up to 1500 events, and drop messages if more than
		// 1500 have come in without getting consumed
		viper.SetDefault("httpSinkBufferSize", 1500)
		viper.SetDefault("httpSinkDiscardMessages", true)
		// Users can optionally specify HTTP headers to be included in every request
		viper.SetDefault("httpHeaders", nil)

		bufferSize := viper.GetInt("httpSinkBufferSize")
		overflow := viper.GetBool("httpSinkDiscardMessages")
		headers := viper.GetStringMapString("httpHeaders")

		// Construct a new HTTP sink or Containership HTTP sink accordingly,
		// kick it off, and return the interface in e
		switch s {
		case "http":
			h := NewHTTPSink(url, overflow, bufferSize, headers)
			go h.Run(make(chan bool))
			e = h
		case "containership_http":
			csType := viper.GetString("containershipType")
			if csType == "" {
				panic("containership_http sink specified but no containershipType")
			}
			h := NewContainershipHTTPSink(url, overflow, bufferSize, headers, csType)
			go h.Run(make(chan bool))
			e = h
		}
	case "kafka":
		viper.SetDefault("kafkaBrokers", []string{"kafka:9092"})
		viper.SetDefault("kafkaTopic", "eventrouter")
		viper.SetDefault("kafkaAsync", true)
		viper.SetDefault("kafkaRetryMax", 5)

		brokers := viper.GetStringSlice("kafkaBrokers")
		topic := viper.GetString("kafkaTopic")
		async := viper.GetBool("kakfkaAsync")
		retryMax := viper.GetInt("kafkaRetryMax")

		e, err := NewKafkaSink(brokers, topic, async, retryMax)
		if err != nil {
			panic(err.Error())
		}
		return e
	// case "logfile"
	default:
		err := errors.New("Invalid Sink Specified")
		panic(err.Error())
	}
	return e
}
