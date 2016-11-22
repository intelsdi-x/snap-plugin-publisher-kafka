/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

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

package kafka

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	"gopkg.in/Shopify/sarama.v1"
	"reflect"
)

const (
	PluginName    = "kafka"
	PluginVersion = 10
	PluginType    = plugin.PublisherPluginType
)

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(PluginName, PluginVersion, PluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

type kafkaPublisher struct{}

func NewKafkaPublisher() *kafkaPublisher {
	return &kafkaPublisher{}
}

type MetricToPublish struct {
	// The timestamp from when the metric was created.
	Timestamp time.Time         `json:"timestamp"`
	Namespace string            `json:"namespace"`
	Data      interface{}       `json:"data"`
	Unit      string            `json:"unit"`
	Tags      map[string]string `json:"tags"`
	Version_  int               `json:"version"`
	// Last advertised time is the last time the snap agent was told about a metric.
	LastAdvertisedTime time.Time `json:"last_advertised_time"`
}

// Publish sends data to a Kafka server
func (k *kafkaPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	var mts []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		// decode incoming metrics types
		if err := dec.Decode(&mts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid incoming content: %v, err=%v", content, err)
			return fmt.Errorf("Cannot decode incoming content, err=%v", err)
		}
	default:
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	topic := config["topic"].(ctypes.ConfigValueStr).Value
	brokers := parseBrokerString(config["brokers"].(ctypes.ConfigValueStr).Value)
	// format metrics types to metrics to be published
	metrics := formatMetricTypes(mts)

	var outputType string
	var key string
	var jsonOut []byte
	var err error
	//valid values = array, tree. set default to array, if not defined
	if value, ok := config["output_type"]; ok {
		outputType = value.(ctypes.ConfigValueStr).Value
	} else {
		outputType = "array"
	}

	//valid values: _none, tag name - e.g. plugin_running_on
	//don't fail if tag is not found, fallback to random partitioning
	if value, ok := config["key"]; ok {
		keyTag := value.(ctypes.ConfigValueStr).Value
		if len(metrics) > 0 {
			key = metrics[0].Tags[keyTag]
		}

	}

	switch outputType {
	case "array":
		jsonOut, err = json.Marshal(metrics)
	case "tree":
		if len(metrics) > 0 {
			var timestamp interface{} = metrics[0].Timestamp.UnixNano()
			var tags interface{} = metrics[0].Tags
			i2 := makeTree(metrics).(map[string]*interface{})
			i2["_timestamp"] = &timestamp
			i2["_tags"] = &tags
			jsonOut, err = json.Marshal(i2)
		} else {
			jsonOut, err = json.Marshal(make(map[string]string))
		}

	default:
		return fmt.Errorf("Unknow format")
	}

	if err != nil {
		return fmt.Errorf("Cannot marshal metrics to JSON format, err=%v", err)
	}

	return k.publish(topic, brokers, []byte(jsonOut), key)
}

func (k *kafkaPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule("topic", false, "snap")
	handleErr(err)
	r1.Description = "Kafka topic for publishing"

	r2, _ := cpolicy.NewStringRule("brokers", false, "localhost:9092")
	handleErr(err)
	r2.Description = "List of brokers separated by semicolon in the format: <broker-ip:port;broker-ip:port> (ex: \"192.168.1.1:9092;172.16.9.99:9092\")"

	config.Add(r1, r2)
	cp.Add([]string{""}, config)
	return cp, nil
}

// Internal method after data has been converted to serialized bytes to send
func (k *kafkaPublisher) publish(topic string, brokers []string, content []byte, key string) error {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		return fmt.Errorf("Cannot initialize a new Sarama SyncProducer using the given broker addresses (%v), err=%v", brokers, err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()
	if key == "" {
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(content),
		})
		return err
	} else {
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(content),
			Key:   sarama.ByteEncoder(key),
		})
		return err
	}

}

func makeTree(metrics []MetricToPublish) interface{} {
	var out interface{} = make(map[string]*interface{})
	for _, v := range metrics {
		s := strings.Split(v.Namespace, "/")[1:]
		putData(&out, s, v.Data)
	}
	return out
}

func putData(i1 *interface{}, path []string, data interface{}) {
	i2 := (*i1).(map[string]*interface{})
	if len(path) == 1 {
		i2[path[0]] = &data
	} else {
		if value, ok := i2[path[0]]; ok {
			if reflect.ValueOf(*value).Kind() == reflect.Map {
				putData(value, path[1:], data)
			} else {
				m := make(map[string]*interface{})
				m["__value"] = value
				var nMap interface{} = m
				i2[path[0]] = &nMap
				putData(i2[path[0]], path[1:], data)
			}
		} else {
			var nMap interface{} = make(map[string]*interface{})
			i2[path[0]] = &nMap
			putData(i2[path[0]], path[1:], data)
		}
	}
}

// formatMetricTypes returns metrics in format to be publish as a JSON based on incoming metrics types;
// i.a. namespace is formatted as a single string
func formatMetricTypes(mts []plugin.MetricType) []MetricToPublish {
	var metrics []MetricToPublish
	for _, mt := range mts {
		metrics = append(metrics, MetricToPublish{
			Timestamp:          mt.Timestamp(),
			Namespace:          mt.Namespace().String(),
			Data:               mt.Data(),
			Unit:               mt.Unit(),
			Tags:               mt.Tags(),
			Version_:           mt.Version(),
			LastAdvertisedTime: mt.LastAdvertisedTime(),
		})
	}
	return metrics
}
func parseBrokerString(brokerStr string) []string {
	// remove spaces from 'brokerStr'
	brokers := strings.Replace(brokerStr, " ", "", -1)

	// return split brokers separated by semicolon
	return strings.Split(brokers, ";")
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
