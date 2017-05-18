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

package newrelic

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	name       = "newrelic"
	version    = 1
	pluginType = plugin.PublisherPluginType
	timeout    = 5
)

var (
	// The maximum time a connection can sit around unused.
	maxConnectionIdle = time.Minute * 5
	// How frequently idle connections are checked
	watchConnectionWait = time.Minute * 1
	// Our connection pool
	clientPool = make(map[string]*clientConnection)
	// Mutex for synchronizing connection pool changes
	m             = &sync.Mutex{}
	invalidMetric = regexp.MustCompile("[^a-zA-Z0-9:_]")
	invalidLabel  = regexp.MustCompile("[^a-zA-Z0-9_]")
)

type clientConnection struct {
	Key      string
	Conn     *http.Client
	LastUsed time.Time
}

func watchConnections() {
	for {
		time.Sleep(watchConnectionWait)
		for k, c := range clientPool {
			if time.Now().Sub(c.LastUsed) > maxConnectionIdle {
				m.Lock()
				delete(clientPool, k)
				m.Unlock()
			}
		}
	}
}

func init() {
	go watchConnections()
}

// Meta returns a plugin meta data
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

//NewNewrelicPublisher returns an instance of the Newrelic publisher
func NewNewrelicPublisher() *newrelicPublisher {
	return &newrelicPublisher{}
}

type newrelicPublisher struct {
}

func (p *newrelicPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule("license_key", true)
	if err != nil {
		panic(err)
	}
	r1.Description = "Newrelic API license key"
	config.Add(r1)

	r2, err := cpolicy.NewStringRule("endpoint", false)
	if err != nil {
		panic(err)
	}
	r2.Description = "Newrelic endpoint"
	config.Add(r2)

	cp.Add([]string{""}, config)
	return cp, nil
}

// Publish publishes metric data to Newrelic.
func (p *newrelicPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := log.New()
	var metrics []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.Printf("Error decoding GOB: error=%v content=%v", err, content)
			return err
		}
	case plugin.SnapJSONContentType:
		err := json.Unmarshal(content, &metrics)
		if err != nil {
			logger.Printf("Error decoding JSON: error=%v content=%v", err, content)
			return err
		}
	default:
		logger.Printf("Error unknown content type '%v'", contentType)
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	client, err := selectClient(config)
	url := newrelicUrl(config)
	if err != nil {
		panic(err)
	}

	sendMetrics(config, url, client, metrics)
	return nil
}

type agentJson struct {
	Host    string `json:"host"`
	Version string `json:"version"`
}

type componentJson struct {
	Name     string                 `json:"name"`
	Guid     string                 `json:"guid"`
	Duration int                    `json:"duration"`
	Metrics  map[string]interface{} `json:"metrics"`
}

type newrelicJson struct {
	Agent      *agentJson       `json:"agent"`
	Components []*componentJson `json:"components"`
}

func sendMetrics(config map[string]ctypes.ConfigValue, url string, client *clientConnection, metrics []plugin.MetricType) {
	logger := getLogger(config)

	reqbody := createNewrelicJson(config, metrics)
	reqbodyJson, err := json.Marshal(reqbody)
	if err != nil {
		logger.WithFields(log.Fields{
			"error": err,
		}).Error("Error serializing Json")
	}
	logger.WithFields(log.Fields{
		"body": string(reqbodyJson),
	}).Debug("NewRelic send")
	req, err := http.NewRequest("POST", url, bytes.NewReader(reqbodyJson))
	req.Header.Set("X-License-Key", config["license_key"].(ctypes.ConfigValueStr).Value)
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Conn.Do(req)
	if err != nil {
		logger.Error("Error sending data to Newrelic: %v", err)
		return
	}
	defer res.Body.Close()
	resbody, err := ioutil.ReadAll(res.Body)
	log.WithFields(log.Fields{
		"response": resbody,
	}).Debug("Got response from Newrelic")
	if err != nil {
		logger.Error("Error getting Newrelic response: %v", err)
	}
}

func createNewrelicJson(config map[string]ctypes.ConfigValue, metrics []plugin.MetricType) (reqbody *newrelicJson) {
	componentsMap := map[string]*componentJson{
		"com.adobe.component.default": {
			Name:     "default",
			Guid:     "com.adobe.component.default",
			Duration: 1,
			Metrics:  make(map[string]interface{}),
		},
	}
	reqbody = &newrelicJson{
		Agent: &agentJson{
			Host:    "",
			Version: "1.0",
		},
		Components: []*componentJson{
			componentsMap["com.adobe.component.default"],
		},
	}

	for _, m := range metrics {
		name, component_name, component_guid, unit, host, value, _ := mangleMetric(m)
		if component_name == "" {
			// add to default component
			component_name = "default"
			component_guid = "com.adobe.component.default"
		} else {
			// create component
			if _, ok := componentsMap[component_guid]; ok == false {
				componentsMap[component_guid] = &componentJson{
					Name:     component_name,
					Guid:     component_guid,
					Duration: 1,
					Metrics:  make(map[string]interface{}),
				}
				reqbody.Components = append(reqbody.Components, componentsMap[component_guid])
			}
		}
		reqbody.Agent.Host = host
		metricName := name
		if unit != "" {
			metricName = fmt.Sprintf("%s[%s]", name, unit)
		}
		componentsMap[component_guid].Metrics[metricName] = value
	}

	return
}

func mangleMetric(m plugin.MetricType) (name string, component_name, component_guid, unit string, host string, value interface{}, ts int64) {
	tags := make(map[string]string)
	ns := m.Namespace().Strings()
	isDynamic, indexes := m.Namespace().IsDynamic()
	if isDynamic {
		for i, j := range indexes {
			ns = append(ns[:j-i], ns[j-i+1:]...)
			tags[m.Namespace()[j].Name] = m.Namespace()[j].Value
		}
	}

	for i, v := range ns {
		ns[i] = invalidMetric.ReplaceAllString(v, "_")
	}

	var keys []string
	for k := range m.Tags() {
		tags[k] = m.Tags()[k]
	}

	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if k == core.STD_TAG_PLUGIN_RUNNING_ON {
			host = tags[k]
		} else if k == "newrelic_guid" {
			component_guid = tags[k]
		} else if k == "newrelic_name" {
			component_name = tags[k]
		} else {
			ns = append(ns, invalidLabel.ReplaceAllString(tags[k], "_"))
		}
	}

	name = strings.Join(ns, "/")
	unit = m.Unit()
	value = m.Data()
	ts = m.Timestamp().Unix() * 1000
	return
}

func newrelicUrl(config map[string]ctypes.ConfigValue) string {
	endpoint := "https://platform-api.newrelic.com/platform/v1/metrics"

	if config["endpoint"].(ctypes.ConfigValueStr).Value != "" {
		endpoint = config["endpoint"].(ctypes.ConfigValueStr).Value
	}
	return endpoint
}

func selectClient(config map[string]ctypes.ConfigValue) (*clientConnection, error) {
	// This is not an ideal way to get the logger but deferring solving this for a later date
	logger := getLogger(config)

	// Pool changes need to be safe (read & write) since the plugin can be called concurrently by snapteld.
	m.Lock()
	defer m.Unlock()

	url := newrelicUrl(config)

	// Do we have a existing client?
	if clientPool[url] == nil {
		// create one and add to the pool
		con := &http.Client{}

		cCon := &clientConnection{
			Key:      url,
			Conn:     con,
			LastUsed: time.Now(),
		}
		// Add to the pool
		clientPool[url] = cCon

		logger.Debug("Opening new Newrelic connection[", url, "]")
		return clientPool[url], nil
	}
	// Update when it was accessed
	clientPool[url].LastUsed = time.Now()
	// Return it
	logger.Debug("Using open Newrelic connection[", url, "]")
	return clientPool[url], nil
}

func getLogger(config map[string]ctypes.ConfigValue) *log.Entry {
	logger := log.WithFields(log.Fields{
		"plugin-name":    name,
		"plugin-version": version,
		"plugin-type":    pluginType.String(),
	})

	// default
	log.SetLevel(log.WarnLevel)

	if debug, ok := config["debug"]; ok {
		switch v := debug.(type) {
		case ctypes.ConfigValueBool:
			if v.Value {
				log.SetLevel(log.DebugLevel)
				return logger
			}
		default:
			logger.WithFields(log.Fields{
				"field":         "debug",
				"type":          v,
				"expected type": "ctypes.ConfigValueBool",
			}).Error("invalid config type")
		}
	}

	if loglevel, ok := config["log-level"]; ok {
		switch v := loglevel.(type) {
		case ctypes.ConfigValueStr:
			switch strings.ToLower(v.Value) {
			case "warn":
				log.SetLevel(log.WarnLevel)
			case "error":
				log.SetLevel(log.ErrorLevel)
			case "debug":
				log.SetLevel(log.DebugLevel)
			case "info":
				log.SetLevel(log.InfoLevel)
			default:
				log.WithFields(log.Fields{
					"value":             strings.ToLower(v.Value),
					"acceptable values": "warn, error, debug, info",
				}).Warn("invalid config value")
			}
		default:
			logger.WithFields(log.Fields{
				"field":         "log-level",
				"type":          v,
				"expected type": "ctypes.ConfigValueStr",
			}).Error("invalid config type")
		}
	}

	return logger
}
