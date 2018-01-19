package sinks

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
)

// TODO much of this can be abstracted back into HTTPSink with some more heavy
// refactoring.

// ContainershipHTTPSink is an HTTPSink that does not use syslog but instead
// packages events directly in JSON with a type field added to each event
type ContainershipHTTPSink struct {
	*HTTPSink
	csType string
}

// EventDataOfType wraps EventData and adds a type string
type EventDataOfType struct {
	Type  string    `json:"type"`
	Event EventData `json:"event"`
}

func newEventDataOfType(t string, evt EventData) EventDataOfType {
	return EventDataOfType{
		Type:  t,
		Event: evt,
	}
}

// NewContainershipHTTPSink constructs a new ContainershipHTTPSink given a sink URL and buffer size
func NewContainershipHTTPSink(sinkURL string, overflow bool, bufferSize int,
	headers map[string]string, csType string) *ContainershipHTTPSink {

	// The Containership sink requires pulling in additional header info from a
	// secret, so let's just add to the headers here
	token := os.Getenv("CONTAINERSHIP_CLOUD_CLUSTER_API_KEY")
	if token == "" {
		glog.Warning("CONTAINERSHIP_CLOUD_CLUSTER_API_KEY not specified")
	} else {
		headers["Authorization"] = fmt.Sprintf("JWT %s", token)
	}

	h := &ContainershipHTTPSink{
		HTTPSink: NewHTTPSink(sinkURL, overflow, bufferSize, headers),
		csType:   csType,
	}

	return h
}

// UpdateEvents implements the EventSinkInterface. It really just writes the
// event data to the event OverflowingChannel, which should never block.
// Messages that are buffered beyond the bufferSize specified for this HTTPSink
// are discarded.
func (h *ContainershipHTTPSink) UpdateEvents(eNew *v1.Event, eOld *v1.Event) {
	h.HTTPSink.UpdateEvents(eNew, eOld)
}

// Run sits in a loop, waiting for data to come in through h.eventCh,
// and forwarding them to the HTTP sink. If multiple events have happened
// between loop iterations, it puts all of them in one request instead of
// making a single request per event.
func (h *ContainershipHTTPSink) Run(stopCh <-chan bool) {
loop:
	for {
		select {
		case e := <-h.eventCh.Out():
			var evt EventData
			var ok bool
			if evt, ok = e.(EventData); !ok {
				glog.Warningf("Invalid type sent through event channel: %T", e)
				continue loop
			}

			// Add a type to the event
			arr := []EventDataOfType{
				newEventDataOfType(h.csType, evt),
			}

			// Consume all buffered events into an array, in case more have been written
			// since we last forwarded them
			numEvents := h.eventCh.Len()
			for i := 0; i < numEvents; i++ {
				e := <-h.eventCh.Out()
				if evt, ok = e.(EventData); ok {
					arr = append(arr, newEventDataOfType(h.csType, evt))
				} else {
					glog.Warningf("Invalid type sent through event channel: %T", e)
				}
			}

			h.drainEvents(arr)
		case <-stopCh:
			break loop
		}
	}
}

// drainEvents takes an array of event data and sends it to the receiving HTTP
// server. This function is *NOT* re-entrant: it re-uses the same body buffer
// for each call, truncating it each time to avoid extra memory allocations.
func (h *ContainershipHTTPSink) drainEvents(events []EventDataOfType) {
	// Reuse the body buffer for each request
	h.bodyBuf.Truncate(0)

	json, err := json.Marshal(events)
	if err != nil {
		glog.Errorf("ContainershipHTTPSink json marshal failed: %s\n", err.Error())
	}

	// Note that Write() will grow the buffer if needed
	h.bodyBuf.Write(json)

	req, err := http.NewRequest("POST", h.SinkURL, h.bodyBuf)
	if err != nil {
		glog.Warningf(err.Error())
		return
	}

	// Add optional http headers
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		glog.Warningf(err.Error())
		return
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		glog.Warningf("Got HTTP code %v from %v", resp.StatusCode, h.SinkURL)
	}
}
