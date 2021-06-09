/*
Copyright The Kubernetes Authors.

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

package cri

import (
	"sync"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/klog/v2"
)

// Interface can be implemented by anything that knows how to watch and report 
// container changes.
type ContainerEventsWatchInterface interface {
	// Stops watching. Will close the channel returned by ResultChan(). Releases
	// any resources used by the watch.
	Stop()

	// Returns a chan which will receive all the events. If an error occurs
	// or Stop() is called, the implementation will close this channel and
	// release any resources used by the watch.
	ResultChan() <-chan runtimeapi.ContainerEventMessage
}

type ContainerEventsFakeWatcher struct {
	result  chan runtimeapi.ContainerEventMessage
	stopped bool
	sync.Mutex
}

func NewContainerEventsFakeWatcher(requestLabels, requestAnnotations []string) *ContainerEventsFakeWatcher {
	return &ContainerEventsFakeWatcher{
		result: make(chan runtimeapi.ContainerEventMessage),
	}
}

func (f *ContainerEventsFakeWatcher) Stop() {
	f.Lock()
	defer f.Unlock()
	if !f.stopped {
		klog.V(4).Infof("Stopping fake watcher.")
		close(f.result)
		f.stopped = true
	}
}

func (f *ContainerEventsFakeWatcher) ResultChan() <-chan runtimeapi.ContainerEventMessage {
	return f.result
}
