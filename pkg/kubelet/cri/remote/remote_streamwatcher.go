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

package remote

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/klog/v2"
)

const relistPeriod = 10 * time.Second

type ContainerEventStreamWatcher struct {
	client             runtimeapi.RuntimeServiceClient
	request            runtimeapi.SubscribeContainerEventRequest
	result             chan runtimeapi.ContainerEventMessage
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	subscribeCh        chan struct{}
	backoff            wait.Backoff
	containerLastEvent map[string]runtimeapi.ContainerEventMessage
}

func newContainerEventStreamWatcher(ctx context.Context, client runtimeapi.RuntimeServiceClient, requestLabels, requestAnnotations []string) *ContainerEventStreamWatcher {
	ctx, cancel := context.WithCancel(ctx)
	sw := &ContainerEventStreamWatcher{
		client: client,
		request: runtimeapi.SubscribeContainerEventRequest{
			WithLabels:      requestLabels,
			WithAnnotations: requestAnnotations,
		},
		result:      make(chan runtimeapi.ContainerEventMessage),
		ctx:         ctx,
		cancel:      cancel,
		wg:          sync.WaitGroup{},
		subscribeCh: make(chan struct{}),
		backoff: wait.Backoff{
			Steps:    6,
			Duration: 1 * time.Second,
			Factor:   2.0,
			Jitter:   0.2,
		},
		containerLastEvent: make(map[string]runtimeapi.ContainerEventMessage),
	}

	go func() {
		relistTicker := time.NewTicker(relistPeriod)
		defer relistTicker.Stop()

		defer close(sw.result)
		defer close(sw.subscribeCh)

		for {
			select {
			case <-sw.subscribeCh:
				klog.V(2).InfoS("[ContainerEventStreamWatcher] subscribe...")

				stream, cancel, err := sw.subscribeWithBackoff()
				if err != nil {
					klog.V(2).ErrorS(err, "[ContainerEventStreamWatcher] subscribe failed")
					break
				}

				go sw.loop(stream, cancel)
			case <-relistTicker.C:
				sw.relist()
			case <-sw.ctx.Done():
				klog.V(2).InfoS("[ContainerEventStreamWatcher] done")
				break
			}
		}
	}()

	sw.subscribeCh <- struct{}{}

	return sw
}

func (sw *ContainerEventStreamWatcher) ResultChan() <-chan runtimeapi.ContainerEventMessage {
	return sw.result
}

func (sw *ContainerEventStreamWatcher) Stop() {
	sw.cancel()
	sw.wg.Wait()
}

func (sw *ContainerEventStreamWatcher) subscribe() (runtimeapi.RuntimeService_SubscribeContainerEventClient, context.CancelFunc, error) {
	// TODO: wrap stream and cancel within new struct
	ctx, cancel := context.WithCancel(sw.ctx)
	stream, err := sw.client.SubscribeContainerEvent(ctx, &sw.request)
	if err != nil {
		cancel()
		klog.ErrorS(err, "[ContainerEventStreamWatcher] SubscribeContainerEvent from runtime service failed")
		return nil, nil, err
	}
	return stream, cancel, err
}

func (sw *ContainerEventStreamWatcher) subscribeWithBackoff() (runtimeapi.RuntimeService_SubscribeContainerEventClient, context.CancelFunc, error) {
	var stream runtimeapi.RuntimeService_SubscribeContainerEventClient
	var cancel context.CancelFunc
	var lastErr error
	wait.ExponentialBackoff(sw.backoff, func() (bool, error) {
		stream, cancel, lastErr = sw.subscribe()
		return lastErr == nil, nil
	})
	return stream, cancel, lastErr
}

func (sw *ContainerEventStreamWatcher) loop(stream runtimeapi.RuntimeService_SubscribeContainerEventClient, cancel context.CancelFunc) {
	klog.V(2).InfoS("[ContainerEventStreamWatcher] loop")
	defer func() {
		klog.V(2).InfoS("[ContainerEventStreamWatcher] loop finished")
	}()

	sw.wg.Add(1)
	defer sw.wg.Done()

	for {
		event, err := stream.Recv()
		if err != nil {
			klog.V(2).Infof("[ContainerEventStreamWatcher] EOF during watch stream event: %v", err)
			cancel()
			sw.subscribeCh <- struct{}{}
			return
		}

		//TODO: fixme
		containerId := event.GetContainerId()
		if containerId == "" {
			containerId = event.GetSandboxId()
		}
		sw.containerLastEvent[containerId] = *event

		select {
		case sw.result <- *event:
		case <-sw.ctx.Done():
			return
		}
	}
}

// TODO: naive implementation
func (sw *ContainerEventStreamWatcher) relist() {
	klog.V(4).InfoS("[ContainerEventStreamWatcher] relist")
	defer func() {
		klog.V(4).InfoS("[ContainerEventStreamWatcher] relist finished")
	}()

	ctx, cancel := context.WithCancel(sw.ctx)
	defer cancel()

	unprocessed := make(map[string]bool)
	for containerId := range sw.containerLastEvent {
		unprocessed[containerId] = true
	}

	respSandboxes, err := sw.client.ListPodSandbox(ctx, &runtimeapi.ListPodSandboxRequest{})
	if err == nil {
		for _, container := range respSandboxes.Items {
			oldEvent := sw.containerLastEvent[container.GetId()]

			newEventType := runtimeapi.ContainerEventMessage_Unspecified
			switch container.State {
			case runtimeapi.PodSandboxState_SANDBOX_READY:
				newEventType = runtimeapi.ContainerEventMessage_ContainerStarted
			case runtimeapi.PodSandboxState_SANDBOX_NOTREADY:
				newEventType = runtimeapi.ContainerEventMessage_ContainerStopped
			}

			klog.V(4).InfoS("[ContainerEventStreamWatcher] relist sandbox",
				"container", container.Id,
				"state", container.State.String(),
				"oldEventType", oldEvent.EventType.String(),
				"newEventType", newEventType.String(),
			)

			if newEventType != runtimeapi.ContainerEventMessage_Unspecified &&
				newEventType != oldEvent.EventType {
				event := runtimeapi.ContainerEventMessage{
					EventType:   newEventType,
					ContainerId: container.GetId(),
					SandboxId:   container.GetId(),
					Labels:      container.GetLabels(),
					Annotations: container.GetAnnotations(),
				}

				sw.containerLastEvent[container.GetId()] = event
				sw.result <- event
			}

			delete(unprocessed, container.GetId())

		}
	}

	respContainers, err := sw.client.ListContainers(ctx, &runtimeapi.ListContainersRequest{})
	if err == nil {
		for _, container := range respContainers.Containers {
			oldEvent := sw.containerLastEvent[container.GetId()]

			newEventType := runtimeapi.ContainerEventMessage_Unspecified
			switch container.State {
			case runtimeapi.ContainerState_CONTAINER_CREATED:
				newEventType = runtimeapi.ContainerEventMessage_ContainerUpdated
			case runtimeapi.ContainerState_CONTAINER_RUNNING:
				newEventType = runtimeapi.ContainerEventMessage_ContainerStarted
			case runtimeapi.ContainerState_CONTAINER_EXITED:
				newEventType = runtimeapi.ContainerEventMessage_ContainerStopped
			case runtimeapi.ContainerState_CONTAINER_UNKNOWN:
				klog.V(2).Infof("[ContainerEventStreamWatcher] container with UNKNOWN state: %v", container)
				newEventType = runtimeapi.ContainerEventMessage_ContainerUpdated
			}

			klog.V(4).InfoS("[ContainerEventStreamWatcher] relist container",
				"container", container.Id,
				"state", container.State.String(),
				"oldEventType", oldEvent.EventType.String(),
				"newEventType", newEventType.String(),
			)

			if newEventType != runtimeapi.ContainerEventMessage_Unspecified &&
				newEventType != oldEvent.EventType {
				event := runtimeapi.ContainerEventMessage{
					EventType:   newEventType,
					ContainerId: container.GetId(),
					SandboxId:   container.GetPodSandboxId(),
					Labels:      container.GetLabels(),
					Annotations: container.GetAnnotations(),
				}

				sw.containerLastEvent[container.GetId()] = event
				sw.result <- event
			}

			delete(unprocessed, container.GetId())
		}
	}

	for containerId := range unprocessed {
		oldEvent := sw.containerLastEvent[containerId]

		sw.result <- runtimeapi.ContainerEventMessage{
			EventType:   runtimeapi.ContainerEventMessage_ContainerDeleted,
			ContainerId: oldEvent.GetContainerId(),
			SandboxId:   oldEvent.GetSandboxId(),
			Labels:      oldEvent.GetLabels(),
			Annotations: oldEvent.GetAnnotations(),
		}

		delete(sw.containerLastEvent, containerId)
	}
}
