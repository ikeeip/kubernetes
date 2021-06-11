package pleg

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/klog/v2"

	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

type EventWatcherPLEG struct {
	ctx           context.Context
	eventCh       chan *PodLifecycleEvent
	eventWatcher  internalapi.ContainerEventsWatchInterface
	runtime       kubecontainer.Runtime
	cache         kubecontainer.Cache
	clock         clock.Clock
	subscribeCh   chan struct{}
	subscribeTime time.Time
}

func NewEventWatcherPLEG(runtime kubecontainer.Runtime, channelCapacity int,
	cache kubecontainer.Cache, clock clock.Clock) PodLifecycleEventGenerator {
	ctx := context.TODO()
	ew := &EventWatcherPLEG{
		ctx:          ctx,
		eventCh:      make(chan *PodLifecycleEvent, channelCapacity),
		eventWatcher: nil,
		runtime:      runtime,
		cache:        cache,
		clock:        clock,
		subscribeCh:  make(chan struct{}),
	}

	go func() {
		defer func() {
			close(ew.subscribeCh)
			if ew.eventWatcher != nil {
				ew.eventWatcher.Stop()
			}
		}()

		for {
			select {
			case <-ew.subscribeCh:
				requestLabels := []string{kubetypes.KubernetesPodNameLabel, kubetypes.KubernetesPodNamespaceLabel, kubetypes.KubernetesPodUIDLabel}
				eventWatcher, err := ew.runtime.SubscribeContainerEvent(requestLabels, []string{})
				if err != nil {
					klog.ErrorS(err, "Failed to subscribe container events")
					return
				}
				ew.eventWatcher = eventWatcher
				ew.subscribeTime = ew.clock.Now()

				go ew.loop()
			case <-ew.ctx.Done():
				return
			}
		}
	}()

	return ew
}

func (ew *EventWatcherPLEG) Watch() chan *PodLifecycleEvent {
	return ew.eventCh
}

func (ew *EventWatcherPLEG) Start() {
	ew.subscribeCh <- struct{}{}
}

func (ew *EventWatcherPLEG) Healthy() (bool, error) {
	// TODO: fixme!
	// if ew.subscribeCh != nil {
	// 	return true, nil
	// }
	elapsed := ew.clock.Since(ew.subscribeTime)
	if elapsed > 10*time.Second {
		return true, nil
	}
	return false, nil
}

func (ew *EventWatcherPLEG) loop() {
	ew.cache.UpdateTime(ew.clock.Now())
	for {
		select {
		case e, open := <-ew.eventWatcher.ResultChan():
			klog.V(2).InfoS("EventWatcherPLEG: event", "event", e, "open", open)
			if !open {
				klog.ErrorS(nil, "EventWatcherPLEG: Container events channel is closed, exiting the loop")
				return
			}

			timestamp := ew.clock.Now()

			podId := e.Labels[kubetypes.KubernetesPodUIDLabel]
			podName := e.Labels[kubetypes.KubernetesPodNameLabel]
			podNamespace := e.Labels[kubetypes.KubernetesPodNamespaceLabel]

			status, err := ew.runtime.GetPodStatus(types.UID(podId), podName, podNamespace)
			if err == nil {
				// TODO: this looks like a workaround
				// Preserve the pod IP across cache updates if the new IP is empty.
				// When a pod is torn down, kubelet may race with PLEG and retrieve
				// a pod status after network teardown, but the kubernetes API expects
				// the completed pod's IP to be available after the pod is dead.
				if len(status.IPs) == 0 {
					// For pods with no ready containers or sandboxes (like exited pods)
					// use the old status' pod IP
					oldStatus, err := ew.cache.Get(types.UID(podId))
					if err == nil && len(oldStatus.IPs) == 0 {
						status.IPs = oldStatus.IPs
					}
				}
			} else {
				klog.V(2).ErrorS(err, "EventWatcherPLEG: cant get pod status", "pod", klog.KRef(podNamespace, podName))
			}

			klog.V(2).InfoS("SyncLoop (CEW): Write status", "pod", klog.KRef(podNamespace, podName), "status", status)
			if len(status.ContainerStatuses) > 0 || len(status.SandboxStatuses) > 0 {
				ew.cache.Set(types.UID(podId), status, err, timestamp)
			} else {
				ew.cache.Delete(types.UID(podId))
			}
			ew.cache.UpdateTime(timestamp)

			plegEvent, err := ConvertCEWtoPLEGevent(e)
			if err == nil {
				ew.eventCh <- plegEvent
			} else {
				klog.ErrorS(err, "EventWatcherPLEG: Drop invalid event")
			}

		case <-ew.ctx.Done():
			return
		}
	}
}

func ConvertCEWtoPLEGevent(event runtimeapi.ContainerEventMessage) (*PodLifecycleEvent, error) {
	var plegType PodLifeCycleEventType

	podId, found := event.Labels[kubetypes.KubernetesPodUIDLabel]
	if !found {
		return nil, fmt.Errorf("can't find podID in event labels")
	}

	switch event.EventType {
	case runtimeapi.ContainerEventMessage_ContainerCreated:
		plegType = ContainerChanged
	case runtimeapi.ContainerEventMessage_ContainerStarted:
		plegType = ContainerStarted
	case runtimeapi.ContainerEventMessage_ContainerStopped:
		plegType = ContainerDied
	case runtimeapi.ContainerEventMessage_ContainerDeleted:
		plegType = ContainerRemoved
	case runtimeapi.ContainerEventMessage_Unspecified:
		plegType = ContainerChanged
	}

	//TODO: fixme
	containerId := event.GetContainerId()
	if containerId == "" {
		containerId = event.GetSandboxId()
	}

	return &PodLifecycleEvent{ID: types.UID(podId), Type: plegType, Data: containerId}, nil
}
