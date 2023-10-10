/*
Copyright 2017 The Kubernetes Authors.

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

package main

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	samplev1alpha1 "k8s.io/sample-controller/pkg/apis/samplecontroller/v1alpha1"
	clientset "k8s.io/sample-controller/pkg/generated/clientset/versioned"
	samplescheme "k8s.io/sample-controller/pkg/generated/clientset/versioned/scheme"
	informers "k8s.io/sample-controller/pkg/generated/informers/externalversions/samplecontroller/v1alpha1"
	listers "k8s.io/sample-controller/pkg/generated/listers/samplecontroller/v1alpha1"
)

const controllerAgentName = "sample-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Foo is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Foo fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Foo"
	// MessageResourceSynced is the message used for an Event fired when a Foo
	// is synced successfully
	MessageResourceSynced = "Foo synced successfully"
)

// Controller is the controller implementation for Foo resources
// Controller 是 Foo resources 的 controller 实现
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	// kubeclientset 是标准的 kubernetes 客户端集
	kubeclientset kubernetes.Interface
	// sampleclientset is a clientset for our own API group
	// ampleclientset 是我们自己的 API 组的客户端集
	sampleclientset clientset.Interface

	deploymentsLister appslisters.DeploymentLister
	deploymentsSynced cache.InformerSynced
	foosLister        listers.FooLister
	foosSynced        cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	//
	// workqueue 是一个速率受限的工作队列。这用于将要处理的工作排队，而不是在发生更改时立即执行。
	// 这意味着我们可以确保我们一次只处理固定数量的资源，并且可以很容易地确保我们不会同时在两个不
	// 同的工作人员中处理相同的项目。
	//
	// workqueue 在以下几种情况会 add obj：
	// - FooInformer 发生了 add 和 update 事件
	// （疑问：没有对 delete 事件进行处理，但是我测试后发现删除 foos 会一并删除其拥有的 deployment，这是如何做到的？答：级联删除，通过设置 OwnerReferences）
	// - DeploymentInformer 发生了 add、update、delete 事件，会调用 handleObject 将该 deployment
	// 所属的 foos 对象添加到 workqueue
	// 最终添加到 workqueue 的都是 obj 的 <namespace>/<name> 形式的 string
	//
	// runWorker() 中不断调用 processNextWorkItem() 从 workqueue 中 pop 元素，然后进一步调用 syncHandler
	// 将 deployment.replicas 和其所属的 Foo 的 spec.replicas 进行同步操作
	// 然后通过 pop 出的元素（记录 <namespace>/<name> 的 string）找到 Foo 实体对象
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	// recorder 是一个事件记录器，用于将 Event 资源记录到 kubernetes api
	// 也就是 kubectl describe 中输出的 event
	recorder record.EventRecorder
}

// NewController returns a new sample controller
func NewController(
	kubeclientset kubernetes.Interface,
	sampleclientset clientset.Interface,
	deploymentInformer appsinformers.DeploymentInformer,
	fooInformer informers.FooInformer) *Controller {

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for sample-controller types.
	// 创建事件广播器
	// 将 sample-controller 的类型信息（Foo）添加到默认 kubernetes Scheme，以便可以
	// 记录 sample-controller 类型的事件
	utilruntime.Must(samplescheme.AddToScheme(scheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	// 基于新 Scheme 创建一个事件记录器 recoreder，用于记录来自 sample-controller 的事件
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	// 构建 controller
	controller := &Controller{
		kubeclientset:     kubeclientset,
		sampleclientset:   sampleclientset,
		deploymentsLister: deploymentInformer.Lister(),
		deploymentsSynced: deploymentInformer.Informer().HasSynced,
		foosLister:        fooInformer.Lister(),
		foosSynced:        fooInformer.Informer().HasSynced,
		workqueue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Foos"),
		recorder:          recorder,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when Foo resources change
	// 给 Foo resources 设置事件回调函数
	fooInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueFoo, // 如果新创建了一个 Foo，添加到 workqueue
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueFoo(new) // 如果更新了一个 Foo，将更新后的 Foo 添加到 workqueue
		},
	})
	// Set up an event handler for when Deployment resources change. This
	// handler will lookup the owner of the given Deployment, and if it is
	// owned by a Foo resource then the handler will enqueue that Foo resource for
	// processing. This way, we don't need to implement custom logic for
	// handling Deployment resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	//
	// 为 deployment 更改时设置事件处理程序。此处理程序将查找给定 deployment 的所有者，如果它由 Foo 资源拥有，
	// 则处理程序将对该 Foo resources 进行排队以进行处理。这样，我们就不需要实现自定义逻辑来处理 deployment。
	// 有关此模式的更多信息：
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	// 其中 Add、Update、Delete 均通过 handleObject 处理
	deploymentInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.handleObject,
		UpdateFunc: func(old, new interface{}) {
			newDepl := new.(*appsv1.Deployment)
			oldDepl := old.(*appsv1.Deployment)
			if newDepl.ResourceVersion == oldDepl.ResourceVersion {
				// Periodic resync will send update events for all known Deployments.
				// Two different versions of the same Deployment will always have different RVs.
				//
				// 定期 resync 也会触发 update 事件，这里通过判断 ResourceVersion 是否相同来判断
				// 对象是否实际发生变更，如果相同则说明是因为 resync 触发的更新事件，无需做进一步处理
				return
			}
			controller.handleObject(new)
		},
		DeleteFunc: controller.handleObject,
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
//
// Run 将为我们感兴趣的类型设置事件处理程序，以及同步通知者缓存和启动工作人员。它将阻塞直到 stopCh 关闭，
// 此时它将关闭工作队列并等待工作人员完成处理他们当前的工作项。
func (c *Controller) Run(workers int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting Foo controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.deploymentsSynced, c.foosSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch two workers to process Foo resources
	// 开启 workers 个 goroutine，每个 goroutine 运行 runWorker() 函数
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
//
// runWorker 是一个长时间运行的函数，它将不断调用 processNextWorkItem 函数以读取和处理工作队列上的消息。
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
//
// processNextWorkItem 将从工作队列中读取单个工作项并尝试通过调用 syncHandler 来处理它。
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		//
		// 我们在这里调用 Done，以便工作队列知道我们已经完成了该项目的处理。如果我们不希望该工作项重新排队，
		// 我们还必须记住调用 Forget。例如，如果发生暂时性错误，我们不会调用 Forget，而是将项目放回到工作队列中，并在退避期后再次尝试。
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		//
		// 我们期望从 queue pop 出的元素实际是 string 类型，存储的字符串内容是 namespace/name
		// 疑问：这里为啥会存在 !ok 的情况？根据其他代码来看，push 到 queue 中的都是 string，不存在其他类型的元素啊？
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Foo resource to be synced.
		//
		// 运行 syncHandler，向其传递要同步的 Foo 资源的 namespace/name 字符串。
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		//
		// 最后，如果没有发生错误，我们会忘记这个 item，这样它就不会再次排队，直到发生另一个更改。
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Foo resource
// with the current status of the resource.
//
// syncHandler 将实际状态与期望的状态进行比较，并尝试将两者收敛。然后它用资源的当前状态
// 更新 Foo 资源的 Status 块。
// 从 workqueu 中 pop 时会调用该函数，将 pop 出的 obj 作为参数，workqueue 中保存的都是
// namespace/name 格式的 string，所以参数 key 是 string 类型
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	// 从 namespace/name string 中获取 namespace 和 name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Foo resource with this namespace/name
	// 通过 namespace 和 name 从 foosLister 中获取对应的 Foo resources
	foo, err := c.foosLister.Foos(namespace).Get(name)
	if err != nil {
		// The Foo resource may no longer exist, in which case we stop
		// processing.
		// Foo 资源可能不再存在，在这种情况下我们停止处理。
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("foo '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	// 获取该 foo 拥有的 deploy（通过 spec.DeploymentName 先获取 deploy name，之后再用这个 name
	// 从 deploymentsLister 中获取具体的 deploy 对象）
	deploymentName := foo.Spec.DeploymentName
	if deploymentName == "" { // 该 foo 没有设置拥有的 deploy，则直接 return，避免该 foo 再次入队
		// We choose to absorb the error here as the worker would requeue the
		// resource otherwise. Instead, the next time the resource is updated
		// the resource will be queued again.
		utilruntime.HandleError(fmt.Errorf("%s: deployment name must be specified", key))
		return nil
	}

	// Get the deployment with the name specified in Foo.spec
	// 然后通过 foo.Spec.DeploymentName 从 deploymentsLister 中获取具体的 deploy 对象
	deployment, err := c.deploymentsLister.Deployments(foo.Namespace).Get(deploymentName)
	// If the resource doesn't exist, we'll create it
	// 如果该 deploy 不存在，则创建它
	if errors.IsNotFound(err) {
		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Create(context.TODO(), newDeployment(foo), metav1.CreateOptions{})
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	//
	// 如果在 Get/Create 期间发生错误，我们将重新排队该项目，以便稍后再次尝试处理。
	// 这可能是由临时网络故障或任何其他暂时性原因引起的。
	if err != nil {
		return err
	}

	// If the Deployment is not controlled by this Foo resource, we should log
	// a warning to the event recorder and return error msg.
	// 如果此 deployment 不受此 Foo 资源控制，我们应该向事件记录器记录警告并返回错误消息。
	if !metav1.IsControlledBy(deployment, foo) {
		msg := fmt.Sprintf(MessageResourceExists, deployment.Name)
		c.recorder.Event(foo, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf("%s", msg)
	}

	// If this number of the replicas on the Foo resource is specified, and the
	// number does not equal the current desired replicas on the Deployment, we
	// should update the Deployment resource.
	//
	// 如果指定了 Foo 资源上的副本数，并且该数量不等于 Deployment 上当前所需的副本数，
	// 我们应该更新 Deployment 资源。
	if foo.Spec.Replicas != nil && *foo.Spec.Replicas != *deployment.Spec.Replicas {
		klog.V(4).Infof("Foo %s replicas: %d, deployment replicas: %d", name, *foo.Spec.Replicas, *deployment.Spec.Replicas)
		deployment, err = c.kubeclientset.AppsV1().Deployments(foo.Namespace).Update(context.TODO(), newDeployment(foo), metav1.UpdateOptions{})
	}

	// If an error occurs during Update, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	//
	// 如果更新期间发生错误，我们将重新排队该项目，以便稍后再次尝试处理。这可能是由临时网络故障或任何其他暂时原因引起的。
	if err != nil {
		return err
	}

	// Finally, we update the status block of the Foo resource to reflect the
	// current state of the world
	err = c.updateFooStatus(foo, deployment)
	if err != nil {
		return err
	}

	c.recorder.Event(foo, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) updateFooStatus(foo *samplev1alpha1.Foo, deployment *appsv1.Deployment) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	fooCopy := foo.DeepCopy()
	fooCopy.Status.AvailableReplicas = deployment.Status.AvailableReplicas
	// If the CustomResourceSubresources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the Foo resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	_, err := c.sampleclientset.SamplecontrollerV1alpha1().Foos(foo.Namespace).UpdateStatus(context.TODO(), fooCopy, metav1.UpdateOptions{})
	return err
}

// enqueueFoo takes a Foo resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Foo.
//
// enqueueFoo 获取 Foo 资源并将其转换为 namespace/name 形式的字符串，然后将其放入 workqueue 中。
// 此方法不应传递除 Foo 之外的任何类型的资源。
func (c *Controller) enqueueFoo(obj interface{}) {
	var key string
	var err error
	// 获取 obj 的 namespace 和 name，然后将其转换为 namespace/name 字符串
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	// 添加到 workqueue
	c.workqueue.Add(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Foo resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Foo resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
//
// handleObject 将获取任何实现 metav1.Object 的资源，并尝试找到“拥有”它的 Foo 资源。
// 它通过查看对象 metadata.ownerReferences 字段中是否有适当的 OwnerReference 来实现此目的。
// 然后，它将要处理的 Foo 资源排入队列。如果该对象没有适当的 OwnerReference，它将被跳过。
//
// 该函数在 deploymentInformer 的事件回调函数中调用，传入的 obj 是 deployment
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	klog.V(4).Infof("Processing object: %s", object.GetName())
	// 获取当前 obj（deployment）的所属者，（从 metadata.ownerReferences 获取）
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Foo, we should not do anything more
		// with it.
		// 如果该 obj 所属者的 Kind 不是 Foo，我们不应该对它做更多的事情
		if ownerRef.Kind != "Foo" {
			return
		}
		// 进一步通过 fooLister 找到具体的 Foo resources（通过 ownerRef.name）
		// （通过 Informer 本地缓存来查找）
		foo, err := c.foosLister.Foos(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			klog.V(4).Infof("ignoring orphaned object '%s/%s' of foo '%s'", object.GetNamespace(), object.GetName(), ownerRef.Name)
			return
		}
		// 将 foo 加入 workqueue
		// deploymentInformers 的 Delete 事件也会调用 handleObject，到这里会找到被删除的 deploy 所属
		// 的 Foo，将其添加到 workqueue，之后处理程序从 workqueue 中 pop 出该 Foo 时，通过检查其
		// 定义的 replicas 进行 deployment 的同步，所以即便手动删除了 deployment，之后也会新创建
		// 出来，保证 deployment 的数量和其所属的 Foo 中定义的 replicas 相同
		c.enqueueFoo(foo)
		return
	}
}

// newDeployment creates a new Deployment for a Foo resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Foo resource that 'owns' it.
//
// newDeployment 为 Foo 资源创建一个新的 deployment 。它还在资源上设置适当的 OwnerReferences，
// 以便 handleObject 可以发现拥有它的 Foo 资源。
func newDeployment(foo *samplev1alpha1.Foo) *appsv1.Deployment {
	labels := map[string]string{
		"app":        "nginx",
		"controller": foo.Name,
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      foo.Spec.DeploymentName,
			Namespace: foo.Namespace,
			OwnerReferences: []metav1.OwnerReference{ // 设置该 deployment 的所属 Foo
				*metav1.NewControllerRef(foo, samplev1alpha1.SchemeGroupVersion.WithKind("Foo")),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: foo.Spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nginx",
							Image: "nginx:latest",
						},
					},
				},
			},
		},
	}
}
