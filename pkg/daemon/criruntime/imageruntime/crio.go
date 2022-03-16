/*
Copyright 2021 The Kruise Authors.

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

package imageruntime

import (
	"context"
	"github.com/alibaba/pouch/pkg/jsonstream"
	daemonutil "github.com/openkruise/kruise/pkg/daemon/util"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"io"
	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cri/remote/util"
	"sync"
	"time"
)

const maxMsgSize = 1024 * 1024 * 16

// NewDockerImageService create a crio runtime
func NewCrioImageService(runtimeURI string, accountManager daemonutil.ImagePullAccountManager) (ImageService, error) {
	klog.V(3).InfoS("Connecting to image service", "endpoint", runtimeURI)
	addr, dialer, err := util.GetAddressAndDialer(runtimeURI)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)))
	if err != nil {
		klog.ErrorS(err, "Connect remote image service failed", "address", addr)
		return nil, err
	}

	klog.V(4).InfoS("Finding the CRI API image version")
	imageClient := runtimeapi.NewImageServiceClient(conn)

	if _, err := imageClient.ImageFsInfo(ctx, &runtimeapi.ImageFsInfoRequest{}); err == nil {
		klog.V(2).InfoS("Using CRI v1 image API")
	}

	return &crioImageService{
		accountManager: accountManager,
		snapshotter:    "",
		criImageClient: imageClient,
		httpProxy:      "",
	}, nil
}

type crioImageService struct {
	accountManager daemonutil.ImagePullAccountManager
	snapshotter    string
	criImageClient runtimeapi.ImageServiceClient
	httpProxy      string
	sync.Mutex
}

// PullImage implements ImageService.PullImage.
func (d *crioImageService) PullImage(ctx context.Context, imageName, tag string, pullSecrets []v1.Secret) (ImagePullStatusReader, error) {
	registry := daemonutil.ParseRegistry(imageName)
	fullImageName := imageName + ":" + tag
	// just for Reader
	pipeR, pipeW := io.Pipe()
	stream := jsonstream.New(pipeW, nil)

	defer stream.Close()
	defer stream.Wait()
	defer pipeW.Close()

	var auth *runtimeapi.AuthConfig
	pullImageReq := &runtimeapi.PullImageRequest{
		Image: &runtimeapi.ImageSpec{
			Image:       fullImageName,
			Annotations: make(map[string]string),
		},
		Auth:          auth, //default is nil
		SandboxConfig: nil,
	}
	var err error
	if len(pullSecrets) > 0 {
		authInfos, err := convertToRegistryAuths(pullSecrets, registry)
		if err == nil {
			var pullErrs []error
			for _, authInfo := range authInfos {
				var pullErr error
				klog.V(5).Infof("Pull image %v:%v with user %v", imageName, tag, authInfo.Username)

				pullImageReq.Auth = &runtimeapi.AuthConfig{
					Username: authInfo.Username,
					Password: authInfo.Password,
				}
				pullImageResp, pullErr := d.criImageClient.PullImage(ctx, pullImageReq)
				if pullErr == nil {
					stream.WriteObject(jsonstream.JSONMessage{
						ID:        pullImageResp.GetImageRef(),
						Status:    jsonstream.PullStatusDone,
						Detail:    nil,
						StartedAt: time.Now(),
						UpdatedAt: time.Now(),
					})
					return newImagePullStatusReader(pipeR), nil
				}
				klog.Warningf("Failed to pull image %v:%v with user %v, err %v", imageName, tag, authInfo.Username, pullErr)
				pullErrs = append(pullErrs, pullErr)

			}
			if len(pullErrs) > 0 {
				err = utilerrors.NewAggregate(pullErrs)
			}
		}
	}

	// Try the default secret
	if d.accountManager != nil {
		var authInfo *daemonutil.AuthInfo
		var defaultErr error
		authInfo, defaultErr = d.accountManager.GetAccountInfo(registry)
		if defaultErr != nil {
			klog.Warningf("Failed to get account for registry %v, err %v", registry, defaultErr)
			// When the default account acquisition fails, try to pull anonymously
		} else if authInfo != nil {
			klog.V(5).Infof("Pull image %v:%v with user %v", imageName, tag, authInfo.Username)
			pullImageReq.Auth = &runtimeapi.AuthConfig{
				Username: authInfo.Username,
				Password: authInfo.Password,
			}
			pullImageResp, err := d.criImageClient.PullImage(ctx, pullImageReq)
			if err == nil {
				stream.WriteObject(jsonstream.JSONMessage{
					ID:        pullImageResp.GetImageRef(),
					Status:    jsonstream.PullStatusDone,
					Detail:    nil,
					StartedAt: time.Now(),
					UpdatedAt: time.Now(),
				})
				return newImagePullStatusReader(pipeR), nil
			}
			d.handleRuntimeError(err)
			klog.Warningf("Failed to pull image %v:%v, err %v", imageName, tag, err)
			return nil, err

		}
	}
	if err != nil {
		return nil, err
	}
	// Anonymous pull
	pullImageResp, err := d.criImageClient.PullImage(ctx, pullImageReq)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to pull image reference %q", fullImageName)
	}

	klog.Infof("duizhang %v", pullImageResp)
	stream.WriteObject(jsonstream.JSONMessage{
		ID:        pullImageResp.GetImageRef(),
		Status:    jsonstream.PullStatusDone,
		Detail:    nil,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	return newImagePullStatusReader(pipeR), nil
}

// ListImages implements ImageService.ListImages.
func (d *crioImageService) ListImages(ctx context.Context) ([]ImageInfo, error) {

	return nil, nil
}

func (d *crioImageService) handleRuntimeError(err error) {
	if daemonutil.FilterCloseErr(err) {
		d.Lock()
		defer d.Unlock()
		d.criImageClient = nil
	}
}
