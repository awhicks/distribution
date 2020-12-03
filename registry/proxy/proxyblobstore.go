package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/proxy/scheduler"
	"github.com/docker/distribution/registry/storage"
	"github.com/docker/distribution/registry/storage/driver"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

type proxyBlobStore struct {
	registrySize   int64
	localRepo      distribution.Repository
	linkPathFns    []linkPathFunc
	driver         storagedriver.StorageDriver
	localStore     distribution.BlobStore
	remoteStore    distribution.BlobService
	scheduler      *scheduler.TTLExpirationScheduler
	repositoryName reference.Named
	authChallenger authChallenger
}

// We specify this, for now 50kb for testing super easy
var storageCap int64 = 50000

// Inflight download size
var bytesDownloading int64 = 0

// Count of usages
var blobUsageCounts = make(map[digest.Digest]int)

// Last usage of blob with digest
var blobLastUse = make(map[digest.Digest]time.Time)

type BlobDigestWithInfo struct {
	digest    digest.Digest
	usages    int
	lastUsage time.Time
}

var _ distribution.BlobStore = &proxyBlobStore{}

// inflight tracks currently downloading blobs
var inflight = make(map[digest.Digest]struct{})

// mu protects inflight
var mu sync.Mutex

func setResponseHeaders(w http.ResponseWriter, length int64, mediaType string, digest digest.Digest) {
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Docker-Content-Digest", digest.String())
	w.Header().Set("Etag", digest.String())
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func (pbs *proxyBlobStore) copyContent(ctx context.Context, dgst digest.Digest, writer io.Writer) (distribution.Descriptor, error) {
	desc, err := pbs.remoteStore.Stat(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	if w, ok := writer.(http.ResponseWriter); ok {
		setResponseHeaders(w, desc.Size, desc.MediaType, dgst)
	}

	remoteReader, err := pbs.remoteStore.Open(ctx, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	defer remoteReader.Close()

	_, err = io.CopyN(writer, remoteReader, desc.Size)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	proxyMetrics.BlobPush(uint64(desc.Size))

	return desc, nil
}

func (pbs *proxyBlobStore) serveLocal(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (bool, error) {
	localDesc, err := pbs.localStore.Stat(ctx, dgst)
	if err != nil {
		// Stat can report a zero sized file here if it's checked between creation
		// and population.  Return nil error, and continue
		return false, nil
	}

	proxyMetrics.BlobPush(uint64(localDesc.Size))
	return true, pbs.localStore.ServeBlob(ctx, w, r, dgst)
}

func (pbs *proxyBlobStore) storeLocal(ctx context.Context, dgst digest.Digest) error {
	defer func() {
		mu.Lock()
		delete(inflight, dgst)
		mu.Unlock()
	}()
	var desc distribution.Descriptor
	var err error
	var bw distribution.BlobWriter

	bw, err = pbs.localStore.Create(ctx)
	if err != nil {
		return err
	}

	desc, err = pbs.copyContent(ctx, dgst, bw)
	if err != nil {
		return err
	}

	_, err = bw.Commit(ctx, desc)
	if err != nil {
		return err
	}

	return nil
}

func (pbs *proxyBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	if val, ok := blobUsageCounts[dgst]; ok {
		blobUsageCounts[dgst] = val + 1
	} else {
		blobUsageCounts[dgst] = 1
	}
	blobLastUse[dgst] = time.Now()
	blobDiskUsage, err := DirSize("/var/lib/registry/docker/registry/v2/blobs/")
	if err != nil {
		return err
	}
	desc, err := pbs.Stat(ctx, dgst)
	if err != nil {
		return err
	}
	log.Printf("Disk usage is %d and incoming is %d", blobDiskUsage, desc.Size)
	bytesDownloading += desc.Size
	// Check if size with new blob exceeds our storageCap, and if the new blob should fit in the cache.
	// If it does, evict things until sizeExceeding is free.
	if sizeExceeding := bytesDownloading + blobDiskUsage - storageCap; desc.Size < storageCap && sizeExceeding > 0 {
		pbs.EvictLRU(ctx, dgst, sizeExceeding)
	}

	served, err := pbs.serveLocal(ctx, w, r, dgst)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("Error serving blob from local storage: %s", err.Error())
		return err
	}

	if served {
		return nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return err
	}

	mu.Lock()
	_, ok := inflight[dgst]
	if ok {
		mu.Unlock()
		_, err := pbs.copyContent(ctx, dgst, w)
		return err
	}
	inflight[dgst] = struct{}{}
	mu.Unlock()

	go func(dgst digest.Digest) {
		if err := pbs.storeLocal(ctx, dgst); err != nil {
			dcontext.GetLogger(ctx).Errorf("Error committing to storage: %s", err.Error())
		}

		blobRef, err := reference.WithDigest(pbs.repositoryName, dgst)
		if err != nil {
			dcontext.GetLogger(ctx).Errorf("Error creating reference: %s", err)
			return
		}

		pbs.scheduler.AddBlob(blobRef, repositoryTTL)
	}(dgst)

	_, err = pbs.copyContent(ctx, dgst, w)
	if err != nil {
		return err
	}
	bytesDownloading -= desc.Size
	return nil
}

func (pbs *proxyBlobStore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	desc, err := pbs.localStore.Stat(ctx, dgst)
	if err == nil {
		return desc, err
	}

	if err != distribution.ErrBlobUnknown {
		return distribution.Descriptor{}, err
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return distribution.Descriptor{}, err
	}

	return pbs.remoteStore.Stat(ctx, dgst)
}

func (pbs *proxyBlobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	blob, err := pbs.localStore.Get(ctx, dgst)
	if err == nil {
		return blob, nil
	}

	if err := pbs.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return []byte{}, err
	}

	blob, err = pbs.remoteStore.Get(ctx, dgst)
	if err != nil {
		return []byte{}, err
	}

	_, err = pbs.localStore.Put(ctx, "", blob)
	if err != nil {
		return []byte{}, err
	}
	return blob, nil
}

// Unsupported functions
func (pbs *proxyBlobStore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest) (distribution.Descriptor, error) {
	return distribution.Descriptor{}, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	return nil, distribution.ErrUnsupported
}

func (pbs *proxyBlobStore) Clear(ctx context.Context, dgst digest.Digest) (err error) {
	// clear any possible existence of a link described in linkPathFns
	for _, linkPathFn := range pbs.linkPathFns {
		blobLinkPath, err := linkPathFn(pbs.repositoryName.String(), dgst)
		if err != nil {
			return err
		}

		err = pbs.driver.Delete(ctx, blobLinkPath)
		if err != nil {
			switch err := err.(type) {
			case driver.PathNotFoundError:
				continue // just ignore this error and continue
			default:
				return err
			}
		}
	}

	return nil
}

func (pbs *proxyBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	v := storage.NewVacuum(ctx, pbs.driver)
	blobs := pbs.localRepo.Blobs(ctx)
	if _, ok := blobUsageCounts[dgst]; ok {
		delete(blobUsageCounts, dgst)
	}
	if _, ok := blobLastUse[dgst]; ok {
		delete(blobLastUse, dgst)
	}
	err := blobs.Delete(ctx, dgst)
	if err != nil {
		return err
	}
	err = v.RemoveBlob(dgst.String())
	if err != nil {
		return err
	}
	return nil
}

func (pbs *proxyBlobStore) EvictLRU(ctx context.Context, dgst digest.Digest, spaceNeeded int64) error {
	log.Println("Evicting cache")
	blobEnumerator, ok := pbs.localStore.(distribution.BlobEnumerator)
	if !ok {
		return fmt.Errorf("Failed to convert localStore to BlobEnumerator")
	}
	blobSlice := make([]BlobDigestWithInfo, 0)
	blobEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		var usageCounts int
		var lastUsage time.Time
		if val, ok := blobUsageCounts[dgst]; ok {
			usageCounts = val
		} else {
			return fmt.Errorf("Blob was not found in usage counts map")
		}
		if val, ok := blobLastUse[dgst]; ok {
			lastUsage = val
		} else {
			return fmt.Errorf("Blob was not found in last use map")
		}
		blobSlice = append(blobSlice, BlobDigestWithInfo{
			digest:    dgst,
			usages:    usageCounts,
			lastUsage: lastUsage,
		})
		return nil
	})
	sort.SliceStable(blobSlice, func(i, j int) bool {
		return blobSlice[i].lastUsage.Before(blobSlice[j].lastUsage)
	})
	for _, blob := range blobSlice {
		desc, err := pbs.Stat(ctx, blob.digest)
		if err != nil {
			log.Printf("Error getting descriptor for blob: %s", err)
		} else {
			pbs.Delete(ctx, blob.digest)
			spaceNeeded -= desc.Size
			log.Printf("Space being freed, space needed going from %d to %d", spaceNeeded+desc.Size, spaceNeeded)
		}
	}
	return nil
}

// linkPathFunc describes a function that can resolve a link based on the
// repository name and digest.
type linkPathFunc func(name string, dgst digest.Digest) (string, error)

// blobLinkPath provides the path to the blob link, also known as layers.
func blobLinkPath(name string, dgst digest.Digest) (string, error) {
	return pathFor(layerLinkPathSpec{name: name, digest: dgst})
}

// manifestRevisionLinkPath provides the path to the manifest revision link.
func manifestRevisionLinkPath(name string, dgst digest.Digest) (string, error) {
	return pathFor(manifestRevisionLinkPathSpec{name: name, revision: dgst})
}
