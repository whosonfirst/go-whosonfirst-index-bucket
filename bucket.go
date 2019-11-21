package bucket

import (
	"context"
	"github.com/whosonfirst/go-whosonfirst-index"
	"gocloud.dev/blob"
	"io"
	_ "log"
	"net/url"
	_ "sync"
)

type BucketDriver struct {
	index.Driver
	bucket *blob.Bucket
}

func init() {
	dr := NewBucketDriver()
	index.Register("bucket", dr)
}

func NewBucketDriver() index.Driver {

	dr := &BucketDriver{}
	return dr
}

func (d *BucketDriver) Open(uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	u.Scheme = u.Host
	u.Host = ""

	uri = u.String()
	
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, uri)

	if err != nil {
		return err
	}

	d.bucket = bucket
	return nil
}

func (d *BucketDriver) IndexURI(ctx context.Context, index_cb index.IndexerFunc, uri string) error {

	// add go routines
	// add throttles

	var list func(context.Context, *blob.Bucket, string) error

	list = func(ctx context.Context, b *blob.Bucket, prefix string) error {

		iter := b.List(&blob.ListOptions{
			Delimiter: "/",
			Prefix:    prefix,
		})

		for {
			obj, err := iter.Next(ctx)

			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			if obj.IsDir {

				err := list(ctx, b, obj.Key)

				if err != nil {
					return err
				}

				continue
			}

			fh, err := d.bucket.NewReader(ctx, obj.Key, nil)

			if err != nil {
				return err
			}

			defer fh.Close()
			
			ctx = index.AssignPathContext(ctx, obj.Key)

			err = index_cb(ctx, fh)

			if err != nil {
				return err
			}
		}

		return nil
	}

	return list(ctx, d.bucket, uri)
}
