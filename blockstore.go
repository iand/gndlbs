package gonubs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/iand/gonudb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const (
	BlockSize  = 4096 // should align with ssd block size
	LoadFactor = 0.5  // target occupancy of buckets
)

type Options = gonudb.StoreOptions

var BlocksBucket = []byte("blocks")

type Blockstore struct {
	store *gonudb.Store
}

var _ blockstore.Blockstore = (*Blockstore)(nil)

func Open(directory string, name string, opts *gonudb.StoreOptions) (*Blockstore, error) {
	datPath := filepath.Join(directory, name+".dat")
	keyPath := filepath.Join(directory, name+".key")
	logPath := filepath.Join(directory, name+".log")

	err := gonudb.CreateStore(datPath, keyPath, logPath, 1, gonudb.NewSalt(), BlockSize, LoadFactor)
	if err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && os.IsExist(pathErr) {
		} else {
			return nil, fmt.Errorf("create store: %w", err)
		}
	}

	s, err := gonudb.OpenStore(datPath, keyPath, logPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	return &Blockstore{store: s}, nil
}

func (b *Blockstore) Close() error {
	return b.store.Close()
}

func (b *Blockstore) Has(cid cid.Cid) (bool, error) {
	_, err := b.store.FetchReader(string(cid.Hash()))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, gonudb.ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func (b *Blockstore) Get(cid cid.Cid) (blocks.Block, error) {
	r, err := b.store.FetchReader(string(cid.Hash()))
	if err != nil {
		if errors.Is(err, gonudb.ErrKeyNotFound) {
			return nil, blockstore.ErrNotFound
		}
		return nil, err
	}

	val, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(val, cid)
}

func (b *Blockstore) GetSize(cid cid.Cid) (int, error) {
	size, err := b.store.DataSize(string(cid.Hash()))
	if errors.Is(err, gonudb.ErrKeyNotFound) {
		return -1, blockstore.ErrNotFound
	}
	return int(size), err
}

func (b *Blockstore) Put(block blocks.Block) error {
	// Note zero length blocks are not supported, will get gonudb.ErrDataMissing
	return b.store.Insert(string(block.Cid().Hash()), block.RawData())
}

func (b *Blockstore) PutMany(blocks []blocks.Block) error {
	for _, block := range blocks {
		if err := b.store.Insert(string(block.Cid().Hash()), block.RawData()); err != nil {
			return err
		}
	}
	return nil
}

func (b *Blockstore) DeleteBlock(cid cid.Cid) error {
	return fmt.Errorf("delete not supported")
}

func (b *Blockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	b.store.Flush()
	ch := make(chan cid.Cid)
	go func() {
		defer close(ch)
		rs := b.store.RecordScanner()
		for rs.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			ch <- cid.NewCidV1(cid.Raw, []byte(rs.Key()))
		}
		// normally would check rs.Err here
	}()

	return ch, nil
}

func (b *Blockstore) HashOnRead(_ bool) {
	// ignore
}
