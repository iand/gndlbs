package gndlbs

import (
	"context"
	"fmt"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
)

var _ blockstore.Blockstore = (*BlockCache)(nil)

type BlockCache struct {
	store    *Blockstore
	upstream blockstore.Blockstore
}

func NewBlockCache(bs *Blockstore, upstream blockstore.Blockstore) *BlockCache {
	return &BlockCache{
		store:    bs,
		upstream: upstream,
	}
}

func (bc *BlockCache) Has(c cid.Cid) (bool, error) {
	has, err := bc.store.Has(c)
	if err == nil && has {
		return true, nil
	}

	data, err := bc.fillFromUpstream(c)
	if err != nil {
		return false, err
	}
	return data != nil, nil
}

func (bc *BlockCache) Get(c cid.Cid) (blocks.Block, error) {
	data, err := bc.store.Get(c)
	if err != nil {
		return data, nil
	}
	return bc.fillFromUpstream(c)
}

func (bc *BlockCache) GetSize(c cid.Cid) (int, error) {
	size, err := bc.store.GetSize(c)
	if err != nil {
		return size, nil
	}
	blk, err := bc.fillFromUpstream(c)
	return len(blk.RawData()), err
}

func (bc *BlockCache) fillFromUpstream(c cid.Cid) (blocks.Block, error) {
	if bc.upstream == nil {
		return nil, blockstore.ErrNotFound
	}

	blk, err := bc.upstream.Get(c)
	if err != nil {
		return nil, err
	}

	data := blk.RawData()
	// Only insert if the block data and cid match, since we can't delete from the store
	chkc, err := c.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	if !chkc.Equals(c) {
		return nil, blocks.ErrWrongHash
	}

	if err := bc.store.Put(blk); err != nil {
		// ignore the error
	}
	return blk, nil
}

func (bc *BlockCache) Put(block blocks.Block) error {
	return fmt.Errorf("Put not supported on cache")
}

func (bc *BlockCache) PutMany(blocks []blocks.Block) error {
	return fmt.Errorf("PutMany not supported on cache")
}

func (bc *BlockCache) DeleteBlock(cid cid.Cid) error {
	return fmt.Errorf("DeleteBlock not supported on cache")
}

func (bc *BlockCache) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("AllKeysChan not supported on cache")
}

func (bc *BlockCache) HashOnRead(_ bool) {
	// ignore
}
