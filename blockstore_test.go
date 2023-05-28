package gonubs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/iand/gonudb"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/stretchr/testify/require"
)

// Tests derived from https://github.com/raulk/lotus-bs-bench

func newBlockstore(tb testing.TB) (*Blockstore, string, string) {
	tb.Helper()

	tmp, err := os.CreateTemp("", "")
	if err != nil {
		tb.Fatalf("unexpected error finding temp file: %v", err)
	}

	path := filepath.Dir(tmp.Name())
	base := filepath.Base(tmp.Name())
	bs, err := Open(path, base, nil)
	if err != nil {
		tb.Fatalf("unexpected error opening blockstore: %v", err)
	}

	tb.Cleanup(func() {
		_ = bs.Close()
		_ = os.RemoveAll(path)
	})

	return bs, path, base
}

func TestGetWhenKeyNotPresent(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	c := cid.NewCidV0(util.Hash([]byte("stuff")))
	bl, err := bs.Get(context.Background(), c)
	require.Nil(t, bl)
	require.True(t, ipld.IsNotFound(err))
}

func TestGetWhenKeyIsNil(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	_, err := bs.Get(context.Background(), cid.Undef)
	require.True(t, ipld.IsNotFound(err))
}

func TestPutThenGetBlock(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(context.Background(), orig)
	require.NoError(t, err)

	fetched, err := bs.Get(context.Background(), orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutEmpty(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	emptyBlock := blocks.NewBlock([]byte{})

	err := bs.Put(context.Background(), emptyBlock)
	require.Equal(t, gonudb.ErrDataMissing, err)
}

func TestHas(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(context.Background(), orig)
	require.NoError(t, err)

	ok, err := bs.Has(context.Background(), orig.Cid())
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = bs.Has(context.Background(), blocks.NewBlock([]byte("another thing")).Cid())
	require.NoError(t, err)
	require.False(t, ok)
}

func TestCidv0v1(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	orig := blocks.NewBlock([]byte("some data"))

	err := bs.Put(context.Background(), orig)
	require.NoError(t, err)

	fetched, err := bs.Get(context.Background(), cid.NewCidV1(cid.DagProtobuf, orig.Cid().Hash()))
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutThenGetSizeBlock(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	block := blocks.NewBlock([]byte("some data"))
	missingBlock := blocks.NewBlock([]byte("missingBlock"))

	err := bs.Put(context.Background(), block)
	require.NoError(t, err)

	blockSize, err := bs.GetSize(context.Background(), block.Cid())
	require.NoError(t, err)
	require.Len(t, block.RawData(), blockSize)

	missingSize, err := bs.GetSize(context.Background(), missingBlock.Cid())
	require.True(t, ipld.IsNotFound(err))
	require.Equal(t, -1, missingSize)
}

func TestAllKeysSimple(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	keys := insertBlocks(t, bs, 100)

	ctx := context.Background()
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)
	actual := collect(ch)

	require.ElementsMatch(t, keys, actual)
}

func TestAllKeysRespectsContext(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	_ = insertBlocks(t, bs, 100)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(t, err)

	// consume 2, then cancel context.
	v, ok := <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	v, ok = <-ch
	require.NotEqual(t, cid.Undef, v)
	require.True(t, ok)

	cancel()

	// Next read after cancel may succeed due to timing
	<-ch

	// Next read must be from a closed channel
	_, ok = <-ch
	require.False(t, ok)
}

func TestDoubleClose(t *testing.T) {
	bs, _, _ := newBlockstore(t)
	require.NoError(t, bs.Close())
	require.NoError(t, bs.Close())
}

func TestReopenPutGet(t *testing.T) {
	bs, path, base := newBlockstore(t)

	orig := blocks.NewBlock([]byte("some data"))
	err := bs.Put(context.Background(), orig)
	require.NoError(t, err)

	err = bs.Close()
	require.NoError(t, err)

	bs, err = Open(path, base, nil)
	require.NoError(t, err)
	defer bs.Close()

	fetched, err := bs.Get(context.Background(), orig.Cid())
	require.NoError(t, err)
	require.Equal(t, orig.RawData(), fetched.RawData())
}

func TestPutMany(t *testing.T) {
	bs, _, _ := newBlockstore(t)

	blks := []blocks.Block{
		blocks.NewBlock([]byte("foo1")),
		blocks.NewBlock([]byte("foo2")),
		blocks.NewBlock([]byte("foo3")),
	}
	err := bs.PutMany(context.Background(), blks)
	require.NoError(t, err)

	for _, blk := range blks {
		fetched, err := bs.Get(context.Background(), blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), fetched.RawData())

		ok, err := bs.Has(context.Background(), blk.Cid())
		require.NoError(t, err)
		require.True(t, ok)
	}

	ch, err := bs.AllKeysChan(context.Background())
	require.NoError(t, err)

	cids := collect(ch)
	require.Len(t, cids, 3)
}

func insertBlocks(t *testing.T, bs blockstore.Blockstore, count int) []cid.Cid {
	keys := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		block := blocks.NewBlock([]byte(fmt.Sprintf("some data %d", i)))
		err := bs.Put(context.Background(), block)
		require.NoError(t, err)
		// NewBlock assigns a CIDv0; we convert it to CIDv1 because that's what
		// the store returns.
		keys[i] = cid.NewCidV1(cid.Raw, block.Multihash())
	}
	return keys
}

func collect(ch <-chan cid.Cid) []cid.Cid {
	var keys []cid.Cid
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}
