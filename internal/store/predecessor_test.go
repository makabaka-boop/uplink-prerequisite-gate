package store_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"deepspace/internal/store"
	"deepspace/internal/testdb"
)

func int64Ptr(v int64) *int64 { return &v }

func mustCreate(t *testing.T, st *store.Store, payload string, predecessor *int64) *store.Command {
	t.Helper()
	c, err := st.CreateCommand(context.Background(), []byte(payload), predecessor)
	if err != nil {
		t.Fatalf("create command: %v", err)
	}
	return c
}

func mustClaim(t *testing.T, st *store.Store, d time.Duration) *store.Claim {
	t.Helper()
	c, err := st.Claim(context.Background(), d)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	return c
}

func mustAck(t *testing.T, st *store.Store, id int64, token, result string) {
	t.Helper()
	if err := st.Ack(context.Background(), id, token, result); err != nil {
		t.Fatalf("ack %d: %v", id, err)
	}
}

// TestPredecessorChainUnlocksInOrder：前驱链全部 delivered 前不得预发租约；
// 送达后仍按最小可领取编号裁决。
func TestPredecessorChainUnlocksInOrder(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := mustCreate(t, st, `{"n":"root"}`, nil)
	child := mustCreate(t, st, `{"n":"child"}`, int64Ptr(root.ID))
	grand := mustCreate(t, st, `{"n":"grand"}`, int64Ptr(child.ID))
	independent := mustCreate(t, st, `{"n":"independent"}`, nil)

	if child.PredecessorID == nil || *child.PredecessorID != root.ID || child.ChainID != root.ID {
		t.Fatalf("child predecessor/chain mismatch: %+v", child)
	}
	if grand.ChainID != root.ID {
		t.Fatalf("grand chain = %d, want %d", grand.ChainID, root.ID)
	}
	if independent.ChainID != independent.ID {
		t.Fatalf("root chain = %d, want own id %d", independent.ChainID, independent.ID)
	}

	// 根未送达时，后继不得领取。
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("waiting predecessor claimed early: %v", err)
	}

	rootLease := mustClaim(t, st, 5*time.Second)
	if rootLease.CommandID != root.ID {
		t.Fatalf("claim root = %d, want %d", rootLease.CommandID, root.ID)
	}
	mustAck(t, st, root.ID, rootLease.LeaseToken, store.StatusDelivered)

	childLease := mustClaim(t, st, 5*time.Second)
	if childLease.CommandID != child.ID {
		t.Fatalf("after root delivery claim = %d, want child %d", childLease.CommandID, child.ID)
	}
	// 子指令只是被领取、尚未送达，孙指令仍不得领取。
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("grand claimed before child delivery: %v", err)
	}
	mustAck(t, st, child.ID, childLease.LeaseToken, store.StatusDelivered)

	grandLease := mustClaim(t, st, 5*time.Second)
	if grandLease.CommandID != grand.ID {
		t.Fatalf("after child delivery claim = %d, want grand %d", grandLease.CommandID, grand.ID)
	}
	mustAck(t, st, grand.ID, grandLease.LeaseToken, store.StatusDelivered)

	last := mustClaim(t, st, 5*time.Second)
	if last.CommandID != independent.ID {
		t.Fatalf("independent claim = %d, want %d", last.CommandID, independent.ID)
	}
}

// TestPredecessorFailurePropagatesBlocked：失败来源的所有未领取后继原子 blocked，
// blocked 没有租约或结算，之后新引用同一失败链的指令也立即 blocked。
func TestPredecessorFailurePropagatesBlocked(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := mustCreate(t, st, `{"n":"root"}`, nil)
	child := mustCreate(t, st, `{"n":"child"}`, int64Ptr(root.ID))
	grand := mustCreate(t, st, `{"n":"grand"}`, int64Ptr(child.ID))
	unrelated := mustCreate(t, st, `{"n":"unrelated"}`, nil)

	lease := mustClaim(t, st, 5*time.Second)
	if lease.CommandID != root.ID {
		t.Fatalf("claim = %d, want root %d", lease.CommandID, root.ID)
	}
	mustAck(t, st, root.ID, lease.LeaseToken, store.StatusFailed)

	for _, c := range []*store.Command{child, grand} {
		d, err := st.GetCommand(ctx, c.ID)
		if err != nil {
			t.Fatalf("get %d: %v", c.ID, err)
		}
		if d.Status != store.StatusBlocked || d.BlockedBy == nil || *d.BlockedBy != root.ID || d.BlockedAt == nil {
			t.Fatalf("command %d not blocked by root: %+v", c.ID, d)
		}
		if d.LeaseGeneration != 0 || len(d.Leases) != 0 || d.Settlement != nil {
			t.Fatalf("blocked command %d generated lease/settlement: %+v", c.ID, d)
		}
		if err := st.Ack(ctx, c.ID, "never-issued", store.StatusDelivered); !errors.Is(err, store.ErrCommandBlocked) {
			t.Fatalf("ack blocked %d: want ErrCommandBlocked, got %v", c.ID, err)
		}
	}

	lateChild := mustCreate(t, st, `{"n":"late"}`, int64Ptr(root.ID))
	if lateChild.Status != store.StatusBlocked || lateChild.BlockedBy == nil || *lateChild.BlockedBy != root.ID {
		t.Fatalf("late child after failure: %+v", lateChild)
	}
	lateGrand := mustCreate(t, st, `{"n":"late-grand"}`, int64Ptr(child.ID))
	if lateGrand.Status != store.StatusBlocked || lateGrand.BlockedBy == nil || *lateGrand.BlockedBy != root.ID {
		t.Fatalf("late grand after blocked parent: %+v", lateGrand)
	}

	next := mustClaim(t, st, time.Second)
	if next.CommandID != unrelated.ID {
		t.Fatalf("claim after blocked chain = %d, want unrelated %d", next.CommandID, unrelated.ID)
	}
}

// TestUnknownPredecessorRejected：引用不存在编号不能创建。
func TestUnknownPredecessorRejected(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)

	_, err := st.CreateCommand(context.Background(), []byte(`{}`), int64Ptr(987654321))
	if !errors.Is(err, store.ErrPredecessorNotFound) {
		t.Fatalf("want ErrPredecessorNotFound, got %v", err)
	}
}

// TestCreateAndAckInterleaveAcrossInstances：两个独立连接池模拟两个 API 实例，
// 并发“创建前驱的后继”和“失败结算前驱”。任何提交顺序后，后继都必须 blocked，
// 且不能在失败瞬间仍保持 pending 或被领取。
func TestCreateAndAckInterleaveAcrossInstances(t *testing.T) {
	_, dbURL := openTestDB(t)

	const iterations = 32
	for i := 0; i < iterations; i++ {
		pool1, err := openPool(dbURL)
		if err != nil {
			t.Fatal(err)
		}
		pool2, err := openPool(dbURL)
		if err != nil {
			pool1.Close()
			t.Fatal(err)
		}
		st1 := store.New(pool1)
		st2 := store.New(pool2)

		root := mustCreate(t, st1, `{"race":1}`, nil)
		lease := mustClaim(t, st1, 5*time.Second)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var child *store.Command
		var createErr error
		var ackErr error
		var claimErr error
		wg.Add(3)
		go func() {
			defer wg.Done()
			<-start
			child, createErr = st1.CreateCommand(context.Background(), []byte(`{"race":2}`), int64Ptr(root.ID))
		}()
		go func() {
			defer wg.Done()
			<-start
			// 让两个事务充分重叠；链咨询锁负责最终裁决。
			time.Sleep(2 * time.Millisecond)
			ackErr = st2.Ack(context.Background(), root.ID, lease.LeaseToken, store.StatusFailed)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, claimErr = st2.Claim(context.Background(), time.Second)
		}()
		close(start)
		wg.Wait()

		if ackErr != nil {
			t.Fatalf("iteration %d ack: %v", i, ackErr)
		}
		if createErr != nil {
			t.Fatalf("iteration %d create: %v", i, createErr)
		}
		if !errors.Is(claimErr, store.ErrNoAvailableCommand) {
			t.Fatalf("iteration %d concurrent claim = %v, want no available", i, claimErr)
		}
		d, err := st1.GetCommand(context.Background(), child.ID)
		if err != nil {
			t.Fatalf("iteration %d get child: %v", i, err)
		}
		if d.Status != store.StatusBlocked || d.BlockedBy == nil || *d.BlockedBy != root.ID {
			t.Fatalf("iteration %d child after race: %+v", i, d)
		}
		if d.LeaseGeneration != 0 || len(d.Leases) != 0 || d.Settlement != nil {
			t.Fatalf("iteration %d blocked child has lease/settlement: %+v", i, d)
		}

		pool1.Close()
		pool2.Close()
	}
}

// TestMigrationKeepsLegacyCommandsCompatible：先用 0001 建库并写入旧指令，
// 再迁移到带前驱的契约；旧指令必须保持 pending、前驱为空且可正常领取结算。
func TestMigrationKeepsLegacyCommandsCompatible(t *testing.T) {
	pool, _ := testdb.NewEmpty(t)
	ctx := context.Background()

	oldSQL, err := os.ReadFile("migrations_sql/0001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(oldSQL)); err != nil {
		t.Fatalf("apply legacy migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO commands (payload) VALUES ($1::jsonb)`, `{"legacy":true}`); err != nil {
		t.Fatalf("insert legacy command: %v", err)
	}

	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate legacy database: %v", err)
	}
	st := store.New(pool)
	var legacyID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM commands WHERE payload @> '{"legacy":true}'`).Scan(&legacyID); err != nil {
		t.Fatal(err)
	}
	d, err := st.GetCommand(ctx, legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusPending || d.PredecessorID != nil || d.ChainID != legacyID ||
		d.BlockedBy != nil || d.BlockedAt != nil {
		t.Fatalf("legacy command after migration: %+v", d)
	}
	lease := mustClaim(t, st, time.Second)
	if lease.CommandID != legacyID {
		t.Fatalf("legacy claim = %d, want %d", lease.CommandID, legacyID)
	}
	mustAck(t, st, legacyID, lease.LeaseToken, store.StatusDelivered)
}
