// Package store 封装 PostgreSQL 持久化与租约裁决逻辑。
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 指令生命周期状态。
const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
)

// 终态结果集合，供 HTTP 层做入参校验。
var ValidResults = map[string]bool{StatusDelivered: true, StatusFailed: true}

var (
	// ErrNoAvailableCommand 没有可领取（未结算且租约未生效）的指令。
	ErrNoAvailableCommand = errors.New("no available command")
	// ErrCommandNotFound 未知指令编号。
	ErrCommandNotFound = errors.New("command not found")
	// ErrInvalidLeaseToken 令牌无法识别（格式错误或从不属于该指令）。
	ErrInvalidLeaseToken = errors.New("invalid lease token")
	// ErrLeaseStale 令牌属于过期/被取代的旧代次，或当前代次已到期。
	ErrLeaseStale = errors.New("lease token is expired or superseded")
	// ErrAlreadySettled 指令已有终态，禁止重复结算或覆盖。
	ErrAlreadySettled = errors.New("command already settled")
)

// Store 基于 pgxpool 的存储实现。
type Store struct {
	pool *pgxpool.Pool
	// now 允许测试注入时钟；生产使用 time.Now。
	now func() time.Time
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, now: time.Now}
}

// Ping 用于健康检查。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Command 是指令聚合根的内存表示。
type Command struct {
	ID              int64
	Payload         []byte
	Status          string
	LeaseGeneration int64
	LeaseExpiresAt  *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// LeaseView 描述某一代租约（观察用，不含令牌原文）。
type LeaseView struct {
	Generation int64
	ExpiresAt  time.Time
	ClaimedAt  time.Time
}

// Settlement 终态结算记录。
type Settlement struct {
	Generation int64
	Result     string
	SettledAt  time.Time
}

// CommandDetail 观察视图：指令 + 各代租约 + 终态。
type CommandDetail struct {
	Command
	Leases     []LeaseView
	Settlement *Settlement
}

// Claim 是领取成功后的返回值。
type Claim struct {
	CommandID      int64
	Generation     int64
	LeaseToken     string
	LeaseExpiresAt time.Time
}

// newToken 生成 256bit 不透明随机令牌（十六进制编码）。
// 每次领取都重新生成，配合 leases.lease_token 全局唯一约束，
// 新令牌在数学上必然不同于任何旧令牌。
func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// CreateCommand 按调用顺序（由 IDENTITY 列保证）写入一条指令。
func (s *Store) CreateCommand(ctx context.Context, payload []byte) (*Command, error) {
	c := &Command{}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO commands (payload)
		VALUES ($1::jsonb)
		RETURNING id, payload, status, lease_generation, lease_expires_at, created_at, updated_at`,
		string(payload),
	).Scan(&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration, &c.LeaseExpiresAt, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("create command: %w", err)
	}
	return c, nil
}

// Claim 原子领取最早可用的一条指令：
//   - FOR UPDATE SKIP LOCKED 保证并发领取者各自拿到不同的行（唯一领取）；
//   - 跳过已有终态、以及仍持有未过期租约的指令；
//   - 在同一事务内推进 lease_generation 并写入新一代租约。
func (s *Store) Claim(ctx context.Context, leaseFor time.Duration) (*Claim, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var id int64
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM commands
		WHERE status = 'pending'
		  AND (lease_expires_at IS NULL OR lease_expires_at <= now())
		ORDER BY id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoAvailableCommand
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}

	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	var gen int64
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE commands
		SET lease_generation = lease_generation + 1,
		    lease_token      = $2,
		    lease_expires_at = now() + ($3::bigint * interval '1 microsecond'),
		    updated_at       = now()
		WHERE id = $1
		RETURNING lease_generation, lease_expires_at`,
		id, token, leaseFor.Microseconds(),
	).Scan(&gen, &expiresAt)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO leases (command_id, generation, lease_token, expires_at)
		VALUES ($1, $2, $3, $4)`,
		id, gen, token, expiresAt); err != nil {
		return nil, fmt.Errorf("claim lease insert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	return &Claim{CommandID: id, Generation: gen, LeaseToken: token, LeaseExpiresAt: expiresAt}, nil
}

// Ack 凭未过期且属于当前代次的令牌，将指令置为终态。
// 旧代次/已到期/已结算等冲突通过哨兵错误返回，状态绝不改变。
func (s *Store) Ack(ctx context.Context, id int64, token, result string) error {
	if !ValidResults[result] {
		// 双保险：HTTP 层已拦截，存储层不接受任何越界值。
		return fmt.Errorf("invalid result %q", result)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ack begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var status string
	var currentGen int64
	err = tx.QueryRow(ctx, `
		SELECT status, lease_generation
		FROM commands
		WHERE id = $1
		FOR UPDATE`, id).Scan(&status, &currentGen)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCommandNotFound
	}
	if err != nil {
		return fmt.Errorf("ack load command: %w", err)
	}

	// 令牌必须是系统签发过、且属于本指令的（其他指令的令牌视为从未签发）。
	var leaseGen int64
	var leaseExpires time.Time
	err = tx.QueryRow(ctx, `
		SELECT generation, expires_at
		FROM leases
		WHERE lease_token = $1 AND command_id = $2`, token, id).Scan(&leaseGen, &leaseExpires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidLeaseToken
	}
	if err != nil {
		return fmt.Errorf("ack load lease: %w", err)
	}

	// 裁决顺序（行锁内，无并发可改写状态）：
	// 1) 已有终态：拒绝重复结算；
	// 2) 令牌非当前代次：旧持有者迟到；
	// 3) 令牌已过期：租约到期。
	if status != StatusPending {
		return ErrAlreadySettled
	}
	if leaseGen != currentGen {
		return ErrLeaseStale
	}
	if !leaseExpires.After(s.now()) {
		return ErrLeaseStale
	}

	// settlements.command_id 主键兜底：即使上层有漏洞也无法二次结算。
	if _, err = tx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result)
		VALUES ($1, $2, $3, $4)`,
		id, currentGen, token, result); err != nil {
		return fmt.Errorf("ack settlement insert: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE commands
		SET status = $2, updated_at = now()
		WHERE id = $1 AND status = 'pending'`, id, result); err != nil {
		return fmt.Errorf("ack status update: %w", err)
	}

	return tx.Commit(ctx)
}

// GetCommand 返回指令详情（含领取代次历史与终态）。未知编号返回 ErrCommandNotFound。
func (s *Store) GetCommand(ctx context.Context, id int64) (*CommandDetail, error) {
	d := &CommandDetail{}
	err := s.pool.QueryRow(ctx, `
		SELECT id, payload, status, lease_generation, lease_expires_at, created_at, updated_at
		FROM commands WHERE id = $1`, id).
		Scan(&d.ID, &d.Payload, &d.Status, &d.LeaseGeneration, &d.LeaseExpiresAt,
			&d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrCommandNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get command: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT generation, expires_at, claimed_at
		FROM leases WHERE command_id = $1
		ORDER BY generation`, id)
	if err != nil {
		return nil, fmt.Errorf("get leases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var lv LeaseView
		if err := rows.Scan(&lv.Generation, &lv.ExpiresAt, &lv.ClaimedAt); err != nil {
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		d.Leases = append(d.Leases, lv)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var st Settlement
	err = s.pool.QueryRow(ctx, `
		SELECT generation, result, settled_at
		FROM settlements WHERE command_id = $1`, id).Scan(&st.Generation, &st.Result, &st.SettledAt)
	switch {
	case errors.Is(err, nil):
		d.Settlement = &st
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, fmt.Errorf("get settlement: %w", err)
	}
	return d, nil
}

// ListCommands 按创建顺序返回指令简要列表。
func (s *Store) ListCommands(ctx context.Context, limit int) ([]Command, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, payload, status, lease_generation, lease_expires_at, created_at, updated_at
		FROM commands
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list commands: %w", err)
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		var c Command
		if err := rows.Scan(&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration,
			&c.LeaseExpiresAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan command: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
