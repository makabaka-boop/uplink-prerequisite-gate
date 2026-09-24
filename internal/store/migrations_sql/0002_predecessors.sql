-- 0002_predecessors.sql
-- 可选前驱指令与阻断传播。
--
-- 设计要点：
--   * predecessor_id 只能引用更早的指令；旧指令 predecessor_id 为 NULL；
--   * chain_id 是整条前驱链的根指令 ID，由数据库触发器在插入时确定，
--     事务用它申请咨询锁，串行化同一链上的“创建后继”和“结算前驱”；
--   * blocked 是只读终态：blocked_by 记录第一个失败来源，blocked_at 记录阻断时间；
--     blocked 指令没有 settlements/leases 记录，也不会再被领取或结算。

ALTER TABLE commands
    ADD COLUMN IF NOT EXISTS predecessor_id BIGINT REFERENCES commands(id),
    ADD COLUMN IF NOT EXISTS chain_id       BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS blocked_by     BIGINT REFERENCES commands(id),
    ADD COLUMN IF NOT EXISTS blocked_at     TIMESTAMPTZ;

-- 旧指令没有前驱，各自自成一条链。
UPDATE commands SET chain_id = id WHERE chain_id = 0;

ALTER TABLE commands
    DROP CONSTRAINT IF EXISTS commands_status_check;

ALTER TABLE commands
    ADD CONSTRAINT commands_status_check
        CHECK (status IN ('pending', 'delivered', 'failed', 'blocked')),
    ADD CONSTRAINT commands_chain_id_valid
        CHECK (chain_id > 0),
    ADD CONSTRAINT commands_predecessor_earlier
        CHECK (predecessor_id IS NULL OR predecessor_id < id),
    ADD CONSTRAINT commands_blocked_state_consistent
        CHECK (
            (status = 'blocked' AND blocked_by IS NOT NULL AND blocked_at IS NOT NULL)
            OR
            (status <> 'blocked' AND blocked_by IS NULL AND blocked_at IS NULL)
        );

CREATE INDEX IF NOT EXISTS commands_predecessor_idx
    ON commands (predecessor_id);

CREATE INDEX IF NOT EXISTS commands_chain_idx
    ON commands (chain_id, id);

-- 插入时由数据库裁决链归属：根指令链 ID 等于自身 ID；后继继承前驱链 ID。
CREATE OR REPLACE FUNCTION commands_set_chain_id()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.predecessor_id IS NULL THEN
        NEW.chain_id := NEW.id;
    ELSE
        SELECT chain_id
        INTO NEW.chain_id
        FROM commands
        WHERE id = NEW.predecessor_id;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'predecessor command % does not exist', NEW.predecessor_id
                USING ERRCODE = 'foreign_key_violation';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS commands_set_chain_id_before_insert ON commands;
CREATE TRIGGER commands_set_chain_id_before_insert
    BEFORE INSERT ON commands
    FOR EACH ROW
    EXECUTE FUNCTION commands_set_chain_id();
