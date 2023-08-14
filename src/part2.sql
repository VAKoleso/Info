----------------------------------------- PART 2.1 -----------------------------------------

CREATE OR REPLACE PROCEDURE add_p2p_check(
  p_checking_peer_nickname VARCHAR(50),
  p_peer_being_checked_nickname VARCHAR(50),
  p_task_name VARCHAR(50),
  p_p2p_check_status CheckStatus,
  p_time TIME
)
AS $$
DECLARE
	p_check_id integer;
BEGIN

  IF p_p2p_check_status = 'Start' THEN
    p_check_id := (select id from checks order by id DESC limit 1) + 1;
    INSERT INTO Checks VALUES (p_check_id, p_checking_peer_nickname, p_task_name, current_date);
  ELSE
    SELECT id INTO p_check_id FROM Checks c
    WHERE c.peer_nickname = p_checking_peer_nickname
      AND c.task_name = p_task_name
      AND EXISTS(SELECT 1 FROM P2P WHERE check_id = c.id AND p2p_check_status = 'Start')
    ORDER BY check_date DESC, id DESC
    LIMIT 1;
  END IF;

  INSERT INTO P2P
  VALUES ((select max(id) from P2P) + 1, p_check_id, p_peer_being_checked_nickname, p_p2p_check_status, p_time);
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 1 -----------------------------------------

--CALL add_p2p_check('xerminia', 'iseadra', 'C3_SimpleBashUtils', 'Start', '13:15:00');
--CALL add_p2p_check('xerminia', 'iseadra', 'C3_SimpleBashUtils', 'Success', '13:53:00');
--
--SELECT * FROM Checks c
--JOIN P2P ON c.id = p2p.check_id
--WHERE c.task_name = 'C3_SimpleBashUtils' AND c.peer_nickname = 'xerminia'

----------------------------------------- TEST 2 -----------------------------------------

--CALL add_p2p_check('xerminia', 'tszechwa', 'C5_s21_math', 'Start', '14:30:00');
--CALL add_p2p_check('xerminia', 'tszechwa', 'C5_s21_math', 'Failure', '15:02:00');
--
--SELECT * FROM Checks c
--JOIN P2P ON c.id = p2p.check_id
--WHERE c.task_name = 'C5_s21_math' AND c.peer_nickname = 'xerminia'

----------------------------------------- PART 2.2 -----------------------------------------

CREATE OR REPLACE PROCEDURE insert_verter(nickname VARCHAR(50), task VARCHAR(50), status CheckStatus, time_add TIME )
AS $$
DECLARE
id_check integer;
BEGIN
id_check := (select check_id from p2p
join checks on checks.id = p2p.check_id
where peer_nickname = nickname and p2p_check_status = 'Success'
and task_name = task
order by time desc
limit 1);
INSERT INTO Verter VALUES ((select max(id) from verter) + 1, id_check, status, time_add);
end;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 1 -----------------------------------------

--CALL insert_verter('xerminia', 'C3_SimpleBashUtils', 'Start', '13:53:10');
--CALL insert_verter('xerminia', 'C3_SimpleBashUtils', 'Success', '13:53:50');
--
--SELECT * FROM Checks c
--JOIN Verter ON c.id = Verter.check_id
--WHERE c.task_name = 'C3_SimpleBashUtils' AND c.peer_nickname = 'xerminia'

----------------------------------------- PART 2.3 -----------------------------------------

CREATE OR REPLACE FUNCTION update_transferredpoints() RETURNS TRIGGER AS $$ BEGIN IF NEW.p2p_check_status = 'Start' THEN IF EXISTS(
    SELECT *
    FROM TransferredPoints
    WHERE checking_peer_nickname = NEW.checking_peer_nickname
      AND peer_being_checked_nickname = (
        SELECT peer_nickname
        FROM checks
        WHERE NEW.check_id = checks.id
      )
  ) THEN
UPDATE transferredpoints
SET num_transferred_points = num_transferred_points + 1
WHERE checking_peer_nickname = NEW.checking_peer_nickname
  AND peer_being_checked_nickname = (
    SELECT peer_nickname
    FROM checks
    WHERE new.check_id = checks.id
  );
ELSE
INSERT INTO transferredpoints
Values (
    (
      SELECT max(id) + 1
      FROM transferredpoints
    ),
    (
      SELECT peer_nickname
      FROM checks
      WHERE NEW.check_id = checks.id
    ),
    NEW.checking_peer_nickname,
    1
  );
END IF;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_p2p_insert BEFORE
INSERT ON P2P FOR EACH ROW EXECUTE FUNCTION update_transferredpoints();

----------------------------------------- PART 2.4 -----------------------------------------

CREATE FUNCTION check_xp_received() RETURNS TRIGGER AS $$ BEGIN IF (
  NEW.num_xp_received > (
    SELECT max_xp
    FROM Tasks
      JOIN Checks ON Tasks.name = Checks.task_name
    WHERE Checks.id = NEW.check_id
  )
) THEN RAISE EXCEPTION 'num_xp_received cannot exceed max_xp';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER xp_received_trigger BEFORE
INSERT
  OR
UPDATE ON XP FOR EACH ROW EXECUTE FUNCTION check_xp_received();

CREATE FUNCTION check_xp_status() RETURNS TRIGGER AS $$ BEGIN IF (
  NOT EXISTS (
    SELECT check_id
    FROM P2P
    WHERE (
        P2P.check_id = NEW.check_id
        AND P2P.p2p_check_status = 'Success'
      )
  )
) THEN RAISE EXCEPTION 'Cannot insert or update XP record: no successful P2P check for check_id';
END IF;
IF (
  NOT EXISTS (
    SELECT check_id
    FROM Verter
    WHERE Verter.check_id = NEW.check_id
      AND Verter.verter_check_status = 'Success'
  )
  AND EXISTS (
    SELECT check_id
    FROM Verter
    WHERE Verter.check_id = NEW.check_id
  )
) THEN RAISE EXCEPTION 'Cannot insert or update XP record: Verter check not successful for check_id';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER xp_status_trigger BEFORE
INSERT
  OR
UPDATE ON XP FOR EACH ROW EXECUTE FUNCTION check_xp_status();

