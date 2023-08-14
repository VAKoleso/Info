----------------------------------------- CREATE TABLE -----------------------------------------
CREATE TABLE Peers (
  nickname VARCHAR(50) PRIMARY KEY,
  birthday DATE
);
CREATE TABLE Tasks (
  name VARCHAR(50) PRIMARY KEY,
  entry_condition VARCHAR(50),
  max_xp INTEGER NOT NULL,
  CONSTRAINT check_entry_condition CHECK (
    entry_condition IS NULL
    OR entry_condition <> name
  )
);
CREATE TABLE Checks (
  id SERIAL PRIMARY KEY,
  peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  task_name VARCHAR(50) REFERENCES Tasks(name),
  check_date DATE NOT NULL
);
CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');
CREATE TABLE P2P (
  id SERIAL PRIMARY KEY,
  check_id INTEGER,
  checking_peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  p2p_check_status CheckStatus NOT NULL,
  time TIME NOT NULL,
  CONSTRAINT fk_entry_condition FOREIGN KEY (check_id) REFERENCES Checks(id),
  CONSTRAINT unique_combination UNIQUE (
    check_id,
    checking_peer_nickname,
    p2p_check_status
  )
);
CREATE TABLE Verter (
  id SERIAL PRIMARY KEY,
  check_id INTEGER REFERENCES Checks(id),
  verter_check_status CheckStatus NOT NULL,
  time TIME NOT NULL,
  CONSTRAINT unique_combination_verter UNIQUE (check_id, verter_check_status)
);
CREATE TABLE TransferredPoints (
  id SERIAL PRIMARY KEY,
  checking_peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  peer_being_checked_nickname VARCHAR(50) REFERENCES Peers(nickname),
  num_transferred_points INTEGER NOT NULL CONSTRAINT ch_transfer_nickname CHECK (
    checking_peer_nickname <> peer_being_checked_nickname
  )
);
CREATE TABLE Friends (
  id SERIAL PRIMARY KEY,
  first_peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  second_peer_nickname VARCHAR(50) REFERENCES Peers(nickname) CONSTRAINT ch_friends_nickname CHECK (first_peer_nickname <> second_peer_nickname)
);
CREATE TABLE Recommendations (
  id SERIAL PRIMARY KEY,
  peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  recommended_peer_nickname VARCHAR(50) REFERENCES Peers(nickname) CONSTRAINT ch_recommend_nickname CHECK (peer_nickname <> recommended_peer_nickname)
);
CREATE TABLE XP (
  id SERIAL PRIMARY KEY,
  check_id INTEGER REFERENCES Checks(id),
  num_xp_received INTEGER NOT NULL
);
CREATE TABLE TimeTracking (
  id SERIAL PRIMARY KEY,
  peer_nickname VARCHAR(50) REFERENCES Peers(nickname),
  date DATE NOT NULL,
  time TIME NOT NULL,
  state INTEGER NOT NULL CONSTRAINT ch_state CHECK (
    state between 1 and 2
  )
);
----------------------------------------- CREATE FUNCTION 1 -----------------------------------------
CREATE FUNCTION check_task_status() RETURNS TRIGGER AS $$ BEGIN IF (
  EXISTS (
    SELECT id
    FROM Checks
      JOIN Tasks ON Tasks.entry_condition = NEW.task_name
      AND Tasks.entry_condition IS NOT NULL
    WHERE Tasks.name = NEW.task_name
  )
) THEN IF (
  NOT EXISTS (
    SELECT check_id
    FROM P2P
      JOIN Checks ON P2p.check_id = checks.id
      JOIN Tasks ON Tasks.entry_condition = NEW.task_name
      AND Tasks.entry_condition IS NOT NULL
    WHERE (
        Tasks.name = NEW.task_name
        AND P2P.p2p_check_status = 'Success'
      )
  )
) THEN RAISE EXCEPTION 'Cannot insert or update Checks record: no successful P2P check for check_id';
END IF;
IF (
  NOT EXISTS (
    SELECT check_id
    FROM Verter
      JOIN Checks ON Verter.check_id = checks.id
      JOIN Tasks ON Tasks.entry_condition = NEW.task_name
    WHERE Tasks.name = NEW.task_name
      AND Verter.verter_check_status = 'Success'
  )
  AND EXISTS (
    SELECT check_id
    FROM Verter
    WHERE Verter.check_id = NEW.check_id
  )
) THEN RAISE EXCEPTION 'Cannot insert or update Checks record: Verter check not successful for check_id';
END IF;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER task_status_trigger BEFORE
INSERT
  OR
UPDATE ON Checks FOR EACH ROW EXECUTE FUNCTION check_task_status();
----------------------------------------- CREATE FUNCTION 2 -----------------------------------------
CREATE FUNCTION check_p2p_status_start() RETURNS TRIGGER AS $$ BEGIN IF NEW.p2p_check_status IN ('Success', 'Failure')
AND NOT EXISTS (
  SELECT 1
  FROM P2P
  WHERE check_id = NEW.check_id
    AND p2p_check_status = 'Start'
) THEN RAISE EXCEPTION 'Check status is not Start';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER check_p2p_status_start BEFORE
INSERT ON P2P FOR EACH ROW EXECUTE FUNCTION check_p2p_status_start();
----------------------------------------- CREATE FUNCTION 3 -----------------------------------------
CREATE FUNCTION check_status() RETURNS TRIGGER AS $$ BEGIN IF NEW.check_id NOT IN (
  SELECT check_id
  FROM P2P
  WHERE p2p_check_status = 'Success'
) THEN RAISE EXCEPTION 'Check status is not Success';
END IF;
IF NEW.verter_check_status IN ('Success', 'Failure')
AND NOT EXISTS (
  SELECT 1
  FROM Verter
  WHERE check_id = NEW.check_id
    AND verter_check_status = 'Start'
) THEN RAISE EXCEPTION 'Check status is not Start';
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER Verter_check_p2p_status BEFORE
INSERT ON Verter FOR EACH ROW EXECUTE FUNCTION check_status();
----------------------------------------- insert into -----------------------------------------
INSERT INTO peers VALUES 
	('xerminia', '1993-10-20'),
	('iseadra', '1995-05-01'),
	('tszechwa', '2021-11-23'),
	('qreiko', '1994-05-09'),
	('jsharika', '1999-02-23'),
	('mugroot', '1995-05-25'),
	('dclaudie', '2001-03-12');
INSERT INTO tasks VALUES
	('C1_Pool', NULL, 1),
	('C2_s21_string+', NULL, 500),
	('C3_SimpleBashUtils', 'C2_s21_string+', 250),
	('C4_s21_decimal', 'C2_s21_string+', 350),
	('C5_s21_math', 'C2_s21_string+', 300),
	('C6_s21_matrix', 'C4_s21_decimal', 200),
	('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
	('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
	('D01_Linux', 'C3_SimpleBashUtils', 300),
	('D02_Linux_Network', 'D01_Linux', 250),
	('D03_Linux_Monitoring_v1.0', 'D02_Linux_Network', 350),
	('D03_Linux_Monitoring_v2.0', 'D03_Linux_Monitoring_v1.0', 350),
	('D05_SimpleDocker', 'D03_Linux_Monitoring_v1.0', 300),
	('D06_CICD', 'D05_SimpleDocker', 300),
	('CPP1_s21Matrix+', 'C8_3DViewer_v1.0', 300),
	('CPP2_s21Containers', 'CPP1_s21Matrix+', 350),
	('CPP3_SmartCalc_v2.0', 'CPP2_s21Containers', 600),
	('CPP4_3DViewer_v2.0', 'CPP3_SmartCalc_v2.0', 750),
	('CPP5_MLP', 'CPP4_3DViewer_v2.0', 700),
	('A1_Maze', 'CPP4_3DViewer_v2.0', 300),
	('A2_SimpleNavigator_v1.0', 'A1_Maze', 400),
	('A3_Parallels', 'A2_SimpleNavigator_v1.0', 300),
	('A4_Transactions', 'A2_SimpleNavigator_v1.0', 700),
	('A5_Algorithmic_trading', 'A2_SimpleNavigator_v1.0', 800);
INSERT INTO checks VALUES
	(1, 'xerminia', 'C2_s21_string+', '2023-04-01'),
	(2, 'xerminia', 'C4_s21_decimal', '2023-04-01'),
	(3, 'xerminia', 'C6_s21_matrix', '2023-04-01'),
	(4, 'xerminia', 'C7_SmartCalc_v1.0', '2023-04-01'),
	(5, 'xerminia', 'C8_3DViewer_v1.0', '2023-04-01'),
	(6, 'xerminia', 'CPP1_s21Matrix+', '2023-04-01'),
	(7, 'xerminia', 'CPP2_s21Containers', '2023-04-01'),
	(8, 'xerminia', 'CPP3_SmartCalc_v2.0', '2023-04-01'),
	(9, 'xerminia', 'CPP4_3DViewer_v2.0', '2023-04-01'),
	(10, 'xerminia', 'CPP5_MLP', '2023-04-01'),
	(11, 'iseadra', 'C2_s21_string+', '2023-04-01'),
	(12, 'iseadra', 'C3_SimpleBashUtils', '2023-04-01'),
	(13, 'iseadra', 'D01_Linux', '2023-04-01'),
	(14, 'iseadra', 'D02_Linux_Network', '2023-04-01'),
	(15, 'iseadra', 'D03_Linux_Monitoring_v1.0', '2023-04-01'),
	(16, 'iseadra', 'D03_Linux_Monitoring_v2.0', '2023-04-01'),
	(17, 'iseadra', 'D05_SimpleDocker', '2023-04-01'),
	(18, 'iseadra', 'D06_CICD', '2023-04-01'),
	(19, 'tszechwa', 'C2_s21_string+', '2023-04-01'),
	(20, 'tszechwa', 'C4_s21_decimal', '2023-04-01'),
	(21, 'tszechwa', 'C6_s21_matrix', '2023-04-01'),
	(22, 'tszechwa', 'C7_SmartCalc_v1.0', '2023-04-01'),
	(23, 'tszechwa', 'C8_3DViewer_v1.0', '2023-04-01'),
	(24, 'tszechwa', 'CPP1_s21Matrix+', '2023-04-01'),
	(25, 'tszechwa', 'CPP2_s21Containers', '2023-04-01'),
	(26, 'tszechwa', 'CPP3_SmartCalc_v2.0', '2023-04-01'),
	(27, 'tszechwa', 'CPP4_3DViewer_v2.0', '2023-04-01'),
	(28, 'tszechwa', 'A1_Maze', '2023-04-01'),
	(29, 'tszechwa', 'A2_SimpleNavigator_v1.0', '2023-04-01'),
	(30, 'tszechwa', 'A3_Parallels', '2023-04-01'),
	(31, 'tszechwa', 'A4_Transactions', '2023-04-01'),
	(32, 'tszechwa', 'A5_Algorithmic_trading', '2023-04-01'),
	(33, 'mugroot',  'C2_s21_string+', '2023-05-25'),
	(34, 'mugroot', 'C3_SimpleBashUtils', '2023-05-25'),
	(35, 'dclaudie', 'C2_s21_string+', '2023-03-12'),
	(36, 'qreiko', 'C2_s21_string+', '2023-05-09');
INSERT INTO p2p VALUES
	(1, 1, 'iseadra', 'Start', '11:32:10'),
	(2, 1, 'iseadra', 'Success', '11:32:10'),
	(3, 2, 'tszechwa', 'Start', '12:02:10'),
	(4, 2, 'tszechwa', 'Success', '12:02:10'),
	(5, 3, 'qreiko', 'Start', '11:32:10'),
	(6, 3, 'qreiko', 'Success', '11:32:10'),
	(7, 4, 'jsharika', 'Start', '12:02:10'),
	(8, 4, 'jsharika', 'Success', '12:02:10'),
	(9, 5, 'iseadra', 'Start', '11:32:10'),
	(10, 5, 'iseadra', 'Success', '11:32:10'),
	(11, 6, 'tszechwa', 'Start', '12:02:10'),
	(12, 6, 'tszechwa', 'Success', '12:02:10'),
	(13, 7, 'qreiko', 'Start', '11:32:10'),
	(14, 7, 'qreiko', 'Success', '11:32:10'),
	(15, 8, 'jsharika', 'Start', '12:02:10'),
	(16, 8, 'jsharika', 'Success', '12:02:10'),
	(17, 9, 'iseadra', 'Start', '11:32:10'),
	(18, 9, 'iseadra', 'Success', '11:32:10'),
	(19, 10, 'tszechwa', 'Start', '12:02:10'),
	(20, 10, 'tszechwa', 'Success', '12:02:10'),
	(21, 11, 'xerminia', 'Start', '11:32:10'),
	(22, 11, 'xerminia', 'Success', '11:32:10'),
	(23, 12, 'tszechwa', 'Start', '12:02:10'),
	(24, 12, 'tszechwa', 'Success', '12:02:10'),
	(25, 13, 'qreiko', 'Start', '11:32:10'),
	(26, 13, 'qreiko', 'Success', '11:32:10'),
	(27, 14, 'jsharika', 'Start', '12:02:10'),
	(28, 14, 'jsharika', 'Success', '12:02:10'),
	(29, 15, 'xerminia', 'Start', '11:32:10'),
	(30, 15, 'xerminia', 'Success', '11:32:10'),
	(31, 16, 'tszechwa', 'Start', '12:02:10'),
	(32, 16, 'tszechwa', 'Success', '12:02:10'),
	(33, 17, 'qreiko', 'Start', '11:32:10'),
	(34, 17, 'qreiko', 'Success', '11:32:10'),
	(35, 18, 'jsharika', 'Start', '12:02:10'),
	(36, 18, 'jsharika', 'Success', '12:02:10'),
	(37, 19, 'xerminia', 'Start', '11:32:10'),
	(38, 19, 'xerminia', 'Success', '11:32:10'),
	(39, 20, 'tszechwa', 'Start', '12:02:10'),
	(40, 20, 'tszechwa', 'Success', '12:02:10'),
	(41, 21, 'xerminia', 'Start', '11:32:10'),
	(42, 21, 'xerminia', 'Success', '11:32:10'),
	(43, 22, 'iseadra', 'Start', '12:02:10'),
	(44, 22, 'iseadra', 'Success', '12:02:10'),
	(45, 23, 'qreiko', 'Start', '11:32:10'),
	(46, 23, 'qreiko', 'Failure', '11:32:10'),
	(47, 24, 'jsharika', 'Start', '12:02:10'),
	(48, 24, 'jsharika', 'Success', '12:02:10'),
	(49, 25, 'xerminia', 'Start', '11:32:10'),
	(50, 25, 'xerminia', 'Success', '11:32:10'),
	(51, 26, 'iseadra', 'Start', '12:02:10'),
	(52, 26, 'iseadra', 'Success', '12:02:10'),
	(53, 27, 'qreiko', 'Start', '11:32:10'),
	(54, 27, 'qreiko', 'Success', '11:32:10'),
	(55, 28, 'jsharika', 'Start', '12:02:10'),
	(56, 28, 'jsharika', 'Success', '12:02:10'),
	(57, 29, 'xerminia', 'Start', '11:32:10'),
	(58, 29, 'xerminia', 'Success', '11:32:10'),
	(59, 30, 'iseadra', 'Start', '12:02:10'),
	(60, 30, 'iseadra', 'Success', '12:02:10'),
	(61, 31, 'dclaudie', 'Start', '17:30:30'),
	(62, 31, 'dclaudie', 'Success', '17:50:30'),
	(63, 32, 'xerminia', 'Start', '17:30:30'),
	(64, 32, 'xerminia', 'Success', '17:50:30'),
	(65, 33, 'tszechwa', 'Start', '17:30:30'),
	(66, 33, 'tszechwa', 'Failure', '17:50:30'),
	(67, 34, 'tszechwa', 'Start', '17:30:30'),
	(68, 34, 'tszechwa', 'Success', '17:50:30');
INSERT INTO verter VALUES
	(1, 1, 'Start', '11:32:10'),
	(2, 1, 'Success', '11:32:10'),
	(3, 2, 'Start', '12:02:10'),
	(4, 2, 'Success', '12:02:10'),
	(5, 3, 'Start', '11:32:10'),
	(6, 3, 'Success', '11:32:10'),
	(7, 4, 'Start', '12:02:10'),
	(8, 4, 'Success', '12:02:10'),
	(9, 5, 'Start', '11:32:10'),
	(10, 5, 'Success', '11:32:10'),
	(11, 6, 'Start', '12:02:10'),
	(12, 6, 'Success', '12:02:10'),
	(13, 7, 'Start', '11:32:10'),
	(14, 7, 'Success', '11:32:10'),
	(15, 8, 'Start', '12:02:10'),
	(16, 8, 'Success', '12:02:10'),
	(17, 9, 'Start', '11:32:10'),
	(18, 9, 'Success', '11:32:10'),
	(19, 10, 'Start', '12:02:10'),
	(20, 10, 'Success', '12:02:10'),
	(21, 11, 'Start', '11:32:10'),
	(22, 11, 'Success', '11:32:10'),
	(23, 12, 'Start', '12:02:10'),
	(24, 12, 'Success', '12:02:10'),
	(25, 13, 'Start', '11:32:10'),
	(26, 13, 'Success', '11:32:10'),
	(27, 14, 'Start', '12:02:10'),
	(28, 14, 'Success', '12:02:10'),
	(29, 15, 'Start', '11:32:10'),
	(30, 15, 'Success', '11:32:10'),
	(31, 16, 'Start', '12:02:10'),
	(32, 16, 'Success', '12:02:10'),
	(33, 17, 'Start', '11:32:10'),
	(34, 17, 'Success', '11:32:10'),
	(35, 18, 'Start', '12:02:10'),
	(36, 18, 'Success', '12:02:10'),
	(37, 19, 'Start', '11:32:10'),
	(38, 19, 'Success', '11:32:10'),
	(39, 20, 'Start', '12:02:10'),
	(40, 20, 'Success', '12:02:10'),
	(41, 21, 'Start', '11:32:10'),
	(42, 21, 'Success', '11:32:10'),
	(43, 22, 'Start', '12:02:10'),
	(44, 22, 'Success', '12:02:10'),
	(45, 24, 'Start', '12:02:10'),
	(46, 24, 'Success', '12:02:10'),
	(47, 25, 'Start', '11:32:10'),
	(48, 25, 'Success', '11:32:10'),
	(49, 26, 'Start', '12:02:10'),
	(50, 26, 'Success', '12:02:10'),
	(51, 27, 'Start', '11:32:10'),
	(52, 27, 'Success', '11:32:10'),
	(53, 28, 'Start', '12:02:10'),
	(54, 28, 'Success', '12:02:10'),
	(55, 29, 'Start', '11:32:10'),
	(56, 29, 'Success', '11:32:10'),
	(57, 30, 'Start', '12:02:10'),
	(58, 30, 'Success', '12:02:10');
INSERT INTO transferredpoints VALUES
	(1, 'iseadra', 'xerminia', 1),
	(2, 'xerminia', 'iseadra', 1),
	(3, 'tszechwa', 'qreiko', 2),
	(4, 'jsharika', 'xerminia', 1),
	(5, 'jsharika', 'tszechwa', 1);
INSERT INTO friends VALUES
	(1, 'xerminia', 'iseadra'),
	(2, 'xerminia', 'tszechwa'),
	(3, 'xerminia', 'qreiko'),
	(4, 'xerminia', 'jsharika'),
	(5, 'iseadra', 'tszechwa'),
	(6, 'tszechwa', 'jsharika');
INSERT INTO recommendations VALUES
	(1, 'xerminia', 'iseadra'),
	(2, 'iseadra', 'xerminia'),
	(3, 'tszechwa', 'qreiko'),
	(4, 'xerminia', 'jsharika'),
	(5, 'tszechwa', 'jsharika');
INSERT INTO xp VALUES
	(1, 1, 500),
	(2, 2, 200),
	(3, 19, 500),
	(4, 20, 200),
	(5, 28, 300),
	(6, 29, 400);
INSERT INTO TimeTracking VALUES
	(1, 'xerminia', '06.04.22', '13:37', 1),
 	(2, 'xerminia', '06.04.22', '15:48', 2),
	(3, 'tszechwa', '06.04.22', '13:37', 1),
	(4, 'tszechwa', '06.04.22', '15:48', 2),
	(5, 'iseadra', '06.04.22', '13:37', 1),
	(6, 'iseadra', '06.04.22', '15:48', 2),
	(7, 'xerminia', '07.04.22', '03:37', 1);

CREATE OR REPLACE PROCEDURE export_csv() AS $$
DECLARE
--export_path varchar = 'C:\Users\111\SQL2_Info21_v1.0-1\src\export\';
export_path varchar = '/Users/lolli/Documents/SQL2_24.05.23/src/export/';
export_name varchar [] = ARRAY['peers', 'friends', 'recommendations', 'transferredpoints', 'timetracking', 'tasks', 'checks', 'p2p', 'verter', 'xp'];
BEGIN
FOR i IN 1..array_length(export_name, 1)
LOOP
EXECUTE format ('copy %s to ''%s%s.csv'' with delimiter '','' csv', export_name[i], export_path, export_name[i]);
END LOOP;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE import_csv() AS $$
DECLARE
--import_path varchar = 'C:\Users\111\SQL2_Info21_v1.0-1\src\import\';
import_path varchar = '/Users/lolli/Documents/SQL2_24.05.23/src/import/';
import_name varchar [] = array['peers', 'friends', 'recommendations', 'transferredpoints', 'timetracking', 'tasks', 'checks', 'p2p', 'verter','xp'];
BEGIN
FOR i IN 1..array_length(import_name, 1)
LOOP
EXECUTE format ('copy %s from ''%s%s.csv'' with delimiter '','' csv', import_name[i], import_path, import_name[i]);
END LOOP;
END;
$$
LANGUAGE plpgsql;

--CALL import_csv();
--CALL export_csv();
