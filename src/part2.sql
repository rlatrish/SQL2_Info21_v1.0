DROP PROCEDURE IF EXISTS AddP2PCheck;
DROP PROCEDURE IF EXISTS AddVerterCheck;
DROP FUNCTION If EXISTS AddTransferredPoints CASCADE;
DROP FUNCTION If EXISTS CorrectRecordsXP CASCADE;

CREATE OR REPLACE PROCEDURE AddP2PCheck
  (Peer2 varchar, Peer1 varchar, CurTask varchar,
    Status CheckStatus, "Time" time = CURRENT_TIME(0))
  AS $$
  DECLARE
    p2p_id BIGINT;
    check_id BIGINT;
    check_id_ns BIGINT;
  BEGIN
    IF Status = 'Start' THEN
      check_id = (SELECT max(ID) + 1 FROM Checks);
      p2p_id = (SELECT max(ID) + 1 FROM P2P);
      INSERT INTO Checks VALUES(check_id, Peer2, CurTask, CURRENT_DATE);
      INSERT INTO P2P VALUES (p2p_id, check_id, Peer1, Status, "Time");
    ELSE
      check_id =
        (SELECT "Check"
        FROM P2P JOIN Checks
        ON P2P."Check" = Checks.ID
        WHERE Checks.Peer = Peer2
        AND P2P.CheckingPeer = Peer1
        AND Checks.Task = CurTask
        AND P2P.State = 'Start'
        ORDER BY Checks.Date DESC, P2P.Time DESC, P2P."Check" DESC
        LIMIT 1);
      check_id_ns = 
        (SELECT "Check" FROM P2P
        WHERE "Check" = check_id
        AND State != 'Start'
        LIMIT 1);
      IF check_id IS NOT NULL
        AND check_id_ns IS NULL
      THEN
        p2p_id = (SELECT max(ID) + 1 FROM P2P);
        INSERT INTO P2P VALUES(p2p_id, check_id, Peer1, Status, "Time");
      END IF;
    END IF;
  END;
  $$LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE AddVerterCheck
  (Peer2 varchar, CurTask varchar, Status CheckStatus, "Time" time = CURRENT_TIME(0))
  AS $$
  DECLARE
    verter_id BIGINT;
    check_id BIGINT;
  BEGIN
    check_id =
      (SELECT "Check"
      FROM P2P JOIN Checks
      ON P2P."Check" = Checks.ID
      WHERE Checks.Task = CurTask
      AND checks.peer = Peer2
      AND P2P.State = 'Success'
      ORDER BY Checks.Date DESC, P2P.Time DESC, P2P."Check" DESC
      LIMIT 1);
    IF check_id IS NOT NULL
    THEN
      verter_id = (SELECT COUNT("Check") FROM Verter WHERE "Check" = check_id GROUP BY "Check");
      IF  (verter_id IS NULL AND Status = 'Start') OR
          (verter_id = 1 AND (Status = 'Success' OR Status = 'Failure'))
      THEN
        verter_id = (SELECT max(ID) + 1 FROM Verter);
        INSERT INTO Verter VALUES(verter_id, check_id, Status, "Time");
      END IF;
    END IF;
  END;
  $$LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION AddTransferredPoints()
  RETURNS TRIGGER AS $AddTransferredPoints$
  DECLARE
    Peer2 VARCHAR;
    TPId BIGINT;
    EXId BIGINT;
  BEGIN
    IF NEW.State = 'Start' THEN
      Peer2 = (SELECT Peer FROM Checks
        WHERE Checks.ID = NEW."Check" LIMIT 1);
      EXId = (SELECT ID FROM TransferredPoints TP
        WHERE TP.CheckingPeer = NEW.CheckingPeer
        AND TP.CheckedPeer = Peer2 LIMIT 1);
      IF EXId IS NOT NULL THEN
        UPDATE TransferredPoints TP
        SET PointsAmount = PointsAmount + 1
        WHERE TP.CheckedPeer = Peer2
        AND TP.CheckingPeer = NEW.CheckingPeer;
      ELSE
        TPId = (SELECT max(ID) + 1
          FROM TransferredPoints);
        INSERT INTO TransferredPoints
          VALUES(TPId, NEW.CheckingPeer, Peer2, 1);
      END IF;
    END IF;
    RETURN NULL;
  END;
$AddTransferredPoints$ LANGUAGE plpgsql;

CREATE TRIGGER TrgInsertP2P
  AFTER INSERT ON P2P FOR EACH ROW
  EXECUTE FUNCTION AddTransferredPoints();

CREATE OR REPLACE FUNCTION CorrectRecordsXP()
  RETURNS TRIGGER AS $CorrectRecordsXP$
  DECLARE
  NoXP BOOLEAN;
  CorrXP INTEGER;
  SuccVerter BOOLEAN;
  NoVerter BOOLEAN;
  SuccP2P BOOLEAN;
  SuccXP BOOLEAN;
  BEGIN

  NoXP = (SELECT ID FROM XP WHERE XP."Check" = NEW."Check" LIMIT 1) IS NULL;
  CorrXP = (SELECT maxxp FROM Tasks JOIN Checks ON Checks.Task = Tasks.Title
    WHERE Checks.ID = NEW."Check");
  SuccVerter = (SELECT State FROM Verter V 
      WHERE V."Check" = NEW."Check"
      AND V.State = 'Success' LIMIT 1) IS NOT NULL;
  NoVerter = (SELECT State FROM Verter V 
      WHERE V."Check" = NEW."Check" LIMIT 1) IS NULL;
  SuccP2P = (SELECT State FROM P2P P 
      WHERE P."Check" = NEW."Check"
      AND P.State = 'Success' LIMIT 1) IS NOT NULL;
  SuccXP = NoXP AND (SuccVerter OR (NoVerter AND SuccP2P));
  IF NEW.XPAmount > CorrXP OR NOT SuccXP THEN
    NEW = NULL;
  END IF;
  RETURN NEW;
  END;
  $CorrectRecordsXP$ LANGUAGE plpgsql;

CREATE TRIGGER TrgInsertXP
  BEFORE INSERT ON XP FOR EACH ROW
  EXECUTE FUNCTION CorrectRecordsXP();

INSERT INTO XP VALUES(5, 8, 1000);
INSERT INTO XP VALUES(5, 10, 500);
INSERT INTO XP VALUES(5, 5, 750);
INSERT INTO XP VALUES(5, 11, 1000);
INSERT INTO XP VALUES(5, 14, 500);

CALL AddVerterCheck('cbebe', 'C1_Simple_bash', 'Start', '10:20:00');
CALL AddVerterCheck('cbebe', 'C1_Simple_bash', 'Success', '10:21:00');
INSERT INTO XP VALUES(8, 1, 300);

CALL AddP2PCheck('cbebe', 'vleida', 'C2_S21_String', 'Start', '08:21:00');
CALL AddP2PCheck('cbebe', 'vleida', 'C2_S21_String', 'Success', '11:52:08');

CALL AddVerterCheck('cbebe', 'C2_S21_String', 'Start', '11:55:00');
CALL AddVerterCheck('cbebe', 'C1_Simple_bash', 'Start', '11:52:08');
CALL AddVerterCheck('cbebe', 'C1_Simple_bash', 'Success', '11:52:11');
CALL AddVerterCheck('cbebe', 'C4_S21_Decimal', 'Start', '11:52:08');
CALL AddVerterCheck('cbebe', 'C4_S21_Decimal', 'Success', '11:52:11');
