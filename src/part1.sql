DROP TABLE IF EXISTS Checks, Peers, Tasks, P2P, Verter, XP, 
  TransferredPoints, Friends, Recommendations, TimeTracking;
DROP PROCEDURE IF EXISTS ExportFromCSV, ExportToCSV;
DROP TYPE IF EXISTS CheckStatus CASCADE;

CREATE OR REPLACE PROCEDURE ExportFromCSV
  (target_table varchar, filepath varchar, delim char(1))
  AS $$
  BEGIN
    EXECUTE FORMAT('COPY '||target_table||' FROM %L CSV DELIMITER %L', filepath, delim);
  END;
  $$LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE ExportToCSV
  (target_table varchar, filepath varchar, delim char(1))
  AS $$
  BEGIN
    EXECUTE FORMAT('COPY '||target_table||' TO %L CSV DELIMITER %L', filepath, delim);
  END;
  $$LANGUAGE plpgsql;

CREATE TABLE Peers(
  Nickname varchar primary key,
  Birthday date
  );

INSERT INTO Peers VALUES('bdump' , '2000-05-01');
INSERT INTO Peers VALUES('oleila', '1990-03-08');
INSERT INTO Peers VALUES('christ');
INSERT INTO Peers VALUES('ohara' , '1998-07-16');
INSERT INTO Peers VALUES('bbrian');

CALL ExportFromCSV(target_table := 'Peers',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Peers.csv', delim := ';');

CREATE TABLE Tasks(
  Title varchar primary key,
  ParentTask varchar references Tasks(Title),
  MaxXP integer not null
  );

INSERT INTO Tasks VALUES('C1_Simple_bash', null, 750);
INSERT INTO Tasks VALUES('C2_S21_String', 'C1_Simple_bash', 500);

CALL ExportFromCSV(target_table := 'Tasks',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Tasks.csv', delim := ',');

CREATE TYPE CheckStatus AS ENUM('Start', 'Success', 'Failure');

CREATE TABLE Checks(
  ID bigint primary key,
  Peer varchar references Peers(Nickname) not null,
  Task varchar references Tasks(Title) not null,
  Date date not null
  );

INSERT INTO Checks VALUES(1, 'cbebe',     'C1_Simple_bash', '2022-01-10');
INSERT INTO Checks VALUES(2, 'maliniris', 'C1_Simple_bash', '2022-01-10');
INSERT INTO Checks VALUES(3, 'rlatrish',  'C1_Simple_bash', '2022-01-11');
INSERT INTO Checks VALUES(4, 'meoneida',  'C1_Simple_bash', '2022-01-12');
INSERT INTO Checks VALUES(5, 'vleida',    'C1_Simple_bash', '2022-01-12');

CALL ExportFromCSV(target_table := 'Checks',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Checks.csv', delim := ',');

CREATE TABLE P2P(
  ID bigint primary key,
  "Check" bigint references Checks(ID) ON DELETE CASCADE,
  CheckingPeer varchar references Peers(Nickname),
  State CheckStatus not null,
  Time time(0) not null
  );

INSERT INTO P2P VALUES(1, 1, 'vleida', 'Start', '10:00:00');
INSERT INTO P2P VALUES(2, 1, 'vleida', 'Success', '10:30:00');
INSERT INTO P2P VALUES(3, 2, 'rlatrish', 'Start', '09:45:00');
INSERT INTO P2P VALUES(4, 2, 'rlatrish', 'Success', '10:14:00');
INSERT INTO P2P VALUES(5, 3, 'cbebe', 'Start', '08:11:00');
INSERT INTO P2P VALUES(6, 3, 'cbebe', 'Success', '09:34:00');
INSERT INTO P2P VALUES(7, 4, 'maliniris', 'Start', '11:56:00');
INSERT INTO P2P VALUES(8, 4, 'maliniris', 'Failure', '12:00:00');
INSERT INTO P2P VALUES(9, 5, 'meoneida', 'Start', '14:00:00');
INSERT INTO P2P VALUES(10, 5, 'meoneida', 'Success', '16:30:00');

CALL ExportFromCSV(target_table := 'P2P',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/P2P.csv', delim := ',');

CREATE TABLE Verter(
  ID bigint primary key,
  "Check" bigint references Checks(ID) ON DELETE CASCADE,
  State CheckStatus not null,
  Time time(0) not null
  );

INSERT INTO Verter VALUES(1, 2, 'Start', '10:15:00');
INSERT INTO Verter VALUES(2, 2, 'Success', '10:16:00');
INSERT INTO Verter VALUES(3, 3, 'Start', '09:35:00');
INSERT INTO Verter VALUES(4, 3, 'Failure', '09:36:00');
INSERT INTO Verter VALUES(5, 5, 'Start', '16:31:00');
INSERT INTO Verter VALUES(6, 5, 'Success', '16:32:00');

CALL ExportFromCSV(target_table := 'Verter',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Verter.csv', delim := ',');

CREATE TABLE TransferredPoints(
  ID bigint primary key,
  CheckingPeer varchar references Peers(Nickname) not null,
  CheckedPeer varchar references Peers(Nickname) not null,
  PointsAmount bigint
);

INSERT INTO TransferredPoints VALUES(1, 'vleida', 'christ', 2);
INSERT INTO TransferredPoints VALUES(2, 'bdump', 'rlatrish', 11);
INSERT INTO TransferredPoints VALUES(3, 'cbebe', 'oleila', 4);
INSERT INTO TransferredPoints VALUES(4, 'ohara', 'maliniris', 8);
INSERT INTO TransferredPoints VALUES(5, 'bbrian', 'bdump', 1);

CREATE TABLE XP(
  ID bigint primary key,
  "Check" bigint references Checks(ID) ON DELETE CASCADE,
  XPAmount INTEGER
);

CALL ExportFromCSV(target_table := 'XP',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/XP.csv', delim := ',');

CREATE TABLE Friends(
  ID bigint primary key,
  Peer1 varchar references Peers(Nickname) not null,
  Peer2 varchar references Peers(Nickname) not null
);

CALL ExportFromCSV(target_table := 'Friends',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Friends.csv', delim := ',');

CREATE TABLE Recommendations(
  ID bigint primary key,
  Peer varchar references Peers(Nickname) not null,
  RecommendedPeer varchar references Peers(Nickname) not null
);

CALL ExportFromCSV(target_table := 'Recommendations',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/Recommendations.csv', delim := ',');

CREATE TABLE TimeTracking(
  ID bigint primary key,
  Peer varchar references Peers(Nickname) not null,
  Date date not null,
  Time time(0) not null,
  State integer not null check(State = 1 OR State = 2)
);

CALL ExportFromCSV(target_table := 'TimeTracking',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/TimeTracking.csv', delim := ',');

CALL ExportToCSV(target_table := 'TimeTracking',
  filepath := '/Java/SQL2_Info21_v1.0-0/src/TimeTrackingLog.csv', delim := ',');
