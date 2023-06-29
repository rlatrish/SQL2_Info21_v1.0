CREATE OR REPLACE FUNCTION FncTransferredPoints()
  RETURNS TABLE(Peer1 VARCHAR, Peer2 VARCHAR, Points NUMERIC) AS $$
  WITH
    a AS (SELECT CheckingPeer, CheckedPeer, PointsAmount
      FROM TransferredPoints WHERE CheckingPeer > CheckedPeer),
    b AS (SELECT CheckedPeer, CheckingPeer, -PointsAmount
      FROM TransferredPoints WHERE CheckedPeer > CheckingPeer),
    c AS (SELECT * FROM a UNION SELECT * FROM b)
  SELECT CheckingPeer, CheckedPeer, sum(PointsAmount)
    FROM c
    GROUP BY (CheckingPeer, CheckedPeer)
    ORDER BY 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeerTaskXP()
  RETURNS TABLE(Peer VARCHAR, Task VARCHAR, XP INTEGER) AS $$
  WITH
    a AS (SELECT Checks.ID
      FROM Checks
      JOIN P2P ON Checks.ID = P2P."Check"
      WHERE P2P.State = 'Success'),
    b AS (SELECT Checks.ID
          FROM Checks
          JOIN Verter ON Checks.ID = Verter."Check"
          WHERE Verter.State = 'Failure'
          UNION
          SELECT Checks.ID
          FROM Checks
          JOIN (SELECT Verter."Check"
                FROM Verter 
                GROUP BY Verter."Check"
                HAVING COUNT(Verter.ID) % 2 = 1) AS v2 ON Checks.ID = v2."Check"
      ),
    c AS (SELECT ID FROM a EXCEPT SELECT ID FROM b)
    SELECT Peer, Task, SUM(XPAmount)
      FROM c
      JOIN Checks d ON c.ID = d.ID
      LEFT JOIN XP ON c.ID = XP."Check"
      GROUP BY Peer, Task
      ORDER BY 1;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeerNotExit(DayX Date DEFAULT CURRENT_DATE)
  RETURNS TABLE(Peer VARCHAR) AS $$
    SELECT Peer
      FROM TimeTracking
      WHERE Date = DayX
      GROUP by Peer
      HAVING (count(State) = 2);
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeerTotalPoints1()
  RETURNS TABLE(Peer VARCHAR, PointsChange INTEGER) AS $$
  WITH
    a AS (SELECT CheckingPeer, SUM(PointsAmount) AS PSum
      FROM TransferredPoints
      GROUP by CheckingPeer),
    b AS (SELECT CheckedPeer, - SUM(PointsAmount) AS PSum
      FROM TransferredPoints
      GROUP by CheckedPeer),
    c AS (SELECT * FROM a UNION SELECT * FROM b)
    SELECT CheckingPeer AS Peer, SUM(PSum) AS PointsChange FROM c
    GROUP by CheckingPeer
    ORDER BY 2 DESC, 1;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeerTotalPoints2()
  RETURNS TABLE(Peer VARCHAR, PointsChange INTEGER) AS $$
  WITH
    a AS (SELECT Peer1, SUM(Points) AS PSum
      FROM FncTransferredPoints()
      GROUP by Peer1),
    b AS (SELECT Peer2, - SUM(Points) AS PSum
      FROM FncTransferredPoints()
      GROUP by Peer2),
    c AS (SELECT * FROM a UNION SELECT * FROM b)
    SELECT Peer1 AS Peer, SUM(PSum) AS PointsChange FROM c
    GROUP by Peer1
    ORDER BY 2 DESC, 1;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncMostCheckedTask()
  RETURNS TABLE(Day DATE, Task VARCHAR)AS $$
  WITH
    a AS (SELECT Date, Title, COUNT(Title) AS CTitle
      FROM Checks
      JOIN Tasks
      ON Checks.Task = Tasks.Title
      GROUP BY Date, Title)
    SELECT a.Date, Title
      FROM a
      JOIN (SELECT Date, MAX(CTitle) AS MaxCTitle
      	      FROM a
	    GROUP BY Date) AS aa
      ON a.Date = aa.Date AND a.CTitle = aa.MaxCtitle
      ORDER BY Date, Title;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeersCompletedBlock(S21Block VARCHAR)
  RETURNS TABLE(Peer VARCHAR, Day DATE) AS $$
  WITH
    a AS (SELECT ParentTask AS Title
      FROM Tasks
      WHERE trim(trailing '0123456789' from split_part(ParentTask, '_', 1)) = S21Block
        AND trim(trailing '0123456789' from split_part(ParentTask, '_', 1)) != 
            trim(trailing '0123456789' from split_part(Title, '_', 1)))
    SELECT Peer, Date
      FROM Checks
      JOIN a
      ON Checks.Task = a.Title
      JOIN P2P
      ON Checks.ID = P2P."Check" AND P2P.State = 'Success'
      JOIN Verter
      ON (Verter."Check" IS NULL) OR (Checks.ID = Verter."Check" AND Verter.State = 'Success')
      ORDER BY Date;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeerForCheck()
  RETURNS TABLE(Peer VARCHAR, RecommendedPeer VARCHAR) AS $$
  WITH
    a AS (SELECT NickName, RecommendedPeer
      FROM Peers
      JOIN Friends 
      ON NickName = Peer1
        JOIN Recommendations
        ON Peer2 = Peer
      WHERE NickName !=RecommendedPeer
      UNION ALL
      SELECT NickName, RecommendedPeer
      FROM Peers
      JOIN Friends 
      ON NickName = Peer2
        JOIN Recommendations
        ON Peer1 = Peer
      WHERE NickName !=RecommendedPeer),
    b AS (SELECT  NickName, RecommendedPeer, COUNT(RecommendedPeer) AS CountRecommended
      FROM a
      GROUP BY NickName, RecommendedPeer),
    c AS (SELECT  NickName, MAX(CountRecommended) AS MaxRecommended
      FROM b
      GROUP BY NickName),
    d AS (SELECT  b.NickName, b.RecommendedPeer
      FROM b
      JOIN c
      ON b.NickName = c.NickName AND CountRecommended = MaxRecommended),
    e AS (SELECT  NickName, MIN(RecommendedPeer) AS MinRecommended
      FROM d
      GROUP BY NickName)
    SELECT d.Nickname, d.RecommendedPeer
    FROM d
    JOIN e
    ON d.NickName = e.NickName AND d.RecommendedPeer = e.MinRecommended
    ORDER BY 1;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPercentagePeers(Block1 VARCHAR, Block2 VARCHAR)
  RETURNS TABLE(StartedBlock1 INTEGER, StartedBlock2 INTEGER,
                StartedBothBlocks INTEGER, DidntStartAnyBlock INTEGER) AS $$
  WITH
    sb1 AS (SELECT DISTINCT Peer 
      FROM Checks
      WHERE trim(trailing '0123456789' from split_part(Task, '_', 1)) = Block1),
    sb2 AS (SELECT DISTINCT Peer 
      FROM Checks
      WHERE trim(trailing '0123456789' from split_part(Task, '_', 1)) = Block2),
    allPeers AS (SELECT COUNT(NickName) AS allP FROM Peers),
    b1 AS (SELECT COUNT(Peer) AS b1
      FROM (SELECT Peer FROM sb1
            EXCEPT 
            SELECT Peer FROM sb2) AS b1Billet),
    b2 AS (SELECT COUNT(Peer) AS b2
      FROM (SELECT Peer FROM sb2
            EXCEPT
            SELECT Peer FROM sb1) AS b2Billet),
    b12 AS (SELECT COUNT(Peer) AS b12
      FROM (SELECT Peer FROM sb1
            INTERSECT
            SELECT Peer FROM sb2) AS b12Billet)
    SELECT
      ROUND(b1 * 100.0 / allP),
      ROUND(b2 * 100.0 / allP),
      ROUND(b12 * 100.0 / allP),
      100 - ROUND(b1 * 100.0 / allP) - ROUND(b2 * 100.0 / allP) - ROUND(b12 * 100.0 / allP)
    FROM b1, b2, b12, allPeers;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPercentagePeersBirthday()
  RETURNS TABLE(SuccessfulChecks INTEGER, UnsuccessfulChecks INTEGER) AS $$
  WITH
    allChecks AS (SELECT ID
      FROM Checks
      JOIN Peers
      ON Checks.Peer = Peers.NickName AND
         EXTRACT(MONTH FROM Checks.Date) = EXTRACT(MONTH FROM Peers.Birthday) AND
         EXTRACT(DAY FROM Checks.Date) = EXTRACT(DAY FROM Peers.Birthday)),
    allPeers AS (SELECT DISTINCT Peer
      FROM Checks
      JOIN allChecks
      ON Checks.ID = allChecks.ID),
    allPeersCount AS (SELECT COUNT(Peer) AS allP FROM allPeers),
    unsuccess AS (SELECT DISTINCT Peer 
      FROM Checks
      JOIN allChecks
      ON Checks.ID = allChecks.ID
      JOIN P2P
      ON allChecks.ID = P2P."Check" AND P2P.State = 'Failure'
      UNION
      SELECT DISTINCT Peer 
      FROM Checks
      JOIN allChecks
      ON Checks.ID = allChecks.ID
      JOIN Verter
      ON allChecks.ID = Verter."Check" AND Verter.State = 'Failure'),
    unsuccessCount AS (SELECT COUNT(Peer) AS un FROM unsuccess)
    SELECT
      CASE WHEN allP <> 0 THEN 100 - ROUND(un * 100.0 / allP)
            ELSE 0
      END,
      CASE WHEN allP <> 0 THEN ROUND(un * 100.0 / allP)
            ELSE 0
      END
    FROM unsuccessCount, allPeersCount;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeersSuccessTask(Task1 VARCHAR)
  RETURNS TABLE(Peer VARCHAR) AS $$
  WITH
    allPeerTaskP2PSuccess AS (SELECT DISTINCT Peer
      FROM Checks
      JOIN P2P
      ON Checks.ID = P2P."Check" AND P2P.State = 'Success'
      WHERE Task = Task1),
    allPeerTaskVerterUnsuccess AS (SELECT DISTINCT Peer
      FROM Checks
      JOIN Verter
      ON Checks.ID = Verter."Check" AND Verter.State = 'Failure'
      WHERE Task = Task1
      EXCEPT
      SELECT DISTINCT Peer
      FROM Checks
      JOIN Verter
      ON Checks.ID = Verter."Check" AND Verter.State = 'Success'
      WHERE Task = Task1)
    SELECT Peer 
    FROM allPeerTaskP2PSuccess
    EXCEPT
    SELECT Peer
    FROM allPeerTaskVerterUnsuccess;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeersGivenTask12(Task1 VARCHAR, Task2 VARCHAR, Task3 VARCHAR)
  RETURNS TABLE(Peer VARCHAR) AS $$
    SELECT * FROM FncPeersSuccessTask(Task1)
    INTERSECT
    SELECT * FROM FncPeersSuccessTask(Task2)
    EXCEPT
    SELECT * FROM FncPeersSuccessTask(Task3);  
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncRecursiveTasks()
  RETURNS TABLE(Task VARCHAR, PrevCount INTEGER) AS $$
WITH RECURSIVE all_tasks(MainTitle, Parent, Step) AS (
    SELECT Title, ParentTask, 0 FROM Tasks
  UNION
    SELECT MainTitle, p.ParentTask, 1
    FROM all_tasks AS pr
    JOIN Tasks AS p
    ON pr.Parent IS NOT NULL AND pr.Parent = p.Title)
SELECT MainTitle, SUM(Step) 
FROM all_tasks
GROUP BY MainTitle
ORDER BY 2;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncLackyDays(N INTEGER)
  RETURNS TABLE(Date DATE) AS $$
  WITH
    base AS (SELECT 
        Date, 
        P2P.Time,
        CASE WHEN (XP.XPAmount IS NULL) OR (Tasks.MaxXP IS NULL) THEN FALSE
             WHEN XP.XPAmount * 100.0 / Tasks.MaxXP >= 80 THEN TRUE
             ELSE FALSE
        END AS Success
      FROM Checks
      JOIN P2P
      ON Checks.ID = P2P."Check" AND P2P.State = 'Start'
      JOIN XP
      ON Checks.ID = XP."Check"
      JOIN Tasks
      ON Checks.Task = Tasks.Title
      ORDER BY 1, 2),
    base2 AS(SELECT Date, Time, Success,
        CASE WHEN Success THEN ROW_NUMBER() OVER()
             ELSE NULL
        END AS Num
        FROM base),
    base3 AS(SELECT Date, Time, Success,
        CASE WHEN Num IS NOT NULL AND 
                  ((LAG(Num, 1, NULL) OVER()) IS NULL OR 
                  (LAG(Date, 1, NULL) OVER()) != Date) THEN Num
             WHEN Num IS NOT NULL AND 
                  ((LAG(Num, 1, NULL) OVER()) IS NOT NULL OR 
                  (LAG(Date, 1, NULL) OVER()) = Date) THEN LAG(Num, 1, 1) OVER()
             ELSE NULL
        END AS Rank
        FROM base2)
    SELECT Date 
    FROM base3
    GROUP BY Date, Rank
    HAVING COUNT(Rank) >= N;
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncHighestAmountXP()
  RETURNS TABLE(Peer VARCHAR, XP INTEGER) AS $$
    SELECT DISTINCT Peer, SUM(XPAmount) AS XP
    FROM Checks
    JOIN XP
    ON Checks.ID = XP."Check"
    GROUP BY Peer
    ORDER BY XP DESC
    LIMIT 1;  
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeersCameBefore(Came TIME, N INTEGER)
  RETURNS TABLE(Peer VARCHAR) AS $$
    SELECT Peer
    FROM TimeTracking
    WHERE State = 1 AND Time < Came
    GROUP BY Peer
    HAVING COUNT(Time) >= N
    ORDER BY 1;
   $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncPeersLeftCampus(N INTEGER, M INTEGER)
  RETURNS TABLE(Peer VARCHAR) AS $$
  WITH 
    last AS (SELECT Peer, Date, MAX(Time) Maxt
      FROM TimeTracking
      WHERE State = 2 AND Date > (CURRENT_DATE - N)
      GROUP BY Peer, Date)
    SELECT t.Peer
    FROM TimeTracking AS t
    JOIN last
    ON t.Peer = last.Peer AND t.Date = last.Date AND t.State = 2 AND t.Time != last.Maxt
    GROUP BY t.Peer
    HAVING COUNT(ID) > M
    ORDER BY 1; 
  $$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION FncEarlyEntries()
  RETURNS TABLE(Month VARCHAR, EarlyEntries VARCHAR) AS $$
  WITH 
    a AS (SELECT Time, EXTRACT(MONTH FROM Birthday) AS MB
      FROM TimeTracking
      JOIN Peers
      ON Peer = NickName AND State = 1),
    allEntries AS (SELECT MB, COUNT(Time) AS allE
      FROM a
      GROUP BY MB),
    early AS (SELECT MB, COUNT(Time) as earlyI
      FROM a
      WHERE Time < '12:00'
      GROUP BY MB)
    SELECT 
      to_char(make_date(2000, CAST(allEntries.MB as integer), 1), 'Month'),
      ROUND(earlyI * 100.0 / allE)
    FROM allEntries
    JOIN early
    ON allEntries.MB = early.MB
    ORDER BY allEntries.MB; 
  $$ LANGUAGE SQL;

  SELECT '1) Write a function that returns the TransferredPoints table in a more human-readable form' AS Part3_1;
  SELECT * FROM FncTransferredPoints();  -- 1
  SELECT '2) Write a function that returns a table of the following form: user name, name of the checked task, number of XP received' AS Part3_2;
  SELECT * FROM FncPeerTaskXP(); -- 2
  SELECT '3) Write a function that finds the peers who have not left campus for the whole day' AS Part3_3;
  SELECT * FROM FncPeerNotExit(); -- 3
  SELECT * FROM FncPeerNotExit('2022-01-10'); -- 3
  SELECT * FROM FncPeerNotExit('2022-01-11'); -- 3
  SELECT '4) Calculate the change in the number of peer points of each peer using the TransferredPoints table' AS Part3_4;
  SELECT * FROM FncPeerTotalPoints1(); -- 4
  SELECT '5) Calculate the change in the number of peer points of each peer using the table returned by the first function from Part 3' AS Part3_5;
  SELECT * FROM FncPeerTotalPoints2(); -- 5
  SELECT '6) Find the most frequently checked task for each day' AS Part3_6;
  SELECT * FROM FncMostCheckedTask(); -- 6
  SELECT '7) Find all peers who have completed the whole given block of tasks and the completion date of the last task' AS Part3_7;
  SELECT * FROM FncPeersCompletedBlock('C'); -- 7
  SELECT * FROM FncPeersCompletedBlock('CPP'); -- 7
  SELECT * FROM FncPeersCompletedBlock('A'); -- 7
  SELECT '8) Determine which peer each student should go to for a check.' AS Part3_8;
  SELECT * FROM FncPeerForCheck(); -- 8
  SELECT '9) Determine the percentage of peers who:' AS Part3_9;
  SELECT * FROM FncPercentagePeers('C','CPP'); -- 9
  SELECT * FROM FncPercentagePeers('C','A'); -- 9
  SELECT * FROM FncPercentagePeers('A','CPP'); -- 9
  SELECT '10) Determine the percentage of peers who have ever successfully passed a check on their birthday' AS Part3_10;
  SELECT * FROM FncPercentagePeersBirthday(); -- 10
  SELECT '11) Determine all peers who did the given tasks 1 and 2, but did not do task 3' AS Part3_11;
  SELECT * FROM FncPeersGivenTask12('C1_Simple_bash', 'C2_S21_String', 'CPP_Matrix'); -- 11
  SELECT '12) Using recursive common table expression, output the number of preceding tasks for each task' AS Part3_12;
  SELECT * FROM FncRecursiveTasks(); -- 12
  SELECT '13) Find "lucky" days for checks. A day is considered "lucky" if it has at least N consecutive successful checks' AS Part3_13;
  SELECT * FROM FncLackyDays(1); -- 13
  SELECT * FROM FncLackyDays(2); -- 13
  SELECT * FROM FncLackyDays(3); -- 13
  SELECT '14) Find the peer with the highest amount of XP' AS Part3_14;
  SELECT * FROM FncHighestAmountXP(); -- 14
  SELECT '15) Determine the peers that came before the given time at least N times during the whole time' AS Part3_15;
  SELECT * FROM FncPeersCameBefore('23:00:00', 1); -- 15
  SELECT * FROM FncPeersCameBefore('23:00:00', 2); -- 15
  SELECT * FROM FncPeersCameBefore('11:00:00', 2); -- 15
  SELECT * FROM FncPeersCameBefore('23:00:00', 3); -- 15
  SELECT * FROM FncPeersCameBefore('11:00:00', 3); -- 15
  SELECT * FROM FncPeersCameBefore('23:00:00', 4); -- 15
  SELECT * FROM FncPeersCameBefore('23:00:00', 6); -- 15
/*
    SELECT  Peer, Date, Time, State
    FROM TimeTracking
    ORDER BY 1, 2, 3;
*/    
  SELECT '16) Determine the peers who left the campus more than M times during the last N days' AS Part3_16;
  SELECT * FROM FncPeersLeftCampus(623, 0); -- 16
  SELECT * FROM FncPeersLeftCampus(623, 1); -- 16
  SELECT * FROM FncPeersLeftCampus(623, 2); -- 16
  SELECT '17) Determine for each month the percentage of early entries' AS Part3_17;
  SELECT * FROM FncEarlyEntries(); --17 
