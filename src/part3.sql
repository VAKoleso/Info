----------------------------------------- PART 3.1 -----------------------------------------

create or replace function TransferredPoints_humanity() returns table (
    Peer1 varchar(50),
    Peer2 varchar(50),
    PointsAmount int
  ) as $$ begin return query (
    SELECT t1.checking_peer_nickname Peer1,
      t1.peer_being_checked_nickname Peer2,
      t1.num_transferred_points - t2.num_transferred_points PointsAmount
    FROM TransferredPoints t1
      JOIN TransferredPoints t2 ON t1.checking_peer_nickname = t2.peer_being_checked_nickname
      AND t1.peer_being_checked_nickname = t2.checking_peer_nickname
    where (
        t1.num_transferred_points - t2.num_transferred_points
      ) <= 0
    GROUP BY t1.peer_being_checked_nickname,
      t1.checking_peer_nickname,
      t1.num_transferred_points,
      t2.num_transferred_points
  )
union
(
  select checking_peer_nickname Peer1,
    peer_being_checked_nickname Peer2,
    num_transferred_points
  from TransferredPoints
  except
  SELECT t1.checking_peer_nickname Peer1,
    t1.peer_being_checked_nickname Peer2,
    t1.num_transferred_points PointsAmount
  FROM TransferredPoints t1
    JOIN TransferredPoints t2 ON t1.checking_peer_nickname = t2.peer_being_checked_nickname
    AND t1.peer_being_checked_nickname = t2.checking_peer_nickname
  GROUP BY t1.peer_being_checked_nickname,
    t1.checking_peer_nickname,
    t1.num_transferred_points,
    t2.num_transferred_points
);
end;
$$ language plpgsql;

----------------------------------------- TEST 3.1 -----------------------------------------

--SELECT * FROM TransferredPoints_humanity();

----------------------------------------- PART 3.2 -----------------------------------------

CREATE OR REPLACE FUNCTION get_user_xp_for_task() RETURNS TABLE (
    peer VARCHAR(50),
    task VARCHAR(50),
    xp INTEGER
  ) AS $$ BEGIN RETURN query (
    SELECT checks.peer_nickname,
      checks.task_name,
      xp.num_xp_received
    FROM checks
      JOIN xp ON checks.id = xp.check_id
  );
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.2 -----------------------------------------

--SELECT * FROM get_user_xp_for_task();

----------------------------------------- PART 3.3 -----------------------------------------

CREATE OR REPLACE FUNCTION peers_in_campus(date_param DATE) RETURNS SETOF VARCHAR(50) AS $$ BEGIN RETURN QUERY
SELECT peer_nickname
FROM TimeTracking
WHERE date = date_param
  AND state = 1
EXCEPT
SELECT peer_nickname
FROM TimeTracking
WHERE date = date_param
  AND state = 2;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.3 -----------------------------------------

--insert into TimeTracking values 
--((select max(id) from TimeTracking) + 1, 'iseadra', '01.02.23', '11:59', 1),
--((select max(id) from TimeTracking) + 2, 'qreiko', '01.02.23', '10:59', 1),
--((select max(id) from TimeTracking) + 3, 'iseadra', '01.02.23', '21:59', 2);

--SELECT * FROM peers_in_campus('2023-01-02');

----------------------------------------- PART 3.4 -----------------------------------------

CREATE OR REPLACE PROCEDURE change_in_quantity(IN REF refcursor) AS $$ BEGIN OPEN REF for
(
(
    select tab1.peer,
      tab2.PointsChange
    from (
        (
          select checking_peer_nickname peer
          from transferredpoints
          group by transferredpoints.checking_peer_nickname
        )
        except (
            select checking_peer_nickname Peer
            from (
                select checking_peer_nickname,
                  sum(num_transferred_points) PointsChange
                from transferredpoints
                group by transferredpoints.checking_peer_nickname
              ) t1
              join (
                select peer_being_checked_nickname,
                  sum(num_transferred_points) * -1
                minus
                from transferredpoints
                group by transferredpoints.peer_being_checked_nickname
              ) t2 on t1.checking_peer_nickname = t2.peer_being_checked_nickname
          )
      ) as tab1
      join (
        select checking_peer_nickname,
          sum(num_transferred_points) PointsChange
        from transferredpoints
        group by transferredpoints.checking_peer_nickname
      ) as tab2 on tab2.checking_peer_nickname = tab1.peer
  )
  union
  (
    select tab1.peer,
      tab2.minus
    from (
        (
          select peer_being_checked_nickname peer
          from transferredpoints
          group by transferredpoints.peer_being_checked_nickname
        )
        except (
            select checking_peer_nickname Peer
            from (
                select checking_peer_nickname,
                  sum(num_transferred_points) PointsChange
                from transferredpoints
                group by transferredpoints.checking_peer_nickname
              ) t1
              join (
                select peer_being_checked_nickname,
                  sum(num_transferred_points) * -1
                minus
                from transferredpoints
                group by transferredpoints.peer_being_checked_nickname
              ) t2 on t1.checking_peer_nickname = t2.peer_being_checked_nickname
          )
      ) as tab1
      join (
        select peer_being_checked_nickname,
          sum(num_transferred_points) * -1
        minus
        from transferredpoints
        group by transferredpoints.peer_being_checked_nickname
      ) as tab2 on tab2.peer_being_checked_nickname = tab1.peer
  )
)
union
(
  select checking_peer_nickname Peer,
    (t1.PointsChange + t2.minus) PointsChange
  from (
      select checking_peer_nickname,
        sum(num_transferred_points) PointsChange
      from transferredpoints
      group by transferredpoints.checking_peer_nickname
    ) t1
    join (
      select peer_being_checked_nickname,
        sum(num_transferred_points) * -1
      minus
      from transferredpoints
      group by transferredpoints.peer_being_checked_nickname
    ) t2 on t1.checking_peer_nickname = t2.peer_being_checked_nickname
)
order by PointsChange desc;
end;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.4 -----------------------------------------

--BEGIN;
--CALL change_in_quantity('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.5 -----------------------------------------

CREATE OR REPLACE PROCEDURE change_in_quantitt_part5(IN REF refcursor) AS $$ BEGIN OPEN REF FOR
WITH table_left AS (
  select peer1,
    sum(pointsamount) pointsamount
  from TransferredPoints_humanity()
  group by transferredpoints_humanity.peer1
),
table_right AS (
  select t1.peer2,
    sum(t1.pointsamount) pointsamount
  from (
      select peer1,
        peer2,
        (pointsamount * -1) pointsamount
      from TransferredPoints_humanity()
    ) t1
  group by t1.peer2
),
table_sum as (
  select peer1,
    (t1.pointsamount + t2.pointsamount) PointsChange
  from table_left t1
    join table_right t2 on t1.peer1 = t2.peer2
),
without_amount_left as (
  select peer1
  from table_left
  except
  select peer1
  from table_sum
),
without_amount_right as (
  select peer2
  from table_right
  except
  select peer1
  from table_sum
),
true_table_left as (
  select without_amount_left.peer1 Peer,
    table_left.pointsamount PointsChange
  from without_amount_left
    join table_left on without_amount_left.peer1 = table_left.peer1
),
true_table_right as (
  select without_amount_right.peer2 Peer,
    table_right.pointsamount PointsChange
  from without_amount_right
    join table_right on without_amount_right.peer2 = table_right.peer2
) (
  select *
  from true_table_left
)
union
(
  select *
  from true_table_right
)
union
(
  select *
  from table_sum
)
order by PointsChange desc;
end;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.5 -----------------------------------------

--BEGIN;
--CALL change_in_quantitt_part5('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.6 -----------------------------------------

CREATE OR REPLACE PROCEDURE most_frequently_checked_task_for_each_day(IN REF refcursor) AS $$ BEGIN OPEN REF FOR
SELECT check_date AS Day,
  substring(
    task_name
    from '^[^_]+'
  ) AS Task
FROM (
    SELECT check_date,
      task_name,
      COUNT(*) AS cnt,
      RANK() OVER (
        PARTITION BY check_date
        ORDER BY COUNT(*) DESC
      ) AS rank
    FROM Checks
    GROUP BY check_date,
      task_name
  ) AS ranked
WHERE rank = 1
ORDER BY check_date DESC;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.6 -----------------------------------------

--BEGIN;
--CALL most_frequently_checked_task_for_each_day('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.7 -----------------------------------------

create or replace function get_peers_who_made_block_tasks(block varchar) returns table
(peer varchar,
day date)
as $$ begin return query
select peer_nickname as Peer, check_date as day
from checks
join p2p on p2p.check_id = checks.id
join verter on verter.check_id = checks.id
where task_name =
(select name from tasks
where name like block
|| 
(select count(name) from tasks
where name like block || '_\_' || '%')
||
'%')
and
p2p_check_status = 'Success'
and
verter_check_status = 'Success';
end;
$$ language plpgsql;

----------------------------------------- TEST 3.7 -----------------------------------------

--select * from get_peers_who_made_block_tasks('C');

----------------------------------------- PART 3.8 -----------------------------------------

CREATE OR REPLACE PROCEDURE recommended_by_friends(IN REF refcursor) AS $$ BEGIN OPEN ref FOR WITH search_friends AS (
    SELECT nickname,
      (
        CASE
          WHEN nickname = friends.first_peer_nickname THEN second_peer_nickname
          ELSE first_peer_nickname
        END
      ) AS frineds
    FROM peers
      JOIN friends ON peers.nickname = friends.first_peer_nickname
      OR peers.nickname = friends.second_peer_nickname
  ),
  search_reccommend AS (
    SELECT nickname,
      COUNT(recommended_peer_nickname) AS count_rec,
      recommended_peer_nickname
    FROM search_friends
      JOIN recommendations ON search_friends.frineds = recommendations.peer_nickname
    WHERE search_friends.nickname != recommendations.recommended_peer_nickname
    GROUP BY nickname,
      recommended_peer_nickname
  ),
  search_max AS (
    SELECT nickname,
      MAX(count_rec) AS max_count
    FROM search_reccommend
    GROUP BY nickname
  )
SELECT search_reccommend.nickname AS peer,
  recommended_peer_nickname
FROM search_reccommend
  JOIN search_max ON search_reccommend.nickname = search_max.nickname
  AND search_reccommend.count_rec = search_max.max_count;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.8 -----------------------------------------

--BEGIN;
--CALL recommended_by_friends('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.9 -----------------------------------------

CREATE OR REPLACE PROCEDURE percentage_of_peers_that_started_blocks(
    blockname1 VARCHAR,
    blockname2 VARCHAR,
    IN REF refcursor
  ) AS $$ BEGIN OPEN REF FOR WITH block1 AS (
    SELECT DISTINCT peer_nickname
    FROM Checks
    WHERE task_name SIMILAR TO blockname1
  ),
  block2 AS (
    SELECT DISTINCT peer_nickname
    FROM Checks
    WHERE task_name SIMILAR TO blockname2
  ),
  both_blocks AS (
    SELECT DISTINCT peer_nickname
    FROM block1
    intersect
    SELECT DISTINCT peer_nickname
    FROM block2
  ),
  any_blocks AS (
    SELECT nickname AS peer_nickname
    FROM Peers
    EXCEPT (
        SELECT DISTINCT peer_nickname
        FROM block1
        UNION
        SELECT DISTINCT peer_nickname
        FROM block2
      )
  )
SELECT (
    SELECT count(peer_nickname)
    FROM block1
  ) * 100 / count(nickname) AS StartedBlock1,
  (
    SELECT count(peer_nickname)
    FROM block2
  ) * 100 / count(nickname) AS StartedBlock2,
  (
    SELECT count(peer_nickname)
    FROM both_blocks
  ) * 100 / count(nickname) AS StartedBothBlocks,
  (
    SELECT count(peer_nickname)
    FROM any_blocks
  ) * 100 / count(nickname) AS DidntStartAnyBlock
FROM Peers;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.9 -----------------------------------------

--BEGIN;
--CALL percentage_of_peers_that_started_blocks('A[0-9]%', 'CPP%', 'ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.10 -----------------------------------------

CREATE OR REPLACE PROCEDURE percentage_of_checks_birthday(IN REF refcursor) AS $$ BEGIN OPEN REF for
with birthday_p2p as (
  select id,
    peer_nickname
  from checks
    join peers on peer_nickname = nickname
  where date_part('day', check_date) = date_part('day', birthday)
    and date_part('month', check_date) = date_part('month', birthday)
),
success_p2p as (
  select distinct on (peer_nickname) birthday_p2p.id,
    peer_nickname,
    p2p_check_status
  from birthday_p2p
    join p2p on birthday_p2p.id = p2p.check_id
  where p2p_check_status = 'Success'
),
failure_p2p as (
  select distinct on (peer_nickname) birthday_p2p.id,
    peer_nickname,
    p2p_check_status
  from birthday_p2p
    join p2p on birthday_p2p.id = p2p.check_id
  where p2p_check_status = 'Failure'
),
count_success as (
  select count(peer_nickname)
  from success_p2p
),
count_failure as (
  select count(peer_nickname)
  from failure_p2p
),
sum_success_failure as (
  select (
      select *
      from count_success
    ) + (
      select *
      from count_failure
    )
)
select (
    (
      select *
      from count_success
    )::numeric(6, 3) / (
      select *
      from sum_success_failure
    )
  )::numeric(6, 3) * 100 SuccessfulChecks,
  (
    (
      select *
      from count_failure
    )::numeric(6, 3) / (
      select *
      from sum_success_failure
    )
  )::numeric(6, 3) * 100 UnsuccessfulChecks
from count_failure;
end;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.10 -----------------------------------------

--BEGIN;
--CALL percentage_of_checks_birthday('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.11 -----------------------------------------

CREATE OR REPLACE PROCEDURE successful_and_not(
    task1 varchar,
    task2 varchar,
    task3 varchar,
    ref refcursor
  ) AS $$ BEGIN OPEN ref FOR WITH task_1 AS (
    SELECT peer
    FROM get_user_xp_for_task()
    WHERE task1 IN (
        SELECT task
        FROM get_user_xp_for_task()
      )
  ),
  task_2 AS (
    SELECT peer
    FROM get_user_xp_for_task()
    WHERE task2 IN (
        SELECT task
        FROM get_user_xp_for_task()
      )
  ),
  task_3 AS (
    SELECT peer
    FROM get_user_xp_for_task()
    WHERE task3 NOT IN (
        SELECT task
        FROM get_user_xp_for_task()
      )
  )
SELECT *
FROM (
    (
      SELECT *
      FROM task_1
    )
    INTERSECT
    (
      SELECT *
      FROM task_2
    )
    INTERSECT
    (
      SELECT *
      FROM task_3
    )
  ) AS new_table;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.11 -----------------------------------------

--BEGIN;
--CALL successful_and_not('C2_s21_string+', 'C4_s21_decimal', 'D01_Linux', 'ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.12 -----------------------------------------

CREATE OR REPLACE PROCEDURE previous_tasks(in ref refcursor) AS $$ BEGIN OPEN REF FOR WITH RECURSIVE recursion AS (
    SELECT 'C2_s21_string+'::VARCHAR AS task_name,
      0::BIGINT AS PrevCount
    UNION
    SELECT name,
      recursion.PrevCount + 1
    FROM recursion,
      Tasks
    WHERE entry_condition = recursion.task_name
      AND PrevCount < (
        SELECT count(*)
        FROM Tasks
      )
  )
SELECT *
FROM recursion;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.12 -----------------------------------------

--BEGIN;
--CALL previous_tasks('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.13 -----------------------------------------

create or replace procedure lucky_day(in n int, IN REF refcursor)
as $$
    begin
        open ref for
                        with t as (select *
                       from checks
                       join p2p on checks.id = p2p.check_id 
                       left join verter on checks.id = verter.check_id
                       join tasks on checks.task_name = tasks.name
                       join xp on checks.id = xp.check_id
                       where p2p.p2p_check_status = 'Success' and (verter.verter_check_status = 'Success' or verter.verter_check_status is null))
        select check_date
        from t
        where t.num_xp_received >= t.max_xp * 0.8
        group by check_date
        having count(check_date) >= n;
    end;
$$ language plpgsql;

----------------------------------------- TEST 3.13 -----------------------------------------

--BEGIN;
--CALL lucky_day(3, 'ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.14 -----------------------------------------

CREATE OR REPLACE PROCEDURE max_peer_xp(IN ref refcursor) AS $$ BEGIN OPEN REF FOR
SELECT checks.peer_nickname,
  SUM(num_xp_received) AS XP
FROM xp
  JOIN checks ON xp.check_id = checks.id
GROUP BY checks.peer_nickname
ORDER BY XP DESC
LIMIT 1;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.14 -----------------------------------------

--BEGIN;
--CALL max_peer_xp('ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.15 -----------------------------------------

CREATE OR REPLACE PROCEDURE peer_came_early(IN "time" time, IN n integer, IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH tmp AS (
    SELECT TT.peer_nickname,
      count(*)
    FROM TimeTracking AS TT
    WHERE TT.state = 1
      AND TT.time <= peer_came_early."time"
    GROUP BY TT.peer_nickname
    HAVING count(*) >= n
  )
SELECT peer_nickname
FROM tmp;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.15 -----------------------------------------

--BEGIN;
--CALL peer_came_early('13:38:00', 2, 'ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.16 -----------------------------------------

CREATE OR REPLACE PROCEDURE peers_leaving_campus(N int, M int, IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH all_out AS (
    SELECT * FROM timetracking
    WHERE state = 2 and date >= (now() - (N - 1 || ' days')::interval)::date
    AND date <= now()::date)
SELECT peer_nickname AS "Peer list" FROM all_out
GROUP BY peer_nickname
HAVING count(state) > M;
END
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.17 -----------------------------------------

--BEGIN;
--CALL peers_leaving_campus(600, 1, 'ref');
--FETCH ALL IN "ref";
--END;

----------------------------------------- PART 3.17 -----------------------------------------

CREATE OR REPLACE PROCEDURE percentage_of_early_entries(IN REF refcursor) AS $$ BEGIN OPEN REF FOR WITH months AS (
    SELECT date '2000-01-01' + interval '1' month * s.a as date
    FROM generate_series(0, 11) AS s(a)
  ),
  person_in as (
    SELECT TT.peer_nickname,
      TT.date,
      TT.time
    FROM TimeTracking TT
    where state = 1
  )
SELECT to_char(m.date, 'Month') AS Month,
  (
    CASE
      WHEN count(peer_nickname) != 0 THEN (
        (
          count(peer_nickname) FILTER (
            WHERE time < '12:00:00'
          ) / count(peer_nickname)::float
        ) * 100
      )::int
      ELSE 0
    END
  ) AS EarlyEntries
FROM months m
  LEFT JOIN peers ON to_char(m.date, 'Month') = to_char(birthday, 'Month')
  LEFT JOIN person_in pi ON peers.nickname = pi.peer_nickname
GROUP BY m.date
ORDER BY m.date;
END;
$$ LANGUAGE plpgsql;

----------------------------------------- TEST 3.17 -----------------------------------------

--BEGIN;
--CALL percentage_of_early_entries('ref');
--FETCH ALL IN "ref";
--END;
