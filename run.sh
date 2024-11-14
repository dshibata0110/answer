.mode csv

CREATE TABLE access_log (
    ID INTEGER PRIMARY KEY,
    IP_ADDRESS TEXT,
    REQUEST_TIME TEXT
);
CREATE TABLE bot_ip_address (
    ID INTEGER PRIMARY KEY,
    BOT_IP_ADDRESS TEXT
);

-- CSVファイルをインポート
-- accesslog_01.csvをaccess_logテーブルにインポート
.import data/accesslog_01.csv access_log
-- botlist_01.csvをbot_ip_addressテーブルにインポート
.import data/botlist_01.csv bot_ip_address
-- 集計クエリの実行
WITH
R1
as
(
SELECT
ID
,IP_ADDRESS
,REQUEST_TIME
,LEFT(REQUEST_TIME,8) as REQUEST_DAY
FROM
data/accesslog_01.csv as t1
LEFT JOIN
(
SELECT
ID
,BOT_IP_ADDRESS
,'1' as BOTflag
FROM
data/botlist_01.csv
) as T2
ON
T1.IP_ADDRESS = T2.BOT_IP_ADDRESS
)
,R2
as
(
SELECT
R1.REQUEST_DAY
,count(*) as TOTAL_access
,sum(BOTflag) as BOT_access
FROM
R1
GROUP BY
R1.REQUEST_DAY
)
.output data/accesslog_01.answer.csv
.mode csv
.headers off
SELECT
R1.REQUEST_DAY
,R2.TOTAL_access - R2.BOT_access as NORMAL_access
FROM
R2
WHERE
R2.BOT_access/R2.TOTAL_access >=0.5
ORDER BY
R1.REQUEST_DAY
;
.quit
