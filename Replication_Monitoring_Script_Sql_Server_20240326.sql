-------------------------------------------------------------------------------------------------------------------------
--  PUBLISHER Status SCRIPT
-------------------------------------------------------------------------------------------------------------------------

USE [distribution]
GO

IF OBJECT_ID ('tempdb..#tmp_replicationPub_monitordata') IS NOT NULL DROP TABLE #tmp_replicationPub_monitordata;

CREATE TABLE #tmp_replicationPub_monitordata (
    publisher_db             sysname
  , publication              sysname
  , publication_id           INT
  , publication_type         INT
  , status                   INT        -- publication status defined as max(status) among all agents
  , warning                  INT        -- publication warning defined as max(isnull(warning,0)) among all agents
  , worst_latency            INT
  , best_latency             INT
  , average_latency          INT
  , last_distsync            DATETIME   -- last sync time
  , retention                INT        -- retention period                                               
  , latencythreshold         INT
  , expirationthreshold      INT
  , agentnotrunningthreshold INT
  , subscriptioncount        INT        -- # of subscription
  , runningdistagentcount    INT        -- # of running agents
  , snapshot_agentname       sysname NULL
  , logreader_agentname      sysname NULL
  , qreader_agentname        sysname NULL
  , worst_runspeedPerf       INT
  , best_runspeedPerf        INT
  , average_runspeedPerf     INT
  , retention_period_unit    TINYINT
  , publisher                sysname NULL
);

INSERT INTO #tmp_replicationPub_monitordata
EXEC sp_replmonitorhelppublication;

SELECT      (CASE WHEN status = '1' THEN 'Start - ' + CAST(status AS VARCHAR)
                 WHEN status = '2' THEN 'Succeed - ' + CAST(status AS VARCHAR)
                 WHEN status = '3' THEN 'InProgress - ' + CAST(status AS VARCHAR)
                 WHEN status = '4' THEN 'Idle - ' + CAST(status AS VARCHAR)
                 WHEN status = '5' THEN 'Retry - ' + CAST(status AS VARCHAR)
                 WHEN status = '6' THEN 'Fail - ' + CAST(status AS VARCHAR)ELSE CAST(status AS VARCHAR)END)  [Run Status]
          , publisher_db
          , publication
          , publication_id
          , (CASE WHEN publication_type = '0' THEN 'Transactional - ' + CAST(publication_type AS VARCHAR)
                 WHEN publication_type = '1' THEN 'Snapshot - ' + CAST(publication_type AS VARCHAR)
                 WHEN publication_type = '2' THEN 'Merge - ' + CAST(publication_type AS VARCHAR)ELSE '' END) AS [Publication Type]
          , (CASE WHEN warning = '1' THEN 'Expiration' + CAST(warning AS VARCHAR)
                 WHEN warning = '2' THEN 'Latency' + CAST(warning AS VARCHAR)
                 WHEN warning = '4' THEN 'Mergeexpiration' + CAST(warning AS VARCHAR)
                 WHEN warning = '16' THEN 'Mergeslowrunduration' + CAST(warning AS VARCHAR)
                 WHEN warning = '32' THEN 'Mergefastrunspeed' + CAST(warning AS VARCHAR)
                 WHEN warning = '64' THEN 'Mergeslowrunspeed' + CAST(warning AS VARCHAR)END)                 warning
          , worst_latency
          , best_latency
          , average_latency
          , last_distsync
          , retention
          , latencythreshold
          , expirationthreshold
          , agentnotrunningthreshold
          , subscriptioncount
          , runningdistagentcount
          , snapshot_agentname
          , logreader_agentname
          , qreader_agentname
          , worst_runspeedPerf
          , best_runspeedPerf
          , average_runspeedPerf
          , retention_period_unit
          , publisher
FROM        #tmp_replicationPub_monitordata
ORDER BY    publication;

-------------------------------------------------------------------------------------------------------------------------
--  SUBSCRIBER Status SCRIPT
-------------------------------------------------------------------------------------------------------------------------


IF OBJECT_ID ('tempdb..#tmp_rep_monitordata  ') IS NOT NULL DROP TABLE #tmp_rep_monitordata;

CREATE TABLE #tmp_rep_monitordata (
    status                        INT        NULL
  , warning                       INT        NULL
  , subscriber                    sysname    NULL
  , subscriber_db                 sysname    NULL
  , publisher_db                  sysname    NULL
  , publication                   sysname    NULL
  , publication_type              INT        NULL
  , subtype                       INT        NULL
  , latency                       INT        NULL
  , latencythreshold              INT        NULL
  , agentnotrunning               INT        NULL
  , agentnotrunningthreshold      INT        NULL
  , timetoexpiration              INT        NULL
  , expirationthreshold           INT        NULL
  , last_distsync                 DATETIME   NULL
  , distribution_agentname        sysname    NULL
  , mergeagentname                sysname    NULL
  , mergesubscriptionfriendlyname sysname    NULL
  , mergeagentlocation            sysname    NULL
  , mergeconnectiontype           INT        NULL
  , mergePerformance              INT        NULL
  , mergerunspeed                 FLOAT      NULL
  , mergerunduration              INT        NULL
  , monitorranking                INT        NULL
  , distributionagentjobid        BINARY(16) NULL
  , mergeagentjobid               BINARY(16) NULL
  , distributionagentid           INT        NULL
  , distributionagentprofileid    INT        NULL
  , mergeagentid                  INT        NULL
  , mergeagentprofileid           INT        NULL
  , logreaderagentname            sysname    NULL
  , publisher                     sysname    NULL
);

INSERT INTO #tmp_rep_monitordata
EXEC sp_replmonitorhelpsubscription @publication_type = 0;

INSERT INTO #tmp_rep_monitordata
EXEC sp_replmonitorhelpsubscription @publication_type = 1;

SELECT      (CASE WHEN status = '1' THEN 'Start - ' + CAST(status AS VARCHAR)
                 WHEN status = '2' THEN 'Succeed - ' + CAST(status AS VARCHAR)
                 WHEN status = '3' THEN 'InProgress - ' + CAST(status AS VARCHAR)
                 WHEN status = '4' THEN 'Idle - ' + CAST(status AS VARCHAR)
                 WHEN status = '5' THEN 'Retry - ' + CAST(status AS VARCHAR)
                 WHEN status = '6' THEN 'Fail - ' + CAST(status AS VARCHAR)ELSE CAST(status AS VARCHAR)END)  [Run Status]
          , publisher_db
          , publication
          , (CASE WHEN warning = '1' THEN 'Expiration' + CAST(warning AS VARCHAR)
                 WHEN warning = '2' THEN 'Latency' + CAST(warning AS VARCHAR)
                 WHEN warning = '4' THEN 'Mergeexpiration' + CAST(warning AS VARCHAR)
                 WHEN warning = '16' THEN 'Mergeslowrunduration' + CAST(warning AS VARCHAR)
                 WHEN warning = '32' THEN 'Mergefastrunspeed' + CAST(warning AS VARCHAR)
                 WHEN warning = '64' THEN 'Mergeslowrunspeed' + CAST(warning AS VARCHAR)END)                 warning
          , subscriber
          , subscriber_db
          , (CASE WHEN publication_type = '0' THEN 'Transactional - ' + CAST(publication_type AS VARCHAR)
                 WHEN publication_type = '1' THEN 'Snapshot - ' + CAST(publication_type AS VARCHAR)
                 WHEN publication_type = '2' THEN 'Merge - ' + CAST(publication_type AS VARCHAR)ELSE '' END) AS [Publication Type]
          , (CASE WHEN subtype = '0' THEN 'Push - ' + CAST(subtype AS VARCHAR)
                 WHEN subtype = '1' THEN 'Pull - ' + CAST(subtype AS VARCHAR)
                 WHEN subtype = '2' THEN 'Anonymous - ' + CAST(subtype AS VARCHAR)ELSE '' END)               AS SubscriptionType
          , latency
          , latencythreshold
          , agentnotrunning
          , agentnotrunningthreshold
          , last_distsync
          , timetoexpiration
          , expirationthreshold
          , distribution_agentname
          , monitorranking
          , distributionagentjobid
          , mergeagentjobid
          , distributionagentid
          , distributionagentprofileid
          , mergeagentid
          , mergeagentprofileid
          , logreaderagentname
          , publisher
FROM        #tmp_rep_monitordata
ORDER BY    publication ASC;