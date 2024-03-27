
-------------------------------------------------------------------------------------------------------------------------
--  PUBLISHER Status SCRIPT
-------------------------------------------------------------------------------------------------------------------------

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
EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublication];




INSERT INTO [DBA].[dbo].[PublicationInfo] (
[Run Status],[publisher_db],[publication],[publication_id],[Publication Type],[warning],[worst_latency],[best_latency],[average_latency],[last_distsync],
[retention],[latencythreshold],[expirationthreshold],[agentnotrunningthreshold],[subscriptioncount],[runningdistagentcount],[snapshot_agentname],
[logreader_agentname],[qreader_agentname],[worst_runspeedPerf],[best_runspeedPerf],[average_runspeedPerf],[retention_period_unit],[publisher]
)

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
EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 0;

INSERT INTO #tmp_rep_monitordata
EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 1;



INSERT INTO [DBA].[dbo].[SubscriptionsInfo](
[Run Status],[publisher_db],[publication],[warning],[subscriber],[subscriber_db],[Publication Type],[SubscriptionType],[latency],[latencythreshold],
[agentnotrunning],[agentnotrunningthreshold],[last_distsync],[timetoexpiration],[expirationthreshold],[distribution_agentname],[monitorranking],
[distributionagentjobid],[mergeagentjobid],[distributionagentid],[distributionagentprofileid],[mergeagentid],[mergeagentprofileid],[logreaderagentname],[publisher]
)

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




-------------------------------------------------------------------------------------------------------------------------
--  Publisher Status SCRIPT
-------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID ('tempdb..#tmp_publisher_monitordata') IS NOT NULL DROP TABLE #tmp_publisher_monitordata;

CREATE TABLE #tmp_publisher_monitordata (
    publisher             sysname
  , distribution_db       varchar(64)
  , status                   INT        
  , warning                  INT
  , publicationcount	         INT
  , returnstamp               varchar(64)
);

INSERT INTO #tmp_publisher_monitordata
EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublisher];


INSERT INTO [DBA].[dbo].[PublisherInfo] (
[publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]      
)
SELECT [publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]
FROM #tmp_publisher_monitordata;





-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================




USE [master]
GO

CREATE DATABASE [DBA]
GO

USE [DBA]
GO


CREATE TABLE [dbo].[PublicationInfo](
	[Run Status] [varchar](43) NULL,
	[publisher_db] [sysname] NOT NULL,
	[publication] [sysname] NOT NULL,
	[publication_id] [int] NULL,
	[Publication Type] [varchar](46) NULL,
	[warning] [varchar](50) NULL,
	[worst_latency] [int] NULL,
	[best_latency] [int] NULL,
	[average_latency] [int] NULL,
	[last_distsync] [datetime] NULL,
	[retention] [int] NULL,
	[latencythreshold] [int] NULL,
	[expirationthreshold] [int] NULL,
	[agentnotrunningthreshold] [int] NULL,
	[subscriptioncount] [int] NULL,
	[runningdistagentcount] [int] NULL,
	[snapshot_agentname] [sysname] NULL,
	[logreader_agentname] [sysname] NULL,
	[qreader_agentname] [sysname] NULL,
	[worst_runspeedPerf] [int] NULL,
	[best_runspeedPerf] [int] NULL,
	[average_runspeedPerf] [int] NULL,
	[retention_period_unit] [tinyint] NULL,
	[publisher] [sysname] NULL,
	[LastSyncTime] [datetime] NOT NULL,
	[EntryDate]  AS (CONVERT([date],[LastSyncTime]))
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[PublisherInfo](
	[publisher] [sysname] NOT NULL,
	[distribution_db] [varchar](64) NULL,
	[status] [int] NULL,
	[warning] [int] NULL,
	[publicationcount] [int] NULL,
	[returnstamp] [varchar](64) NULL,
	[LastSyncTime] [datetime] NOT NULL,
	[EntryDate]  AS (CONVERT([date],[LastSyncTime]))
) ON [PRIMARY]
GO


CREATE TABLE [dbo].[SubscriptionsInfo](
	[Run Status] [varchar](43) NULL,
	[publisher_db] [sysname] NULL,
	[publication] [sysname] NULL,
	[warning] [varchar](50) NULL,
	[subscriber] [sysname] NULL,
	[subscriber_db] [sysname] NULL,
	[Publication Type] [varchar](46) NULL,
	[SubscriptionType] [varchar](42) NULL,
	[latency] [int] NULL,
	[latencythreshold] [int] NULL,
	[agentnotrunning] [int] NULL,
	[agentnotrunningthreshold] [int] NULL,
	[last_distsync] [datetime] NULL,
	[timetoexpiration] [int] NULL,
	[expirationthreshold] [int] NULL,
	[distribution_agentname] [sysname] NULL,
	[monitorranking] [int] NULL,
	[distributionagentjobid] [binary](16) NULL,
	[mergeagentjobid] [binary](16) NULL,
	[distributionagentid] [int] NULL,
	[distributionagentprofileid] [int] NULL,
	[mergeagentid] [int] NULL,
	[mergeagentprofileid] [int] NULL,
	[logreaderagentname] [sysname] NULL,
	[publisher] [sysname] NULL,
	[LastSyncTime] [datetime] NOT NULL,
	[EntryDate]  AS (CONVERT([date],[LastSyncTime]))
) ON [PRIMARY]
GO


ALTER TABLE [dbo].[PublicationInfo] ADD  CONSTRAINT [DF_PublicationInfo_LastSyncTime]  DEFAULT (getdate()) FOR [LastSyncTime]
GO
ALTER TABLE [dbo].[PublisherInfo] ADD  CONSTRAINT [DF_PublisherInfo_LastSyncTime]  DEFAULT (getdate()) FOR [LastSyncTime]
GO
ALTER TABLE [dbo].[SubscriptionsInfo] ADD  CONSTRAINT [DF_SubscriptionsInfo_LastSyncTime]  DEFAULT (getdate()) FOR [LastSyncTime]
GO



CREATE PROCEDURE [dbo].[AutomateMonitoringOfReplication]
AS
BEGIN
	-------------------------------------------------------------------------------------------------------------------------
	--  PUBLISHER Status SCRIPT
	-------------------------------------------------------------------------------------------------------------------------

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
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublication];




	INSERT INTO [DBA].[dbo].[PublicationInfo] (
	[Run Status],[publisher_db],[publication],[publication_id],[Publication Type],[warning],[worst_latency],[best_latency],[average_latency],[last_distsync],
	[retention],[latencythreshold],[expirationthreshold],[agentnotrunningthreshold],[subscriptioncount],[runningdistagentcount],[snapshot_agentname],
	[logreader_agentname],[qreader_agentname],[worst_runspeedPerf],[best_runspeedPerf],[average_runspeedPerf],[retention_period_unit],[publisher]
	)

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
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 0;

	INSERT INTO #tmp_rep_monitordata
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 1;



	INSERT INTO [DBA].[dbo].[SubscriptionsInfo](
	[Run Status],[publisher_db],[publication],[warning],[subscriber],[subscriber_db],[Publication Type],[SubscriptionType],[latency],[latencythreshold],
	[agentnotrunning],[agentnotrunningthreshold],[last_distsync],[timetoexpiration],[expirationthreshold],[distribution_agentname],[monitorranking],
	[distributionagentjobid],[mergeagentjobid],[distributionagentid],[distributionagentprofileid],[mergeagentid],[mergeagentprofileid],[logreaderagentname],[publisher]
	)

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




	-------------------------------------------------------------------------------------------------------------------------
	--  Publisher Status SCRIPT
	-------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID ('tempdb..#tmp_publisher_monitordata') IS NOT NULL DROP TABLE #tmp_publisher_monitordata;

	CREATE TABLE #tmp_publisher_monitordata (
		publisher             sysname
	  , distribution_db       varchar(64)
	  , status                   INT        
	  , warning                  INT
	  , publicationcount	         INT
	  , returnstamp               varchar(64)
	);

	INSERT INTO #tmp_publisher_monitordata
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublisher];


	INSERT INTO [DBA].[dbo].[PublisherInfo] (
	[publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]      
	)
	SELECT [publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]
	FROM #tmp_publisher_monitordata;

END
GO




-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================

-- ==========================================================================================================================================================================================
-- ==========================================================================================================================================================================================



CREATE PROCEDURE [dbo].[AutomateMonitoringOfReplication]
AS
BEGIN
	-------------------------------------------------------------------------------------------------------------------------
	--  PUBLISHER Status SCRIPT
	-------------------------------------------------------------------------------------------------------------------------

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
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublication];




	INSERT INTO [DBA].[dbo].[PublicationInfo] (
	[Run Status],[publisher_db],[publication],[publication_id],[Publication Type],[warning],[worst_latency],[best_latency],[average_latency],[last_distsync],
	[retention],[latencythreshold],[expirationthreshold],[agentnotrunningthreshold],[subscriptioncount],[runningdistagentcount],[snapshot_agentname],
	[logreader_agentname],[qreader_agentname],[worst_runspeedPerf],[best_runspeedPerf],[average_runspeedPerf],[retention_period_unit],[publisher]
	)

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
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 0;

	INSERT INTO #tmp_rep_monitordata
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelpsubscription] @publication_type = 1;



	INSERT INTO [DBA].[dbo].[SubscriptionsInfo](
	[Run Status],[publisher_db],[publication],[warning],[subscriber],[subscriber_db],[Publication Type],[SubscriptionType],[latency],[latencythreshold],
	[agentnotrunning],[agentnotrunningthreshold],[last_distsync],[timetoexpiration],[expirationthreshold],[distribution_agentname],[monitorranking],
	[distributionagentjobid],[mergeagentjobid],[distributionagentid],[distributionagentprofileid],[mergeagentid],[mergeagentprofileid],[logreaderagentname],[publisher]
	)

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




	-------------------------------------------------------------------------------------------------------------------------
	--  Publisher Status SCRIPT
	-------------------------------------------------------------------------------------------------------------------------

	IF OBJECT_ID ('tempdb..#tmp_publisher_monitordata') IS NOT NULL DROP TABLE #tmp_publisher_monitordata;

	CREATE TABLE #tmp_publisher_monitordata (
		publisher             sysname
	  , distribution_db       varchar(64)
	  , status                   INT        
	  , warning                  INT
	  , publicationcount	         INT
	  , returnstamp               varchar(64)
	);

	INSERT INTO #tmp_publisher_monitordata
	EXEC [SelfPointing].[distribution].[dbo].[sp_replmonitorhelppublisher];


	INSERT INTO [DBA].[dbo].[PublisherInfo] (
	[publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]      
	)
	SELECT [publisher],[distribution_db],[status],[warning],[publicationcount],[returnstamp]
	FROM #tmp_publisher_monitordata;

END