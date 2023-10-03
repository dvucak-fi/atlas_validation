
DECLARE @DWUpdatedDatetime DATETIME = GETDATE()		
DECLARE @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
DECLARE @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()



IF OBJECT_ID ('TEMPDB..#DimTerminationType') IS NOT NULL DROP TABLE #DimTerminationType
IF OBJECT_ID ('TEMPDB..#FactClientTermination') IS NOT NULL DROP TABLE #FactClientTermination

CREATE TABLE #FactClientTermination
(
	[DimDateKey] [int] NOT NULL,
	[DimClientKey] [int] NOT NULL,
	[DimClientAssetsKey] [int] NOT NULL,
	[DimClientTenureKey] [int] NOT NULL,
	[DimAgeGroupKey] [int] NOT NULL,
	[DimTeamMemberKey] [int] NOT NULL,
	[DimTeamMemberKey30DLag] [int] NOT NULL,
	[DimEmployeeKey] [int] NOT NULL,
	[DimEmployeeKey30DLag] [int] NOT NULL,
	[DimEmployeeMgmtKey] [int] NOT NULL,
	[DimEmployeeMgmtKey30DLag] [int] NOT NULL,
	[DimPeerGroupKey] [int] NOT NULL,
	[DimPeerGroupKey30DLag] [int] NOT NULL,
	[DimTerminationThreatDateKey] [int] NOT NULL,
	[DimTerminationSaveDateKey] [int] NOT NULL,
	[DimTerminationDateKey] [int] NOT NULL,
	[DimTermSurveyKey] [int] NOT NULL,
	[DimTerminationRequestTypekey] [int] NOT NULL,
	[DimTerminationTypeKey] [int] NOT NULL,
	[CatchAllTerminationCount] [int] NULL,
	[TerminationThreatCount] [int] NULL,
	[TerminationSaveCount] [int] NULL,
	[TerminationCount] [int] NULL,
	[TerminationAmount] [decimal](15, 2) NULL,
	[TerminationSurveyAmount] [decimal](15, 2) NULL,
	[CompletedEvent] [int] NULL,
	[CaseNumber] [nvarchar](30) NULL,
	[DWCreatedDateTime] [datetime] NULL,
	[DWUpdatedDateTime] [datetime] NULL,
	[ETLJobProcessRunId] [uniqueidentifier] NULL,
	[ETLJobSystemRunId] [uniqueidentifier] NULL
)
WITH
(
	DISTRIBUTION = HASH ( [DimClientKey] ),
	CLUSTERED COLUMNSTORE INDEX
)

CREATE TABLE #DimTerminationType (
        DimTerminationTypeKey INT 
      , TerminationType NVARCHAR(200)
	  , DWCreatedDateTime DATETIME 
	  , DWUpdatedDateTime DATETIME 
	  , ETLJobProcessRunId UNIQUEIDENTIFIER 
	  , ETLJobSystemRunId UNIQUEIDENTIFIER 
)
WITH
(
	HEAP
)


	;WITH TerminationTypes AS ( 
		
		SELECT DISTINCT
               CASE 
                WHEN TRIM(SUBSTRING(Termination_request_type__c, CHARINDEX ('-', Termination_request_type__c) + 1, LEN(Termination_request_type__c))) = 'Trading' 
                THEN 1
                ELSE 2
               END AS DimTerminationTypeKey
             , TRIM(SUBSTRING(Termination_request_type__c, CHARINDEX ('-', Termination_request_type__c) + 1, LEN(Termination_request_type__c))) AS TerminationType
  		  FROM PCGSF.[CASE] AS SRC
         WHERE Termination_request_type__c LIKE '%Trading%' --FILTER FOR TRADING TERMINATION CASES
            OR Termination_request_type__c LIKE '%Non-Trading%' --FILTER FOR NON-TRADING TERMINATION CASES

	)

	INSERT 
	  INTO #DimTerminationType (
		   [DimTerminationTypeKey]
         , [TerminationType] 
		 , [DWCreatedDateTime]
		 , [DWUpdatedDateTime]
		 , [ETLJobProcessRunId]
		 , [ETLJobSystemRunId]
	)

	SELECT SRC.DimTerminationTypeKey
         , SRC.TerminationType
		 , @DWUpdatedDatetime
		 , @DWUpdatedDatetime
		 , @ETLJobProcessRunId
		 , @ETLJobSystemRunId
      FROM TerminationTypes AS SRC


    INSERT INTO #DimTerminationType ([DimTerminationTypeKey], [TerminationType]) VALUES (-1, '[Unknown]')


/*
    START PROC UPSERT
*/


/*
==========================================================================================================================================================
 Author: Joe, Armando, Dado
 Modified Date & Modified By: 
	PDDTI-304, 4/6/23 by Rachel Platt
	PDDTI-524, 5/31/23 by Rachel Platt
	PDDTI-71, 5/31/23 by Rachel Platt
	PDDTI-562, 6/13/23 by Rachel Platt
	PDDTI-603, 07/17/2023 by Nadar Chandran
	PDDTI-799, 08-30-2023 by Joe Freiert
	PDDTI-1041, 09-29-2023 by Dado Vucak

 Description: Update #FactClientTermination with data from Iris and Salesforce
 Parameters:
   @ETLJobSystemRunId - ELT field passed during pipeline run
   @ETLJobProcessRunId - ELT field passed during pipeline run
   @ComponentName - Name of this component, for logging. Passed by ELT during pipeline run.
 Returns: 
   Status | ErrorMessage - return is required by ELT
 ===========================================================================================================================================================
*/

--CREATE PROC [FDW].[spUpsertFactClientTermination] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
--BEGIN

DECLARE @StartTime DATETIME
      , @EndTime DATETIME
      , @DurationInSeconds INT
      , @Source NVARCHAR(255)
      , @Target NVARCHAR(255)
      , @Status INT
      , @ErrorMessage NVARCHAR(512)

DECLARE @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
DECLARE @InsertCount INT
DECLARE @UpdateCount INT
       
	SET @DWUpdatedDatetime = GETDATE()
	SET @Status = 1

DECLARE @UnknownTextValue NVARCHAR(512)
       ,@UnknownTextValueAbbreviated NVARCHAR(10)
       ,@NotAvailableTextValue NVARCHAR(512)
       ,@NotAvailableTextValueAbbreviated NVARCHAR(10)
       ,@NotApplicableTextValue NVARCHAR(25)
       ,@UnknownNumberValue INT
       ,@MinDateValue DATE 
       ,@MaxDateValue DATE
       ,@UnknownGuid UNIQUEIDENTIFIER
       ,@DefaultNumberValue INT
       ,@DefaultMoneyValue MONEY

SELECT TOP 1 @UnknownNumberValue = CONVERT(INT,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownNumberValue' 

SELECT TOP 1 @DefaultNumberValue = CONVERT(INT,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'DefaultNumberValue'

SELECT TOP 1 @UnknownTextValue = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValue'

SELECT TOP 1 @UnknownTextValueAbbreviated = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValueAbbreviated'

SELECT TOP 1 @NotAvailableTextValue = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotAvailableTextValue'

SELECT TOP 1 @NotAvailableTextValueAbbreviated = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotAvailableTextValueAbbreviated'

SELECT TOP 1 @NotApplicableTextValue = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotApplicableTextValue'

SELECT TOP 1 @MinDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MinDateValue'

SELECT TOP 1 @MaxDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MaxDateValue'

SELECT TOP 1 @UnknownGuid = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownGuid'

SELECT TOP 1 @DefaultMoneyValue = CONVERT(MONEY,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'DefaultMoneyValue'


IF OBJECT_ID ('TEMPDB..#AllTerms') IS NOT NULL
    DROP TABLE #AllTerms

CREATE TABLE #AllTerms (
  DimClientKey   INT
, ClientNumber	 INT
, ClientId	     UNIQUEIDENTIFIER
, EventDate      DATE	
, TermThreat	 INT
, TermSave		 INT
, Term           INT
, TermThreatDate DATE
, SaveDate		 DATE
, TermDate       DATE
, CompletedEvent INT
, OldClientTypeName    nvarchar(100)
, CurClientTypeName    nvarchar(100)
)
WITH (DISTRIBUTION = HASH(ClientId),HEAP)

IF OBJECT_ID ('TEMPDB..#FactClientTermination_Temp') IS NOT NULL
    DROP TABLE #FactClientTermination_Temp

CREATE TABLE #FactClientTermination_Temp (
         ClientId								UNIQUEIDENTIFIER
	   , DimDateKey								INT
	   , DimClientKey							INT
	   , DimClientAssetsKey				        INT
	   , DimClientTenureKey				        INT
	   , DimAgeGroupKey							INT
	   , DimTeamMemberKey				        INT
	   , DimTeamMemberKey30DLag			        INT
	   , DimEmployeeKey							INT
	   , DimEmployeeKey30DLag			        INT
	   , DimEmployeeMgmtKey				        INT
	   , DimEmployeeMgmtKey30DLag		        INT
	   , DimPeerGroupKey				        INT
	   , DimPeerGroupKey30DLag			        INT
	   , CatchAllTerminationCount		        INT
	   , TerminationThreatCount			        INT
	   , TerminationSaveCount			        INT
	   , TerminationCount				        INT
       , DimTerminationThreatDateKey	        INT
	   , DimTerminationSaveDateKey		        INT
	   , DimTerminationDateKey			        INT 
	   , DimTermSurveyKey				        INT 
	   , DimTerminationRequestTypekey	        INT
	   , DimTerminationTypeKey					INT
	   , TerminationId					        UNIQUEIDENTIFIER
	   , TerminationAmount				        DECIMAL(15,2)
	   , TerminationSurveyAmount		        DECIMAL(15,2)
	   , CompletedEvent                         INT
	   , CaseNumber						        NVARCHAR(30)
	   , TerminationReason				        NVARCHAR(4000)
)
WITH (DISTRIBUTION = HASH(ClientId),HEAP)

--BEGIN TRY

--/* 
--	START Salesforce
--*/
 
--BEGIN TRANSACTION

	SET @Source = '[{"SourceTable":"PcgSf.Case"}]'
	SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
	SET @StartTime = GETDATE()

    -- "Unclose" any cases that are no longer in Closed status
	UPDATE TGT
		SET TGT.TerminationSaveCount = 0
			, TGT.TerminationCount = 0
			, TGT.DimTerminationSaveDateKey = @UnknownNumberValue
			, TGT.CompletedEvent = 0
			, TGT.DWUpdatedDateTime = @DWUpdatedDateTime
			, [ETLJobProcessRunId] = @ETLJobProcessRunId
			, [ETLJobSystemRunId] = @ETLJobSystemRunId 
	FROM #FactClientTermination AS TGT
	    INNER JOIN PcgSf.[Case] C
			ON TGT.[CaseNumber] = C.[CaseNumber]
	WHERE TGT.CompletedEvent = 1
		AND C.IsDeleted = 0
		AND C.[Type] = 'Client Termination'
		AND C.[Termination_request_type__c] IN (
			'Household - Trading', 'Household - Non-Trading'
			)
	    AND (
			(C.[Status] <> 'Closed') OR -- Case re-opened
			(C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Canceled' AND TGT.TerminationCount = 1) OR -- Case flipped from Term to Save
			(C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Completed Successfully' AND TGT.TerminationSaveCount = 1) -- Case flipped from Save to Term
			)
	--OPTION (Label = '#FactClientTerminationUnclose-Update-Query')
	--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTerminationUnclose-Update-Query', @UpdateCount OUT

	--SET @EndTime = GETDATE()
	--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--EXEC MDR.spProcessTaskLogUpdateRowCount
	--		  @ETLJobProcessRunId 
	--		, @ComponentName
	--		, @Source 
	--		, @Target 
	--		, @UpdateCount	 
	--		, @DurationInSeconds

	
	-- Find any new or incomplete cases in the ODS table
	;WITH SFDailyTerms AS (
	  SELECT C.[CaseNumber]
		  ,C.[AccountId]
		  ,EventDate = CASE WHEN C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Completed Successfully' THEN CAST(C.Termination_date__c as date)  
				WHEN C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Canceled' THEN  CONVERT(DATE,C.ClosedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
				ELSE CONVERT(DATE, C.[CreatedDate] AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
				END
		  ,CAST(C.[Termination_date__c] as date) AS TerminationDate
		  ,CONVERT(DATE, C.[CreatedDate] AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedDate
		  ,CONVERT(DATE,C.ClosedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS ClosedDate
		  ,TRY_CAST(C.[Legacy_ID__c] as int) AS CID -- Iris CID
		 -- ,CAST(C.LastModifiedDate  as datetime2) AS LastModifiedDate
		  ,EffectiveStatus = CASE WHEN C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Completed Successfully' THEN 'Term Done'
				WHEN C.[Status] = 'Closed' AND C.[Close_Reason__c] = 'Canceled' THEN 'Term Save'
				ELSE 'Term Threat'
				END
		  ,LEFT(Reason_for_Termination__c, 4000) AS TerminationReason
		  , C.Termination_request_type__c AS OldClientTypeName
	  FROM PcgSf.[Case] C
	  WHERE C.IsDeleted = 0
		AND C.[Type] = 'Client Termination'
		AND C.[Termination_request_type__c] IN (
			'Household - Trading', 'Household - Non-Trading'
			)
		AND NOT EXISTS (SELECT * FROM #FactClientTermination WHERE CompletedEvent = 1 AND [CaseNumber] = C.[CaseNumber])
	)
	,SFAccount AS (
		SELECT T.[CaseNumber]
			, AG.DimAgeGroupKey
			, A.[Id] AS HouseholdUID
		FROM SFDailyTerms T
			INNER JOIN [PcgSf].[Account] A
				ON T.[AccountId] = A.[Id]
			LEFT JOIN [FDW].[DimAgeGroup] AG
				ON FLOOR(DATEDIFF(Day, A.[Date_of_Birth__pc], T.[EventDate])/365.25) >= AG.[StartAge]
				AND FLOOR(DATEDIFF(Day, A.[Date_of_Birth__pc], T.[EventDate])/365.25) < AG.[EndAge]
		WHERE A.IsDeleted = 0
	)

,ClientAssets as (	 
	
SELECT 
  CASENUMBER
, DimClientAssetsKey
, DimTenureKey
, AUM_USD
 FROM ( 
	  SELECT 
		 CASENUMBER
		,CID
		, ROW_NUMBER() OVER (PARTITION BY CASENUMBER ORDER BY DATEDIFF(Day,CATK.CalendarDate,T.EventDate)) rownum
	    ,CATK.DimClientAssetsKey
		,CATK.DimTenureKey
		,CATK.AUM_USD
		FROM SFDailyTerms T
		LEFT JOIN STG.DailyClientAssetsandTenureKeys CATK ON T.CID = CATK.ClientNumber
			                                             AND  CATK.CalendarDate <= T.EventDate --Find latest snapshot on or before term date
) CA
WHERE ROWNUM = 1
)

, ClearanceWindows AS ( 

    SELECT REF.ClientId	
         , REF.ClientNumber	
         , REF.InitialAccountSetupDate AS ClearanceWindowStart
         , REF.ClearanceDate
         , LEAD (REF.InitialAccountSetupDate, 1, @MaxDateValue) OVER (PARTITION BY REF.ClientId ORDER BY REF.InitialAccountSetupDate) AS ClearanceWindowEnd
      FROM REF.ClientOnboarding AS REF
	  JOIN (SELECT DISTINCT CID FROM SFDailyTerms) AS DC 
	    ON REF.ClientNumber = DC.CID

) 

, ClearanceWinowTerms AS ( 

	SELECT T.CID  	    
	     , T.EventDate
		 , CASE WHEN CW.ClearanceDate IS NULL THEN 'Non-Trading' ELSE 'Trading' END AS TerminationType
		 , T.CaseNumber
	  FROM SFDailyTerms AS T
	  LEFT
	  JOIN ClearanceWindows AS CW
		ON T.CID = CW.ClientNumber	
		AND T.EventDate >= CW.ClearanceWindowStart	
		AND T.EventDate < CW.ClearanceWindowEnd		  
)

INSERT INTO #FactClientTermination_Temp (			
			 DimDateKey	
		   , DimClientKey
		   , DimClientAssetsKey				
		   , DimClientTenureKey				
		   , DimAgeGroupKey					
		   , DimTeamMemberKey				
		   , DimTeamMemberKey30DLag			
		   , DimEmployeeKey					
		   , DimEmployeeKey30DLag			
		   , DimEmployeeMgmtKey				
		   , DimEmployeeMgmtKey30DLag		
		   , DimPeerGroupKey				
		   , DimPeerGroupKey30DLag
		   , CatchAllTerminationCount
		   , TerminationThreatCount			
		   , TerminationSaveCount			
		   , TerminationCount		
		   , DimTerminationThreatDateKey	
		   , DimTerminationSaveDateKey		
		   , DimTerminationDateKey	
		   , DimTermSurveyKey
		   , DimTerminationRequestTypekey
		   , DimTerminationTypeKey
		   , TerminationAmount				
		   , TerminationSurveyAmount	
		   , CompletedEvent
		   , CaseNumber
		   , TerminationReason
	)
	SELECT COALESCE(D.DimDateKey, @UnknownNumberValue) AS DimDateKey
		, COALESCE(DC.DimClientKey,@UnknownNumberValue) AS DimClientKey
		, COALESCE(CATK.DimClientAssetsKey, @UnknownNumberValue) AS DimClientAssetsKey
		, COALESCE(CATK.DimTenureKey, @UnknownNumberValue) AS DimClientTenureKey
		, COALESCE(SFAC.DimAgeGroupKey, @UnknownNumberValue) AS DimAgeGroupKey
		, COALESCE(TM.DimTeamMemberKey ,@UnknownNumberValue) AS DimTeamMemberKey
		, COALESCE(TM30D.DimTeamMemberKey,@UnknownNumberValue) 	AS DimTeamMemberKey30DLag
		, COALESCE(E.DimEmployeeKey ,@UnknownNumberValue) AS DimEmployeeKey
		, COALESCE(E30D.DimEmployeeKey,@UnknownNumberValue) AS DimEmployeeKey30DLag
		, COALESCE(DEM.DimEmployeeMgmtKey ,@UnknownNumberValue) AS DimEmployeeMgmtKey
		, COALESCE(DEM30D.DimEmployeeMgmtKey ,@UnknownNumberValue) AS DimEmployeeMgmtKey30DLag
		, COALESCE(PGIC.DimPeerGroupKey ,@UnknownNumberValue) AS DimPeerGroupKey
		, COALESCE(PGIC30D.DimPeerGroupKey ,@UnknownNumberValue) AS DimPeerGroupKey30DLag
		, 1 AS CatchAllTerminationCount
		, TerminationThreatCount = CASE T.EffectiveStatus
				WHEN 'Term Threat' THEN 1
				ELSE 0
				END
		, TerminationSaveCount = CASE T.EffectiveStatus
				WHEN 'Term Save' THEN 1
				ELSE 0
				END
		, TerminationCount = CASE T.EffectiveStatus
				WHEN 'Term Done' THEN 1
				ELSE 0
				END
		, COALESCE(DCD.DimDateKey, @UnknownNumberValue) AS DimTerminationThreatDateKey
		, DimTerminationSaveDateKey	= CASE WHEN T.EffectiveStatus = 'Term Save' THEN COALESCE(DTSD.DimDateKey, @UnknownNumberValue) ELSE @UnknownNumberValue END
		, DimTerminationDateKey = CASE WHEN T.EffectiveStatus = 'Term Done' THEN COALESCE(DTRD.DimDateKey, @UnknownNumberValue) ELSE @UnknownNumberValue END	
		, COALESCE(DTS.DimTermSurveyKey, @UnknownNumberValue) AS DimTermSurveyKey
		, COALESCE(DTermStatus.DimTerminationRequestTypekey ,@UnknownNumberValue) AS DimTerminationRequestTypekey
	    , COALESCE(TT.DimTerminationTypeKey, @UnknownNumberValue) AS DimTerminationTypeKey		
		, CATK.AUM_USD AS TerminationAmount
		, NULL AS TerminationSurveyAmount
		, CompletedEvent = CASE WHEN T.[ClosedDate] IS NULL THEN 0 ELSE 1 END
		, T.[CaseNumber]
		, T.[TerminationReason]
	FROM SFDailyTerms T
		LEFT JOIN FDW.DimDate D
			ON D.CalendarDate = T.[EventDate]
		LEFT JOIN FDW.DimDate DCD --DimTerminationThreatDateKey
			ON DCD.CalendarDate =  CAST(T.[CreatedDate] as date)
		LEFT JOIN FDW.DimClient AS DC
			ON DC.ClientNumber = T.CID
			AND DC.EffectiveStartDate <= T.[CreatedDate]
			AND DC.EffectiveEndDate > T.[CreatedDate]
		LEFT JOIN SFAccount SFAC
			ON SFAC.[CaseNumber] = T.[CaseNumber]
	    LEFT JOIN ClientAssets CATK
			ON T.CaseNumber  = CATK.CaseNumber
		LEFT JOIN FDW.DimDate DTSD --DimTerminationSaveDateKey
			ON DTSD.CalendarDate =  CAST(T.[ClosedDate] as date)
		LEFT JOIN FDW.DimDate DTRD --DimTerminationDateKey
			ON DTRD.CalendarDate =  CAST(T.[TerminationDate] as date)
		LEFT JOIN [REF].[vwRelationshipManagementAssignmentWindow] AS RM
			ON RM.ClientNumber = T.CID
			AND T.[EventDate] >= RM.AssignmentWindowStartDate
			AND T.[EventDate] < RM.AssignmentWindowEndDate
		LEFT JOIN [REF].[vwRelationshipManagementAssignmentWindow] AS RM30D
			ON RM30D.ClientNumber = T.CID
			AND DATEADD(d,-30,T.[EventDate]) >= RM30D.AssignmentWindowStartDate 
			AND DATEADD(d,-30,T.[EventDate]) < RM30D.AssignmentWindowEndDate
		LEFT JOIN FDW.DimTeamMember AS TM
			ON RM.AssignedToGUID = TM.TeamMemberGUID
			AND T.[EventDate] >= TM.EffectiveStartDate 
			AND T.[EventDate] < TM.EffectiveEndDate
		LEFT JOIN FDW.DimTeamMember AS TM30D
			ON RM30D.AssignedToGUID = TM30D.TeamMemberGUID
			AND DATEADD(d,-30,T.[EventDate]) >= TM30D.EffectiveStartDate 
			AND DATEADD(d,-30,T.[EventDate]) < TM30D.EffectiveEndDate
		LEFT JOIN [FDW].[DimEmployee] AS E
			ON E.ActiveDirectoryUserIdWithDomain = TM.TeamMemberActiveDirectoryUserIdWithDomain
			AND T.[EventDate] >= E.EffectiveStartDate 
			AND T.[EventDate] < E.EffectiveEndDate
			AND E.TerminationRecord = 'No'
		LEFT JOIN [FDW].[DimEmployee] AS E30D
			ON E30D.ActiveDirectoryUserIdWithDomain = TM30D.TeamMemberActiveDirectoryUserIdWithDomain
			AND DATEADD(d,-30,T.[EventDate]) >= E30D.EffectiveStartDate 
			AND DATEADD(d,-30,T.[EventDate]) < E30D.EffectiveEndDate
			AND E30D.TerminationRecord = 'No'
		LEFT JOIN FDW.DimEmployeeMgmt AS DEM
			ON DEM.EmployeeID = E.EmployeeID
			AND T.[EventDate] >= DEM.EffectiveStartDate
			AND T.[EventDate] < DEM.EffectiveEndDate
		LEFT JOIN FDW.DimEmployeeMgmt AS DEM30D
			ON DEM30D.EmployeeID = E30D.EmployeeID
			AND DATEADD(d,-30,T.[EventDate]) >= DEM30D.EffectiveStartDate 
			AND DATEADD(d,-30,T.[EventDate])< DEM30D.EffectiveEndDate
		LEFT JOIN FDW.DimPeerGroupIC PGIC
			ON PGIC.TeamMemberSpecialty = TM.TeamMemberSpecialty
			AND PGIC.TenureInMonths = DATEDIFF(M, E.RoleStartDate, T.[EventDate])	
		LEFT JOIN FDW.DimPeerGroupIC PGIC30D
			ON PGIC30D.TeamMemberSpecialty = TM30D.TeamMemberSpecialty
			AND PGIC30D.TenureInMonths = DATEDIFF(M, E30D.RoleStartDate, T.[EventDate])
		LEFT OUTER JOIN FDW.DimTermSurvey DTS
			ON DTS.[CaseNumber] = T.[CaseNumber]
		LEFT JOIN [FDW].[DimTerminationRequestType] DTermStatus
		ON   T.OldClientTypeName  = DTermStatus.RequestName
		
		LEFT
		JOIN ClearanceWinowTerms AS CWT
		  ON T.CID = CWT.CID 
		 AND T.EventDate = CWT.EventDate	
		 AND T.CaseNumber = CWT.CaseNumber		 

		LEFT
		JOIN #DimTerminationType AS TT  
		  ON CWT.TerminationType = TT.TerminationType 		
		

	SET @Source = '[{"SourceTable":"#FactClientTermination_Temp"}]'
	SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
	SET @StartTime = GETDATE()

	--For Incomplete Events, update them if they have since be termed/saved.
	--LEAVING DIM TERMINATOIN TYPE KEY OUT OF UPDATE. WE WANT TO SEE THE TERMINATION TYPE AS OF THE INITIAL CASE DATE NOT WHEN SOMEONE COMPLETED THE CASE
	UPDATE TGT
		SET TGT.DimDateKey = SRC.DimDateKey
			, TGT.DimClientAssetsKey = SRC.DimClientAssetsKey
			, TGT.DimClientTenureKey = SRC.DimClientTenureKey
			, TGT.DimAgeGroupKey = SRC.DimAgeGroupKey
			, TGT.DimTeamMemberKey = SRC.DimTeamMemberKey
			, TGT.DimEmployeeKey = SRC.DimEmployeeKey
			, TGT.DimEmployeeMgmtKey = SRC.DimEmployeeMgmtKey
			, TGT.DimPeerGroupKey = SRC.DimPeerGroupKey
			, TGT.TerminationThreatCount = SRC.TerminationThreatCount
			, TGT.TerminationSaveCount = SRC.TerminationSaveCount
			, TGT.TerminationCount = SRC.TerminationCount
			, TGT.DimTerminationSaveDateKey = SRC.DimTerminationSaveDateKey
			, TGT.DimTerminationDateKey = SRC.DimTerminationDateKey	
			, TGT.DimTeamMemberKey30DLag = SRC.DimTeamMemberKey30DLag
			, TGT.DimEmployeeKey30DLag = SRC.DimEmployeeKey30DLag
			, TGT.DimEmployeeMgmtKey30DLag = SRC.DimEmployeeMgmtKey30DLag
			, TGT.DimPeerGroupKey30DLag = SRC.DimPeerGroupKey30DLag
			, TGT.DimTermSurveyKey = SRC.DimTermSurveyKey
			, TGT.DimTerminationRequestTypekey=SRC.DimTerminationRequestTypekey
			, TGT.TerminationAmount  = SRC.TerminationAmount
			, TGT.CompletedEvent = SRC.CompletedEvent
			, TGT.DWUpdatedDateTime = @DWUpdatedDateTime
			, [ETLJobProcessRunId] = @ETLJobProcessRunId
			, [ETLJobSystemRunId] = @ETLJobSystemRunId 
	 FROM #FactClientTermination AS TGT
		INNER JOIN #FactClientTermination_Temp AS SRC
			ON TGT.[CaseNumber] = SRC.[CaseNumber]

	--OPTION (Label = '#FactClientTerminationSF-Update-Query')
	--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTerminationSF-Update-Query', @UpdateCount OUT

	--SET @EndTime = GETDATE()
	--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--EXEC MDR.spProcessTaskLogUpdateRowCount
	--		  @ETLJobProcessRunId 
	--		, @ComponentName
	--		, @Source 
	--		, @Target 
	--		, @UpdateCount	 
	--		, @DurationInSeconds


	SET @Source = '[{"SourceTable":"#FactClientTermination_Temp"}]'
	SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
	SET @StartTime = GETDATE()

	-- Insert for new terminations not yet in the table
	INSERT INTO #FactClientTermination (		
		  DimDateKey            					
	    , DimClientKey
	    , DimClientAssetsKey				
	    , DimClientTenureKey				
	    , DimAgeGroupKey					
	    , DimTeamMemberKey				
	    , DimTeamMemberKey30DLag			
	    , DimEmployeeKey					
	    , DimEmployeeKey30DLag			
	    , DimEmployeeMgmtKey				
	    , DimEmployeeMgmtKey30DLag		
	    , DimPeerGroupKey				
	    , DimPeerGroupKey30DLag			
	    , CatchAllTerminationCount		
	    , TerminationThreatCount			
	    , TerminationSaveCount			
	    , TerminationCount				
            , DimTerminationThreatDateKey	
	    , DimTerminationSaveDateKey		
	    , DimTerminationDateKey	
	    , DimTermSurveyKey
	    , DimTerminationRequestTypekey
		, DimTerminationTypeKey
	    , TerminationAmount				
	    , TerminationSurveyAmount	
	    , CompletedEvent
	    , CaseNumber
	    , DWCreatedDateTime
	    , DWUpdatedDateTime
	    , ETLJobProcessRunId
	    , ETLJobSystemRunId
		)
	SELECT 	  Src.DimDateKey
			, Src.DimClientKey
			, Src.DimClientAssetsKey
			, Src.DimClientTenureKey
			, Src.DimAgeGroupKey
			, Src.DimTeamMemberKey
			, Src.DimTeamMemberKey30DLag
			, Src.DimEmployeeKey
			, Src.DimEmployeeKey30DLag
			, Src.DimEmployeeMgmtKey
			, Src.DimEmployeeMgmtKey30DLag
			, Src.DimPeerGroupKey
			, Src.DimPeerGroupKey30DLag
			, Src.CatchAllTerminationCount		
			, Src.TerminationThreatCount			
			, Src.TerminationSaveCount			
			, Src.TerminationCount
			, Src.DimTerminationThreatDateKey
			, Src.DimTerminationSaveDateKey
			, Src.DimTerminationDateKey
			, Src.DimTermSurveyKey
			, Src.DimTerminationRequestTypekey
			, Src.DimTerminationTypeKey
			, Src.TerminationAmount			
			, Src.TerminationSurveyAmount
			, Src.CompletedEvent
			, Src.CaseNumber
			, @DWUpdatedDateTime AS DWCreatedDateTime
			, @DWUpdatedDateTime AS DWUpdatedDateTime
			, @ETLJobProcessRunId AS ETLJobProcessRunId
			, @ETLJobSystemRunId AS ETLJobSystemRunId
		FROM #FactClientTermination_Temp AS Src
			LEFT JOIN #FactClientTermination AS TGT
				ON Src.CaseNumber = TGT.CaseNumber
		WHERE TGT.DimClientKey IS NULL

	--OPTION (Label = '#FactClientTerminationSF-Insert-Query')

	--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTerminationSF-Insert-Query', @InsertCount OUT

	--SET @EndTime = GETDATE()
	--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--EXEC MDR.spProcessTaskLogInsertRowCount
	--			@ETLJobProcessRunId 
	--		, @ComponentName
	--		, @Source 
	--		, @Target 
	--		, @InsertCount	 
	--		, @DurationInSeconds

	-- Insert termination reasons
	--DELETE DTSR
	--FROM [FDW].[BridgeDimTermSurveyDimTermSurveyReason] DTSR
	--	INNER JOIN FDW.DimTermSurvey TS
	--		ON DTSR.DimTermSurveyKey = TS.DimTermSurveyKey
	--WHERE EXISTS (SELECT * FROM #FactClientTermination_Temp WHERE [CaseNumber] = TS.[CaseNumber])

	--INSERT INTO [FDW].[BridgeDimTermSurveyDimTermSurveyReason]
 --          ([DimTermSurveyKey]
 --          ,[DimTermSurveyReasonKey]
 --          ,[CountOfReason]
 --          ,[DWCreatedDateTime]
 --          ,[DWUpdatedDateTime]
 --          ,[ETLJobProcessRunId]
 --          ,[ETLJobSystemRunId])
	-- SELECT CT.DimTermSurveyKey
	--	, SR.DimTermSurveyReasonKey
	--	, 1
	--	, @DWUpdatedDateTime
	--	, @DWUpdatedDateTime
	--	, @ETLJobProcessRunId
	--	, @ETLJobSystemRunId
	-- FROM #FactClientTermination_Temp CT
	--	 CROSS APPLY STRING_SPLIT(TerminationReason, ';')
	--	 LEFT JOIN FDW.DimTermSurveyReason SR
	--		ON SR.[TermSurveyReasonName] = value


--COMMIT TRANSACTION

/*
	End Salesforce
*/

	--TRUNCATE STAGING TABLE TO PREP FOR IRIS TERMS 
	TRUNCATE TABLE #FactClientTermination_Temp

/* 
	START Iris
*/

--BEGIN TRANSACTION 

 ;WITH ClientSetup as (

SELECT
  dc.DimClientKey
, dc.ClientNumber
, dc.ClientId	
, isnull(dc.ClientType, @UnknownTextValue) ClientTypeName
, isnull(LAG(DC.ClientType, 1, @UnknownTextValue) OVER (PARTITION BY dC.ClientId  ORDER BY DC.EffectiveStartDate),@UnknownTextValue)  AS OldClientTypeName
, isnull(dc.StrengthCode, @UnknownTextValue) StrengthCodeName
, isnull(LAG(DC.StrengthCode, 1, @UnknownTextValue) OVER (PARTITION BY dC.ClientId  ORDER BY DC.EffectiveStartDate),@UnknownTextValue) AS PreviousStrengthCodeName
, dc.DimStrengthCodeKey
, dc.EffectiveStartDate
, dc.EffectiveEndDate
from fdw.dimclient dc

)
--Filter for Events where client moved in/out of E or into Former Client
, ClientEvents as (
SELECT 
  DimClientKey
, ClientNumber
, ClientId	
, ClientTypeName	
, OldClientTypeName
, DimStrengthCodeKey
, EffectiveStartDate
, EffectiveEndDate	
, PreviousStrengthCodeName
, StrengthCodeName
,  CASE WHEN StrengthCodeName = 'E' AND PreviousStrengthCodeName <>'E' AND OldClientTypeName in ('Client - Trading','Deceased Client') 
	   THEN 1
	   ELSE 0
	   END TermThreat
, CASE WHEN StrengthCodeName <> 'E' AND PreviousStrengthCodeName = 'E' AND OldClientTypeName in ('Client - Trading','Deceased Client') AND ClientTypeName in ('Client - Trading','Deceased Client') 
	   THEN 1
	   ELSE 0
	   END TermSave
, CASE WHEN ClientTypeName = 'Former Client - Trading' AND OldClientTypeName in('Client - Trading','Deceased Client') 
			THEN 1
			ELSE 0
			END Term
FROM ClientSetup
WHERE 
((PreviousStrengthCodeName <> StrengthCodeName) AND (PreviousStrengthCodeName = 'E' OR StrengthCodeName = 'E'))
OR 
(OldClientTypeName IN ('Client - Trading','Deceased Client') AND ClientTypeName = 'Former Client - Trading')
UNION
SELECT 
  DimClientKey
, ClientNumber
, ClientId	
, ClientTypeName	
, OldClientTypeName
, DimStrengthCodeKey
, EffectiveStartDate
, EffectiveEndDate	
, PreviousStrengthCodeName
, StrengthCodeName
,  CASE WHEN StrengthCodeName = 'E' AND PreviousStrengthCodeName <>'E' AND OldClientTypeName in ('Client - Non Trading','Deceased Client') 
	   THEN 1
	   ELSE 0
	   END TermThreat
, CASE WHEN StrengthCodeName <> 'E' AND PreviousStrengthCodeName = 'E' AND OldClientTypeName in ('Client - Non Trading','Deceased Client') AND ClientTypeName in ('Client - Non Trading','Deceased Client') 
	   THEN 1
	   ELSE 0
	   END TermSave
, CASE WHEN ClientTypeName = 'Former Client - Non Trading' AND OldClientTypeName in('Client - Non Trading','Deceased Client') 
			THEN 1
			ELSE 0
			END Term
FROM ClientSetup
WHERE 
((PreviousStrengthCodeName <> StrengthCodeName) AND (PreviousStrengthCodeName = 'E' OR StrengthCodeName = 'E'))
OR 
(OldClientTypeName IN ('Client - Non Trading','Deceased Client') AND ClientTypeName = 'Former Client - Non Trading')

)
--Find Date of next Event
, EventFlags as (
SELECT 
DimClientKey
,ClientNumber	
,ClientId	
,ClientTypeName	
,OldClientTypeName
,DimStrengthCodeKey
,EffectiveStartDate	EventDate
,PreviousStrengthCodeName
,StrengthCodeName	
,TermThreat	
,TermSave	
,Term
, LEAD(EffectiveStartDate,1,@maxdatevalue) over (partition by ClientID ORDER BY EffectiveStartDate) NextEventDate
, termthreat+termsave+term EventCount
FROM CLIENTEVENTS
WHERE (termthreat+termsave+term) >0
)
--Records that Term Threat AND Term same day. These are complete events
, SameDayThreatTerms as (
SELECT 
DimClientKey
,ClientNumber	
,ClientId	
,ClientTypeName	
,OldClientTypeName
,DimStrengthCodeKey
, EventDate
,PreviousStrengthCodeName
,StrengthCodeName	
,TermThreat	
,TermSave	
,Term
, NextEventDate
, EventCount
, 1 CompletedEvent
, EventDate TermThreatDate
, NULL SaveDate
, EventDate TermDate
FROM EventFlags
WHERE TermThreat = 1 AND Term = 1
)
--Term threat but no term on the same day
,ThreatsOnly as (

SELECT 
DimClientKey
,ClientNumber	
,ClientId	
,ClientTypeName	
,OldClientTypeName
,DimStrengthCodeKey
,EventDate
,PreviousStrengthCodeName
,StrengthCodeName	
,TermThreat	
,TermSave	
, Term
, NextEventDate
, EventCount
FROM EventFlags
WHERE TermThreat = 1 AND Term = 0

)
--Find events that are either terms or saves. These are closing events
,NonThreats as (
SELECT 
DimClientKey
,ClientNumber	
,ClientId	
,ClientTypeName	
,OldClientTypeName
,DimStrengthCodeKey
,EventDate
,PreviousStrengthCodeName
,StrengthCodeName	
,TermThreat	
,CASE WHEN Term = 0 THEN EventFlags.TermSave ELSE 0 END TermSave --If "Saves" are on the same day as a term, only count the term	
, Term
, NextEventDate
, EventCount
FROM EventFlags
WHERE TermThreat = 0 AND (Term = 1 OR TermSave = 1)
)
-- Join Open Term Threats to the next Save/Term that follows them. This closes the event.
, CompleteTheThreat as (
SELECT
 T.DimClientKey
,T.ClientNumber	
,T.ClientId	
,T.ClientTypeName	
,T.OldClientTypeName
,T.DimStrengthCodeKey
,T.EventDate
,T.PreviousStrengthCodeName
,T.StrengthCodeName	
,T.TermThreat	
,CASE WHEN NT.TermSave IS NOT NULL THEN NT.TermSave ELSE T.TermSave	END TermSave
,CASE WHEN NT.Term IS NOT NULL THEN NT.Term ELSE T.Term END Term
,T.NextEventDate
,CASE WHEN NT.ClientID IS NOT NULL THEN 1 ELSE 0 END CompletedEvent
, T.EventDate TermThreatDate
, CASE WHEN NT.TermSave = 1 THEN NT.EventDate
	   ELSE NULL
	   END SaveDate
, CASE WHEN NT.Term = 1 THEN NT.EventDate
	   ELSE NULL
	   END TermDate
FROM ThreatsOnly T
LEFT JOIN NonThreats NT ON NT.ClientID = T.ClientID
					   AND NT.EventDate = T.NextEventDate
					   )
--Occasionally, clients will be moved directly into Term and not into E.
,TermsWithNoThreat as (
SELECT
 NT.DimClientKey
,NT.ClientNumber	
,NT.ClientId	
,NT.ClientTypeName	
,NT.OldClientTypeName
,NT.DimStrengthCodeKey
,NT.EventDate
,NT.PreviousStrengthCodeName
,NT.StrengthCodeName	
,NT.TermThreat	
,NT.TermSave
,NT.Term
,NT.NextEventDate
,NT.EventCount
, 1 as CompletedEvent
, NULL TermThreatDate
, NULL SaveDate
, NT.EventDate TermDate
FROM NonThreats NT
LEFT JOIN CompleteTheThreat CT ON CT.ClientID = NT.ClientID
					          AND NT.EventDate = CT.NextEventDate
WHERE NT.Term = 1 AND CT.ClientID IS NULL

)


--Union all events together 
 INSERT INTO #AllTerms (
  DimClientKey
, ClientNumber	
, ClientId	
, EventDate	
, TermThreat	
, TermSave	
, Term
, TermThreatDate
, SaveDate
, TermDate
, CompletedEvent
, OldClientTypeName    
, CurClientTypeName)

SELECT
  DimClientKey
, ClientNumber	
, ClientId	
, EventDate	
, TermThreat	
, TermSave	
, Term
, TermThreatDate
, SaveDate
, TermDate
, CompletedEvent
, OldClientTypeName
, ClientTypeName
FROM SameDayThreatTerms

UNION 

SELECT
  DimClientKey
, ClientNumber	
, ClientId	
, EventDate	
, TermThreat	
, TermSave	
, Term
, TermThreatDate
, SaveDate
, TermDate
, CompletedEvent
, OldClientTypeName
, ClientTypeName
FROM CompleteTheThreat

UNION 

SELECT
  DimClientKey
, ClientNumber	
, ClientId	
, EventDate	
, TermThreat	
, TermSave	
, Term
, TermThreatDate
, SaveDate
, TermDate
, CompletedEvent
, OldClientTypeName
, ClientTypeName
FROM TermsWithNoThreat



 --Get the Termination amount using DRR
; WITH AUMSnapshots AS(
			SELECT 
			  T.ClientID
			, HAA.AccountID
			, T.EventDate
			, T.TermThreatDate
			, T.TermDate
			, T.OldClientTypeName 
			, SaveDate
			, haum.AUM_USD AS AUMUSD
			, ROW_NUMBER() OVER (PARTITION BY  HAA.AccountID, EventDate ORDER BY DATEDIFF(D, haum.CalendarDate, T.EventDate)) ROWNum

		FROM #AllTerms T 
		INNER JOIN REF.HistoricalAccountAttributes HAA ON  T.ClientId = HAA.ClientId
		INNER JOIN Fin.account_info_ai AI  ON HAA.FinAccountNumber = AI.ai_advisor_acct_no   
		INNER JOIN FDW.DimFinancialAccount  DFA	ON DFA.FinAccountNumber = HAA.FinAccountNumber

			--use AUM reference table
		LEFT JOIN Ref.HistoricalAccountAUM haum ON haum.FinAccountNumber = HAA.FinAccountNumber

		WHERE
		    --Term Date is either in between the snapshots or 90 day before the term
			(
			haum.CalendarDate >= DATEADD(D,-90,EventDate)
			and haum.CalendarDate <= EventDate
			)

			AND T.EventDate  >= HAA.EffectiveStartDate AND EventDate < HAA.EffectiveEndDate
			AND AI.ai_initial_perf_date IS NOT NULL  --Account must have IPD	  	  
			AND AI.ai_selection_field4 IN ('I','P')  --Individual/Participant
			AND AI.ai_combining_acct_code <> 'C'  --Exclude combined accounts
			AND AI.ai_selection_field2 = '2' --PCG 
			AND ISNUMERIC(AI.ai_special_field_1) = 1 --Ensure value in SF1 is numeric
			AND EventDate >= DFA.AccountOpenDate
			AND COALESCE(DFA.AccountCloseDate , @TODAY)  >= DATEADD(D,-90,EventDate)
	)

, AUMTotal AS
	(
		SELECT
			  AUMS.ClientID
			, AUMS.EventDate
			, AUMS.TermThreatDate
			, AUMS.TermDate
			, AUMS.OldClientTypeName 
			, AUMS.SaveDate
			,SUM(AUMS.AUMUSD) TerminationAmount
		FROM AUMSnapshots AUMS
		WHERE AUMS.ROWNum = 1
		GROUP BY
			  AUMS.ClientID
			, AUMS.EventDate
			, AUMS.TermThreatDate
			, AUMS.TermDate
			, AUMS.SaveDate
			,AUMS.OldClientTypeName
	)

, TermSurvey AS (

SELECT
	  CB.ContactId						AS ClientId
	 , DD.DimDateKey
	 , DFA.DimFinancialAccountKey
	 , DCC.DimCurrencyKey
	 , TB.fi_terminationId               AS TerminationId				     
	 , CB.fi_Id						     AS ClientNumber
	 , AB.fi_financialaccountId		     AS AccountId
	 , AB.fi_Id						     AS AccountNumber 	 
	 , TB.fi_IsResignation               AS IsResignation     
	 , AB.fi_TerminationValue            AS TerminationAmount
    

  --Base Iris Accounts Object
  FROM Iris.fi_financialaccountBase AS AB

  -- Base Iris Terminations Object  
  JOIN Iris.fi_terminationBase AS TB
    ON AB.fi_TerminationId = TB.fi_terminationId

  -- Base Iris Contact Object 
  JOIN Iris.ContactBase AS CB
    ON TB.fi_ContactId = CB.ContactId

  -- Dim Client Lookup
  JOIN FDW.DimClient AS DC
    ON DC.ClientId = CB.ContactId
   AND TB.fi_TerminationDate >= DC.EffectiveStartDate 
   AND TB.fi_TerminationDate < DC.EffectiveEndDate

  -- Dim Financial Account Lookup
  JOIN FDW.DimFinancialAccount DFA
    ON AB.fi_Id	= DFA.AccountNumber

	  -- Dim Date Lookup
  JOIN FDW.DimDate AS DD
    ON CONVERT(DATE, TB.fi_TerminationDate)  = DD.CalendarDate

 -- Lookup Transaction Currency	 
  JOIN Iris.TransactionCurrencyBase AS TCB
	ON TCB.TransactionCurrencyId = TB.TransactionCurrencyId

  -- Dim Currency Lookup
  JOIN FDW.DimCurrency AS DCC
    ON TCB.ISOCurrencyCode = DCC.CurrencyCode

 WHERE TB.StatusCode = 2 --Approved
	AND TB.fi_TerminationTypeCode IN(157610000,157610002) --Client - Trading Termination, Client - Non Trading Termination

)
, TermSurveyConnected AS
(
	SELECT 
		  T.ClientID
		, T.EventDate
		, T.TermThreatDate
		, T.TermDate
		, T.SaveDate
		, T.OldClientTypeName 
		,Max(Terminationid) as Terminationid
		,SUM(TS.TerminationAmount/CE.ExchangeRate) TerminationSurveyAmount
	FROM #AllTerms T 
	INNER JOIN REF.HistoricalAccountAttributes HAA	ON  T.ClientId = HAA.ClientId

	INNER JOIN TermSurvey AS TS	ON HAA.AccountId = TS.AccountId


	INNER JOIN FDW.DimCurrency C ON TS.DimCurrencyKey = C.DimCurrencyKey

	INNER JOIN FDW.DimDate D ON D.DimDateKey = TS.DimDateKey

	INNER JOIN REF.CurrencyExchangeUSD CE ON C.CurrencyCode = CE.BaseCurrency
										 AND D.CalendarDate = CE.EffectiveDate

	WHERE D.CalendarDate >=  DATEADD(d,-90,T.EventDate) AND D.CalendarDate < DATEADD(d,360,T.EventDate)
	  AND T.EventDate  >=  HAA.EffectiveStartDate AND T.EventDate < HAA.EffectiveEndDate

	GROUP BY
		  T.ClientID
		, T.EventDate
		, T.TermThreatDate
		, T.TermDate
		, T.SaveDate
		, T.OldClientTypeName
	)


--Client Asset Queries--

, TermsWithAssets AS (
	
	SELECT 
		   T.DimClientKey
		 , T.ClientNumber	
		 , T.ClientId	
		 , T.EventDate
		 , T.TermThreat	
		 , T.TermSave	
		 , T.Term
		 , T.TermThreatDate
		 , T.SaveDate
		 , T.TermDate
		 , T.CompletedEvent
		 , T.OldClientTypeName
		 , T.CurClientTypeName

  FROM #AllTerms T
)

,ClientBDays as (

	SELECT
	  ContactId as ClientID
	, fi_Id_search AS ClientNumber
	, BirthDate as DateofBirth
	FROM [Iris].[ContactBase]
	WHERE BirthDate IS NOT NULL
)

, ClearanceWindows AS ( 

    SELECT REF.ClientId	
         , REF.ClientNumber	
         , REF.InitialAccountSetupDate AS ClearanceWindowStart
         , REF.ClearanceDate
         , LEAD (REF.InitialAccountSetupDate, 1, @MaxDateValue) OVER (PARTITION BY REF.ClientId ORDER BY REF.InitialAccountSetupDate) AS ClearanceWindowEnd
      FROM REF.ClientOnboarding AS REF
	  JOIN (SELECT DISTINCT ClientId FROM #AllTerms) AS DC 
	    ON REF.ClientId = DC.CLientId

) 

, ClearanceWinowTerms AS ( 

	SELECT T.ClientId  
	     , T.EventDate
		 , CASE WHEN CW.ClearanceDate IS NULL THEN 'Non-Trading' ELSE 'Trading' END AS TerminationType
	  FROM #AllTerms AS T
	  LEFT
	  JOIN ClearanceWindows AS CW
		ON T.ClientId = CW.ClientId	
		AND T.EventDate >= CW.ClearanceWindowStart	
		AND T.EventDate < CW.ClearanceWindowEnd		  
)

-- select 'INSERT INTO #FactClientTermination_Temp'
--Set the Clientkey, Employee/teammember/peer group keys for both the person within the assignment date and 30 days prior the assignment date
INSERT INTO #FactClientTermination_Temp (
					
		 						
	     DimDateKey						
	   , DimClientKey					
	   , DimClientAssetsKey				
	   , DimClientTenureKey				
	   , DimAgeGroupKey					
	   , DimTeamMemberKey				
	   , DimTeamMemberKey30DLag			
	   , DimEmployeeKey					
	   , DimEmployeeKey30DLag			
	   , DimEmployeeMgmtKey				
	   , DimEmployeeMgmtKey30DLag		
	   , DimPeerGroupKey				
	   , DimPeerGroupKey30DLag			
	   , CatchAllTerminationCount		
	   , TerminationThreatCount			
	   , TerminationSaveCount			
	   , TerminationCount				
       , DimTerminationThreatDateKey	
	   , DimTerminationSaveDateKey		
	   , DimTerminationDateKey	
	   , DimTermSurveyKey
	   , TerminationId
	   , TerminationAmount				
	   , TerminationSurveyAmount	
	   , CompletedEvent
	   ,DimTerminationRequestTypekey
	   , DimTerminationTypeKey
)
	SELECT
		  COALESCE(D.DimDateKey, @UnknownNumberValue)
		, COALESCE(T.DimClientKey,@UnknownNumberValue)
		, COALESCE(CATK.DimClientAssetsKey, @UnknownNumberValue) AS DimClientAssetsKey
		, COALESCE(CATK.DimTenureKey, @UnknownNumberValue) AS DimClientTenureKey
		, COALESCE(AG.DimAgeGroupKey, @UnknownNumberValue) AS DimAgeGroupKey
		, COALESCE(TM.DimTeamMemberKey ,@UnknownNumberValue) AS DimTeamMemberKey
		, COALESCE(TM30D.DimTeamMemberKey,@UnknownNumberValue) 	AS DimTeamMemberKey30DLag
		, COALESCE(E.DimEmployeeKey ,@UnknownNumberValue) AS DimEmployeeKey
		, COALESCE(E30D.DimEmployeeKey,@UnknownNumberValue) AS DimEmployeeKey30DLag
		, COALESCE(DEM.DimEmployeeMgmtKey ,@UnknownNumberValue) AS DimEmployeeMgmtKey
		, COALESCE(DEM30D.DimEmployeeMgmtKey ,@UnknownNumberValue) AS DimEmployeeMgmtKey30DLag
		, COALESCE(PGIC.DimPeerGroupKey ,@UnknownNumberValue) AS DimPeerGroupKey
		, COALESCE(PGIC30D.DimPeerGroupKey ,@UnknownNumberValue) AS DimPeerGroupKey30DLag
		, 1 AS CatchAllTerminationCount
		, TermThreat TerminationThreatCount
		, TermSave TerminationSaveCount
		, Term TerminationCount
		, COALESCE(DTT.DimDateKey,@UnknownNumberValue) AS DimTerminationThreatDateKey 
		, COALESCE(DTS.DimDateKey,@UnknownNumberValue) AS DimTerminationSaveDateKey
		, COALESCE(DT.DimDateKey,@UnknownNumberValue) AS DimTerminationDateKey
			,	   CASE WHEN DTSrv.DimTermSurveyKey is null  THEN @UnknownNumberValue
				   ELSE DTSrv.DimTermSurveyKey END AS DimTermSurveyKey
		, TSC.TerminationId
		, AUMT.TerminationAmount
		, TSC.TerminationSurveyAmount
		, T.CompletedEvent
		, COALESCE(DTermStatus.DimTerminationRequestTypekey ,@UnknownNumberValue) AS DimTerminationRequestTypekey
		, COALESCE(TT.DimTerminationTypeKey, @UnknownNumberValue) AS DimTerminationTypeKey		
	FROM TermsWithAssets T 
		JOIN FDW.DimDate D
			ON D.CalendarDate =  T.EventDate
		LEFT JOIN FDW.DimDate DTT
			ON DTT.CalendarDate =  T.TermThreatDate
		LEFT JOIN FDW.DimDate DTS
			ON DTS.CalendarDate =  T.SaveDate
		LEFT JOIN FDW.DimDate DT
			ON DT.CalendarDate =  T.TermDate

		JOIN Iris.ContactBase AS CB
			ON T.ClientId = CB.ContactId
			AND ISNULL(CB.fi_InSalesforce,0) = 0 -- filter out any clients in Salesforce

	    LEFT JOIN STG.DailyClientAssetsandTenureKeys CATK
			ON T.ClientID = CATK.ClientID 
			AND D.DimDateKey = CATK.DimDateKey

		JOIN [REF].[vwRelationshipManagementAssignmentWindow] AS RM
			ON T.ClientId = RM.ClientId 
			AND T.EventDate >= RM.AssignmentWindowStartDate AND T.EventDate < RM.AssignmentWindowEndDate

		LEFT JOIN FDW.DimTeamMember AS TM
			ON RM.AssignedToGUID = TM.TeamMemberGUID
			AND T.EventDate >= TM.EffectiveStartDate AND T.EventDate < TM.EffectiveEndDate
		
		LEFT JOIN (SELECT * FROM [FDW].[DimEmployee] WHERE TerminationRecord = 'No') AS E
			ON E.ActiveDirectoryUserIdWithDomain = TM.TeamMemberActiveDirectoryUserIdWithDomain
			AND T.EventDate >= E.EffectiveStartDate AND T.EventDate < E.EffectiveEndDate
		
		LEFT JOIN FDW.DimEmployeeMgmt AS DEM
			ON DEM.EmployeeID = E.EmployeeID
			AND T.EventDate >= DEM.EffectiveStartDate AND T.EventDate < DEM.EffectiveEndDate

		LEFT JOIN FDW.DimPeerGroupIC PGIC
			ON PGIC.TeamMemberSpecialty = TM.TeamMemberSpecialty
			AND PGIC.TenureInMonths = DATEDIFF(M, E.RoleStartDate, T.EventDate)	

		LEFT JOIN [REF].[vwRelationshipManagementAssignmentWindow] AS RM30D
			ON T.ClientId = RM30D.ClientId 
			AND DATEADD(d,-30,T.EventDate) >= RM30D.AssignmentWindowStartDate AND DATEADD(d,-30,T.EventDate) < RM30D.AssignmentWindowEndDate
		
		LEFT JOIN FDW.DimTeamMember AS TM30D
			ON RM30D.AssignedToGUID = TM30D.TeamMemberGUID
			AND DATEADD(d,-30,T.EventDate) >= TM30D.EffectiveStartDate AND DATEADD(d,-30,T.EventDate) < TM30D.EffectiveEndDate
		
		LEFT JOIN (SELECT * FROM [FDW].[DimEmployee] WHERE TerminationRecord = 'No') AS E30D
			ON E30D.ActiveDirectoryUserIdWithDomain = TM30D.TeamMemberActiveDirectoryUserIdWithDomain
			AND DATEADD(d,-30,T.EventDate) >= E30D.EffectiveStartDate AND DATEADD(d,-30,T.EventDate) < E30D.EffectiveEndDate
		
		LEFT JOIN FDW.DimEmployeeMgmt AS DEM30D
			ON DEM30D.EmployeeID = E30D.EmployeeID
			AND DATEADD(d,-30,T.EventDate) >= DEM30D.EffectiveStartDate AND DATEADD(d,-30,T.EventDate)< DEM30D.EffectiveEndDate

		LEFT JOIN FDW.DimPeerGroupIC PGIC30D
			ON PGIC30D.TeamMemberSpecialty = TM30D.TeamMemberSpecialty
			AND PGIC30D.TenureInMonths = DATEDIFF(M, E30D.RoleStartDate, T.EventDate)	

		LEFT JOIN AUMTotal AUMT
			ON AUMT.ClientId = T.ClientId 
			AND  AUMT.EventDate = T.EventDate
		
		LEFT JOIN TermSurveyConnected TSC
			ON TSC.ClientId = T.ClientId 
		   AND TSC.EventDate = T.EventDate

	   LEFT JOIN [FDW].[DimTermSurvey] DTSrv
			ON TSC.terminationId = DTSrv.TerminationId

		LEFT JOIN ClientBDays AS CBD 
			ON T.ClientId = CBD.ClientId

		LEFT JOIN [FDW].[DimAgeGroup] AS AG
			ON FLOOR(DATEDIFF(Day, CBD.[DateOfBirth], T.EventDate)/365.25) >= AG.[StartAge]
		   AND FLOOR(DATEDIFF(Day, CBD.[DateOfBirth], T.EventDate)/365.25) < AG.[EndAge]	

		LEFT JOIN [FDW].[DimTerminationRequestType] DTermStatus
		ON   T.OldClientTypeName  = DTermStatus.RequestName

		LEFT
		JOIN ClearanceWinowTerms AS CWT
		  ON T.ClientId = CWT.ClientId 
		 AND T.EventDate = CWT.EventDate	 		

		LEFT
		JOIN #DimTerminationType AS TT  
		  ON CWT.TerminationType = TT.TerminationType 		
	
	/*
	INSERT NEW RECORDS INTO DIMENSION THAT DID NOT PREVIOUSLY EXIST
*/

SET @Source = '[{"SourceTable":"#FactClientTermination_Temp"}]'
SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
SET @StartTime = GETDATE()

/*
	Update the Values from the Temp table to our source table, if they arent populated
*/
--For Incomplete Events, update them if they have since be termed/saved.
UPDATE TGT
	SET  
		  TGT.DimTerminationSaveDateKey = CASE WHEN TGT.DimTerminationSaveDateKey = @UnknownNumberValue THEN SRC.DimTerminationSaveDateKey ELSE TGT.DimTerminationSaveDateKey END
		, TGT.TerminationSaveCount = CASE WHEN TGT.TerminationSaveCount =  0 THEN SRC.TerminationSaveCount ELSE TGT.TerminationSaveCount END
		, TGT.DimTerminationDateKey = CASE WHEN TGT.DimTerminationDateKey = @UnknownNumberValue THEN SRC.DimTerminationDateKey ELSE TGT.DimTerminationDateKey END	
		, TGT.DimTermSurveyKey = CASE WHEN TGT.DimTermSurveyKey = @UnknownNumberValue THEN SRC.DimTermSurveyKey ELSE TGT.DimTermSurveyKey END	
		, TGT.TerminationCount = CASE WHEN TGT.TerminationCount =  0 THEN SRC.TerminationCount ELSE TGT.TerminationCount END
		, TGT.TerminationAmount  = CASE WHEN TGT.TerminationAmount IS NULL THEN SRC.TerminationAmount ELSE TGT.TerminationAmount END	
		, TGT.CompletedEvent = CASE WHEN SRC.TerminationCount =  1 OR SRC.TerminationSaveCount = 1 THEN SRC.CompletedEvent ELSE TGT.CompletedEvent END		
		, TGT.DWUpdatedDateTime = @DWUpdatedDateTime
		, [ETLJobProcessRunId] = @ETLJobProcessRunId
        , [ETLJobSystemRunId] = @ETLJobSystemRunId 
  FROM #FactClientTermination_Temp AS Src  
 INNER JOIN #FactClientTermination AS TGT ON Src.DimClientKey = TGT.DimClientKey AND Src.DimDateKey = TGT.DimDateKey
 WHERE TGT.CompletedEvent = 0 
--OPTION (Label = '#FactClientTermination-Update-Query')


--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTermination-Update-Query', @UpdateCount OUT

--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--EXEC MDR.spProcessTaskLogUpdateRowCount
--		  @ETLJobProcessRunId 
--		, @ComponentName
--		, @Source 
--		, @Target 
--		, @UpdateCount	 
--        , @DurationInSeconds
	
	/*
	INSERT NEW RECORDS INTO DIMENSION THAT DID NOT PREVIOUSLY EXIST
*/

SET @Source = '[{"SourceTable":"#FactClientTermination_Temp"}]'
SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
SET @StartTime = GETDATE()

--Term Surveys can be very late arriving - Check for records missing survey amount and update accordingly
UPDATE TGT
	SET  				
		  TGT.TerminationSurveyAmount  = CASE WHEN TGT.TerminationSurveyAmount IS NULL THEN SRC.TerminationSurveyAmount ELSE TGT.TerminationSurveyAmount END	
		, TGT.DWUpdatedDateTime = @DWUpdatedDateTime
		, [ETLJobProcessRunId] = @ETLJobProcessRunId
        , [ETLJobSystemRunId] = @ETLJobSystemRunId 
  FROM #FactClientTermination_Temp AS Src  
 INNER JOIN #FactClientTermination AS TGT ON Src.DimClientKey = TGT.DimClientKey AND Src.DimDateKey = TGT.DimDateKey
 WHERE TGT.TerminationSurveyAmount IS NULL AND SRC.TerminationSurveyAmount IS NOT NULL
-- OPTION (Label = '#FactClientTerminationLate-Update-Query')

--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTerminationLate-Update-Query', @UpdateCount OUT

--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--EXEC MDR.spProcessTaskLogUpdateRowCount
--		  @ETLJobProcessRunId 
--		, @ComponentName
--		, @Source 
--		, @Target 
--		, @UpdateCount	 
        --, @DurationInSeconds

SET @Source = '[{"SourceTable":"#FactClientTermination_Temp"}]'
SET @Target = '[{"TargetTable":"#FactClientTermination"}]'
SET @StartTime = GETDATE()



 INSERT INTO #FactClientTermination (
		
		DimDateKey                 					
	    , DimClientKey
	    , DimClientAssetsKey				
	    , DimClientTenureKey				
	    , DimAgeGroupKey					
	    , DimTeamMemberKey				
	    , DimTeamMemberKey30DLag			
	    , DimEmployeeKey					
	    , DimEmployeeKey30DLag			
	    , DimEmployeeMgmtKey				
	    , DimEmployeeMgmtKey30DLag		
	    , DimPeerGroupKey				
	    , DimPeerGroupKey30DLag			
	    , CatchAllTerminationCount		
	    , TerminationThreatCount			
	    , TerminationSaveCount			
	    , TerminationCount				
            , DimTerminationThreatDateKey	
	    , DimTerminationSaveDateKey		
	    , DimTerminationDateKey	
	    , DimTermSurveyKey
	    , DimTerminationRequestTypekey
		, DimTerminationTypeKey
	    , TerminationAmount				
	    , TerminationSurveyAmount	
	    , CompletedEvent
	    , DWCreatedDateTime
	    , DWUpdatedDateTime
	    , ETLJobProcessRunId
	    , ETLJobSystemRunId
)

SELECT 
		  Src.DimDateKey
		, Src.DimClientKey
		, Src.DimClientAssetsKey
	        , Src.DimClientTenureKey
	        , Src.DimAgeGroupKey
		, Src.DimTeamMemberKey
		, Src.DimTeamMemberKey30DLag
		, Src.DimEmployeeKey
		, Src.DimEmployeeKey30DLag
		, Src.DimEmployeeMgmtKey
		, Src.DimEmployeeMgmtKey30DLag
		, Src.DimPeerGroupKey
		, Src.DimPeerGroupKey30DLag
		, Src.CatchAllTerminationCount		
	    , Src.TerminationThreatCount			
	    , Src.TerminationSaveCount			
	    , Src.TerminationCount
		, Src.DimTerminationThreatDateKey
		, Src.DimTerminationSaveDateKey
		, Src.DimTerminationDateKey
		, Src.DimTermSurveyKey
		, Src.DimTerminationRequestTypekey
		, Src.DimTerminationTypeKey
		, Src.TerminationAmount			
		, Src.TerminationSurveyAmount
		, Src.CompletedEvent
		, @DWUpdatedDateTime AS DWCreatedDateTime
		, @DWUpdatedDateTime AS DWUpdatedDateTime
		, @ETLJobProcessRunId AS ETLJobProcessRunId
		, @ETLJobSystemRunId AS ETLJobSystemRunId
  FROM #FactClientTermination_Temp AS Src  
  LEFT  JOIN #FactClientTermination AS TGT ON Src.DimClientKey = TGT.DimClientKey AND Src.DimDateKey = TGT.DimDateKey
 WHERE TGT.DimClientKey IS NULL 

--OPTION (Label = '#FactClientTermination-Insert-Query')

--EXEC MDR.spGetRowCountByQueryLabel '#FactClientTermination-Insert-Query', @InsertCount OUT
 
--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--EXEC MDR.spProcessTaskLogInsertRowCount
--		  @ETLJobProcessRunId 
--		, @ComponentName
--		, @Source 
--		, @Target 
--		, @InsertCount	 
--        , @DurationInSeconds


--COMMIT TRANSACTION

/*
	End Iris
*/


--END TRY
--BEGIN CATCH 
-- --An error occurred; simply rollback the transaction started at the beginning the of the procedure.

--ROLLBACK TRANSACTION;

--SET @Status = 0
--SET @ErrorMessage = CONCAT('#FactClientTermination:', ERROR_MESSAGE())

--END CATCH 

----Drop temp tables to free up Temp DB space
--IF OBJECT_ID ('TEMPDB..#FactClientTermination_Temp') IS NOT NULL DROP TABLE #FactClientTermination_Temp
--IF OBJECT_ID ('TEMPDB..#AllTerms') IS NOT NULL DROP TABLE #AllTerms

--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END
--GO


SELECT DimDateKey
     , DimClientKey 
     , COUNT(1) AS RecCount
     , COUNT(DISTINCT CaseNumber) AS CaseCount
  FROM #FactClientTermination 
 GROUP 
    BY DimDateKey
     , DimClientKey
HAVING COUNT(1) > 1 --WE CAN'T DO MUCH ABOUT DUPLICATE TERM CASES BEING CREATED IN SFDC
   AND COUNT(DISTINCT CaseNumber) = 1 --WE SHOULDN'T SEE MULTIPLE TERM RECORDS FOR THE CASE CLIENT/TERM DATE

SELECT TerminationType
     , COUNT(1) AS RecCount
  FROM #FactClientTermination AS FCT
  LEFT
  JOIN #DimTerminationType AS TT
    ON FCT.DimTerminationTypeKey = TT.DimTerminationTypeKey
 GROUP 
    BY TerminationType




