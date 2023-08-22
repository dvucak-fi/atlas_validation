CREATE PROC [FDW].[spUpsertFactClientSnapshotDailyBackfill] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements

DECLARE @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
DECLARE @AUMLBVAR MONEY = CAST(1000000000000 AS MONEY)
DECLARE @AUMUBVAR MONEY = CAST(1000000000001 AS MONEY)
DECLARE @SOWLBVAR FLOAT = CAST(1000000000000 AS FLOAT)
DECLARE @SOWUBVAR FLOAT = CAST(1000000000001 AS FLOAT)
DECLARE @DWUpdatedDatetime DATETIME
      , @StartTime DATETIME
      , @EndTime DATETIME
      , @DurationInSeconds INT
      , @Source NVARCHAR(255)
      , @Target NVARCHAR(255)
      , @Status INT
      , @ErrorMessage NVARCHAR(512)
       
DECLARE @InsertCount INT
       
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


IF OBJECT_ID('tempdb..#DimClientandAgeGroupKeys', 'U') IS NOT NULL
    DROP TABLE #DimClientandAgeGroupKeys	

CREATE TABLE #DimClientandAgeGroupKeys (	
		   CalendarDate                 DATE
         , DimDateKey                   INT
		 , DimClientKey	                INT
		 , DimAgeGroupKey               INT
		 , ClientId                     UNIQUEIDENTIFIER
		 , WealthBuilderCallCycle       INT
		 , WealthBuilderOver60CallCycle INT
		 , TradingClientCallCycle       INT
		 , PCCallCycle                  INT
		 , HAUM_HLNW_CallCycle          INT
		 , CustomCallCycle              INT
		 , ExtendedCallCycle            INT
		 , DailyCallCycle               INT
)
WITH (DISTRIBUTION = HASH([ClientID]), HEAP)

 IF OBJECT_ID('tempdb..#DailyEmployeeKeys', 'U') IS NOT NULL
    DROP TABLE #DailyEmployeeKeys	

CREATE TABLE #DailyEmployeeKeys (
		   CalendarDate 	  DATE
         , DimDateKey 		  INT
		 , ClientId   		  UNIQUEIDENTIFIER
		 , DimTeamMemberKey	  INT
	     , DimEmployeeKey	  INT
	     , DimEmployeeMgmtKey INT
		 , DimPeerGroupKey    INT
		 )
WITH (DISTRIBUTION = HASH([ClientID]), HEAP)

IF OBJECT_ID('tempdb..#FactClientSnapshotDaily_Temp', 'U') IS NOT NULL
    DROP TABLE #FactClientSnapshotDaily_Temp	

CREATE TABLE #FactClientSnapshotDaily_Temp ( 
	[DimDateKey] [int] NOT NULL,
	[DimClientKey] [int] NOT NULL,
	[DimTeamMemberKey] [int] NOT NULL,
	[DimClientAssetsKey] [int] NOT NULL,
	[DimClientTenureKey] [int] NOT NULL,
	[DimEmployeeKey] [int] NOT NULL,
	[DimEmployeeMgmtKey] [int] NOT NULL,
	[DimAgeGroupKey] [int] NOT NULL,
	[DimKYCStatusKey] [int] NOT NULL,
	[DimPeerGroupKey] [int] NOT NULL,
	[DimCallCycleKey] [int] NOT NULL,
	[ClientId] [uniqueidentifier] NULL,
	[ClientNumber] [int] NULL,
	[AssetsUnderManagementUSD] [decimal](18, 2) NULL,
	[NetLiquidAssets] [decimal](18, 2) NULL,
	[TotalLiquidAssets] [decimal](18, 2) NULL,
	[TotalLiabilities] [decimal](18, 2) NULL,
	[NetWorthUSD] [decimal](18, 2) NULL,
	[ContactLast90] [INT] NULL,
    [BioReviewLast365] [INT] NULL,
	[NetworkReviewLast365] [INT] NULL,
	[SuitWizReviewLast365] [INT] NULL, 
	[CallCycleContact] [INT] NULL, 
	[ContactLag] [INT] NULL
)
WITH (DISTRIBUTION = HASH([ClientId]), CLUSTERED COLUMNSTORE INDEX)


IF OBJECT_ID('tempdb..#SWKYC', 'U') IS NOT NULL
    DROP TABLE #SWKYC	

CREATE TABLE #SWKYC (	
	CalendarDate          DATE
  , DimDateKey            INT
  , ClientId		  	  UNIQUEIDENTIFIER
  , ClientNumber		  INT
  , ContactLast90		  INT
  , BioReviewLast365 	  INT
  , NetworkReviewLast365  INT
  , SuitWizReviewLast365  INT
)
WITH (DISTRIBUTION = HASH([ClientID]), HEAP)


IF OBJECT_ID('tempdb..#DailyActivities') IS NOT NULL 
	DROP TABLE #DailyActivities

CREATE TABLE #DailyActivities (
    InteractionId   NVARCHAR(36) NULL
  , ClientID       UniqueIdentifier
  , CreatedOn      Datetime
  , ActivityID     INT
  , SFValue        NVARCHAR(200)
  , PrimaryICFlag  INT       
)
WITH
(
	DISTRIBUTION = HASH ([ClientID]), 
	HEAP		
)


IF OBJECT_ID('tempdb..#DailyClientAgeGroupCallCycle', 'U') IS NOT NULL
    DROP TABLE #DailyClientAgeGroupCallCycle	

CREATE TABLE #DailyClientAgeGroupCallCycle (	
		   CalendarDate                 DATE
         , DimDateKey                   INT
		 , DimClientKey	                INT
		 , DimAgeGroupKey               INT
		 , ClientId                     UNIQUEIDENTIFIER
		 , DailyCallCycle               INT
	     , DimCallCycleKey              INT
	     , CallCycleContact             INT
	     , ContactLag                   INT
)
WITH (DISTRIBUTION = HASH([ClientID]), HEAP)


-- Combining IRIS Activities with SFDC Interactions. Later used for Contact Rates and Bio/Network Review rates
;WITH PIVOTED as (
 
	  SELECT RM.fi_fi_relationshipmanagementauditlogId AS InteractionId
		   , RM.fi_AssignedToUserId 
		   , RM.CreatedBy
		   , RM.CreatedOn
		   , CONVERT(DATE, RM.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') CreatedOnPST 
		   , fi_ContactId
		   , Ap.ActivityID
		   , 1 AS ActivityCount
       
	    FROM Iris.fi_fi_relationshipmanagementauditlogBase RM
 
	   OUTER 
	   APPLY OPENJSON(CONCAT('[
			  {"ActivityId":"', RM.[fi_ActivityTopic1], '","ActivityCounter":1},
			  {"ActivityId":"', RM.[fi_ActivityTopic2], '","ActivityCounter":2},
			  {"ActivityId":"', RM.[fi_ActivityTopic3], '","ActivityCounter":3},
			  {"ActivityId":"', RM.[fi_ActivityTopic4], '","ActivityCounter":4},
			  {"ActivityId":"', RM.[fi_ActivityTopic5], '","ActivityCounter":5}
			  ]'),'$')
	    WITH ( ActivityId NVARCHAR(36) '$.ActivityId' 
		     , ActivityCounter TINYINT '$.ActivityCounter'
		     ) Ap 
 
 )

, IRISActivities AS (

	  SELECT CONVERT(NVARCHAR(36),InteractionId) InteractionId
		   , P.fi_AssignedToUserId 
		   , P.CreatedBy
		   , CASE WHEN UB1.domainname is null THEN  1 --Audit Log entry has an activedirectoryID that is not in WD, meaning that it is a service account. Primary IC gets credit
				  WHEN p.CreatedBy = p.fi_AssignedtoUserID THEN  1 --Assigned IC did the activity
				  ELSE 0
				  END PrimaryICFlag 
			
		   , P.CreatedOn
		   , P.fi_ContactId ClientID
		   , TB.fi_ID ActivityID
		   , CreatedOnPST 
		   , SFM.SFValue       
		FROM PIVOTED P
	   INNER JOIN Iris.fi_topicBase TB on TB.fi_topicId = CASE WHEN P.ActivityID = '' THEN @UnknownGuid ELSE P.ActivityID END	  
	   INNER JOIN [Iris].[SystemUserBase] UB1 ON UB1.SystemUserId = P.createdby
		LEFT JOIN REF.PcgSfMapping SFM ON TB.fi_ID = SFM.OldCode
       WHERE TB.fi_ID IN (102072, 102073, 102075, 102076, 112800,106707,106731) -- Activities from Iris

)

	  --INSERT ACTIVITEIS ENTERED WITHIN IRIS CRM
      INSERT 
	    INTO #DailyActivities (
			 InteractionId
		   , ClientID      
		   , CreatedOn      
		   , ActivityID   
		   , SFValue
		   , PrimaryICFlag       
	  )
	

	  SELECT InteractionId
		   , ClientID
		   , CreatedOnPST 
		   , ActivityID
		   , SFValue
		   , PrimaryICFlag 	
	    FROM IrisActivities 



--REPEAT SAME PROCESS BUT WITH SFDC ACTIVITIES 
;WITH SFDCActivities AS (

	  SELECT I.Id AS InteractionId
		   , CalendarDAte CreatedOnPST
		   , DCH.ClientID
		   , I.InteractionType
		   , CASE 
				WHEN U.EmployeeNumber = DE.EmployeeID THEN 1
				ELSE 0
		     END AS PrimaryICFlag      
	    
		FROM PcgSf.Interaction I

	    -- -- DimDateKey
	    JOIN FDW.DimDate DD
		  ON CONVERT(DATE, I.CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') = DD.CalendarDate

		-- DimClientKey
		LEFT
		JOIN FDW.DimClient DCH
		  ON I.AccountId = DCH.HouseholdUID
		 AND DD.CalendarDate >=  DCH.EffectiveStartDate
		 AND DD.CalendarDate <  DCH.EffectiveEndDate

	    LEFT
	    JOIN Pcgsf.InteractionAttendee IA
		  ON IA.InteractionId = I.Id

	    LEFT
	    JOIN Pcgsf.[User] U 
		  ON U.Id = IA.UserId

	    LEFT
	    JOIN REF.vwRelationshipManagementAssignmentWindow RMA
		  ON DCH.ClientNumber = RMA.ClientNumber
	     AND DD.CalendarDate >= RMA.AssignmentWindowStartDate
	     AND DD.CalendarDate < RMA.AssignmentWindowEndDate

	    LEFT
	    JOIN FDW.DimEmployee DE --AssignedEmployee
		  ON RMA.AssignedToActiveDirectoryUserIdWithDomain = DE.ActiveDirectoryUserIdWithDomain
	     AND DD.CalendarDate >= DE.EffectiveStartDate
	     AND DD.CalendarDate < DE.EffectiveEndDate
	     AND DE.TerminationRecord = 'No'

	   WHERE IA.AttendeeType = 'Internal'
)


	  --INSERT ACTIVITEIS ENTERED WITHIN SFDC CRM
      INSERT 
	    INTO #DailyActivities (
             InteractionId
	       , ClientID      
		   , CreatedOn      
		   , SFValue
		   , PrimaryICFlag       
	  )

	  SELECT InteractionID	
	       , ClientID
	       , CreatedOnPST
	       , InteractionType	
	       , PrimaryICFlag
        FROM SFDCActivities



--BUILD HISTORY OF ALL SW TICKETS COMPLETED 
;WITH SWSnapShots AS (
   
      SELECT CONVERT(DATE, ISNULL(R.dtSubmissionDate, T.dtSubmissionDate)) AS EffectiveStartDate
		   , T.iTicketId
		   , AL.iAccountHolderCID AS ClientNumber		
		   , ROW_NUMBER() OVER(PARTITION BY AL.iAccountHolderCID, CONVERT(DATE, ISNULL(R.dtSubmissionDate, T.dtSubmissionDate)) ORDER BY T.dtUpdatedDatetime DESC) AS RowNum

	    FROM [SWRptSTG].[csuSWAssetLiability_SNPST] AS AL 
	
	    JOIN [SWRptSTG].[csuSWTickets] AS T
	      ON AL.iSnapshotId = T.iIWSnapshotId
	
	    JOIN [SWRptSTG].[csuSWRecommendation] AS R
	      ON T.iTicketId = R.iTicketId 
	
	    JOIN [SWRptSTG].[csuSWSubmissionTypes] AS ST
	      ON R.iSubmissionTypeId = ST.iSubmissionTypesId
   
       WHERE T.iStatusId = 306 --Completed
	     AND ST.vchName Not In ('Custom (CUS) Account','Hypothetical Cash Flow Analysis Only')  --Exclude Hypothetical Recs	

)

, DailySW AS (

	  SELECT DC.CalendarDate
           , DC.DimDateKey
           , DC.ClientId
		   , DC.ClientNumber
		   , SS.EffectiveStartDate AS LastSWCompletionDate
		   , ROW_NUMBER() OVER (PARTITION BY DC.ClientNumber ORDER BY DC.CalendarDate) AS RowNum

	    FROM STG.DailyClientAssetsandTenureKeys DC--Staging table that has daily grain of trading, non trading, and deceased clients with their assets and associated keys
	    
		LEFT 
		JOIN SWSnapShots SS 
		  ON DC.ClientNumber = SS.ClientNumber
         AND DC.CalendarDate = SS.EffectiveStartDate
         AND SS.RowNum = 1 --If multiple tickets are submitted within the same day, use the ticket with the latest dtUpdateDate

)

, SWGapsIslands AS (

      SELECT CalendarDate
           , DimDateKey
           , ClientId
           , ClientNumber
		   , LastSWCompletionDate
		   , MAX(CASE WHEN LastSWCompletionDate IS NOT NULL THEN RowNum END) OVER (PARTITION BY ClientNumber ORDER BY CalendarDate) AS GrpSWDBase
		   , RowNum
	    FROM DailySW

)

, DailyClientSnapshots AS (

      SELECT CalendarDate
           , DimDateKey
           , ClientId
           , ClientNumber
		   , LastSWCompletionDate
		   , MAX(LastSWCompletionDate) OVER (PARTITION BY ClientNumber, GrpSWDBase) AS LastSWDate
	    FROM SWGapsIslands

)

      INSERT 
	    INTO #SWKYC  (
             CalendarDate
		   , DimDateKey
		   , ClientId
		   , ClientNumber
		   , ContactLast90
		   , BioReviewLast365 
		   , NetworkReviewLast365
		   , SuitWizReviewLast365  
	  )

	  SELECT DC.CalendarDate
		   , DC.DimDateKey
		   , DC.ClientId
		   , DC.ClientNumber
		   , MAX(CASE WHEN DA.SFValue in ('Inbound Phone Call','In-Person','Outbound Phone Call','Virtual')--Contacts
				   AND PrimaryICFlag = 1
				   AND DATEDIFF(DD, DA.CreatedOn, DC.CalendarDate) BETWEEN 0 AND 90 
				  THEN 1 
			      ELSE 0 
		     END) ContactLast90
		   , MAX(CASE WHEN DA.ActivityID = 106731 --Bio Review
		           AND DATEDIFF(DD, DA.CreatedOn, DC.CalendarDate) BETWEEN 0 AND 365 
				  THEN 1 
				  ELSE 0 
		     END) BioReviewLast365 
		   , MAX(CASE WHEN DA.ActivityID = 106707 --Network Review
		         AND DATEDIFF(DD, DA.CreatedOn, DC.CalendarDate) BETWEEN 0 AND 365 
				THEN 1 
				ELSE 0 
		     END) NetworkReviewLast365
		   , CASE WHEN  DATEDIFF(DD, DC.LastSWDate, DC.CalendarDate) BETWEEN 0 AND 365 
		        THEN 1 
				ELSE 0 
		     END SuitWizReviewLast365  

	    FROM DailyClientSnapshots DC

	    LEFT 
		JOIN #DailyActivities DA 
		  ON DC.ClientId = DA.ClientId
       
	   GROUP 
	      BY DC.CalendarDate
		   , DC.DimDateKey
		   , DC.ClientId
		   , DC.ClientNumber
		   , DC.LastSWDate



/*The way the following is structured may seem strange in terms of storing lots of data in seperate temp tables. However, this is purposely done for a few reasons.
SonarQube recommends having fewer than 10 joins in a single query, and ideally five or less.
The way Synapse stores and moves data from CTEs performs much worse than storing data more frequesntly in well designed temp tables with Hash distributions.*/


-- Gather Client and Age Group Keys
      INSERT 
	    INTO #DimClientandAgeGroupKeys (
		     CalendarDate 
           , DimDateKey 
		   , DimClientKey
  		   , DimAgeGroupKey
		   , ClientId        
		   , WealthBuilderCallCycle
		   , WealthBuilderOver60CallCycle
		   , TradingClientCallCycle
      )
      
	  SELECT STG.CalendarDate 
           , STG.DimDateKey 
		   , DC.DimClientKey
		   , AG.DimAgeGroupKey
		   , STG.ClientId 
  		   , CASE WHEN DC.ServiceProduct = 'WealthBuilder' THEN 180 ELSE NULL END AS WealthBuilderCallCycle
		   , CASE WHEN DC.ServiceProduct = 'WealthBuilder' AND AG.GroupName = '< 60 Years' THEN 90 ELSE NULL END AS WealthBuilderOver60CallCycle
		   , CASE WHEN DC.ClientType = 'Client - Trading' THEN 90 ELSE NULL END AS TradingClientCallCycle
        FROM STG.DailyClientAssetsandTenureKeys STG
        
		LEFT 
		JOIN [FDW].[DimClient] DC 
		  ON DC.ClientID = STG.ClientID 
         AND DC.EffectiveStartDate <= STG.CalendarDate
         AND DC.EffectiveEndDate > STG.CalendarDate

        LEFT 
		JOIN [Iris].[ContactBase] CB 
		  ON CB.ContactId = STG.ClientID 

        LEFT 
		JOIN [FDW].[DimAgeGroup] AG 
		  ON FLOOR(DATEDIFF(Day, CB.[BirthDate], STG.CalendarDate)/365.25) >= AG.[StartAge] 
         AND FLOOR(DATEDIFF(Day, CB.[BirthDate], STG.CalendarDate)/365.25) < AG.[EndAge]


-- PORTFOLIO COUNSELOR (PC) LOGIC. ANY CLIENT THAT HAD THE PC ATTRIBUTE SHOULD HAVE BEEN ON A 30/60 DAY CALL CYCLE FOR THE FIRST 180 DAYS, UNLESS A NEW ATTRIBUTE WAS CREATED PRIOR TO 180 DAYS
;WITH CRMServiceTypes AS ( 

	  SELECT CB.ContactId AS ClientId
		   , CB.fi_Id_Search AS ClientNumber
		   , STB.fi_StartDate AS StartDate
		   , CASE 
			  WHEN LEAD (STB.fi_StartDate, 1, @MaxDateValue) OVER (PARTITION BY CB.ContactId ORDER BY STB.fi_StartDate) < DATEADD(D, 180, STB.fi_StartDate) 
			  THEN LEAD (STB.fi_StartDate, 1, @MaxDateValue) OVER (PARTITION BY CB.ContactId ORDER BY STB.fi_StartDate)
 			  ELSE DATEADD(D, 180, STB.fi_StartDate)
		     END AS EndDate
	    FROM Iris.fi_servicetypeBase AS STB
	    JOIN Iris.fi_relationshipmanagementBase AS RM
		  ON STB.fi_ParentRM = RM.fi_relationshipmanagementId
	    JOIN Iris.ContactBase AS CB
		  ON CB.ContactId = RM.fi_ContactId
	    JOIN Iris.fi_servicetypedefinitionBase AS STDB
		  ON STB.fi_ServiceTypeDefn = STDB.fi_servicetypedefinitionId
	    LEFT
	    JOIN Iris.StringMapBase AS SMB  
		  ON SMB.AttributeValue = STB.statuscode
	     AND SMB.AttributeName = 'statuscode'
	     AND SMB.ObjectTypeCode = 10090 --fi_servicetypeBase
	   WHERE STDB.fi_Description = 'Portfolio Counselor'  
	     AND SMB.[Value] <> 'Canceled'  
	   	    
) 

, PCCallCycles AS ( 

	  SELECT CCA.CalendarDate		
		   , CCA.ClientID	
		   , CASE 
				WHEN CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) AND CCA.CalendarDate <= DATEADD(D, 90, CONVERT(DATE, ST.StartDate)) THEN 30
				WHEN CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) AND CCA.CalendarDate <= DATEADD(D, 180, CONVERT(DATE, ST.StartDate)) THEN 60
		     END AS PCCallCycle
	    FROM #DimClientandAgeGroupKeys AS CCA
	    --INNER JOIN SO WE ONLY RETURN DAYS WHERE PC ATTRIBUTE WAS ACTIVE 
	    JOIN CRMServiceTypes AS ST
		  ON CCA.ClientId = ST.ClientId 
	     AND CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) 
	     AND CCA.CalendarDate <= CONVERT(DATE, ST.EndDate)

)


, CallCycleRowNum AS ( 

      SELECT CalendarDate		
		   , ClientID		
		   , PCCallCycle
		   , ROW_NUMBER() OVER (PARTITION BY ClientID, CalendarDate ORDER BY PCCallCycle) AS RowNum
        FROM PCCallCycles 
     
)

, PCCallCyclesFinal AS ( 

      SELECT CalendarDate		
		   , ClientID	
		   , PCCallCycle
        FROM CallCycleRowNum
       WHERE RowNum = 1 --IF MULTIPLE CALL CYCLES ON THE SAME DAY PER CLIENT, USE LOWEST CALL CYCLE
	     AND PCCallCycle IS NOT NULL

) 

/*
	UPDATE #DimClientandAgeGroupKeys TEMP TABLE WITH PC CALL CYCLES  
*/

	  UPDATE #DimClientandAgeGroupKeys
	     SET PCCallCycle = SRC.PCCallCycle
	    FROM PCCallCyclesFinal AS SRC
	    JOIN #DimClientandAgeGroupKeys AS TGT
	      ON SRC.CalendarDate = TGT.CalendarDate
	     AND SRC.ClientID = TGT.ClientID 


/*
	UPDATE #DimClientandAgeGroupKeys TEMP TABLE WITH HAUM/HLNW CALL CYCLES AS DEFINED IN IC CALL CYCLES CONFLUENCE DOC WITHIN PCG BI COMMUNITY PAGE 
*/
	
	  UPDATE #DimClientandAgeGroupKeys
	     SET HAUM_HLNW_CallCycle = CASE WHEN SRC.ClientAssetsType = 'HAUM' THEN 60 WHEN SRC.ClientAssetsType = 'HLNW' THEN 75 END 
	    FROM REF.vwDailyClientAssets AS SRC
	    JOIN #DimClientandAgeGroupKeys AS TGT
	      ON SRC.ClientId = TGT.ClientId
	     AND SRC.CalendarDate = TGT.CalendarDate 
	   WHERE SRC.ClientAssetsType IN ('HAUM', 'HLNW')



;WITH CustomCallCycles AS ( 

	  SELECT CB.ContactId AS ClientId
		   , CB.fi_Id_Search AS ClientNumber
		   , CC.iCallCycle AS CallCycle
		   , CC.dtCreated AS StartDate
  		   , CASE 
			  WHEN LEAD (CC.dtCreated, 1, @MaxDateValue) OVER (PARTITION BY CC.iCID ORDER BY CC.dtCreated) < DATEADD(D, 365, CC.dtCreated) 
			  THEN LEAD (CC.dtCreated, 1, @MaxDateValue) OVER (PARTITION BY CC.iCID ORDER BY CC.dtCreated) 
			  ELSE DATEADD(D, 365, CC.dtCreated) 
		     END AS EndDate
	    FROM BitRpt.CallCycleCustom AS CC
	    JOIN Iris.ContactBase AS CB
	      ON CC.iCID = CB.fi_Id_Search 
	   
) 

, DupeProtect AS ( 

	  SELECT CCA.CalendarDate		
		   , CCA.ClientID	
		   , CC.CallCycle
		   , ROW_NUMBER() OVER (PARTITION BY CCA.ClientID, CCA.CalendarDate ORDER BY CC.CallCycle) AS RowNum
	    FROM #DimClientandAgeGroupKeys AS CCA
	    --INNER JOIN SO WE ONLY RETURN DAYS WHERE CUSTOM CALL CYCLE WAS ACTIVE 
	    JOIN CustomCallCycles AS CC
		  ON CCA.ClientId = CC.ClientId 
	     AND CCA.CalendarDate >= CONVERT(DATE, CC.StartDate) 
	     AND CCA.CalendarDate <= CONVERT(DATE, CC.EndDate)

) 

, CustomCallCycleFinal AS ( 

      SELECT CalendarDate		
		   , ClientID	
		   , CallCycle
	    FROM DupeProtect
	   WHERE RowNum = 1 

) 

/*
	UPDATE #DimClientandAgeGroupKeys TO ACCOUNT FOR CLIENTS ON CUSTOM CALL CYCLES  
*/

	  UPDATE #DimClientandAgeGroupKeys
	     SET CustomCallCycle = SRC.CallCycle
	    FROM CustomCallCycleFinal AS SRC
	    JOIN #DimClientandAgeGroupKeys AS TGT
	      ON SRC.CalendarDate = TGT.CalendarDate
	     AND SRC.ClientID = TGT.ClientID 


;WITH TeamSpecialties AS ( 

      SELECT chUserId
           , DatePoint
           , ISNULL(TeamSpecialty, @UnknownTextValue) AS TeamSpecialty
           , LAG (ISNULL(TeamSpecialty, @UnknownTextValue), 1, @UnknownTextValue) OVER (PARTITION BY chUserId ORDER BY DatePoint) AS PrevTeamSpecialty
        FROM BitRpt.ICClient_withGroup

) 

, TeamSpecialtyOrdered AS ( 

      SELECT chUserId
           , TeamSpecialty
           , DatePoint AS StartDate
           , LEAD(DatePoint, 1, @MaxDateValue) OVER (PARTITION BY chUserId ORDER BY DatePoint) AS EndDate
        FROM TeamSpecialties
       WHERE TeamSpecialty <> PrevTeamSpecialty

) 

, ExtendedCallCycles AS ( 

	  SELECT chUserId
           , TeamSpecialty
           , StartDate
           , EndDate
	    FROM TeamSpecialtyOrdered
	   WHERE TeamSpecialty = 'Test - Extended Call Cycle' --TEAM SPECIALTY THAT FOCUSES ON EXTENTED CALL CYCLE CONTACT WITH CLIENTS

 )

 , ExtendedCallCycleWindows AS ( 

      SELECT RMA.ClientId
           , RMA.ClientNumber
           , RMA.AssignedToGUID	
           , RMA.AssignedToUserId	
           , RMA.AssignedToActiveDirectoryUserIdWithDomain
		   , RMA.AssignmentWindowStartDate AS ExtendedCallCycleStart
		   , CASE WHEN RMA.AssignmentWindowEndDate <= ECC.EndDate THEN RMA.AssignmentWindowEndDate ELSE ECC.EndDate END AS ExtendedCallCycleEnd
        FROM ExtendedCallCycles AS ECC
        JOIN REF.vwRelationshipManagementAssignmentWindow AS RMA
          ON ECC.chUserId = RMA.AssignedToUserId
         AND RMA.AssignmentWindowStartDate >= ECC.StartDate
         AND RMA.AssignmentWindowStartDate <= ECC.EndDate

)

, ExtendedCallCycleFinal AS ( 

	  SELECT CCA.CalendarDate		
		   , CCA.ClientID	
		   , 180 AS ExtendedCallCycle --180 IS THE EXTENDED CALL CYCLE TEST DURATION
	    FROM #DimClientandAgeGroupKeys AS CCA
	    JOIN ExtendedCallCycleWindows AS ECC
		  ON CCA.ClientId = ECC.ClientId 
	     AND CCA.CalendarDate >= CONVERT(DATE, ECC.ExtendedCallCycleStart) 
	     AND CCA.CalendarDate <= CONVERT(DATE, ECC.ExtendedCallCycleEnd)

)


/*
	UPDATE #DimClientandAgeGroupKeys TO ACCOUNT FOR CLIENTS ON EXTENDED CALL CYCLE TESTS
*/

	  UPDATE #DimClientandAgeGroupKeys
	     SET ExtendedCallCycle = SRC.ExtendedCallCycle
	    FROM ExtendedCallCycleFinal AS SRC
	    JOIN #DimClientandAgeGroupKeys AS TGT
	      ON SRC.CalendarDate = TGT.CalendarDate
	     AND SRC.ClientID = TGT.ClientID 


/*
	UPDATE #DimClientandAgeGroupKeys WITH FINAL CALL CYCLE EVALUATION 
*/

	  UPDATE #DimClientandAgeGroupKeys
	     SET DailyCallCycle = CASE 
								WHEN CustomCallCycle IS NOT NULL THEN CustomCallCycle
								WHEN PCCallCycle IS NOT NULL THEN PCCallCycle
								WHEN WealthbuilderOver60CallCycle IS NOT NULL THEN WealthbuilderOver60CallCycle
								WHEN WealthbuilderCallCycle IS NOT NULL THEN WealthbuilderCallCycle
								WHEN HAUM_HLNW_CallCycle IS NOT NULL THEN HAUM_HLNW_CallCycle
								WHEN ExtendedCallCycle IS NOT NULL THEN ExtendedCallCycle
								WHEN TradingClientCallCycle IS NOT NULL THEN TradingClientCallCycle
								ELSE 90 --DEFAULT CALL CYCLE IF NO OTHER CONDITION IS MET
							 END 



      INSERT 
	    INTO #DailyClientAgeGroupCallCycle ( 
		     CalendarDate                 
           , DimDateKey                   
		   , DimClientKey	                
		   , DimAgeGroupKey               
		   , ClientId                     
		   , DailyCallCycle               
	       , DimCallCycleKey              
	       , CallCycleContact             
	       , ContactLag                   
	  )

	  SELECT DC.CalendarDate
	       , DC.DimDateKey
		   , DC.DimClientKey
		   , DC.DimAgeGroupKey
		   , DC.ClientId
		   , DC.DailyCallCycle
		   , CC.DimCallCycleKey
		   , MAX(CASE 
					WHEN DA.SFValue in ('Inbound Phone Call','In-Person','Outbound Phone Call','Virtual')--Contacts
					 AND PrimaryICFlag = 1
					 AND DATEDIFF(DD, DA.CreatedOn, DC.CalendarDate) BETWEEN 0 AND DC.DailyCallCycle
			        THEN 1 
			        ELSE 0 
			   END) CallCycleContact
		   , DATEDIFF(DD, MAX(CASE 
				WHEN DA.SFValue in ('Inbound Phone Call','In-Person','Outbound Phone Call','Virtual')--Contacts
				 AND PrimaryICFlag = 1
				 AND CONVERT(DATE, DA.CreatedOn) <= DC.CalendarDate
			    THEN  DA.CreatedOn
			 END), DC.CalendarDate) AS ContactLag 
		FROM #DimClientandAgeGroupKeys DC
		LEFT 
		JOIN #DailyActivities DA 
		  ON DC.ClientId = DA.ClientId
		LEFT
		JOIN FDW.DimCallCycle AS CC
		  ON DC.DailyCallCycle = CC.CallCycle
	   GROUP 
		  BY DC.CalendarDate
	       , DC.DimDateKey
		   , DC.DimClientKey
		   , DC.DimAgeGroupKey
		   , DC.ClientId
		   , DC.DailyCallCycle
		   , CC.DimCallCycleKey



--Gather all employee related keys for final recordset
      INSERT 
	    INTO #DailyEmployeeKeys (
		     CalendarDate 
           , DimDateKey 
		   , ClientId   
		   , DimTeamMemberKey
	       , DimEmployeeKey
	       , DimEmployeeMgmtKey
		   , DimPeerGroupKey
	  )
      
	  SELECT STG.CalendarDate 
           , STG.DimDateKey 
		   , STG.ClientID
		   , TM.DimTeamMemberKey
	       , E.DimEmployeeKey
	       , EM.DimEmployeeMgmtKey
		   , PG.DimPeerGroupKey
        FROM STG.DailyClientAssetsandTenureKeys STG

        LEFT 
		JOIN [REF].[vwRelationshipManagementAssignmentWindow] RM 
		  ON STG.ClientId = RM.ClientId 
         AND RM.AssignmentWINDOWStartDate <= STG.CalendarDate
         AND RM.AssignmentWINDOWEndDate > STG.CalendarDate

	    LEFT 
		JOIN [FDW].[DimTeamMember] TM 
		  ON RM.AssignedToGUID = TM.TeamMemberGUID	  
		 AND TM.EffectiveStartDate <= STG.CalendarDate
		 AND TM.EffectiveEndDate > STG.CalendarDate 	

		LEFT 
		JOIN [FDW].[DimEmployee] E 
		  ON E.ActiveDirectoryUserIdWithDomain = TM.TeamMemberActiveDirectoryUserIdWithDomain
		 AND STG.CalendarDate >= E.EffectiveStartDate
		 AND STG.CalendarDate < E.EffectiveEndDate
		 AND E.TerminationRecord = 'No' --Some ADUserIDs are reused amongst EmployeeIDs, generally when someone is hired  as fulltime from contractor.
 																--In order to combat this, we exclude employee records where they were terminated.

        LEFT 
		JOIN [FDW].[DimEmployeeMgmt] EM 
		  ON E.EmployeeID = EM.EmployeeID --By joining on EmployeeID through DimEmployee, we emilnate the ADUserID problem above for this join.
		 AND STG.CalendarDate >= EM.EffectiveStartDate
		 AND STG.CalendarDate <= EM.EffectiveEndDate

		LEFT 
		JOIN FDW.DimPeerGroupIC PG 
	      ON PG.TeamMemberSpecialty = TM.TeamMemberSpecialty
		 AND PG.TenureInMonths = FDW.fnGetTenureInMonths(E.RoleStartDate, STG.CalendarDate)	




--Insert joint result sets into temp table for staging
      INSERT
        INTO #FactClientSnapshotDaily_Temp ( 
	         DimDateKey 
	       , DimClientKey
	       , DimTeamMemberKey
	       , DimClientAssetsKey
	       , DimClientTenureKey
	       , DimEmployeeKey
	       , DimEmployeeMgmtKey
	       , DimAgeGroupKey
	       , DimKYCStatusKey
		   , DimPeerGroupKey
		   , DimCallCycleKey
	       , ClientId
	       , ClientNumber
	       , AssetsUnderManagementUSD 
	       , NetLiquidAssets 
		   , TotalLiquidAssets
		   , TotalLiabilities
		   , NetWorthUSD
		   , ContactLast90
		   , BioReviewLast365
		   , NetworkReviewLast365
		   , SuitWizReviewLast365 
		   , CallCycleContact
		   , ContactLag

	  ) 

      SELECT ISNULL(STG.DimDateKey, @UnknownNumberValue) AS DimDateKey
		   , ISNULL(DC.DimClientKey, @UnknownNumberValue) AS DimClientKey
		   , ISNULL(DE.DimTeamMemberKey, @UnknownNumberValue) AS DimTeamMemberKey
		   , ISNULL(STG.DimClientAssetsKey, @UnknownNumberValue) AS DimClientAssetsKey
		   , ISNULL(STG.DimTenureKey, @UnknownNumberValue) AS DimClientTenureKey
		   , ISNULL(DE.DimEmployeeKey, @UnknownNumberValue) AS DimEmployeeKey
		   , ISNULL(DE.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimEmployeeMgmtKey
		   , ISNULL(DC.DimAgeGroupKey, @UnknownNumberValue) AS DimAgeGroupKey
		   , ISNULL(KYC.DimKYCStatusKey, @UnknownNumberValue) AS DimKYCStatusKey
		   , ISNULL(DE.DimPeerGroupKey, @UnknownNumberValue) AS DimPeerGroupKey
		   , ISNULL(DC.DimCallCycleKey, @UnknownNumberValue) AS DimCallCycleKey
		   , STG.ClientId
		   , STG.ClientNumber
		   , STG.AUM_USD AS AssetsUnderManagementUSD
		   , STG.NetLiquidAssets 
		   , STG.TotalLiquidAssets 
		   , STG.TotalLiabilities 
		   , STG.TNW AS NetWorthUSD --Since we are using SWRpt, which is US Clients only, base values are already all in USD
		   , SW.ContactLast90
		   , SW.BioReviewLast365
		   , SW.NetworkReviewLast365 
		   , SW.SuitWizReviewLast365 
		   , DC.CallCycleContact
		   , DC.ContactLag
		  
	    FROM STG.DailyClientAssetsandTenureKeys STG

		LEFT
		JOIN #DailyClientAgeGroupCallCycle DC ON DC.DimDateKey = STG.DimDateKey AND DC.ClientID = STG.ClientID

	    LEFT 
		JOIN #DailyEmployeeKeys DE ON DE.DimDateKey = STG.DimDateKey AND DE.ClientID = STG.ClientID
	  
	    LEFT 
		JOIN #SWKYC SW ON SW.DimDateKey = STG.DimDateKey AND SW.ClientID = STG.ClientID 

	    LEFT 
		JOIN FDW.DimKYCStatus AS KYC
	      ON CASE WHEN SW.BioReviewLast365 = 1 THEN 'Bio Review Completed' ELSE 'Bio Review Not Completed' END = KYC.BioReviewStatus
	     AND CASE WHEN SW.ContactLast90 = 1 THEN 'Contacted Within Last 90 Days' ELSE 'Not Contacted Within Last 90 Days' END = KYC.[90DayContactStatus]
	     AND CASE WHEN SW.NetworkReviewLast365 = 1 THEN 'Network Review Completed' ELSE 'Network Review Not Completed' END = KYC.NetworkReviewStatus
	     AND CASE WHEN SW.SuitWizReviewLast365 = 1 THEN 'SuitWiz Review Completed' ELSE 'SuitWiz Review Not Completed' END = KYC.SuitWizReviewStatus


--SET TRANSACTION SCOPE
BEGIN TRY

BEGIN TRANSACTION 	   
	 
SET @Source = '[{"SourceTable":"SWRptStg.csuSWAssetLiability_SNPST"},{"SourceTable":"SWRptStg.csuSWRecommendation"},{"SourceTable":"SWRptStg.csuSWTickets"},{"SourceTable":"SWRptStg.csuSWSubmissionTypes"},{"SourceTable":"SSRSDS.FINRvFIAcctDrrSumBalHistory"}]'
SET @Target = '[{"TargetTable":"FDW.FactClientSnapshotDaily"}]' 
SET @StartTime = GETDATE()

/*
	INSERT NEW RECORDS INTO FACT TABLE THAT DID NOT PREVIOUSLY EXIST
*/

   INSERT 
     INTO [FDW].[FactClientSnapshotDaily] (
          [DimDateKey]
        , [DimClientKey]
        , [DimTeamMemberKey]
		, [DimClientAssetsKey]
		, [DimClientTenureKey] 
		, [DimEmployeeKey] 
		, [DimEmployeeMgmtKey]
		, [DimAgeGroupKey]
		, [DimKYCStatusKey]
		, [DimPeerGroupKey]
		, [DimCallCycleKey]
		, [ClientId]
		, [ClientNumber]
		, [AssetsUnderManagementUSD]
		, [NetLiquidAssets]
		, [TotalLiquidAssets]
		, [TotalLiabilities]
		, [NetWorthUSD]
		, [ContactLast90]
		, [BioReviewLast365]
		, [NetworkReviewLast365]
		, [SuitWizReviewLast365]
		, [CallCycleContact]
	    , [ContactLag]
		, [ClientCount]
		, [DWCreatedDateTime]
		, [DWUpdatedDateTime]
		, [ETLJobProcessRunId]
		, [ETLJobSystemRunId]
)
   SELECT  
          Src.[DimDateKey]
        , Src.[DimClientKey]
        , Src.[DimTeamMemberKey]
		, Src.[DimClientAssetsKey]
		, Src.[DimClientTenureKey]
		, Src.[DimEmployeeKey]
		, Src.[DimEmployeeMgmtKey]
		, Src.[DimAgeGroupKey]
		, Src.[DimKYCStatusKey]
		, Src.[DimPeerGroupKey]
		, Src.[DimCallCycleKey]
		, Src.[ClientId]
		, Src.[ClientNumber]
		, Src.[AssetsUnderManagementUSD]
		, Src.[NetLiquidAssets]
		, Src.[TotalLiquidAssets]
		, Src.[TotalLiabilities]
		, Src.[NetWorthUSD]
		, Src.[ContactLast90]
		, Src.[BioReviewLast365]
		, Src.[NetworkReviewLast365]
		, Src.[SuitWizReviewLast365]
		, Src.[CallCycleContact]
	    , Src.[ContactLag]
		, 1
		, @DWUpdatedDateTime AS DWCreatedDateTime
		, @DWUpdatedDateTime AS DWUpdatedDateTime
		, @ETLJobProcessRunId AS ETLJobProcessRunId
		, @ETLJobSystemRunId AS ETLJobSystemRunId
     FROM #FactClientSnapshotDaily_Temp Src
     LEFT
     JOIN [FDW].[FactClientSnapshotDaily] Tgt
       ON Src.DimDateKey = Tgt.DimDateKey
      AND Src.ClientID = Tgt.ClientID
    WHERE Tgt.ClientID IS NULL
   OPTION (Label = 'FDW.FactClientSnapshotDaily-Insert-Query')

     EXEC MDR.spGetRowCountByQueryLabel 'FDW.FactClientSnapshotDaily-Insert-Query', @InsertCount OUT

      SET @EndTime = GETDATE()
      SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

     EXEC MDR.spProcessTaskLogInsertRowCount
		  @ETLJobProcessRunId 
		, @ComponentName
		, @Source 
		, @Target 
		, @InsertCount	 
        , @DurationInSeconds



COMMIT TRANSACTION

END TRY

BEGIN CATCH 

	-- An error occurred; simply rollback the transaction started at the beginnig the of the procedure.
	ROLLBACK TRANSACTION;

	SET @Status = 0
	SET @ErrorMessage = CONCAT('FDW.FactClientSnapshotDaily:', ERROR_MESSAGE())

END CATCH 

--Drop temp tables to free up Temp DB space
IF OBJECT_ID('tempdb..#DimClientandAgeGroupKeys') IS NOT NULL DROP TABLE #DimClientandAgeGroupKeys
IF OBJECT_ID('tempdb..#DailyEmployeeKeys') IS NOT NULL DROP TABLE #DailyEmployeeKeys
IF OBJECT_ID('TEMPDB..#FactClientSnapshotDaily_Temp') IS NOT NULL DROP TABLE #FactClientSnapshotDaily_Temp
IF OBJECT_ID('TEMPDB..#SWKYC') IS NOT NULL DROP TABLE #SWKYC

SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

END
GO
