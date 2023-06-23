DECLARE @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
DECLARE @YESTERDAY DATE =  DateAdd(DAY,-1,@TODAY)

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

SELECT TOP 1 @UnknownNumberValue = CONVERT(INT, DF.[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownNumberValue' 

SELECT TOP 1 @DefaultNumberValue = CONVERT(INT, DF.[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'DefaultNumberValue'

SELECT TOP 1 @UnknownTextValue = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValue'

SELECT TOP 1 @UnknownTextValueAbbreviated = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValueAbbreviated'

SELECT TOP 1 @NotAvailableTextValue = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotAvailableTextValue'

SELECT TOP 1 @NotAvailableTextValueAbbreviated = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotAvailableTextValueAbbreviated'

SELECT TOP 1 @NotApplicableTextValue = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'NotApplicableTextValue'

SELECT TOP 1 @MinDateValue = CONVERT(DATE, DF.[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MinDateValue'

SELECT TOP 1 @MaxDateValue = CONVERT(DATE, DF.[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MaxDateValue'

SELECT TOP 1 @UnknownGuid = DF.[Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownGuid'

SELECT TOP 1 @DefaultMoneyValue = CONVERT(MONEY,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'DefaultMoneyValue'

 
IF OBJECT_ID('tempdb..#ContactAttributes') IS NOT NULL DROP TABLE #ContactAttributes

CREATE TABLE #ContactAttributes (
	[HouseholdUID] NVARCHAR(255) NULL,
	[HouseholdId] NVARCHAR(255) NULL,
    [ClientID] UNIQUEIDENTIFIER NULL,
	[ClientNumber] INT NULL,	
	[ServiceProduct] NVARCHAR(255) NULL,
	[ClientType] NVARCHAR(100) NULL,
	[Bday] DATE,
	[EffectiveStartDate] DATETIME,
	[EffectiveEndDate] DATETIME,
	[CurrentRecord] INT NULL 
)
WITH (DISTRIBUTION = HASH(ClientID), CLUSTERED COLUMNSTORE INDEX) 


IF OBJECT_ID('tempdb..#DailyCallCycleAttributes') IS NOT NULL DROP TABLE #DailyCallCycleAttributes

CREATE TABLE #DailyCallCycleAttributes (
	[CalendarDate] DATE, 
	[HouseholdUID] NVARCHAR(255),
	[HouseholdId] NVARCHAR(255),
    [ClientID] UNIQUEIDENTIFIER,
	[ClientNumber] INT,	
	[ClientPartitionKey] NVARCHAR(255),
	[ClientAge] INT,
	[ServiceProduct] NVARCHAR(255),
	[ClientType] NVARCHAR(100),	
	[WealthbuilderCallCycle] INT,
	[WealthbuilderOver60CallCycle] INT,
	[TradingClientCallCycle] INT, 
	[PCCallCycle] INT, 
	[ExtendedCallCycle] INT,
	[HAUM_HLNW] INT,
	[CustomCallCycle] INT,
	[EffectiveStartDate] DATETIME,
	[EffectiveEndDate] DATETIME,
	[CurrentRecord] INT NULL 
)
WITH (DISTRIBUTION = HASH(ClientID), CLUSTERED COLUMNSTORE INDEX) 

IF OBJECT_ID('tempdb..#DailyCallCycle') IS NOT NULL DROP TABLE #DailyCallCycle

CREATE TABLE #DailyCallCycle (
	[CalendarDate] DATE, 
	[HouseholdUID] NVARCHAR(255),
	[HouseholdId] NVARCHAR(255),
    [ClientID] UNIQUEIDENTIFIER,
	[ClientNumber] INT,	
	[ClientPartitionKey] NVARCHAR(255),
	[WealthbuilderCallCycle] INT,
	[WealthbuilderOver60CallCycle] INT,
	[TradingClientCallCycle] INT, 
	[PCCallCycle] INT, 
	[ExtendedCallCycle] INT,
	[HAUM_HLNW] INT,
	[CustomCallCycle] INT,
	[FinalCallCycle] INT
)
WITH (DISTRIBUTION = HASH(ClientID), CLUSTERED COLUMNSTORE INDEX) 


IF OBJECT_ID('tempdb..#DailyActivities') IS NOT NULL DROP TABLE #DailyActivities

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


IF OBJECT_ID('tempdb..#DailyCallCycleContact') IS NOT NULL DROP TABLE #DailyCallCycleContact

CREATE TABLE #DailyCallCycleContact (
	  DimDateKey Int
	, CalendarDate Date
	, ClientId UniqueIdentifier
	, ClientNumber Int
	, FinalCallCycle Int 
	, DimCallCycleKey Int
	, CallCycleContact Int
	, ContactLag Int
)
WITH
(
	DISTRIBUTION = HASH ([ClientID]), 
	HEAP		
)


;WITH IrisHistory AS ( 

  SELECT ISNULL(SFDC.HouseholdUID, @UnknownTextValue) AS HouseholdUID
       , ISNULL(SFDC.HouseholdId, @UnknownTextValue) AS HouseholdId
	   , CA.[fi_contactid] AS ClientId
       , CB.fi_Id_Search AS ClientNumber
	   , SP.[fi_Name] AS ServiceProduct
	   , CType.[Value] AS ClientType	 
	   , CONVERT(DATE, CB.fi_birthdate) AS Bday
	   , CONVERT(DATETIME, CA.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedOn  --CONVERT DATES TO PST
	   , ROW_NUMBER() OVER (PARTITION BY CA.[fi_contactid], CONVERT(DATE,  CA.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') ORDER BY CA.fi_Id DESC, CONVERT(DATETIME, CA.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') DESC) AS DayRowNum

	FROM Iris.fi_contactauditlogBase AS CA	
	
    JOIN Iris.ContactBase AS CB ON CA.fi_ContactId = CB.ContactId   

	--INNER JOIN TO RM ENTITY TO ONLY INCLUDE CLIENTS HAVE HAVE BEEN WITIN RELATIONSHIP MANAGEMENT
    JOIN Iris.fi_relationshipmanagementBase AS RM 
      ON CB.ContactId = RM.fi_ContactId

    LEFT 
    JOIN Iris.fi_serviceproductBase AS SP 
      ON SP.fi_serviceproductId = CA.fi_serviceproductid

    LEFT 
    JOIN Iris.StringMapBase CType  
      ON CType.AttributeValue = CA.fi_customertypecode
     AND CType.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND CType.AttributeName = 'fi_customertypecode'

    LEFT 
    JOIN REF.CRMClientMapping AS SFDC 
      ON CB.fi_Id_search = SFDC.ClientNumber_IRIS

   --TESTING: TO BE REMOVED!!
   --WHERE CB.fi_Id_Search = 3999732


) 

, LastDailyChange AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId
	   , ClientNumber
	   , ServiceProduct
	   , ClientType
	   , Bday
	   , CreatedOn
	FROM IrisHistory  
   WHERE DayRowNum = 1 --LAST CHANGE IN DAY 

) 

, AttributeGroups AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId
	   , ClientNumber
	   , ServiceProduct
	   , COUNT(ServiceProduct) OVER (PARTITION BY ClientID ORDER BY CreatedOn) AS GrpServiceProduct
	   , ClientType
	   , COUNT(ClientType) OVER (PARTITION BY ClientID ORDER BY CreatedOn) AS GrpClientType
	   , Bday
	   , CONVERT(DATE,CreatedOn) CreatedOn
	FROM LastDailyChange  

) 

, ClientHistory AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientID 
	   , ClientNumber
	   , FIRST_VALUE(ServiceProduct) OVER (PARTITION BY ClientID, GrpServiceProduct ORDER BY CreatedOn) AS ServiceProduct
	   , FIRST_VALUE(ClientType) OVER (PARTITION BY ClientID, GrpClientType ORDER BY CreatedOn) AS ClientType
	   , Bday
	   , CreatedOn
    FROM AttributeGroups

)

, ChangeTracking AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId 
	   , ClientNumber
	   , ServiceProduct
	   , ClientType
	   , Bday
	   , CreatedOn
       , HASHBYTES('SHA2_256', CONCAT(ServiceProduct, '|', ClientType)) AS RowHash 
  
    FROM ClientHistory 

)

, PriorRowHash AS (

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId 
	   , ClientNumber
	   , ServiceProduct
	   , ClientType
	   , Bday
	   , CreatedOn
       , RowHash 
	   , LAG (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId ORDER BY CreatedOn) AS PriorRowHash
    FROM ChangeTracking

)

, DistinctChanges AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId 
	   , ClientNumber
	   , ISNULL(ServiceProduct, @UnknownTextValue) AS ServiceProduct
	   , ISNULL(ClientType, @UnknownTextValue) AS ClientType
	   , Bday
       , RowHash 
	   , CreatedOn AS EffectiveStartDate
       , LEAD(CreatedOn, 1, @MaxDateValue) OVER(PARTITION BY ClientID ORDER BY CreatedOn) AS EffectiveEndDate
       , CASE WHEN LEAD(CreatedOn, 1, @MaxDateValue) OVER(PARTITION BY ClientID ORDER BY CreatedOn) = @MaxDateValue THEN 1 ELSE 0 END AS CurrentRecord 
    FROM PriorRowHash AS PRH
   WHERE RowHash <> PriorRowHash

)

  INSERT 
    INTO #ContactAttributes (
         HouseholdUID
       , HouseholdId
	   , ClientID 
	   , ClientNumber
	   , ServiceProduct
	   , ClientType
	   , Bday
	   , EffectiveStartDate
       , EffectiveEndDate
	   , C.CurrentRecord 
  )


  SELECT C.HouseholdUID
       , C.HouseholdId
	   , C.ClientID 
	   , C.ClientNumber
	   , C.ServiceProduct
	   , C.ClientType
	   , C.Bday
	   , C.EffectiveStartDate
       , C.EffectiveEndDate
       , C.CurrentRecord 

    FROM DistinctChanges AS C	


   INSERT
     INTO #DailyCallCycleAttributes (
		  CalendarDate
		, HouseholdUID
		, HouseholdId 
		, ClientID
		, ClientNumber
		, ClientPartitionKey
		, ClientAge
		, ServiceProduct
		, ClientType
		, WealthbuilderCallCycle
		, WealthbuilderOver60CallCycle
		, TradingClientCallCycle
		, EffectiveStartDate
		, EffectiveEndDate
		, CurrentRecord		 
   
   )
   
   SELECT CalendarDate
        , HouseholdUID	
        , HouseholdId	
        , ClientID	
        , ClientNumber	
		, ISNULL(CASE WHEN HouseholdUID <> '[Unknown]' THEN HouseholdUID ELSE NULL END, ClientID) AS ClientPartitionKey
		, FLOOR(DATEDIFF(D, BDay, CalendarDate)/365.25) AS ClientAge        
		, ServiceProduct	
        , ClientType       
		, CASE WHEN ServiceProduct = 'WealthBuilder' THEN 180 ELSE NULL END AS WealthBuilderCallCycle
		, CASE WHEN ServiceProduct = 'WealthBuilder' AND FLOOR(DATEDIFF(D, BDay, CalendarDate)/365.25) >= 60 THEN 90 ELSE NULL END AS WealthBuilderOver60CallCycle
		, CASE WHEN ClientType = 'Client - Trading' THEN 90 ELSE NULL END AS TradingClientCallCycle
		, EffectiveStartDate
		, EffectiveEndDate
		, CurrentRecord		
     FROM #ContactAttributes
	CROSS JOIN FDW.DIMDATE
	WHERE ClientType IN ('Deceased Client','Client - Trading','Client - Non Trading') --LIMIT TO ACTIVE CLIENTS ONLY 
      AND CalendarDate >= EffectiveStartDate 
	  AND CalendarDate < CASE WHEN EffectiveEndDate < @TODAY 
							  THEN EffectiveEndDate
                              ELSE @TODAY 
                          END


						  
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
	   
	    --TESTING: TO BE REMOVED!!
       --and CB.fi_Id_Search = 3999732
) 

, PCCallCycles AS ( 

	SELECT CCA.CalendarDate	
		 , CCA.HouseholdUID	
		 , CCA.HouseholdId	
		 , CCA.ClientID	
		 , CCA.ClientNumber	
		 , CCA.ClientPartitionKey	
		 , CASE 
				WHEN CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) AND CCA.CalendarDate <= DATEADD(D, 90, CONVERT(DATE, ST.StartDate)) THEN 30
				WHEN CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) AND CCA.CalendarDate <= DATEADD(D, 180, CONVERT(DATE, ST.StartDate)) THEN 60
		   END AS PCCallCycle
	  FROM #DailyCallCycleAttributes AS CCA
	  --INNER JOIN SO WE ONLY RETURN DAYS WHERE PC ATTRIBUTE WAS ACTIVE 
	  JOIN CRMServiceTypes AS ST
		ON CCA.ClientId = ST.ClientId 
	   AND CCA.CalendarDate >= CONVERT(DATE, ST.StartDate) 
	   AND CCA.CalendarDate <= CONVERT(DATE, ST.EndDate)

)


, CallCycleRowNum AS ( 

    SELECT CalendarDate	
		 , HouseholdUID	
		 , HouseholdId	
		 , ClientID	
		 , ClientNumber	
		 , ClientPartitionKey		
		 , PCCallCycle
		 , ROW_NUMBER() OVER (PARTITION BY ClientID, CalendarDate ORDER BY PCCallCycle) AS RowNum
      FROM PCCallCycles 
     
)

, PCCallCyclesFinal AS ( 

    SELECT CalendarDate	
		 , HouseholdUID	
		 , HouseholdId	
		 , ClientID	
		 , ClientNumber	
		 , ClientPartitionKey		
		 , PCCallCycle
      FROM CallCycleRowNum
     WHERE RowNum = 1 --IF MULTIPLE CALL CYCLES ON THE SAME DAY PER CLIENT, USE LOWEST CALL CYCLE
	   AND PCCallCycle IS NOT NULL

) 

/*
	UPDATE PC CALL CYCLES  
*/

	UPDATE #DailyCallCycleAttributes
	   SET PCCallCycle = SRC.PCCallCycle
	  FROM PCCallCyclesFinal AS SRC
	  JOIN #DailyCallCycleAttributes AS TGT
	    ON SRC.CalendarDate = TGT.CalendarDate
	   AND SRC.ClientPartitionKey = TGT.ClientPartitionKey 


/*
	UPDATE HAUM/HLNW CALL CYCLES AS DEFINED IN IC CALL CYCLES CONFLUENCE DOC WITHIN PCG BI COMMUNITY PAGE 
*/
	
	UPDATE #DailyCallCycleAttributes
	   SET HAUM_HLNW = CASE WHEN SRC.ClientAssetsType = 'HAUM' THEN 60 WHEN SRC.ClientAssetsType = 'HLNW' THEN 75 END 
	  FROM REF.vwDailyClientAssets AS SRC
	  JOIN #DailyCallCycleAttributes AS TGT
	    ON SRC.ClientId = TGT.ClientId
	   AND SRC.CalendarDate = TGT.CalendarDate 
	 WHERE SRC.ClientAssetsType IN ('HAUM', 'HLNW')

	 --TESTING: TO BE REMOVED!!
	 --AND SRC.ClientNumber = 3999732





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
		 , CCA.HouseholdUID	
		 , CCA.HouseholdId	
		 , CCA.ClientID	
		 , CCA.ClientNumber	
		 , CCA.ClientPartitionKey	
		 , CC.CallCycle
		 , ROW_NUMBER() OVER (PARTITION BY CCA.ClientID, CCA.CalendarDate ORDER BY CC.CallCycle) AS RowNum
	  FROM #DailyCallCycleAttributes AS CCA
	  --INNER JOIN SO WE ONLY RETURN DAYS WHERE CUSTOM CALL CYCLE WAS ACTIVE 
	  JOIN CustomCallCycles AS CC
		ON CCA.ClientId = CC.ClientId 
	   AND CCA.CalendarDate >= CONVERT(DATE, CC.StartDate) 
	   AND CCA.CalendarDate <= CONVERT(DATE, CC.EndDate)

) 

, CustomCallCycleFinal AS ( 

    SELECT CalendarDate	
		 , HouseholdUID	
		 , HouseholdId	
		 , ClientID	
		 , ClientNumber	
		 , ClientPartitionKey	
		 , CallCycle
	  FROM DupeProtect
	 WHERE RowNum = 1 

) 

/*
	UPDATE CUSTOM CALL CYCLES  
*/

	UPDATE #DailyCallCycleAttributes
	   SET CustomCallCycle = SRC.CallCycle
	  FROM CustomCallCycleFinal AS SRC
	  JOIN #DailyCallCycleAttributes AS TGT
	    ON SRC.CalendarDate = TGT.CalendarDate
	   AND SRC.ClientPartitionKey = TGT.ClientPartitionKey 



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
	 WHERE TeamSpecialty = 'Test - Extended Call Cycle'

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
		 , CCA.HouseholdUID	
		 , CCA.HouseholdId	
		 , CCA.ClientID	
		 , CCA.ClientNumber	
		 , CCA.ClientPartitionKey	
		 , 180 AS ExtendedCallCycle --180 IS THE EXTENDED CALL CYCLE TEST DURATION
	  FROM #DailyCallCycleAttributes AS CCA
	  JOIN ExtendedCallCycleWindows AS ECC
		ON CCA.ClientId = ECC.ClientId 
	   AND CCA.CalendarDate >= CONVERT(DATE, ECC.ExtendedCallCycleStart) 
	   AND CCA.CalendarDate <= CONVERT(DATE, ECC.ExtendedCallCycleEnd)

)


/*
	UPDATE CUSTOM CALL CYCLES  
*/

	UPDATE #DailyCallCycleAttributes
	   SET ExtendedCallCycle = SRC.ExtendedCallCycle
	  FROM ExtendedCallCycleFinal AS SRC
	  JOIN #DailyCallCycleAttributes AS TGT
	    ON SRC.CalendarDate = TGT.CalendarDate
	   AND SRC.ClientPartitionKey = TGT.ClientPartitionKey 


/*
	FINAL CALL CYCLE INSERT
*/

    INSERT 
      INTO #DailyCallCycle (
		   CalendarDate
		 , HouseholdUID
		 , HouseholdId
		 , ClientID
		 , ClientNumber	
		 , ClientPartitionKey
		 , WealthbuilderCallCycle
		 , WealthbuilderOver60CallCycle
		 , TradingClientCallCycle
		 , PCCallCycle
		 , ExtendedCallCycle
		 , HAUM_HLNW
		 , CustomCallCycle
		 , FinalCallCycle
)

 
    SELECT CalendarDate	
		 , HouseholdUID	
		 , HouseholdId	
		 , ClientID	
		 , ClientNumber	
		 , ClientPartitionKey	
		 , WealthbuilderCallCycle
		 , WealthbuilderOver60CallCycle
		 , TradingClientCallCycle
		 , PCCallCycle
		 , ExtendedCallCycle
		 , HAUM_HLNW
		 , CustomCallCycle
         , CASE 
				WHEN CustomCallCycle IS NOT NULL THEN CustomCallCycle
				WHEN PCCallCycle IS NOT NULL THEN PCCallCycle
				WHEN WealthbuilderOver60CallCycle IS NOT NULL THEN WealthbuilderOver60CallCycle
				WHEN WealthbuilderCallCycle IS NOT NULL THEN WealthbuilderCallCycle
				WHEN HAUM_HLNW IS NOT NULL THEN HAUM_HLNW
				WHEN ExtendedCallCycle IS NOT NULL THEN ExtendedCallCycle
				WHEN TradingClientCallCycle IS NOT NULL THEN TradingClientCallCycle
				ELSE 90 --DEFAULT CALL CYCLE
	       END AS FinalCallCycle
      FROM #DailyCallCycleAttributes




-- Combining IRIS Activities with SFDC Interactions. Later used for Contact Rates and Bio/Network Review rates
;WITH IrisActivitiesPivoted AS (
 
	 SELECT  RM.fi_fi_relationshipmanagementauditlogId AS InteractionId
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
		FROM IrisActivitiesPivoted P

		JOIN Iris.fi_topicBase TB 
		  ON TB.fi_topicId = CASE WHEN P.ActivityID = '' THEN @UnknownGuid ELSE P.ActivityID END
		
		JOIN [Iris].[SystemUserBase] UB1 
		  ON UB1.SystemUserId = P.createdby
		
		LEFT 
		JOIN REF.PcgSfMapping SFM 
		  ON TB.fi_ID = SFM.OldCode
	   
	   WHERE TB.fi_ID IN (102072, 102073, 102075, 102076, 112800,106707,106731) -- Activities from Iris

		 
)
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

		-- DimClientHouseholdKey
		LEFT
		JOIN FDW.DimClient DCH
		  ON I.AccountId = DCH.HouseholdUID
		 AND DD.CalendarDate >= DCH.EffectiveStartDate
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



      INSERT 
	    INTO #DailyCallCycleContact (
		     DimDateKey
		   , CalendarDate
		   , ClientId
		   , ClientNumber
		   , FinalCallCycle
		   , DimCallCycleKey
		   , CallCycleContact
		   , ContactLag
	  )


	  SELECT DD.DimDateKey
	       , DC.CalendarDate
		   , DC.ClientId
		   , DC.ClientNumber
		   , DC.FinalCallCycle
		   , CC.DimCallCycleKey
		   , MAX(CASE 
					WHEN DA.SFValue in ('Inbound Phone Call','In-Person','Outbound Phone Call','Virtual')--Contacts
					 AND PrimaryICFlag = 1
					 AND DATEDIFF(DD, DA.CreatedOn, DC.CalendarDate) BETWEEN 0 AND DC.FinalCallCycle
			        THEN 1 
			        ELSE 0 
			   END) CallCycleContact
		   , DATEDIFF(DD, MAX(CASE 
				WHEN DA.SFValue in ('Inbound Phone Call','In-Person','Outbound Phone Call','Virtual')--Contacts
				 AND PrimaryICFlag = 1
				 AND CONVERT(DATE, DA.CreatedOn) <= DC.CalendarDate
			    THEN  DA.CreatedOn
			 END), DC.CalendarDate) AS ContactLag 
		FROM #DailyCallCycle DC
		LEFT 
		JOIN #DailyActivities DA 
		  ON DC.ClientId = DA.ClientId
		LEFT
		JOIN FDW.DimCallCycle AS CC
		  ON DC.FinalCallCycle = CC.CallCycle
		LEFT
		JOIN FDW.DimDate AS DD
		  ON DD.CalendarDate = DC.CalendarDate
	   GROUP 
		  BY DD.DimDateKey
		   , DC.CalendarDate
		   , DC.ClientId
		   , DC.ClientNumber
		   , DC.FinalCallCycle
		   , CC.DimCallCycleKey


/*
	UPDATE KEYS AND MEASURES IN FACT TABLE
*/

	UPDATE FDW.FactClientSnapshotDaily
	   SET DimCallCycleKey = SRC.DimCallCycleKey
	     , ContactLag = SRC.ContactLag
		 , CallCycleContact = SRC.CallCycleContact
	  FROM #DailyCallCycleContact AS SRC
	  JOIN FDW.FactClientSnapshotDaily AS TGT
	    ON SRC.DimDateKey = TGT.DimDateKey
	   AND SRC.ClientId = TGT.ClientId



