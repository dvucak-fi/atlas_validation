IF OBJECT_ID ('TEMPDB..#AssignmentHistoryIC') IS NOT NULL
    DROP TABLE #AssignmentHistoryIC

CREATE TABLE #AssignmentHistoryIC (
	[HouseholdUID] [nvarchar](4000) NULL,
	[HouseholdID] [nvarchar](4000) NULL,
	[ClientId] [uniqueidentifier] NULL,
	[ClientNumber] [int] NULL,
	[AssignmentType] [nvarchar](255) NULL,
	[SystemUserId_IRIS] [uniqueidentifier] NULL,
	[SystemUserId_SFDC] [nvarchar](18)  NULL,	
	[AssignedToFullName] [nvarchar](255) NULL,
	[AssignedToActiveDirectoryUserIdWithDomain] [nvarchar](255) NULL,
	[AssignmentStartDate] [datetime] NULL,
	[AssignmentEndDate] [datetime] NULL,
	[AssignmentOrder] [int] NULL,
	[IsEndOfDayAssignment] [int] NULL,
	[SystemOfRecord] [nvarchar](25) NULL,
	[DWCreatedDateTime] [datetime] NULL,
	[DWUpdatedDateTime] [datetime] NULL,
	[ETLJobProcessRunId] [uniqueidentifier] NULL,
	[ETLJobSystemRunId] [uniqueidentifier] NULL
)
WITH
(
	DISTRIBUTION = HASH(ClientId),
	HEAP
)
GO

DECLARE @UnknownTextValue NVARCHAR(255) = '[Unknown]'
      , @MinDateValue DATE = '1900-01-01'
      , @MaxDateValue DATE = '9999-12-31'     
      , @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
 
DECLARE @YESTERDAY DATE =  DateAdd(DAY,-1,@TODAY)


IF OBJECT_ID ('TEMPDB..#TermWindows') IS NOT NULL
    DROP TABLE #TermWindows

CREATE TABLE #TermWindows (
	    ClientId UNIQUEIDENTIFIER
      , ClientNumber INT
      , HouseholdUID NVARCHAR(4000)
	  , HouseholdId NVARCHAR(4000)
      , TerminationType NVARCHAR(255)
      , TerminationDate DATETIME
      , TermWindowStartDate DATETIME
      , TermWindowEndDate DATETIME
)
WITH
(
	DISTRIBUTION = HASH (ClientId),
	HEAP
)


;WITH SFDCTerms AS (

       SELECT Id
            , CaseNumber
	        , Termination_date__c AS TerminationDate  
            , COALESCE(FinServ__Household__c, AccountId) AS HouseholdUID
            , Termination_request_type__c AS TerminationTypeDesc
         FROM PcgSf.[Case] C
        WHERE C.IsDeleted = 0
          AND C.[Type] = 'Client Termination'
          AND C.Termination_request_type__c in ('Household - Non-Trading','Household - Trading')
          AND C.Close_Reason__c <> 'Canceled'
			
)

, IRISTerms AS (

       SELECT CB.ContactId AS ClientId 
            , CB.fi_Id_Search AS ClientNumber
            , TB.fi_TerminationTypeCode AS TerminationTypeCode
            , SMB.[Value] AS TerminationTypeDesc
            , TB.fi_TerminationDate AS TerminationDate  
         FROM Iris.fi_terminationBase AS TB  
         JOIN Iris.ContactBase AS CB 
           ON CB.ContactId = TB.fi_ContactId
         LEFT
         JOIN Iris.StringMapBase AS SMB
           ON TB.fi_TerminationTypeCode = SMB.AttributeValue
          AND SMB.AttributeName = 'fi_TerminationTypeCode'
          AND SMB.ObjectTypeCode = 10024 --fi_termination  
        WHERE TB.fi_TerminationTypeCode IN (157610000, 157610002) --CLIENT TRADING AND NON-TRADING TERMINATION 
          AND TB.statuscode = 2 --APPROVED    
)

, TermedClients AS ( 

      SELECT ClientId
           , ClientNumber	
           , HouseholdUID
           , HouseholdID	
           , CASE WHEN TerminationTypeDesc in ('Client - Trading Termination','Household - Trading') THEN 'Trading'
	              ELSE 'Non Trading' 
	              End TerminationType
           , TerminationDate
           , ROW_NUMBER() OVER (PARTITION BY ClientNumber ORDER BY TerminationDate DESC) AS RowNum  
      FROM (
          --IRIS Terms
               SELECT  
				 IRIS.ClientId 
                , IRIS.ClientNumber
				, REF.HouseholdUID
				, REF.HouseholdID
				,TerminationTypeDesc
                , IRIS.TerminationDate  
				FROM IRISTerms IRIS
				LEFT JOIN REF.CRMClientMapping REF ON REF.ClientID_IRIS = IRIS.ClientId

				UNION
           --SFDC Terms
				SELECT  
				  REF.ClientId_IRIS ClientId 
                , REF.ClientNumber_IRIS ClientNumber
				, SFDC.HouseholdUID
				, REF.HouseholdID
				,TerminationTypeDesc
				, SFDC.TerminationDate 
				FROM SFDCTerms SFDC
				LEFT JOIN REF.CRMClientMapping REF ON REF.HouseholdUID = SFDC.HouseholdUID
      ) UnionedTerms 

)


       INSERT
       INTO #TermWindows (
              ClientId 
            , ClientNumber
            , HouseholdUID
			, HouseholdID
			, TerminationType
            , TerminationDate
            , TermWindowStartDate
            , TermWindowEndDate
      ) 

       SELECT ClientId 
            , ClientNumber
            , HouseholdUID
			, HouseholdID
			, TerminationType
            , TerminationDate
            , LAG (TerminationDate, 1, @MinDateValue) OVER (PARTITION BY ClientId ORDER BY TerminationDate) AS TermWindowStartDate
            , TerminationDate AS TermWindowEndDate         
         FROM TermedClients

        UNION 

	   /*
	   		THIS CREATES A FULL TERM WINDOW FORM THE LAST TERM DATE TO 12/31/9999. 
			FOR EXAMPLE, IF A CLIENT TERMED ON 1/1/2010, THE ABOVE WOULD GIVE US 1/1/1900 TO 1/1/2010
			AND THE BELOW WOULD GIVE US 1/1/2010 TO 12/31/9999. WE NEED THIS SO WE CAN IDENTIFY INITIAL ACCOUNT SETUP INCIDENTS
			AFTER A CLIENT TERMS SINCE WE HANDLE TERMINATED CLIENTS SLIGHTLY DIFFRENTLY.
	   */

       SELECT ClientId 
            , ClientNumber
            , HouseholdUID
			, HouseholdID
			, NULL AS TerminationType
            , NULL AS TerminationDate
            , TerminationDate AS TermWindowStart  
            , LEAD (TerminationDate, 1, @MaxDateValue) OVER (PARTITION BY ClientId ORDER BY TerminationDate) AS TermWindowEndDate           
         FROM TermedClients
        WHERE RowNum = 1 



;WITH SDFC_MigrationDates AS ( 

    SELECT AH.Id
         , CRM.HouseholdUID	
         , CRM.HouseholdId	
         , CRM.ClientId_Iris AS ClientId
         , CRM.ClientNumber_Iris AS ClientNumber
         , CreatedDate AS SFDC_StartDate
      FROM PCGSF.AccountHistory AS AH
      JOIN REF.CRMClientMapping AS CRM
        ON AH.AccountId = CRM.HouseholdUID 
     WHERE AH.Field = 'created'

) 

, UserMapping AS ( 

    SELECT WD.EmployeeId
         , WD.PreferredFullName AS EmployeeName
         , SUB.SystemUserId AS SystemUserId_IRIS
         , SUB.IsDisabled 
         , U.Id AS SystemUserId_SFDC
         , U.IsActive
      FROM WD.WDWorkers AS WD
      LEFT
      JOIN Iris.SystemUserBase AS SUB
        ON WD.NetworkUser = SUB.DomainName
      LEFT
      JOIN PCGSF.[User] AS U
        ON TRY_CAST(U.EmployeeNumber AS INT) = WD.EmployeeId 
     WHERE SUB.SystemUserId IS NOT NULL --USER MUST EXIST IN IRIS SYSTEM USER BASE TABLE
       AND U.Id IS NOT NULL --USER MUST ALSO EXIST IN SFDC USER TABLE

) 

, DupeIrisUsers AS ( 

    SELECT SystemUserId_IRIS
      FROM UserMapping
     GROUP 
        BY SystemUserId_IRIS
    HAVING COUNT(1) > 1 

) 

, DupeSFDCUsers AS ( 

    SELECT SystemUserId_SFDC
      FROM UserMapping
     GROUP 
        BY SystemUserId_SFDC
    HAVING COUNT(1) > 1 

) 

, HandleIrisDupes AS ( 

    SELECT UM.EmployeeId	
         , UM.EmployeeName	
         , UM.SystemUserId_IRIS	
         , UM.IsDisabled
         , UM.SystemUserId_SFDC	
         , UM.IsActive
      FROM UserMapping AS UM
      JOIN DupeIrisUsers AS DIU
        ON UM.SystemUserId_IRIS = DIU.SystemUserId_IRIS
     WHERE UM.IsActive = 1
 
     UNION

    SELECT UM.EmployeeId	
         , UM.EmployeeName	
         , UM.SystemUserId_IRIS	
         , UM.IsDisabled    
         , UM.SystemUserId_SFDC	
         , UM.IsActive
      FROM UserMapping AS UM
     WHERE NOT EXISTS (SELECT 1 FROM DupeIrisUsers AS DIU WHERE UM.SystemUserId_IRIS = DIU.SystemUserId_IRIS)

 ) 

, UserMappingFinal AS ( 

    SELECT HID.EmployeeId	
         , HID.EmployeeName	
         , HID.SystemUserId_IRIS	
         , HID.IsDisabled
         , HID.SystemUserId_SFDC	
         , HID.IsActive
      FROM HandleIrisDupes AS HID
      JOIN DupeSFDCUsers AS DSU
        ON HID.SystemUserId_SFDC = DSU.SystemUserId_SFDC
     WHERE HID.IsDisabled = 0 --NOT DISABLED 
 
     UNION 

    SELECT HID.EmployeeId	
         , HID.EmployeeName	
         , HID.SystemUserId_IRIS	
         , HID.IsDisabled    
         , HID.SystemUserId_SFDC	
         , HID.IsActive
      FROM HandleIrisDupes AS HID
     WHERE NOT EXISTS (SELECT 1 FROM DupeSFDCUsers AS DSU WHERE HID.SystemUserId_SFDC = DSU.SystemUserId_SFDC)

) 

/*
    IRIS ASSIGNMENT HISTORY - CAP HISTORY @ SFDC ACCOUNT CREATION DATE
*/

, RMAssignments AS (

    SELECT RM.fi_Id_Search AS RelationshipManagementAuditLogId	
         , CRM.HouseholdUID	
         , CRM.HouseholdId	
         , CB.ContactId AS ClientId
         , CB.fi_id_Search AS ClientNumber
		 , RM.fi_AssignedToUserId AS AssignedToSystemUserId 
         , SM.[Value] AS AssignmentType  
		 , CONVERT(DATETIME, RM.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedOn

      FROM Iris.ContactBase AS CB
  
      JOIN Iris.fi_fi_relationshipmanagementauditlogBase AS RM
        ON CB.ContactId = RM.fi_ContactId 	

      LEFT
      JOIN REF.CRMClientMapping AS CRM
        ON CB.fi_id_Search = CRM.ClientNumber_Iris 

	  LEFT
      JOIN Iris.StringMapBase AS SM
        ON RM.fi_TypeCode = SM.AttributeValue 
       AND SM.AttributeName = 'fi_TypeCode'
       AND SM.ObjectTypeCode = 10073 --fi_fi_relationshipmanagementauditlogBase

      LEFT
      JOIN SDFC_MigrationDates AS SFDC
        ON CB.ContactId = SFDC.ClientId
		
	 WHERE RM.fi_TypeCode IS NOT NULL  --Must have an RM record	 
       AND RM.fi_AssignedToUserId IS NOT NULL
       AND CONVERT(DATE, RM.CreatedOn) < CONVERT(DATE, ISNULL(SFDC.SFDC_StartDate, @MaxDateValue)) --LIMIT ASSIGNMENT HISTORY TO RECORDS PRIOR TO SFDC
	   AND RM.CreatedOn < @TODAY  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST 

)	

, RowHash AS ( 

	SELECT RelationshipManagementAuditLogId
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
		 , AssignedToSystemUserId
         , AssignmentType   
         , HASHBYTES ('SHA2_256', CONCAT(AssignedToSystemUserId, '|', AssignmentType)) AS RowHash
		 , RM.CreatedOn	
	  FROM RMAssignments AS RM

) 

, PreviousRowHash  AS (

	SELECT RelationshipManagementAuditLogId
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
		 , AssignedToSystemUserId
         , AssignmentType     
		 , RowHash
		 , LAG(RowHash, 1, HASHBYTES('SHA2_256', @UnknownTextValue)) OVER (PARTITION BY ClientId ORDER BY CreatedOn, RelationshipManagementAuditLogId DESC) AS PreviousRowHash
		 , CreatedOn
	  FROM RowHash AS RM 	 

)

, AssignmentHistoryIris AS (

	SELECT PRH.HouseholdUID	
         , PRH.HouseholdId	
         , PRH.ClientId
         , PRH.ClientNumber
         , PRH.AssignmentType  
		 , PRH.AssignedToSystemUserId AS SystemUserId_IRIS
         , UM.SystemUserId_SFDC
         , SUB.FullName AS AssignedToFullName
         , SUB.DomainName AS AssignedToActiveDirectoryUserIdWithDomain      
		 , PRH.CreatedOn AS AssignmentStartDate
         , ROW_NUMBER() OVER (PARTITION BY PRH.ClientId ORDER BY PRH.CreatedOn, PRH.RelationshipManagementAuditLogId) AS AssignmentOrder
         , 'Iris' AS SystemOfRecord
	  FROM PreviousRowHash PRH
      LEFT
      JOIN Iris.SystemUserBase AS SUB
        ON PRH.AssignedToSystemUserId = SUB.SystemUserId
      LEFT
      JOIN UserMappingFinal AS UM
        ON SUB.SystemUserId = UM.SystemUserId_IRIS
     WHERE RowHash <> PreviousRowHash  --Limit to distinct changes only
     
)

/*
    11/30/2023 - PER PRODUCT OWNER: SFDC DOES NOT HAVE PRE-ASSIGNMENT IDENTIFIER. WE NEED TO USE LOGIC TO DETERMINE 
    IF CLIENT IS A PRE-ASSIGNMENT. WE LOOK FOR CLIENTS WITH AN OPEN SFDC ONBOARDING CASE PRIOR
    TO THE CONTRACT DATE. 

    WE ALSO DON'T HAVE THE ABILITY TO IDENTIFY ROUND TRIP CLIENTS WITHIN SFDC. THAT BEING SAID, 
    WE CANNOT JUST LOOK FOR ONBOARDING CASES PRIOR TO A CONTRACT DATE AS WE HAVE TO LOOK AT A CLIENT'S LIFECYCLE
    TO IDENTIFY ONBOARDING/CONTRACT DATES WITHIN THE LIFECYCLE.
*/

, ClientLifecycles AS ( 

    SELECT CM.ClientId_Iris AS ClientId
         , CM.ClientNumber_Iris AS ClientNumber
         , CM.HouseholdUID
         , CM.HouseholdID
         , TW.TermWindowStartDate
         , TW.TermWindowEndDate
      FROM REF.CRMClientMapping AS CM
      JOIN #TermWindows AS TW
        ON TW.HouseholdUID = CM.HouseholdUID 

     UNION ALL --UNION NON TERMS

    SELECT CM.ClientId_Iris AS ClientId
         , CM.ClientNumber_Iris AS ClientNumber
         , CM.HouseholdUID
         , CM.HouseholdID
         , @MinDateValue AS TermWindowStartDate
         , @MaxDateValue AS TermWindowEndDate
      FROM REF.CRMClientMapping AS CM
     WHERE NOT EXISTS (SELECT 1 FROM #TermWindows AS TW WHERE CM.HouseholdUID = TW.HouseholdUID)

)

, SFDC_OnboardingCases AS ( 

    SELECT Id AS CaseId
         , CaseNumber
         , COALESCE([FinServ__Household__c],[AccountId]) AS HouseholdUID
         , IsDeleted
         , [Status] AS CaseStatus
         , CreatedDate AS OnboardingDate
         , LastModifiedDate
      FROM PCGSF.[CASE]
     WHERE [Type] = 'Onboarding'

)

, SFDC_ContractDates AS ( 

    SELECT FinServ__Household__c AS HouseholdUID
         , FIN_Account_Number__c AS FinAccountNumber
         , Contract_Date__c AS ContractDate
      FROM PCGSF.FinServ__FinancialAccount__c
     WHERE Contract_Date__c IS NOT NULL

) 

, SFDC_AuditHistory AS ( 

    SELECT AH.Id AS AuditLogId
         , CRM.HouseholdUID	
         , CRM.HouseholdId	
         , CRM.ClientId_Iris AS ClientId
         , CRM.ClientNumber_Iris AS ClientNumber
         , AH.OldValue AS OldUserId
         , AH.NewValue AS NewUserId
         , AH.CreatedDate
         , ROW_NUMBER() OVER (PARTITION BY AH.AccountId ORDER BY AH.CreatedDate, AH.Id COLLATE Latin1_General_100_BIN2_UTF8) AS RowNum
      FROM PCGSF.AccountHistory AS AH
      JOIN REF.CRMClientMapping AS CRM
        ON AH.AccountId = CRM.HouseholdUID      
      JOIN SDFC_MigrationDates AS SFDC
        ON AH.AccountId = SFDC.HouseholdUID
     WHERE Field = 'Investment_Counselor__c'
       AND DataType = 'EntityId'
	   AND AH.CreatedDate >= SFDC_StartDate --EVENT HISTORY MUST BE AFTER ACCOUNT CREATION DATE 
       AND AH.CreatedDate < @TODAY  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST      

)

, FullAuditHistory AS ( 

    SELECT AH.AuditLogId
         , AH.HouseholdUID	
         , AH.HouseholdId	
         , AH.ClientId
         , AH.ClientNumber
         , AH.OldUserId
         , ISNULL(SFDC.SFDC_StartDate, AH.CreatedDate) AS CreatedDate
      FROM SFDC_AuditHistory AS AH
      LEFT
      JOIN SDFC_MigrationDates AS SFDC
        ON AH.HouseholdUID = SFDC.HouseholdUID
     WHERE AH.RowNum = 1

     UNION 

    SELECT AuditLogId
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , NewUserId
         , CreatedDate
      FROM SFDC_AuditHistory
 
 )

 , AuditHistoryCleansed AS ( 

    SELECT FAH.AuditLogId	
         , FAH.HouseholdUID	
         , FAH.HouseholdId	
         , FAH.ClientId
         , FAH.ClientNumber
		 , CASE 
			  WHEN CONVERT(DATE, OC.OnboardingDate) <= CONVERT(DATE, CD.ContractDate) --PER SCOTT CROCKER & RACHEL STEAKLEY. PRE-ASSIGNMENT FLAG DOES NOT EXIST IN SFDC - USE THIS FOR NOW.
			  THEN 'Prospect Assignment' 
			  ELSE 'Client Assignment'
		   END AS AssignmentType
         , OldUserId AS AssignedToUserId
		 , CONVERT(DATETIME, CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedDate                 
         , ROW_NUMBER() OVER (PARTITION BY FAH.HouseholdUID ORDER BY FAH.CreatedDate, FAH.AuditLogId COLLATE Latin1_General_100_BIN2_UTF8) AS AssignmentOrder
      FROM FullAuditHistory AS FAH
      LEFT
      JOIN ClientLifecycles AS CL
        ON CL.HouseholdUID = FAH.HouseholdUID
       AND CL.TermWindowStartDate <= FAH.CreatedDate
       AND CL.TermWindowEndDate > FAH.CreatedDate
      LEFT
      JOIN SFDC_OnboardingCases AS OC
        ON CL.HouseholdUID = OC.HouseholdUID
       AND CL.TermWindowStartDate <= OC.OnboardingDate
       AND CL.TermWindowEndDate > OC.OnboardingDate
      LEFT
      JOIN SFDC_ContractDates AS CD
        ON CL.HouseholdUID = CD.HouseholdUID
       AND CL.TermWindowStartDate <= CD.ContractDate
       AND CL.TermWindowStartDate > CD.ContractDate
     WHERE OldUserId IS NOT NULL --IGNORE RECORDS WHERE NO USER WAS ASSIGNED 

)

, SFDC_RowHash AS ( 

    SELECT AuditLogId	
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , AssignedToUserId	
         , HASHBYTES ('SHA2_256', CONCAT(AssignedToUserId, '|', AssignmentType)) AS RowHash
         , CreatedDate
         , AssignmentOrder
      FROM AuditHistoryCleansed 

)

, SFDC_PrevRowHash AS ( 

    SELECT AuditLogId	
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , AssignedToUserId	
         , RowHash
         , LAG(RowHash, 1, HASHBYTES('SHA2_256', @UnknownTextValue)) OVER (PARTITION BY HouseholdUID ORDER BY CreatedDate, AssignmentOrder) AS PrevRowHash
         , CreatedDate
      FROM SFDC_RowHash 

)

, AssignmentHistorySFDC AS ( 

    SELECT PRH.AuditLogId
         , PRH.HouseholdUID	
         , PRH.HouseholdId	
         , PRH.ClientId
         , PRH.ClientNumber
         , PRH.AssignmentType
         , PRH.AssignedToUserId	AS SystemUserId_SFDC
         , UM.SystemUserId_IRIS
         , U.[name] AS AssignedToFullName
         , WD.NetworkUser AS AssignedToActiveDirectoryUserIdWithDomain
         , PRH.CreatedDate AS AssignmentStartDate
         , ROW_NUMBER() OVER (PARTITION BY PRH.HouseholdUID ORDER BY PRH.CreatedDate, PRH.AuditLogId COLLATE Latin1_General_100_BIN2_UTF8) AS AssignmentOrder --REORDERING DUE TO WHERE CLAUSE DROPPING SOME ASSIGNMENTS
         , 'SFDC' AS SystemOfRecord
      FROM SFDC_PrevRowHash AS PRH
      LEFT
      JOIN PCGSF.[User] AS U 
        ON PRH.AssignedToUserId	= U.Id
      LEFT
      JOIN UserMappingFinal AS UM
        ON U.Id = UM.SystemUserId_SFDC
      LEFT
      JOIN WD.WDWorkers AS WD
        ON TRY_CAST(U.EmployeeNumber AS INT) = WD.EmployeeId 
     WHERE PRH.RowHash <> PRH.PrevRowHash

)

, UnionedHistory AS ( 

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , SystemUserId_IRIS
         , SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate
         , AssignmentOrder
         , SystemOfRecord
      FROM AssignmentHistoryIris

     UNION

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , SystemUserId_IRIS
         , SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate
         , AssignmentOrder
         , SystemOfRecord
      FROM AssignmentHistorySFDC

) 

, MaxIrisAssignmentOrder AS ( 

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , MAX(AssignmentOrder) AS MaxIrisAssignmentOrder
      FROM AssignmentHistoryIris
     GROUP 
        BY HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber

) 

, IrisSFDC_RowHash AS (

    SELECT UH.HouseholdUID	
         , UH.HouseholdId	
         , UH.ClientId
         , UH.ClientNumber
         , UH.AssignmentType
         , UH.SystemUserId_IRIS
         , UH.SystemUserId_SFDC
         , UH.AssignedToFullName
         , UH.AssignedToActiveDirectoryUserIdWithDomain
         , UH.AssignmentStartDate
         , CASE 
                WHEN UH.SystemOfRecord = 'Iris' THEN UH.AssignmentOrder 
                --IF IRIS HISTORY EXISTS, TAKE MAX IRIS ASSIGNMENT ORDER + SFDC ASSIGNMENT ORDER
                --IF NO IRIS RECORDS EXIST, 0 + SFDC ASSIGNMENT ORDER 
                WHEN UH.SystemOfRecord = 'SFDC' THEN ISNULL(AO.MaxIrisAssignmentOrder, 0) + UH.AssignmentOrder
                ELSE NULL
           END AS AssignmentOrder
         , UH.SystemOfRecord
         --ADDING ROW HASH ONCE AGAIN AS LAST IRIS ASSIGNMENT CAN BE THE SAME AS FIRST SFDC ASSIGNMENT 
         , HASHBYTES ('SHA2_256', CONCAT(UH.AssignedToActiveDirectoryUserIdWithDomain, '|', UH.AssignmentType)) AS RowHash
      FROM UnionedHistory AS UH
      LEFT
      JOIN MaxIrisAssignmentOrder AS AO
        ON UH.ClientId = AO.ClientId

) 

, Previous_IrisSFDC_RowHash AS (

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , SystemUserId_IRIS
         , SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate
         , AssignmentOrder
         , SystemOfRecord
         , RowHash
         , LAG(RowHash, 1, HASHBYTES('SHA2_256', @UnknownTextValue)) OVER (PARTITION BY ISNULL(HouseholdUID, ClientId) ORDER BY AssignmentOrder) AS PrevRowHash
      FROM IrisSFDC_RowHash

) 

, AssignmentHistoryIC AS ( 

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , SystemUserId_IRIS
         , SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate              
         , LEAD(AssignmentStartDate, 1, @MaxDateValue) OVER (PARTITION BY ISNULL(HouseholdUID, ClientId) ORDER BY AssignmentOrder) AS AssignmentEndDate 
         , ROW_NUMBER() OVER (PARTITION BY ISNULL(HouseholdUID, ClientId) ORDER BY AssignmentOrder) AS AssignmentOrder --REORDER AS WE MAY HAVE DROPPED RECORDS IN THE WHERE CLAUSE
         , CASE 
                WHEN ROW_NUMBER() OVER (PARTITION BY ISNULL(HouseholdUID, ClientId), CONVERT(DATE, AssignmentStartDate) ORDER BY AssignmentOrder DESC) = 1 
                THEN 1 
                ELSE 0 
           END AS IsEndOfDayAssignment
         , SystemOfRecord
     FROM Previous_IrisSFDC_RowHash
    WHERE RowHash <> PrevRowHash

) 


   INSERT 
	 INTO #AssignmentHistoryIC (  
		  HouseholdUID
 		, HouseholdID
		, ClientId
		, ClientNumber
		, AssignmentType 
		, SystemUserId_IRIS 
		, SystemUserId_SFDC 	
		, AssignedToFullName 
		, AssignedToActiveDirectoryUserIdWithDomain 
		, AssignmentStartDate
		, AssignmentEndDate 
		, AssignmentOrder 
		, IsEndOfDayAssignment 
		, SystemOfRecord 
		--, DWCreatedDateTime 
		--, DWUpdatedDateTime
		--, ETLJobProcessRunId 
		--, ETLJobSystemRunId 
	 )

   SELECT HouseholdUID	
 		, HouseholdID
		, ClientId
		, ClientNumber
		, AssignmentType 
		, SystemUserId_IRIS 
		, SystemUserId_SFDC 	
		, AssignedToFullName 
		, AssignedToActiveDirectoryUserIdWithDomain 
		, AssignmentStartDate
		, AssignmentEndDate 
		, AssignmentOrder 
		, IsEndOfDayAssignment 
		, SystemOfRecord 
	    --, @DWUpdatedDateTime AS DWCreatedDateTime
	    --, @DWUpdatedDateTime AS DWUpdatedDateTime
	    --, @ETLJobProcessRunId AS ETLJobProcessRunId
	    --, @ETLJobSystemRunId AS ETLJobSystemRunId  
     FROM AssignmentHistoryIC



/*
    EOD TEMP VIEW LOGIC
*/

IF OBJECT_ID('TEMPDB..#vwEODAssignmentHistoryIC') IS NOT NULL
    DROP TABLE #vwEODAssignmentHistoryIC

CREATE TABLE #vwEODAssignmentHistoryIC (
	[HouseholdUID] [nvarchar](4000) NULL,
	[HouseholdID] [nvarchar](4000) NULL,
	[ClientId] [uniqueidentifier] NULL,
	[ClientNumber] [int] NULL,
	[AssignmentType] [nvarchar](255) NULL,
	[SystemUserId_IRIS] [uniqueidentifier] NULL,
	[SystemUserId_SFDC] [nvarchar](18)  NULL,	
	[AssignedToFullName] [nvarchar](255) NULL,
	[AssignedToActiveDirectoryUserIdWithDomain] [nvarchar](255) NULL,
	[AssignmentWindowStartDate] [datetime] NULL,
	[AssignmentWindowEndDate] [datetime] NULL,
    [CurrentRecord] [int]
)



;WITH DailyChanges AS ( 

    SELECT HouseholdUID	
         , HouseholdID	
         , ClientId	
         , ClientNumber	
         , AssignmentType	
         , SystemUserId_IRIS	
         , SystemUserId_SFDC	
         , AssignedToFullName	
         , AssignedToActiveDirectoryUserIdWithDomain	
         , AssignmentStartDate	
         , AssignmentEndDate	
         , AssignmentOrder	
         , SystemOfRecord	
	  FROM #AssignmentHistoryIC WITH (NOLOCK)
	 WHERE IsEndOfDayAssignment = 1  --IF CLIENT WAS ASSIGNED TO NUMEROUS ICS WITHIN THE DAY, ONLY TAKE THE LAST CHANGE
) 

, RowHash AS ( 

    SELECT HouseholdUID	
         , HouseholdID	
         , ClientId	
         , ClientNumber	
         , AssignmentType	
         , SystemUserId_IRIS	
         , SystemUserId_SFDC	
         , AssignedToFullName	
         , AssignedToActiveDirectoryUserIdWithDomain	
         , AssignmentStartDate	
         , AssignmentEndDate	
         , AssignmentOrder	
         , SystemOfRecord	
		 , HASHBYTES ('SHA2_256', CONCAT(AssignedToActiveDirectoryUserIdWithDomain, '|', AssignmentType)) AS RowHash
	  FROM DailyChanges

) 

, PreviousAssignments  AS (

    SELECT HouseholdUID	
         , HouseholdID	
         , ClientId	
         , ClientNumber	
         , AssignmentType	
         , SystemUserId_IRIS	
         , SystemUserId_SFDC	
         , AssignedToFullName	
         , AssignedToActiveDirectoryUserIdWithDomain	
         , AssignmentStartDate	
         , AssignmentEndDate	
         , AssignmentOrder	
         , SystemOfRecord	
		 , RowHash
		 , LAG(RowHash, 1, HASHBYTES('SHA2_256', 'Unknown')) OVER (PARTITION BY ISNULL(CONVERT(NVARCHAR(50), ClientId), HouseholdUID) ORDER BY AssignmentOrder) AS PreviousRowHash
	  FROM RowHash 

)

    INSERT 
      INTO #vwEODAssignmentHistoryIC (
           HouseholdUID	
         , HouseholdID	
         , ClientId	
         , ClientNumber	
         , AssignmentType	
         , SystemUserId_IRIS	
         , SystemUserId_SFDC	
         , AssignedToFullName	
         , AssignedToActiveDirectoryUserIdWithDomain	
         , AssignmentWindowStartDate	
	     , AssignmentWindowEndDate
	     , CurrentRecord
) 

    SELECT HouseholdUID	
         , HouseholdID	
         , ClientId	
         , ClientNumber	
         , AssignmentType	
         , SystemUserId_IRIS	
         , SystemUserId_SFDC	
         , AssignedToFullName	
         , AssignedToActiveDirectoryUserIdWithDomain	
	     , CONVERT(DATETIME, DATEDIFF(DAY, 0, AssignmentStartDate)) AS AssignmentWindowStartDate        
         , CONVERT(DATETIME, DATEDIFF(DAY, 0, LEAD(AssignmentStartDate, 1, '9999-12-31') OVER (PARTITION BY ISNULL(CONVERT(NVARCHAR(50), ClientId), HouseholdUID) ORDER BY AssignmentStartDate, AssignmentOrder))) AS AssignmentWindowEndDate
	     , CASE 
			  WHEN LEAD(AssignmentStartDate, 1, '9999-12-31') OVER (PARTITION BY ISNULL(CONVERT(NVARCHAR(50), ClientId), HouseholdUID) ORDER BY AssignmentOrder) = '9999-12-31'
			  THEN 1 
			  ELSE 0 
		   END AS CurrentRecord
      FROM PreviousAssignments
     WHERE RowHash <> PreviousRowHash  --Limit to distinct changes only


select *
  from #AssignmentHistoryIC
 where clientnumber = 1501670
 order by assignmentorder

select *
  from #vwEODAssignmentHistoryIC
  where clientnumber = 1501670


/*
    TESTING
*/

   --duplicate record check
   select isnull(HouseholdUID, clientid) 
        , count(*)
     from #vwEODAssignmentHistoryIC
    where CurrentRecord = 1 
    group 
       by isnull(HouseholdUID, clientid) 
   having count(*) > 1

   --duplicate assignment start date check
   select isnull(HouseholdUID, clientid) 
        , AssignmentWindowStartDate
        , count(*)
     from #vwEODAssignmentHistoryIC
    --where CurrentRecord = 1 
    group 
       by isnull(HouseholdUID, clientid) 
        , AssignmentWindowStartDate
   having count(*) > 1

   --check for overlapping start/end dates
   select * 
     from ( 
            select 
                   isnull(HouseholdUID, clientid) AS PartitionKey
                 , AssignmentWindowEndDate
                 , LEAD (AssignmentWindowStartDate, 1, '9999-12-31') OVER (PARTITION BY isnull(HouseholdUID, clientid) ORDER BY AssignmentWindowStartDate) AS NextStartDate
                 , CASE 
                    WHEN AssignmentWindowEndDate = LEAD (AssignmentWindowStartDate, 1, '9999-12-31') OVER (PARTITION BY isnull(HouseholdUID, clientid) ORDER BY AssignmentWindowStartDate)
                    THEN 1 
                    ELSE 0 
                   END EndDateMatchesNextStartDate
              from #vwEODAssignmentHistoryIC
          ) as a 
    where a.EndDateMatchesNextStartDate = 0 
