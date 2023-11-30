         --, LEAD(AssignmentStartDate, 1, @MaxDateValue) OVER (PARTITION BY ISNULL(ClientId, HouseholdUID) ORDER BY AssignmentStartDate) AS AssignmentEndDate 
         --, CASE 
         --       WHEN ROW_NUMBER() OVER (PARTITION BY ISNULL(ClientId, HouseholdUID), CONVERT(DATE, AssignmentStartDate) ORDER BY AssignmentStartDate DESC) = 1 
         --       THEN 1 
         --       ELSE 0 
         --  END AS IsEndOfDayAssignment

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
		 , CONVERT(NVARCHAR(36), PRH.AssignedToSystemUserId) AS AssignedToSystemUserId
         , SUB.FullName AS AssignedToFullName
         , SUB.DomainName AS AssignedToActiveDirectoryUserIdWithDomain      
		 , PRH.CreatedOn AS AssignmentStartDate
         , ROW_NUMBER() OVER (PARTITION BY PRH.ClientId ORDER BY PRH.CreatedOn, PRH.RelationshipManagementAuditLogId DESC) AS AssignmentOrder
         , 'Iris' AS SystemOfRecord
	  FROM PreviousRowHash PRH
      LEFT
      JOIN Iris.SystemUserBase AS SUB
        ON PRH.AssignedToSystemUserId = SUB.SystemUserId
     WHERE RowHash <> PreviousRowHash  --Limit to distinct changes only
     
)

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
         , ROW_NUMBER() OVER (PARTITION BY AH.AccountId ORDER BY AH.CreatedDate, AH.Id COLLATE Latin1_General_100_BIN2_UTF8) AS AssignmentOrder
      FROM PCGSF.AccountHistory AS AH
      JOIN REF.CRMClientMapping AS CRM
        ON AH.AccountId = CRM.HouseholdUID
     WHERE Field = 'Investment_Counselor__c'
       AND DataType = 'EntityId'

)

, FullAuditHistory AS ( 

    SELECT AH.AuditLogId
         , AH.HouseholdUID	
         , AH.HouseholdId	
         , AH.ClientId
         , AH.ClientNumber
         , AH.OldUserId
         , ISNULL(SFDC.SFDC_StartDate, AH.CreatedDate) AS CreatedDate
         , AH.AssignmentOrder
      FROM SFDC_AuditHistory AS AH
      LEFT
      JOIN SDFC_MigrationDates AS SFDC
        ON AH.HouseholdUID = SFDC.HouseholdUID
     WHERE AH.AssignmentOrder = 1

     UNION 

    SELECT AuditLogId
         , HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , NewUserId
         , CreatedDate
         , AssignmentOrder
      FROM SFDC_AuditHistory
 
 )

 , AuditHistoryCleansed AS ( 

    SELECT FAH.AuditLogId	
         , FAH.HouseholdUID	
         , FAH.HouseholdId	
         , FAH.ClientId
         , FAH.ClientNumber
		 , CASE 
			  WHEN OC.OnboardingDate < CD.ContractDate --PER SCOTT CROCKER & RACHEL STAKEY. PRE-ASSIGNMENT FLAG DOES NOT EXIST IN SFDC - USE THIS FOR NOW.
			  THEN 'Prospect Assignment' 
			  ELSE 'Client Assignment'
		   END AS AssignmentType
         , OldUserId AS AssignedToUserId
         , CreatedDate         
         , AssignmentOrder
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
     WHERE OldUserId IS NOT NULL

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
         , AssignedToSystemUserId AS SystemUserId_IRIS
         , NULL AS SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate
         , AssignmentOrder
         , MAX(AssignmentOrder) OVER (PARTITION BY ClientId) AS MaxAssignmentOrderPerSystemOfRecord
         , SystemOfRecord
      FROM AssignmentHistoryIris

     UNION

    SELECT HouseholdUID	
         , HouseholdId	
         , ClientId
         , ClientNumber
         , AssignmentType
         , NULL AS SystemUserId_IRIS
         , SystemUserId_SFDC
         , AssignedToFullName
         , AssignedToActiveDirectoryUserIdWithDomain
         , AssignmentStartDate
         , AssignmentOrder
         , MAX(AssignmentOrder) OVER (PARTITION BY HouseholdUID) AS MaxAssignmentOrderPerSystemOfRecord
         , SystemOfRecord
      FROM AssignmentHistorySFDC

) 

, IrisSFDC_RowHash AS (

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
         , MaxAssignmentOrderPerSystemOfRecord
         , SystemOfRecord
         --ADDING ROW HASH ONCE AGAIN AS LAST IRIS ASSIGNMENT CAN BE THE SAME AS FIRST SFDC ASSIGNMENT 
         , HASHBYTES ('SHA2_256', CONCAT(AssignedToActiveDirectoryUserIdWithDomain, '|', AssignmentType)) AS RowHash
      FROM UnionedHistory

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
         , MaxAssignmentOrderPerSystemOfRecord
         , SystemOfRecord
         , RowHash
         , LAG(RowHash, 1, HASHBYTES('SHA2_256', @UnknownTextValue)) OVER (PARTITION BY ISNULL(HouseholdUID, ClientId) ORDER BY AssignmentStartDate, AssignmentOrder) AS PrevRowHash
      FROM IrisSFDC_RowHash

) 

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
         , MaxAssignmentOrderPerSystemOfRecord
         , CASE WHEN SystemOfRecord= 'Iris' THEN AssignmentOrder
         --, LEAD(AssignmentStartDate, 1, @MaxDateValue) OVER (PARTITION BY ISNULL(HouseholdUID, ClientId) ORDER BY AssignmentStartDate) AS AssignmentEndDate 
         --, CASE 
         --       WHEN ROW_NUMBER() OVER (PARTITION BY ISNULL(HouseholdUID, ClientId), CONVERT(DATE, AssignmentStartDate) ORDER BY AssignmentStartDate DESC) = 1 
         --       THEN 1 
         --       ELSE 0 
         --  END AS IsEndOfDayAssignment
         , SystemOfRecord
     FROM Previous_IrisSFDC_RowHash
    WHERE RowHash <> PrevRowHash
       and clientnumber = 3831461
     order by AssignmentStartDate


select * 
from [REF].[RelationshipManagementAssignment]
where clientnumber = 1501670
order by assignmentstartdate


select * 
  from [REF].[vwRelationshipManagementAssignmentWindow]
  where clientnumber = 1501670


select FullName
     , fi_ID
     , *
  from iris.systemuserbase
  where fi_id = 'dvucak'

