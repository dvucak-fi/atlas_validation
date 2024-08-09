IF OBJECT_ID('TEMPDB..#MetricAdjustmentHistoryReferral') IS NOT NULL DROP TABLE #MetricAdjustmentHistoryReferral
CREATE TABLE #MetricAdjustmentHistoryReferral (
	[ReferredClientNumber] [NVARCHAR](255) NULL,
	[ReferringClientNumber] [NVARCHAR](255) NULL,	
	[ReferralID] [INT] NULL,
	[SourceIndicator] [VARCHAR](4) NULL,
	[ReferralDate] [DATETIME] NULL,
	[ClientTradingDate] [DATETIME] NULL,
	[ReferralSource] [NVARCHAR](50) NULL,
	[ReferralCreditApproval] [NVARCHAR](50) NULL,
	[ReferralClientDevelopmentApproval] [NVARCHAR](50) NULL,
	[ReferralType] [NVARCHAR](50) NULL,
	[ReferralStatus] [NVARCHAR](50) NULL,
	[SubmittedICEmployeeId] [int] NULL,
	[FundedICEmployeeId] [int] NULL,
	[SubmittedOSPEmployeeId] [int] NULL,
	[FundedOSPEmployeeId] [int] NULL,
	[SubmittedAssignedToEmployeeId] [int] NULL,
	[FundedAssignedToEmployeeId] [int] NULL,
	[RowHash] [VARBINARY](8000) NULL,
	[EffectiveStartDate] [DATETIME] NULL,
	[EffectiveEndDate] [DATETIME] NULL,	
	[CurrentRecord] [INT] NULL,	
	[DWCreatedDateTime] [DATETIME] NULL,
	[DWUpdatedDateTime] [DATETIME] NULL,
	[ETLJobProcessRunId] [UNIQUEIDENTIFIER] NULL,
	[ETLJobSystemRunId] [UNIQUEIDENTIFIER] NULL
)
WITH
(
	DISTRIBUTION = HASH(ReferralID),
	CLUSTERED COLUMNSTORE INDEX
)



DECLARE @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
      , @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()


--CREATE PROC [REF].[spUpsertMetricAdjustmentHistoryReferral] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
--BEGIN 

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements

/*
==========================================================================================================================================================
 Author: Armando
 Modified Date & Modified By: 
	PDDTI-1860, 05-22-2024 by Armando <akuri-fi>
 	Description: Created type 2 ref table to track changes to metrics adjustments within the Referral Database
	PDDTI-2084, 08-07-2024 by Dado <dvucak-fi>
 	Description: Updated referral attribution logic for IC submitted and funded referrals
 Parameters:
   @ETLJobSystemRunId - ELT field passed during pipeline run
   @ETLJobProcessRunId - ELT field passed during pipeline run
   @ComponentName - Name of this component, for logging. Passed by ELT during pipeline run.
 Returns: 
   Status | ErrorMessage - return is required by ELT   
	===========================================================================================================================================================
*/

DECLARE @DWUpdatedDatetime DATETIME
      , @Rows INT
      , @StartTime DATETIME
      , @EndTime DATETIME
      , @DurationInSeconds INT
      , @Source NVARCHAR(255)
      , @Target NVARCHAR(255)
      , @Status INT
      , @ErrorMessage NVARCHAR(512)
	  , @UnknownTextValue NVARCHAR(512)
	  , @UnknownNumberValue INT
	  , @MinDateValue DATE
	  , @MaxDateValue DATE

DECLARE @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
       
DECLARE @InsertCount BIGINT
	, @InsertOperation NVARCHAR(20) 
	, @UpdateCount BIGINT
	, @UpdateOperation NVARCHAR(20) 


SET @InsertOperation = 'INSERT'
SET @UpdateOperation = 'UPDATE'

       
SET @DWUpdatedDatetime = GETDATE()
SET @Status = 1
SET @Rows = 0


SELECT TOP 1 @UnknownNumberValue = CONVERT(INT,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownNumberValue' 

 SELECT TOP 1 @UnknownTextValue = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValue'

 SELECT TOP 1 @MinDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MinDateValue'

 SELECT TOP 1 @MaxDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MaxDateValue'

IF OBJECT_ID('tempdb..#Stg_Ext_Referral', 'U') IS NOT NULL
    DROP TABLE #Stg_Ext_Referral

CREATE TABLE #Stg_Ext_Referral (
	   UseDate date 
	 , ReferralDate date 
	 , ClientTradingDate date 
	 , ReferralID INT 
	 , ReferringCID INT 
	 , ProspectCID INT 
	 , SubmittedICEmployeeId INT 
	 , FundedICEmployeeId INT 
	 , SubmittedOSPEmployeeId INT 
	 , FundedOSPEmployeeId INT 
	 , SubmittedAssignedToEmployeeId INT 
	 , FundedAssignedToEmployeeId INT 
	 , ReferralType NVARCHAR(50) 
	 , ReferralSource NVARCHAR(50) 
	 , ReferralCredit NVARCHAR(50) 
	 , ReferralStatus NVARCHAR(150) 
	 , ClientDevelopmentApproval NVARCHAR(50) 
	 , ClientAlreadyReferred12Mos INT 
	 , SalesMeeting12Mos INT 
	 , ClientDevelopmentMeeting12Mos INT 
	 , SourceIndicator NVARCHAR(4) 
	 , RowHash VARBINARY(8000) 
  )
WITH (DISTRIBUTION = HASH(ReferralID), HEAP)  

IF OBJECT_ID('tempdb..#Referrals') IS NOT NULL
    DROP TABLE #Referrals

CREATE TABLE #Referrals (
	   ReferralId INT
	 , ReferringClientNumber NVARCHAR(255)
	 , ReferredClientNumber	NVARCHAR(255)
	 , ReferralDate	DATETIME
	 , OSPUserId NVARCHAR(4000)
	 , ICUserId NVARCHAR(4000)
	 , ICUserName NVARCHAR(4000)
	 , AssignedToUserId NVARCHAR(4000)
	 , ClientDevApproval NVARCHAR(255)
	 , ReferralStatus NVARCHAR(255)
	 , ReferralSource NVARCHAR(4000)
	 , ReferralCredit NVARCHAR(4000)
	 , SourceIndicator NVARCHAR(25)
)
WITH (DISTRIBUTION = HASH(ReferralId), HEAP)  

IF OBJECT_ID('tempdb..#ReferralCredit') IS NOT NULL
    DROP TABLE #ReferralCredit

CREATE TABLE #ReferralCredit (
	   ReferralId INT
	 , ReferringClientNumber NVARCHAR(255)
	 , ReferredClientNumber	NVARCHAR(255)
	 , ReferralDate	DATETIME
	 , SubmittedICEmployeeId INT
	 , FundedICEmployeeId NVARCHAR(255)
	 , AttributionType NVARCHAR(25)
	 , ReferralType	NVARCHAR(255)
	 , SourceIndicator NVARCHAR(25)
)
WITH (DISTRIBUTION = HASH(ReferralId), HEAP)  


IF OBJECT_ID('tempdb..#MinDC', 'U') IS NOT NULL
    DROP TABLE #MinDC

CREATE TABLE #MinDC
(
	iProspectCID INT NULL
  , ClientTradingDate DATE NULL
)
WITH (DISTRIBUTION = HASH(iProspectCID), HEAP)  


-- Obtain earliest client trading date for CIDs 
INSERT INTO #MinDC (iProspectCID, ClientTradingDate)
SELECT RM.iProspectCID
     , MIN(DC.ClientTradingDate) AS ClientTradingDate 
  FROM BAS.Referral_Main RM 
  JOIN FDW.DimClient DC ON RM.iProspectCID = TRY_CAST(DC.ClientNumber AS INT)
 WHERE RM.iProspectCID IS NOT NULL
	   AND RM.iStatus IN (4,5) -- Status: ISO Completed, Completed  
	   AND DC.[ClientTradingDate] >= COALESCE(RM.dtReferralDate, RM.dtSubmitted_Date, RM.dtPresubmission) 
 GROUP BY RM.iProspectCID
 UNION
SELECT RM.iProspectCID
     , MIN(DC.ClientTradingDate) AS ClientTradingDate 
  FROM BAS.EUReferralMain RM 
  JOIN FDW.DimClient DC ON RM.iProspectCID = TRY_CAST(DC.ClientNumber AS INT)
 WHERE RM.iProspectCID IS NOT NULL
	   AND DC.[ClientTradingDate] >= COALESCE(RM.dtReferral, RM.dtEntered) 
 GROUP BY RM.iProspectCID
 UNION
SELECT RM.iProspectCID
     , MIN(DC.ClientTradingDate) AS ClientTradingDate 
  FROM BAS.UKReferral_Main RM 
  JOIN FDW.DimClient DC ON RM.iProspectCID = TRY_CAST(DC.ClientNumber AS INT)
 WHERE RM.iProspectCID IS NOT NULL
	   AND DC.[ClientTradingDate] >= RM.ReferralDate
 GROUP BY RM.iProspectCID


/*
	COMBINE REFERRALS FROM ALL SOURCES INTO SINGLE STANDARDIZED OBJECT TO LIMIT CODE REPLICATION
*/

    INSERT
	  INTO #Referrals (
		   ReferralId 
		 , ReferringClientNumber 
		 , ReferredClientNumber	
		 , ReferralDate	
		 , OSPUserId 
		 , ICUserId 
		 , ICUserName
		 , AssignedToUserId 
		 , ClientDevApproval 
		 , ReferralStatus 
		 , ReferralSource 
		 , ReferralCredit 
		 , SourceIndicator 
	)

	SELECT iID AS ReferralId
		 , CONVERT(NVARCHAR(255), iOnyxID) AS ReferringClientNumber
		 , CONVERT(NVARCHAR(255), iProspectCID) AS ReferredClientNumber
		 , COALESCE(RM.dtReferralDate, RM.dtSubmitted_Date, RM.dtPresubmission) AS ReferralDate
		 , vchOSP_ID AS OSPUserId
		 , vchIC_ID AS ICUserId
		 , vchIC AS ICUserName
		 , vchAssignedTo AssignedToUserId
		 , ISNULL(CASE WHEN RL.[Description] = 'Yes' THEN 'Approved' WHEN RL.[Description] = 'No' THEN 'Declined' ELSE RL.[Description] END,  @UnknownTextValue) AS ClientDevApproval
		 , ISNULL(RL2.[Description],  @UnknownTextValue) as ReferralStatus
		 , ISNULL(TRIM(vchSource), @UnknownTextValue) AS ReferralSource
		 , ISNULL(vchReferral_Credit, @UnknownTextValue) AS ReferralCredit
		 , 'US' AS SourceIndicator
	  FROM BAS.Referral_Main AS RM
	  LEFT 
	  JOIN BAS.REF_Lookup AS RL
		ON RM.iCD_Approval = RL.ID
	  LEFT
	  JOIN BAS.REF_Lookup AS RL2
	    ON RM.iStatus = RL2.SortOrder
	   AND RL2.[Application] = 'Referrals'
       AND RL2.[Field] = 'Status'

	 WHERE RM.iStatus IN (4,5) -- Status: ISO Completed, Completed  
	  
	 UNION --UNION EU REFERRALS

	SELECT iPrimaryKey AS ReferralId
		 , CONVERT(NVARCHAR(255), iReferringCID) AS ReferringClientNumber
		 , CONVERT(NVARCHAR(255), iProspectCID) AS ReferredClientNumber
		 , COALESCE(RM.dtReferral, RM.dtEntered) AS ReferralDate
		 , vchOSP_ID AS OSPUserId
		 , vchIC_ID AS ICUserId
		 , NULL AS ICUserName
		 , vchIC_ID AssignedToUserId
		 , @UnknownTextValue AS ClientDevApproval 
		 , ISNULL(RL2.[Description], @UnknownTextValue) AS ReferralStatus
		 , ISNULL(TRIM(vchSource), @UnknownTextValue) AS ReferralSource
		 , CASE iCredit_Approval WHEN 1 THEN 'Approved' WHEN 0 THEN 'Declined' ELSE @UnknownTextValue END AS ReferralCredit
		 , 'EU' AS SourceIndicator
	  FROM BAS.EUReferralMain AS RM
	  LEFT
	  JOIN BAS.REF_Lookup AS RL2
	    ON RM.iStatus = RL2.SortOrder
	   AND RL2.[Application] = 'Referrals'
       AND RL2.[Field] = 'Status'

	 UNION --UNION UK REFERRALS

	SELECT PrimaryKey AS ReferralId
		 , CONVERT(NVARCHAR(255), OnyxID) AS ReferringClientNumber
		 , CONVERT(NVARCHAR(255), iProspectCID) AS ReferredClientNumber
		 , ReferralDate
		 , OSP AS OSPUserId
		 , UserID AS ICUserId
		 , NULL AS ICUserName
		 , UserID AssignedToUserId
		 , NULL AS ClientDevApproval 
		 , COALESCE(RL2.[Description], @UnknownTextValue) AS ReferralStatus
		 , COALESCE(TRIM(RM.Source),  @UnknownTextValue) AS ReferralSource
		 ,CASE WHEN RM.IC_Referral_Credit IN ('Approved','Declined') THEN RM.IC_Referral_Credit ELSE @UnknownTextValue END AS ReferralCredit
		 , 'UK' AS SourceIndicator
	  FROM BAS.UKReferral_Main AS RM
	  LEFT
	  JOIN BAS.REF_Lookup AS RL2
	    ON RM.iStatus = RL2.SortOrder
	   AND RL2.[Application] = 'Referrals'
       AND RL2.[Field] = 'Status'



;WITH Workday AS (

	SELECT WD.EmployeeId
		 , WD.PreferredFullName
		 , WD.OnyxUserID
		 , WD.NetworkUser AS NetworkUserId
		 , JH.Original_Hire_Date AS OriginalHireDate
		 , WD.HireDate
		 , ISNULL(WD.TerminationDate, @MaxDateValue) AS TerminationDate
	  FROM WD.WDWorkers AS WD
	  LEFT
	  JOIN (SELECT DISTINCT EmployeeId, Original_Hire_Date FROM WD.WDJobHistory) AS JH
	    ON WD.EmployeeId = JH.EmployeeId 

)

, CreditedReferrals AS ( 

	SELECT R.ReferralId
		 , R.ReferringClientNumber
		 , R.ReferredClientNumber
		 , R.ICUserId
		 , R.ICUserName
		 , WD.EmployeeId AS ICEmployeeId
		 , WD.NetworkUserId AS ICNetworkUserId
		 , R.ReferralDate
		 , R.SourceIndicator
	  FROM #Referrals AS R
	  JOIN Workday AS WD
		ON TRIM(R.ICUserId) = TRIM(WD.OnyxUserID)
	   AND CASE 
			WHEN CONVERT(DATE, R.ReferralDate) < CONVERT(DATE, WD.HireDate)
			THEN CONVERT(DATE, WD.OriginalHireDate) --ROUND TRIP EMPLOYEES USE ORIGINAL HIRE DATE
			ELSE CONVERT(DATE, WD.HireDate) --ELSE USE CURRENT HIRE DATE
		   END <= CONVERT(DATE, R.ReferralDate)
	   AND CONVERT(DATE, WD.TerminationDate) >= CONVERT(DATE, R.ReferralDate)

	 UNION --SOME US REFERRALS ARE ENTERED WITH THE EMPLOYEE NAME IN THE USER ID FIELD AND THE USER ID IN THE EMPLOYEE NAME FIELD

	SELECT R.ReferralId
		 , R.ReferringClientNumber
		 , R.ReferredClientNumber
		 , R.ICUserId
		 , R.ICUserName
		 , WD.EmployeeId AS ICEmployeeId
		 , WD.NetworkUserId AS ICNetworkUserId
		 , R.ReferralDate
		 , R.SourceIndicator
	  FROM #Referrals AS R
	  JOIN Workday AS WD
		ON TRIM(R.ICUserName) = TRIM(WD.OnyxUserID)
	   AND CASE 
			WHEN CONVERT(DATE, R.ReferralDate) < CONVERT(DATE, WD.HireDate)
			THEN CONVERT(DATE, WD.OriginalHireDate) --ROUND TRIP EMPLOYEES USE ORIGINAL HIRE DATE
			ELSE CONVERT(DATE, WD.HireDate) --ELSE USE CURRENT HIRE DATE
		   END <= CONVERT(DATE, R.ReferralDate)
	   AND CONVERT(DATE, WD.TerminationDate) >= CONVERT(DATE, R.ReferralDate)
	 WHERE R.SourceIndicator = 'US'
	
)

, DupeProtectCreditedReferrals AS ( 

	SELECT ReferralId
		 , ReferringClientNumber
		 , ReferredClientNumber
		 , ReferralDate
		 , ICUserId
		 , ICUserName
		 , ICEmployeeId
		 , ICNetworkUserId
		 , SourceIndicator
		 --SOME REFERRALS HAVE TWO DIFFERENT EMPLOYEES WITHIN THE NAME AND USER ID FIELDS. LIMIT TO ONE REFERRAL ONLY.
		 , ROW_NUMBER() OVER (PARTITION BY SourceIndicator, ReferralId ORDER BY ICEmployeeId) AS RowNum 
	  FROM CreditedReferrals 

) 

, CreditedReferralsFinal AS ( 

	SELECT R.ReferralId
		 , R.ReferringClientNumber
		 , R.ReferredClientNumber
		 , R.ReferralDate
		 , R.ICEmployeeId AS SubmittedICEmployeeId
		 , CASE WHEN CD.iProspectCID IS NOT NULL THEN R.ICEmployeeId ELSE NULL END AS FundedICEmployeeId
		 , 'Credited' AS AttributionType
		 , CASE WHEN CD.iProspectCID IS NOT NULL THEN 'Funded Referral' ELSE 'Submitted Referral' END AS ReferralType
		 , R.SourceIndicator
	  FROM DupeProtectCreditedReferrals AS R 
	  LEFT
	  JOIN #MinDC AS CD
	    ON R.ReferredClientNumber = CD.iProspectCID
	 WHERE R.RowNum = 1 

) 

, NonCreditedReferrals AS (

	SELECT R.ReferralId
		 , R.ReferringClientNumber
		 , R.ReferredClientNumber
		 , R.ReferralDate
		 , WD_RD.EmployeeId AS SubmittedICEmployeeId
		 , CASE WHEN CD.iProspectCID IS NOT NULL THEN WD_CD.EmployeeId ELSE NULL END AS FundedICEmployeeId
		 , 'Derived' AS AttributionType
		 , CASE WHEN CD.iProspectCID IS NOT NULL THEN 'Funded Referral' ELSE 'Submitted Referral' END AS ReferralType
		 , SourceIndicator
	  FROM #Referrals AS R

	  LEFT
	  JOIN #MinDC AS CD
	    ON R.ReferredClientNumber = CD.iProspectCID

	  LEFT
	  JOIN REF.vwEODAssignmentHistoryIC AS AH
	    ON R.ReferringClientNumber = AH.ClientNumber
	   AND CONVERT(DATE, R.ReferralDate) >= CONVERT(DATE, AH.AssignmentWindowStartDate)
	   AND CONVERT(DATE, R.ReferralDate) < CONVERT(DATE, AH.AssignmentWindowEndDate)

	  LEFT
	  JOIN Workday AS WD_RD
		ON WD_RD.NetworkUserId = AH.AssignedToActiveDirectoryUserIdWithDomain
	   AND CASE 
			WHEN CONVERT(DATE, R.ReferralDate) < CONVERT(DATE, WD_RD.HireDate)
			THEN CONVERT(DATE, WD_RD.OriginalHireDate) --ROUND TRIP EMPLOYEES USE ORIGINAL HIRE DATE
			ELSE CONVERT(DATE, WD_RD.HireDate) --ELSE USE CURRENT HIRE DATE
		   END <= CONVERT(DATE, R.ReferralDate)
	   AND CONVERT(DATE, WD_RD.TerminationDate) >= CONVERT(DATE, R.ReferralDate)

	  LEFT
	  JOIN REF.vwEODAssignmentHistoryIC AS AHC
	    ON R.ReferredClientNumber = AHC.ClientNumber
	   AND CONVERT(DATE, CD.ClientTradingDate) >= CONVERT(DATE, AHC.AssignmentWindowStartDate)
	   AND CONVERT(DATE, CD.ClientTradingDate) < CONVERT(DATE, AHC.AssignmentWindowEndDate)

      LEFT
	  JOIN Workday AS WD_CD
		ON WD_CD.NetworkUserId = AHC.AssignedToActiveDirectoryUserIdWithDomain
	   AND CASE 
			WHEN CONVERT(DATE, CD.ClientTradingDate) < CONVERT(DATE, WD_CD.HireDate)
			THEN CONVERT(DATE, WD_CD.OriginalHireDate) --ROUND TRIP EMPLOYEES USE ORIGINAL HIRE DATE
			ELSE CONVERT(DATE, WD_CD.HireDate) --ELSE USE CURRENT HIRE DATE
		   END <= CONVERT(DATE, CD.ClientTradingDate)
	   AND CONVERT(DATE, WD_CD.TerminationDate) >= CONVERT(DATE, CD.ClientTradingDate)

	 WHERE NOT EXISTS (SELECT 1 FROM CreditedReferralsFinal AS CRF WHERE CRF.ReferralId = R.ReferralId AND CRF.SourceIndicator = R.SourceIndicator) 	

)

	INSERT
	  INTO #ReferralCredit (
		   ReferralId
		 , ReferringClientNumber
		 , ReferredClientNumber
		 , ReferralDate
		 , SubmittedICEmployeeId 
		 , FundedICEmployeeId	
		 , AttributionType
		 , ReferralType
		 , SourceIndicator
	)

	SELECT ReferralId
		 , ReferringClientNumber
		 , ReferredClientNumber
		 , ReferralDate
		 , SubmittedICEmployeeId 
		 , FundedICEmployeeId	
		 , AttributionType
		 , ReferralType
		 , SourceIndicator
	  FROM CreditedReferralsFinal   	  

     UNION --UNION REFERRALS WHERE THE IC FIELD IN THE SOURCE WAS NOT POPULATED

	SELECT ReferralId
		 , ReferringClientNumber
		 , ReferredClientNumber
		 , ReferralDate
		 , SubmittedICEmployeeId 
		 , FundedICEmployeeId	
		 , AttributionType
		 , ReferralType
		 , SourceIndicator
      FROM NonCreditedReferrals 



    INSERT 
	  INTO #Stg_Ext_Referral (
	       UseDate
	     , ReferralDate
		 , ClientTradingDate
		 , ReferralID
		 , ReferringCID
		 , ProspectCID
		 , SubmittedICEmployeeId
		 , FundedICEmployeeId 
		 , SubmittedOSPEmployeeId
		 , FundedOSPEmployeeId
		 , SubmittedAssignedToEmployeeId
		 , FundedAssignedToEmployeeId
		 , ReferralType
		 , ReferralSource
		 , ReferralCredit
		 , ReferralStatus
		 , ClientDevelopmentApproval
		 , SourceIndicator
		 , RowHash
	)
	
    SELECT CASE WHEN MDC.iProspectCID IS NOT NULL THEN MDC.ClientTradingDate ELSE R.ReferralDate END AS UseDate
	     , R.ReferralDate
	     , MDC.ClientTradingDate
	     , R.ReferralId
	     , R.ReferringClientNumber
	     , R.ReferredClientNumber
	     , RC.SubmittedICEmployeeId
	     , RC.FundedICEmployeeId
	     , FIOSP.EmployeeID as SubmittedOSPEmployeeId
	     , FIOSP.EmployeeID as FundedOSPEmployeeId
	     , FIAT.EmployeeID as SubmittedAssignedToEmployeeId
	     , FIAT.EmployeeID as FundedAssignedToEmployeeId
	     , CASE WHEN MDC.iProspectCID IS NOT NULL THEN 'Funded Referral' ELSE 'Submitted Referral' END AS ReferralType
	     , R.ReferralSource
	     , R.ReferralCredit
	     , R.ReferralStatus
	     , R.ClientDevApproval
	     , R.SourceIndicator
	     , HASHBYTES('SHA2_256', CONCAT(ISNULL(R.ReferredClientNumber, @UnknownNumberValue)
							, '|', ISNULL(R.ReferralId, @UnknownNumberValue)
							, '|', R.SourceIndicator
							, '|', R.ReferralDate
							, '|', ISNULL(MDC.ClientTradingDate, @MinDateValue)
							, '|', R.ReferralSource
							, '|', R.ReferralCredit
							, '|', R.ClientDevApproval
							, '|', CASE WHEN MDC.iProspectCID IS NOT NULL THEN 'Funded Referral' ELSE 'Submitted Referral' END
							, '|', R.ReferralStatus
							, '|', ISNULL(RC.SubmittedICEmployeeId, @UnknownNumberValue)
							, '|', ISNULL(RC.FundedICEmployeeId, @UnknownNumberValue)
							, '|', ISNULL(FIOSP.EmployeeID, @UnknownNumberValue)
							, '|', ISNULL(FIOSP.EmployeeID, @UnknownNumberValue)
							, '|', ISNULL(FIAT.EmployeeID, @UnknownNumberValue)
							, '|', ISNULL(FIAT.EmployeeID, @UnknownNumberValue))) AS RowHash
      FROM #Referrals AS R --IB
	  
	  LEFT  
	  JOIN #MinDC AS MDC
		ON MDC.iProspectCID = R.ReferredClientNumber
	  
	  --IC
	  LEFT
	  JOIN #ReferralCredit AS RC
	    ON R.ReferralId = RC.ReferralId
	   AND R.SourceIndicator = RC.SourceIndicator

	  --OSP
	  LEFT 
	  JOIN WD.WDWorkers AS FIOSP
		ON R.OSPUserId = FIOSP.OnyxUserID
	   AND CASE WHEN MDC.iProspectCID IS NOT NULL THEN MDC.ClientTradingDate ELSE R.ReferralDate END <= ISNULL(FIOSP.TerminationDate, @MaxDateValue)
	   AND CASE WHEN MDC.iProspectCID IS NOT NULL THEN MDC.ClientTradingDate ELSE R.ReferralDate END > FIOSP.HireDate

	  --Assigned To
	  LEFT  
	  JOIN WD.WDWorkers AS FIAT
		ON R.AssignedToUserId = FIAT.OnyxUserID
	   AND CASE WHEN MDC.iProspectCID IS NOT NULL THEN MDC.ClientTradingDate ELSE R.ReferralDate END <= ISNULL(FIAT.TerminationDate, @MaxDateValue)
	   AND CASE WHEN MDC.iProspectCID IS NOT NULL THEN MDC.ClientTradingDate ELSE R.ReferralDate END > FIAT.HireDate

     WHERE R.ReferralDate IS NOT NULL
	   AND ISNULL(R.ReferredClientNumber, @UnknownNumberValue) <> @UnknownNumberValue --NEW REFERRED CLIENT MUST HAVE VALID CID
	   --Excluding -1s since they were linking to the Unknown member of our dim and causing issues with the rest of the dim keys (like Client Tenure)
	   AND (R.ReferringClientNumber <> -1 OR R.ReferringClientNumber IS NULL) 
	   

	
BEGIN TRY

BEGIN TRANSACTION 

/*
	TYPE 2 UPDATE - DEACTIVATE ACTIVE RECS THAT HAVE HAD AN UPDATE SINCE LAST ETL
*/

SET @StartTime = GETDATE()

	UPDATE #MetricAdjustmentHistoryReferral
	   SET CurrentRecord = 0 
	     , EffectiveEndDate = @Today
		 , DWUpdatedDateTime = @DWUpdatedDateTime
		 , ETLJobProcessRunId = @ETLJobProcessRunId
		 , ETLJobSystemRunId = @ETLJobSystemRunId  
	  FROM #MetricAdjustmentHistoryReferral AS TGT
	  JOIN #Stg_Ext_Referral AS SRC 
	    ON TGT.ReferralID = SRC.ReferralID
	   AND TGT.SourceIndicator = SRC.SourceIndicator
	   AND SRC.RowHash <> TGT.RowHash 
	 WHERE TGT.CurrentRecord = 1 

	--OPTION (Label = '#MetricAdjustmentHistoryReferral-Update')
 --     EXEC MDR.spGetRowCountByQueryLabel '#MetricAdjustmentHistoryReferral-Update', @UpdateCount OUT

 --      SET @EndTime = GETDATE()
 --      SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

 --     EXEC MDR.spProcessTaskLogUpdateRowCount
	--       @ETLJobProcessRunId 
 --        , @ComponentName
 --        , @Source 
 --        , @Target 
 --        , @UpdateCount	 
 --        , @DurationInSeconds
	   


/*
	TYPE 2 INSERT - INSERT NEW RECS THAT HAVE HAD AN UPDATE SINCE LAST ETL OR RECS THAT DIDN'T PREVIOUSLY EXIST
*/

SET @StartTime = GETDATE()

	INSERT 
	  INTO #MetricAdjustmentHistoryReferral ( 
		   ReferredClientNumber
		 , ReferringClientNumber
		 , ReferralID
		 , SourceIndicator
		 , ReferralDate
		 , ClientTradingDate
		 , ReferralSource
		 , ReferralCreditApproval
		 , ReferralClientDevelopmentApproval
		 , ReferralType
		 , ReferralStatus
		 , SubmittedICEmployeeId
		 , FundedICEmployeeId 
		 , SubmittedOSPEmployeeId
		 , FundedOSPEmployeeId 
		 , SubmittedAssignedToEmployeeId 
		 , FundedAssignedToEmployeeId
		 , RowHash
		 , EffectiveStartDate
		 , EffectiveEndDate
		 , CurrentRecord
		 , DWCreatedDateTime
		 , DWUpdatedDateTime
		 , ETLJobProcessRunId
		 , ETLJobSystemRunId
	) 

	SELECT SRC.ProspectCID
	     , SRC.ReferringCID
		 , SRC.ReferralID
		 , SRC.SourceIndicator
		 , SRC.ReferralDate
		 , SRC.ClientTradingDate
		 , SRC.ReferralSource
		 , SRC.ReferralCredit
		 , SRC.ClientDevelopmentApproval
		 , SRC.ReferralType
		 , SRC.ReferralStatus
		 , SRC.SubmittedICEmployeeId
		 , SRC.FundedICEmployeeId 
		 , SRC.SubmittedOSPEmployeeId
		 , SRC.FundedOSPEmployeeId 
		 , SRC.SubmittedAssignedToEmployeeId 
		 , SRC.FundedAssignedToEmployeeId
		 , SRC.RowHash
		 , @Today AS EffectiveStartDate
		 , @MaxDateValue AS EffectiveEndDate 
		 , 1 AS CurrentRecord 
		 , @DWUpdatedDatetime
		 , @DWUpdatedDatetime
		 , @ETLJobProcessRunId
		 , @ETLJobSystemRunId
	  FROM #Stg_Ext_Referral SRC
	  LEFT 
	  JOIN #MetricAdjustmentHistoryReferral AS TGT 
	    ON TGT.ReferralID = SRC.ReferralID
	   AND TGT.SourceIndicator = SRC.SourceIndicator
	 WHERE TGT.RowHash IS NULL 
		OR (TGT.DWUpdatedDateTime = @DWUpdatedDateTime AND SRC.RowHash <> TGT.RowHash) 


--	OPTION (Label = '#MetricAdjustmentHistoryReferral-Insert')
--      EXEC MDR.spGetRowCountByQueryLabel '#MetricAdjustmentHistoryReferral-UpInsertdate', @InsertCount OUT


--	   SET @EndTime = GETDATE()
--	   SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--	  EXEC MDR.spProcessTaskLogInsertRowCount
--		   @ETLJobProcessRunId 
--		 , @ComponentName
--		 , @Source 
--		 , @Target 
--		 , @InsertCount	 
--		 , @DurationInSeconds


COMMIT TRANSACTION

END TRY
BEGIN CATCH 
-- An error occurred; simply rollback the transaction started at the beginnig the of the procedure.

ROLLBACK TRANSACTION;

SET @Status = 0
SET @ErrorMessage = CONCAT(@Source,'-',@Target,':', ERROR_MESSAGE())

END CATCH 

--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END
--GO

--64,474
--select *
--  from #MetricAdjustmentHistoryReferral

--select *
--  from #Stg_Ext_Referral



--64506
select *
  from #MetricAdjustmentHistoryReferral




/***********************************************************************

					FACT CLIENT REFERRAL UPSERT	

************************************************************************/
IF OBJECT_ID('TEMPDB..#FactClientReferral') IS NOT NULL DROP TABLE #FactClientReferral

CREATE TABLE #FactClientReferral (
	[DimDateKey] [int] NOT NULL,
	[ReferralID] [int] NOT NULL,
	[DimReferralTypeKey] [int] NOT NULL,
	[DimReferredContactKey] [int] NOT NULL,
	[DimReferredClientKey] [int] NOT NULL,
	[DimReferredClientTenureKey] [int] NOT NULL,
	[DimReferredClientAgeGroupKey] [int] NOT NULL,
	[DimReferredClientAssetsKey] [int] NOT NULL,
	[DimReferringClientKey] [int] NOT NULL,
	[DimReferringClientTenureKey] [int] NOT NULL,
	[DimReferringClientAgeGroupKey] [int] NOT NULL,
	[DimReferringClientAssetsKey] [int] NOT NULL,
	[DimICEmployeeKey] [int] NOT NULL,
	[DimICEmployeeFundedKey] [int] NOT NULL,
	[DimICEmployeeMgmtKey] [int] NOT NULL,
	[DimICEmployeeMgmtFundedKey] [int] NOT NULL,
	[DimICTeamMemberKey] [int] NOT NULL,
	[DimICTeamMemberFundedKey] [int] NOT NULL,
	[DimICPeerGroupKey] [int] NOT NULL,
	[DimICPeerGroupFundedKey] [int] NOT NULL,
	[DimOSPEmployeeKey] [int] NOT NULL,
	[DimOSPEmployeeFundedKey] [int] NOT NULL,
	[DimOSPEmployeeMgmtKey] [int] NOT NULL,
	[DimOSPEmployeeMgmtFundedKey] [int] NOT NULL,
	[DimOSPTeamMemberKey] [int] NOT NULL,
	[DimOSPTeamMemberFundedKey] [int] NOT NULL,
	[DimAssignedToEmployeeKey] [int] NOT NULL,
	[DimAssignedToEmployeeFundedKey] [int] NOT NULL,
	[DimAssignedToEmployeeMgmtKey] [int] NOT NULL,
	[DimAssignedToEmployeeMgmtFundedKey] [int] NOT NULL,
	[DimAssignedToTeamMemberKey] [int] NOT NULL,
	[DimAssignedToTeamMemberFundedKey] [int] NOT NULL,
	[DimReferralDateKey] [int] NOT NULL,
	[DimClientTradingDateKey] [int] NOT NULL,
	[ReferralCount] [int] NULL,
	[SubmittedReferralCount] [int] NULL,
	[FundedReferralCount] [int] NULL,
	[MaxReferralCount] [int] NULL,
	[ClientAlreadyReferred12Mos] [int] NULL,
	[SalesMeeting12Mos] [int] NULL,
	[ClientDevelopmentMeeting12Mos] [int] NULL,
	[FundingDaysLag] [int] NULL,
	[CompletedEvent] [int] NULL,
	[SourceIndicator] varchar(4) NULL,
	[RowHash] [VARBINARY](8000) NULL,
	[DWCreatedDateTime] [datetime] NULL,
	[DWUpdatedDateTime] [datetime] NULL,
	[ETLJobProcessRunId] [uniqueidentifier] NULL,
	[ETLJobSystemRunId] [uniqueidentifier] NULL
)
WITH 
(
	DISTRIBUTION = HASH ( [ReferralID] ),
	CLUSTERED COLUMNSTORE INDEX
)




DECLARE @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
      , @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()



-- ======================================================================================

-- Authors:      Rachel Platt, Armando Kuri

-- Create date: 5/15/2023
-- Update date: 7/6/2023  PDDTI-673           
-- Update date: 2/7/2024  PDDTI-1483 - Removed Status 4 and 5 for UK and EU Referrals. Changed assignment to be the IC assigned to Referring CID
-- Update Date: 5/23/2024 PDDTI-1855 -Edits DimPeerGroup join to leverage AdjustedJobGroupingRoleStartDate from DimEmployee rather than RoleStartDate
-- Update date: 5/16/2024 PDDTI-1779 - Pulls bday from REF table rather than Iris
-- Update date: 5/20/2024 PDDTI-1848 - Changes the US logic to attribute the Funding IC, When the assignment is null in the transition DB. It also saves all updates into a reference table.
-- Update date: 6/20/2024 PDDTI-1906 - Added a Row hash to the fact table, to optimize the update stament of the Sproc
-- Update date: 6/20/2024 PDDTI-1820 - Added the MaxReferralCount to capture the latest referral for a prospect under the 12 mo maturity period between entries. Added this flag to ref net flows
-- Update date: 6/20/2024 PDDTI-1959 - patched a bug for clients with an ID of -1 that mapped to the Unknown member in the Reference sproc used in this table
-- Update date: 8/07/2024 PDDTI-2084 - Refactored proc logic for efficiency and updated IC referral attribution logic
-- Description: This sproc populates FactClientReferral with various Dim Keys which
--              contain attributes related to the referral activity and the IC and OSP
--              involved in the interaction.
--              
--              This is an accummulating snapshot fact with one record per unique referral.

-- ======================================================================================

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.

DECLARE @DWUpdatedDatetime DATETIME
      , @Rows INT
      , @StartTime DATETIME
      , @EndTime DATETIME
      , @DurationInSeconds INT
      , @Source NVARCHAR(255)
      , @Target NVARCHAR(255)
      , @Status INT
      , @ErrorMessage NVARCHAR(512)
	  , @UnknownTextValue NVARCHAR(512)
	  , @UnknownNumberValue INT
	  , @MinDateValue DATE
	  , @MaxDateValue DATE

DECLARE @TODAY DATE  = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
       
DECLARE @InsertCount BIGINT
	, @InsertOperation NVARCHAR(20) 
	, @UpdateCount BIGINT
	, @UpdateOperation NVARCHAR(20) 


SET @InsertOperation = 'INSERT'
SET @UpdateOperation = 'UPDATE'

       
SET @DWUpdatedDatetime = GETDATE()
SET @Status = 1
SET @Rows = 0



SELECT TOP 1 @UnknownNumberValue = CONVERT(INT,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownNumberValue' 

 SELECT TOP 1 @UnknownTextValue = [Value]
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'UnknownTextValue'

 SELECT TOP 1 @MinDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MinDateValue'

 SELECT TOP 1 @MaxDateValue = CONVERT(DATE,[Value])
  FROM [MDR].DefaultDataConfiguration DF 
 WHERE DF.Name = 'MaxDateValue'


IF OBJECT_ID('tempdb..#Stg_ReferralKeys', 'U') IS NOT NULL
	DROP TABLE #Stg_ReferralKeys

CREATE TABLE #Stg_ReferralKeys (
	   DimDateKey INT 
	 , ReferralID INT 
	 , DimReferralTypeKey INT 
	 , DimReferredContactKey INT 
	 , DimReferredClientKey INT 
	 , DimReferredClientTenureKey INT 
	 , DimReferredClientAgeGroupKey INT 
	 , DimReferredClientAssetsKey INT 
	 , DimReferringClientKey INT 
	 , DimReferringClientTenureKey INT 
	 , DimReferringClientAgeGroupKey INT 
	 , DimReferringClientAssetsKey INT 
	 , DimICEmployeeKey INT 
	 , DimICEmployeeFundedKey INT 
	 , DimICEmployeeMgmtKey INT 
	 , DimICEmployeeMgmtFundedKey INT 
	 , DimICTeamMemberKey INT 
	 , DimICTeamMemberFundedKey INT 
	 , DimICPeerGroupKey INT 
	 , DimICPeerGroupFundedKey INT 
	 , DimOSPEmployeeKey INT 
	 , DimOSPEmployeeFundedKey INT 
	 , DimOSPEmployeeMgmtKey INT 
	 , DimOSPEmployeeMgmtFundedKey INT 
	 , DimOSPTeamMemberKey INT 
	 , DimOSPTeamMemberFundedKey INT 
	 , DimAssignedToEmployeeKey INT 
	 , DimAssignedToEmployeeFundedKey INT 
	 , DimAssignedToEmployeeMgmtKey INT 
	 , DimAssignedToEmployeeMgmtFundedKey INT 
	 , DimAssignedToTeamMemberKey INT 
	 , DimAssignedToTeamMemberFundedKey INT 
	 , DimReferralDateKey INT 
	 , DimClientTradingDateKey INT 
	 , ReferralCount INT 
	 , SubmittedReferralCount INT 
	 , FundedReferralCount INT 
	 , MaxReferralCount INT 
	 , ClientAlreadyReferred12Mos INT 
	 , SalesMeeting12Mos INT 
	 , ClientDevelopmentMeeting12Mos INT 
	 , FundingDaysLag INT 
	 , CompletedEvent INT 
	 , SourceIndicator VARCHAR(4) 
	 , RowHash VARBINARY(8000) 
)
WITH (DISTRIBUTION = HASH(ReferralID), HEAP)


IF OBJECT_ID('tempdb..#MinDC', 'U') IS NOT NULL
    DROP TABLE #MinDC

CREATE TABLE #MinDC (
	   ReferredClientNumber NVARCHAR(255)
     , ClientTradingDate DATE NULL
)
WITH (DISTRIBUTION = HASH(ReferredClientNumber), HEAP)


IF OBJECT_ID('tempdb..#ClientMeetings', 'U') IS NOT NULL
    DROP TABLE #ClientMeetings

CREATE TABLE #ClientMeetings (
	   ReferredClientNumber NVARCHAR(255)
	 , MeetingType varchar(50)
     , MeetingDate DATE NULL
)
WITH (DISTRIBUTION = HASH(ReferredClientNumber), HEAP)


IF OBJECT_ID('tempdb..#MultipleReferrals', 'U') IS NOT NULL
    DROP TABLE #MultipleReferrals

CREATE TABLE #MultipleReferrals (
	   ReferredClientNumber NVARCHAR(255)
	 , ReferralID INT NULL
     , RefDateID varchar(30) NULL
	 , ReferralDate DATE NULL
	 , SourceIndicator varchar(4) NULL
)
WITH (DISTRIBUTION = HASH(ReferralID), HEAP)


IF OBJECT_ID('tempdb..#StgDailyClientAssetsTenureKeys', 'U') IS NOT NULL
	DROP TABLE #StgDailyClientAssetsTenureKeys

CREATE TABLE #StgDailyClientAssetsTenureKeys
WITH (DISTRIBUTION = HASH(ClientNumber), HEAP)
AS
SELECT STG.CalendarDate
     , STG.DimDateKey
     , STG.ClientId
     , STG.ClientNumber
     , STG.HouseholdUID
     , STG.HouseholdId
     , STG.AUM_USD
     , STG.NetLiquidAssets
     , STG.TotalLiquidAssets
     , STG.TotalLiabilities
     , STG.TNW
     , STG.ClientAssetsType
     , STG.SOW
     , STG.DimTenureKey
     , STG.DimClientAssetsKey
  FROM STG.DailyClientAssetsandTenureKeys AS STG
  JOIN #MetricAdjustmentHistoryReferral AS REF
    ON STG.ClientNumber = REF.ReferringClientNumber
   AND REF.CurrentRecord = 1
 UNION --UNION REFERRED CLIENT 
SELECT STG.CalendarDate
     , STG.DimDateKey
     , STG.ClientId
     , STG.ClientNumber
     , STG.HouseholdUID
     , STG.HouseholdId
     , STG.AUM_USD
     , STG.NetLiquidAssets
     , STG.TotalLiquidAssets
     , STG.TotalLiabilities
     , STG.TNW
     , STG.ClientAssetsType
     , STG.SOW
     , STG.DimTenureKey
     , STG.DimClientAssetsKey
  FROM STG.DailyClientAssetsandTenureKeys AS STG
  JOIN #MetricAdjustmentHistoryReferral AS REF
    ON STG.ClientNumber = REF.ReferredClientNumber
   AND REF.CurrentRecord = 1

/*
	GATHER CLIENT MEETINGS
*/

	INSERT 
	  INTO #ClientMeetings (
		   ReferredClientNumber
		 , MeetingType
		 , MeetingDate
    )

	SELECT OA.fi_id_search AS ReferredClientNumber
		 , 'Sales Meeting'
		 , MAX(OA.ModifiedOn) AS MeetingValidDate
	  FROM Iris.fi_opportunityauditlogBase AS OA
	  JOIN Iris.fi_topicBase AS Topic1
		ON Topic1.fi_TopicId = OA.fi_ActivityTopic1
	  JOIN Iris.ContactBase AS C
		ON C.ContactId = OA.fi_CustomerId
	 GROUP 
	    BY OA.fi_id_search, Topic1.fi_id
    HAVING Topic1.fi_id = 101961
	   AND NULLIF(ISDATE(MAX(OA.ModifiedOn)),0) > 0


	INSERT 
	  INTO #ClientMeetings (
	       ReferredClientNumber
	     , MeetingType
         , MeetingDate
	)
	SELECT C.fi_id_search AS ReferredClientNumber
		 , 'CD Meeting'
		 , MAX(IA.ModifiedOn) AS ActivityDate
	  FROM Iris.fi_incidentauditlogBase AS IA
	  JOIN Iris.fi_topicBase AS Topic1
		ON Topic1.fi_TopicId = IA.fi_ActivityTopic1
	   AND Topic1.fi_id IN (112800 -- Met Client Virtually
		                  , 102076 -- Met Client Here	
		                  , 102075 -- Met Client There
						  )
	  JOIN Iris.ContactBase AS C
		ON C.ContactId = IA.fi_CustomerId
     GROUP 
	    BY C.fi_id_search, IA.fi_CaseTypeCode
	HAVING IA.fi_CaseTypeCode = 157610001
	   AND NULLIF(ISDATE(MAX(IA.ModifiedOn)),0) > 0


/*
	Business deems that the maturity of a referral should be 12 mo. So This CTE yields all
	Prospects with multiple referral entries, so we can determine if its been at least 12 mo
	since the last referral. If the referral is on the same date the go with the Referral ID to
	make the determination. 
*/

;WITH MultiRef AS (

	SELECT iProspectCID AS ReferredClientNumber
		 , iId AS ReferralID
		 , CONCAT(CAST(COALESCE(dtReferralDate, dtSubmitted_Date, dtPresubmission) AS DATE), '-', iId) AS RefDateID
		 , COALESCE(dtReferralDate, dtSubmitted_Date, dtPresubmission) AS ReferralDate
		 , 'US' AS SourceIndicator
	  FROM BAS.Referral_Main
	 WHERE iProspectCID IN (
			SELECT iProspectCID
			  FROM BAS.Referral_Main RM
			 WHERE iProspectCID IS NOT NULL
			   AND iStatus IN (4,5)
			 GROUP BY iProspectCID
			HAVING COUNT(1) > 1
		 )

	 UNION --UNION EU MULTIPLE REFERRALS
	
	SELECT iProspectCID AS ReferredClientNumber
		 , [iPrimaryKey] AS ReferralID
		 , CONCAT(CAST(COALESCE(dtReferral, dtEntered) AS DATE), '-', iPrimaryKey) AS RefDateID
		 , COALESCE(dtReferral, dtEntered) AS ReferralDate
		 , 'EU' AS SourceIndicator
	  FROM BAS.EUReferralMain
	 WHERE iProspectCID IN (
			SELECT iProspectCID
			  FROM BAS.EUReferralMain RM
		     WHERE iProspectCID IS NOT NULL
		     GROUP BY iProspectCID
		    HAVING COUNT(1) > 1
		 )

	 UNION --UNION UK MULTIPLE REFERRALS

	SELECT iProspectCID AS ReferredClientNumber
		 , [PrimaryKey] AS ReferralID
		 , CONCAT(CAST(ReferralDate AS DATE), '-', PrimaryKey) AS RefDateID
		 , ReferralDate
		 , 'UK' AS SourceIndicator
	  FROM BAS.UKReferral_Main
	 WHERE iProspectCID IN (
			SELECT iProspectCID
		      FROM BAS.EUReferralMain RM
		     WHERE iProspectCID IS NOT NULL
		     GROUP BY iProspectCID
		    HAVING COUNT(1) > 1
		 )

)

	INSERT 
	  INTO #MultipleReferrals (
		   ReferredClientNumber
		 , ReferralID
		 , ReferralDate
		 , RefDateID
		 , SourceIndicator
	)

	SELECT DISTINCT
	       Ref.ReferredClientNumber
	     , Ref.ReferralID
	     , Ref.ReferralDate
	     , Ref.RefDateID
	     , Ref.SourceIndicator 
      FROM MultiRef AS Ref
	  --Join to itself and identify all of the referrals that have a prior one from at least 12 mo.
      JOIN MultiRef AS OlderRef
		ON Ref.ReferredClientNumber = OlderRef.ReferredClientNumber
	   AND Ref.SourceIndicator = OlderRef.SourceIndicator
	   AND Ref.ReferralID <> OlderRef.ReferralID
	   AND Ref.RefDateID > OlderRef.RefDateID
	   AND Ref.ReferralDate <= DATEADD(mm, 12, OlderRef.ReferralDate)


/*
	1. LOAD STAGING TEMP TABLE WITH CURRENT RECORD FROM #MetricAdjustmentHistoryReferral
	2. ADD ADDITIONAL DESCRIPTIVE ATTRIBUTES FROM DW DIMENSIONS
	3. ADD MEASURES FOR FACT TABLE
*/

	INSERT 
	  INTO #Stg_ReferralKeys (
		   DimDateKey 
		 , ReferralID 
		 , DimReferralTypeKey 
		 -- Referred Client
		 , DimReferredContactKey 
		 , DimReferredClientKey
		 , DimReferredClientTenureKey
		 , DimReferredClientAgeGroupKey
		 , DimReferredClientAssetsKey 
		 --Referring Client		
		 , DimReferringClientKey 
		 , DimReferringClientTenureKey 
		 , DimReferringClientAgeGroupKey 
		 , DimReferringClientAssetsKey 
		 --IC
		 , DimICEmployeeKey
		 , DimICEmployeeFundedKey 
		 , DimICEmployeeMgmtKey 
		 , DimICEmployeeMgmtFundedKey 
		 , DimICTeamMemberKey
		 , DimICTeamMemberFundedKey 
		 , DimICPeerGroupKey
		 , DimICPeerGroupFundedKey 
		 --OSP
		 , DimOSPEmployeeKey
		 , DimOSPEmployeeFundedKey
		 , DimOSPEmployeeMgmtKey
		 , DimOSPEmployeeMgmtFundedKey 
		 , DimOSPTeamMemberKey
		 , DimOSPTeamMemberFundedKey 
		 --Assigned To
		 , DimAssignedToEmployeeKey 
		 , DimAssignedToEmployeeFundedKey 
		 , DimAssignedToEmployeeMgmtKey
		 , DimAssignedToEmployeeMgmtFundedKey  
		 , DimAssignedToTeamMemberKey
		 , DimAssignedToTeamMemberFundedKey
		 --Days
		 , DimReferralDateKey 
		 , DimClientTradingDateKey
		 --Facts
		 , ReferralCount
		 , SubmittedReferralCount
		 , FundedReferralCount
		 , ClientAlreadyReferred12Mos 
		 , SalesMeeting12Mos
		 , ClientDevelopmentMeeting12Mos
		 , MaxReferralCount
		 , FundingDaysLag
		 , CompletedEvent
		 , SourceIndicator
		 , RowHash
	)
    
	SELECT ISNULL(DD.DimDateKey, @UnknownNumberValue) AS DimDateKey
	     , REF.ReferralID
	     , ISNULL(DimType.DimReferralTypeKey, @UnknownNumberValue) AS DimReferralTypeKey

	     --REFERRED CLIENT
		 , ISNULL(Cont.DimContactKey, @UnknownNumberValue) AS DimReferredContactKey
		 , ISNULL(DimReferralC.DimClientKey, @UnknownNumberValue) AS DimReferredClientKey
		 , ISNULL(CATK.DimTenureKey, @UnknownNumberValue) AS DimReferredClientTenureKey
		 , ISNULL(AGED.DimAgeGroupKey, @UnknownNumberValue) AS DimReferredClientAgeGroupKey
		 , ISNULL(CATK.DimClientAssetsKey, @UnknownNumberValue) AS DimReferredClientAssetsKey

	     --REFERRING CLIENT	
		 , ISNULL(DimReferringC.DimClientKey, @UnknownNumberValue) AS DimReferringClientKey
		 , ISNULL(CATK2.DimTenureKey, @UnknownNumberValue) AS DimReferringClientTenureKey
		 , ISNULL(AG.DimAgeGroupKey, @UnknownNumberValue) AS DimReferringClientAgeGroupKey
		 , ISNULL(CATK2.DimClientAssetsKey, @UnknownNumberValue) AS DimReferringClientAssetsKey

		 --IC
		 , ISNULL(SIC_E.DimEmployeeKey, @UnknownNumberValue) AS DimICEmployeeKey
		 , ISNULL(FIC_E.DimEmployeeKey, @UnknownNumberValue) AS DimICEmployeeFundedKey
		 , ISNULL(SIC_EM.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimICEmployeeMgmtKey
		 , ISNULL(FIC_EM.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimICEmployeeMgmtFundedKey
		 , ISNULL(SIC_TM.DimTeamMemberKey, @UnknownNumberValue) AS DimICTeamMemberKey
		 , ISNULL(FIC_TM.DimTeamMemberKey, @UnknownNumberValue) AS DimICTeamMemberFundedKey
		 , ISNULL(SIC_PG.DimPeerGroupKey, @UnknownNumberValue) AS DimICPeerGroupKey
		 , ISNULL(FIC_PG.DimPeerGroupKey, @UnknownNumberValue) AS DimICPeerGroupFundedKey
		 
		 --OSP
		 , ISNULL(EOSP.DimEmployeeKey, @UnknownNumberValue) AS DimOSPEmployeeKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EOSP.DimEmployeeKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimOSPEmployeeFundedKey
		 , ISNULL(EMGMTOSP.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimOSPEmployeeMgmtKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EMGMTOSP.DimEmployeeMgmtKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimOSPEmployeeMgmtFundedKey
		 , ISNULL(TMOSP.DimTeamMemberKey, @UnknownNumberValue) AS DimOSPTeamMemberKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(TMOSP.DimTeamMemberKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimOSPTeamMemberFundedKey
		 
		 --ASSIGNED TO 
		 , ISNULL(EA.DimEmployeeKey, @UnknownNumberValue) AS DimAssignedToEmployeeKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EA.DimEmployeeKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimAssignedToEmployeeFundedKey
		 , ISNULL(EMGMTA.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimAssignedToEmployeeMgmtKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EMGMTA.DimEmployeeMgmtKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimAssignedToEmployeeMgmtFundedKey
		 , ISNULL(TMA.DimTeamMemberKey, @UnknownNumberValue) AS DimAssignedToTeamMemberKey
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(TMA.DimTeamMemberKey, @UnknownNumberValue) ELSE @UnknownNumberValue END AS DimAssignedToTeamMemberFundedKey
	     
		 --DAYS
		 , ISNULL(RefDate.DimDateKey, @UnknownNumberValue) AS DimReferralDateKey
		 , ISNULL(CTDDate.DimDateKey, @UnknownNumberValue) AS DimClientTradingDateKey
	     
		 --FACTS
		 , 1 AS ReferralCount
		 , CASE WHEN REF.ReferralType ='Submitted Referral' THEN 1 ELSE 0 END AS  SubmittedReferralCount
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN 1 ELSE 0 END AS  FundedReferralCount
	     , CASE WHEN prCTE.ReferralID IS NULL THEN 0 ELSE 1 END AS ClientAlreadyReferred12Mos
	     , CASE WHEN smCTE.ReferredClientNumber IS NULL THEN 0 ELSE 1 END AS SalesMeeting12Mos
	     , CASE WHEN cdCTE.ReferredClientNumber IS NULL THEN 0 ELSE 1 END AS ClientDevelopmentMeeting12Mos
	     , CASE WHEN MAXR.MAXRefID IS NULL THEN 0 ELSE 1 END AS MaxReferralCount
		 , DATEDIFF(DAY, REF.ReferralDate, REF.ClientTradingDate) AS FundingDaysLag
		 , CASE WHEN REF.ReferralType ='Funded Referral' THEN 1 ELSE 0 END AS CompletedEvent
		 , REF.SourceIndicator
		 , HASHBYTES('SHA2_256', CONCAT(						
							  ISNULL(DD.DimDateKey, @UnknownNumberValue) 
							, '|', REF.ReferralID
							, '|', ISNULL(DimType.DimReferralTypeKey, @UnknownNumberValue) 
							-- Referred Client
							, '|', ISNULL(Cont.DimContactKey, @UnknownNumberValue) 
							, '|', ISNULL(DimReferralC.DimClientKey, @UnknownNumberValue) 
							, '|', ISNULL(CATK.DimTenureKey, @UnknownNumberValue)
							, '|', ISNULL(AGED.DimAgeGroupKey, @UnknownNumberValue) 
							, '|', ISNULL(CATK.DimClientAssetsKey, @UnknownNumberValue)
							--Referring Client	
							, '|', ISNULL(DimReferringC.DimClientKey, @UnknownNumberValue)
							, '|', ISNULL(CATK2.DimTenureKey, @UnknownNumberValue)
							, '|', ISNULL(AG.DimAgeGroupKey, @UnknownNumberValue) 
							, '|', ISNULL(CATK2.DimClientAssetsKey, @UnknownNumberValue) 
							--IC
							, '|', ISNULL(SIC_E.DimEmployeeKey, @UnknownNumberValue) 
							, '|', ISNULL(FIC_E.DimEmployeeKey, @UnknownNumberValue) 
							, '|', ISNULL(SIC_EM.DimEmployeeMgmtKey, @UnknownNumberValue) 
							, '|', ISNULL(FIC_EM.DimEmployeeMgmtKey, @UnknownNumberValue) 
							, '|', ISNULL(SIC_TM.DimTeamMemberKey, @UnknownNumberValue) 
							, '|', ISNULL(FIC_TM.DimTeamMemberKey, @UnknownNumberValue) 
		                    , '|', ISNULL(SIC_PG.DimPeerGroupKey, @UnknownNumberValue) 
		                    , '|', ISNULL(FIC_PG.DimPeerGroupKey, @UnknownNumberValue) 
							--OSP
							, '|', ISNULL(EOSP.DimEmployeeKey, @UnknownNumberValue) 
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EOSP.DimEmployeeKey, @UnknownNumberValue) ELSE @UnknownNumberValue END 
							, '|', ISNULL(EMGMTOSP.DimEmployeeMgmtKey, @UnknownNumberValue)
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EMGMTOSP.DimEmployeeMgmtKey, @UnknownNumberValue) ELSE @UnknownNumberValue END 
							, '|', ISNULL(TMOSP.DimTeamMemberKey, @UnknownNumberValue) 
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(TMOSP.DimTeamMemberKey, @UnknownNumberValue) ELSE @UnknownNumberValue END 
							--Assigned To
							, '|', ISNULL(EA.DimEmployeeKey, @UnknownNumberValue) 
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EA.DimEmployeeKey, @UnknownNumberValue) ELSE @UnknownNumberValue END
							, '|', ISNULL(EMGMTA.DimEmployeeMgmtKey, @UnknownNumberValue)
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(EMGMTA.DimEmployeeMgmtKey, @UnknownNumberValue) ELSE @UnknownNumberValue END 
							, '|', ISNULL(TMA.DimTeamMemberKey, @UnknownNumberValue)
							, '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN ISNULL(TMA.DimTeamMemberKey, @UnknownNumberValue) ELSE @UnknownNumberValue END
							--Days
							, '|', ISNULL(RefDate.DimDateKey, @UnknownNumberValue)
							, '|', ISNULL(CTDDate.DimDateKey, @UnknownNumberValue)
							--Facts
						    , '|', CASE WHEN REF.ReferralType ='Submitted Referral' THEN 1 ELSE 0 END
						    , '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN 1 ELSE 0 END
						    , '|', CASE WHEN prCTE.ReferralID IS NULL THEN 0 ELSE 1 END
						    , '|', CASE WHEN smCTE.ReferredClientNumber IS NULL THEN 0 ELSE 1 END
						    , '|', CASE WHEN cdCTE.ReferredClientNumber IS NULL THEN 0 ELSE 1 END
						    , '|', CASE WHEN MAXR.MAXRefID IS NULL THEN 0 ELSE 1 END
						    , '|', DATEDIFF(DAY, REF.ReferralDate, REF.ClientTradingDate)
						    , '|', CASE WHEN REF.ReferralType ='Funded Referral' THEN 1 ELSE 0 END
						    , '|', REF.SourceIndicator
						)) AS RowHash

      FROM #MetricAdjustmentHistoryReferral AS REF
	        
	  --Max referral that hasnt been referred 12 mo after a prior referral maturity
	  LEFT 
	  JOIN (SELECT MaxRef.ReferredClientNumber
	             , MaxRef.SourceIndicator
	             , MAX(MaxRef.ReferralID) MAXRefID 
			  FROM #MetricAdjustmentHistoryReferral MaxRef 
			  LEFT 
			  JOIN #MultipleReferrals prCTE
				ON MaxRef.ReferredClientNumber = prCTE.ReferredClientNumber
			   AND prCTE.SourceIndicator = MaxRef.SourceIndicator
			   AND MaxRef.ReferralID = prCTE.ReferralID 
			 WHERE prCTE.ReferralID IS NULL 
			 GROUP BY MaxRef.ReferredClientNumber, MaxRef.SourceIndicator
		   ) AS MAXR
		ON REF.ReferralID = MAXR.MAXRefID
	   AND REF.SourceIndicator = MAXR.SourceIndicator

	  -- referral after previous referral in year since previous referral 
	  LEFT  
	  JOIN #MultipleReferrals prCTE
		ON REF.ReferredClientNumber = prCTE.ReferredClientNumber
	   AND REF.SourceIndicator = prCTE.SourceIndicator
	   AND REF.ReferralId = prCTE.ReferralID

	  -- referral after sales meeting and in year since that sales meeting
	  LEFT  
	  JOIN #ClientMeetings smCTE
		ON REF.ReferredClientNumber = smCTE.ReferredClientNumber
	   AND REF.ClientTradingDate > smCTE.MeetingDate
	   AND REF.ClientTradingDate <= DATEADD(mm, 12, smCTE.MeetingDate)
	   AND smCTE.MeetingType = 'Sales Meeting'

	  -- referral after CD meeting and in year since that meeting
	  LEFT  
	  JOIN #ClientMeetings cdCTE
		ON REF.ReferredClientNumber = cdCTE.ReferredClientNumber
	   AND REF.ClientTradingDate > cdCTE.MeetingDate
	   AND REF.ClientTradingDate <= DATEADD(mm, 12, cdCTE.MeetingDate)
	   AND cdCTE.MeetingType = 'CD Meeting'

	  LEFT 
	  JOIN FDW.DimDate AS DD 
	    ON CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END = DD.CalendarDate

	  LEFT 
	  JOIN FDW.DimDate AS RefDate
		ON CONVERT(DATE, REF.ReferralDate) = RefDate.CalendarDate

	  LEFT 
	  JOIN FDW.DimDate AS CTDDate
		ON CONVERT(DATE, REF.ClientTradingDate) = CTDDate.CalendarDate

	  LEFT 
	  JOIN FDW.DimReferralType AS DimType
		ON DimType.ReferralType =REF.ReferralType
	   AND DimType.ReferralSource = REF.ReferralSource
	   AND DimType.ReferralStatus = REF.ReferralStatus
	   AND DimType.ReferralCreditApproval = REF.ReferralCreditApproval
	   AND DimType.ReferralClientDevelopmentApproval = REF.ReferralClientDevelopmentApproval
	   
	  --SUBMITTED IC JOINS
	  LEFT 
	  JOIN FDW.DimEmployee AS SIC_E
		ON REF.SubmittedICEmployeeId = SIC_E.EmployeeId
	   AND REF.ReferralDate >= SIC_E.EffectiveStartDate
	   AND REF.ReferralDate < SIC_E.EffectiveEndDate
	   
	  LEFT 
	  JOIN FDW.DimEmployeeMgmt AS SIC_EM
		ON SIC_E.EmployeeID = SIC_EM.EmployeeId
 	   AND REF.ReferralDate >= SIC_EM.EffectiveStartDate
	   AND REF.ReferralDate <= SIC_EM.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimTeamMember AS SIC_TM
		ON SIC_TM.TeamMemberActiveDirectoryUserIdWithDomain = SIC_E.ActiveDirectoryUserIdWithDomain
	   AND REF.ReferralDate >= SIC_TM.EffectiveStartDate
	   AND REF.ReferralDate < SIC_TM.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimPeerGroupIC AS SIC_PG
		ON SIC_PG.TeamMemberSpecialty = SIC_TM.TeamMemberSpecialty
	   --AND SIC_PG.TenureInMonths = FDW.fnGetTenureInMonths(SIC_E.AdjustedJobGroupingRoleStartDate, REF.ReferralDate)	
	   AND SIC_PG.TenureInMonths = DATEDIFF(M, SIC_E.AdjustedJobGroupingRoleStartDate, REF.ReferralDate)

	  --FUNDED IC JOINS
	  LEFT 
	  JOIN FDW.DimEmployee AS FIC_E
		ON REF.FundedICEmployeeId = FIC_E.EmployeeId
	   AND REF.ClientTradingDate >= FIC_E.EffectiveStartDate
	   AND REF.ClientTradingDate < FIC_E.EffectiveEndDate
	   
	  LEFT 
	  JOIN FDW.DimEmployeeMgmt AS FIC_EM
		ON FIC_E.EmployeeID = FIC_EM.EmployeeId
 	   AND REF.ClientTradingDate >= FIC_EM.EffectiveStartDate
	   AND REF.ClientTradingDate <= FIC_EM.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimTeamMember AS FIC_TM
		ON FIC_TM.TeamMemberActiveDirectoryUserIdWithDomain = FIC_E.ActiveDirectoryUserIdWithDomain
	   AND REF.ClientTradingDate >= FIC_TM.EffectiveStartDate
	   AND REF.ClientTradingDate < FIC_TM.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimPeerGroupIC AS FIC_PG
		ON FIC_PG.TeamMemberSpecialty = FIC_TM.TeamMemberSpecialty
	   --AND FIC_PG.TenureInMonths = FDW.fnGetTenureInMonths(FIC_E.AdjustedJobGroupingRoleStartDate, REF.ClientTradingDate)
	   AND FIC_PG.TenureInMonths = DATEDIFF(M, FIC_E.AdjustedJobGroupingRoleStartDate, REF.ClientTradingDate)		  

	  --OSP JOINS
	  LEFT 
	  JOIN FDW.DimEmployee AS EOSP
		ON REF.SubmittedOSPEmployeeId = EOSP.EmployeeId
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= EOSP.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END < EOSP.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimEmployeeMgmt AS EMGMTOSP
		ON EOSP.EmployeeID = EMGMTOSP.EmployeeId
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= EMGMTOSP.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END <= EMGMTOSP.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimTeamMember AS TMOSP
		ON TMOSP.TeamMemberActiveDirectoryUserIdWithDomain = EOSP.ActiveDirectoryUserIdWithDomain
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= TMOSP.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END < TMOSP.EffectiveEndDate	   

	  --Assigned To JOINS
	  LEFT 
	  JOIN FDW.DimEmployee AS EA
		ON REF.SubmittedAssignedToEmployeeId = EA.EmployeeId
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= EA.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END < EA.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimEmployeeMgmt AS EMGMTA
		ON EA.EmployeeID = EMGMTA.EmployeeId
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= EMGMTA.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END <= EMGMTA.EffectiveEndDate

	  LEFT 
	  JOIN FDW.DimTeamMember AS TMA
		ON TMA.TeamMemberActiveDirectoryUserIdWithDomain = EA.ActiveDirectoryUserIdWithDomain
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END >= TMA.EffectiveStartDate
	   AND CASE WHEN REF.ReferralType = 'Funded Referral' THEN CONVERT(DATE, REF.ClientTradingDate) ELSE CONVERT(DATE, REF.ReferralDate) END < TMA.EffectiveEndDate	
	   
	  --Referred Client JOINS
	  LEFT 
	  JOIN FDW.DimContact AS Cont
		ON Cont.ContactNumber = REF.ReferredClientNumber

	  LEFT 
	  JOIN FDW.DimClient AS DimReferralC
		ON DimReferralC.ClientNumber = REF.ReferredClientNumber
	   AND DimReferralC.EffectiveStartDate <= DD.CalendarDate
	   AND DimReferralC.EffectiveEndDate > DD.CalendarDate

	  LEFT
      JOIN FDW.DimTenure AS ReferralT
	    ON COALESCE(DATEDIFF(Day, DimReferralC.ClientTradingDate, DD.CalendarDate), @UnknownNumberValue) = ReferralT.TenureInDays

	  LEFT 
	  JOIN REF.CRMClientMapping AS CRM
	    ON CRM.ClientNumber_Iris = DimReferralC.ClientNumber

      LEFT 
	  JOIN FDW.DimAgeGroup AS AGED
        ON ISNULL(FLOOR(DATEDIFF(Day, CRM.BirthDate, DD.CalendarDate)/365.25), @UnknownNumberValue) >= AGED.StartAge 
	   AND ISNULL(FLOOR(DATEDIFF(Day, CRM.BirthDate, DD.CalendarDate)/365.25), @UnknownNumberValue) < AGED.EndAge	
	 
	  LEFT 
	  JOIN #StgDailyClientAssetsTenureKeys CATK
		ON DimReferralC.ClientNumber = CATK.ClientNumber
	   AND DD.DimDateKey = CATK.DimDateKey	

	  --Referring Client Joins
	  LEFT 
	  JOIN FDW.DimClient AS DimReferringC
		ON DimReferringC.ClientNumber = REF.ReferringClientNumber
       AND DimReferringC.EffectiveStartDate <= DD.CalendarDate
	   AND DimReferringC.EffectiveEndDate > DD.CalendarDate
	   
	  LEFT
      JOIN FDW.DimTenure AS ReferringT
	    ON COALESCE(DATEDIFF(DAY, DimReferringC.ClientTradingDate, DD.CalendarDate), @UnknownNumberValue) = ReferringT.TenureInDays

	  LEFT 
	  JOIN REF.CRMClientMapping AS CBD
	    ON CBD.ClientNumber_Iris = DimReferringC.ClientNumber

      LEFT 
	  JOIN FDW.DimAgeGroup AS AG
        ON ISNULL(FLOOR(DATEDIFF(DAY, CBD.BirthDate, DD.CalendarDate)/365.25), @UnknownNumberValue) >= AG.StartAge 
	   AND ISNULL(FLOOR(DATEDIFF(DAY, CBD.BirthDate, DD.CalendarDate)/365.25), @UnknownNumberValue) < AG.EndAge

	  LEFT 
	  JOIN #StgDailyClientAssetsTenureKeys CATK2
	    ON DimReferringC.ClientNumber = CATK2.ClientNumber
	   AND DD.DimDateKey = CATK2.DimDateKey

	 WHERE REF.CurrentRecord = 1 --FILTER REF TABLE FOR CURRENT RECORD


BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
-- If any batch fail, it will be caught in the CATCH block and will be rolled back.
BEGIN TRY


SET @StartTime = GETDATE()
SET @DWUpdatedDateTime = GETDATE()


	UPDATE CR
	   SET DimDateKey = REF.DimDateKey
		 --Referred Client
		 , DimReferralTypeKey = REF.DimReferralTypeKey
		 , DimReferredContactKey = REF.DimReferredContactKey
		 , DimReferredClientKey = REF.DimReferredClientKey
		 , DimReferredClientTenureKey = REF.DimReferredClientTenureKey
		 , DimReferredClientAgeGroupKey = REF.DimReferredClientAgeGroupKey
		 , DimReferredClientAssetsKey = REF.DimReferredClientAssetsKey
		 --Referring Client
		 , DimReferringClientKey = REF.DimReferringClientKey
		 , DimReferringClientTenureKey = REF.DimReferringClientTenureKey
		 , DimReferringClientAgeGroupKey = REF.DimReferringClientAgeGroupKey
		 , DimReferringClientAssetsKey = REF.DimReferringClientAssetsKey
		 --IC (only update if submitted)
		 , DimICEmployeeKey = REF.DimICEmployeeKey
		 , DimICEmployeeMgmtKey = REF.DimICEmployeeMgmtKey
		 , DimICTeamMemberKey = REF.DimICTeamMemberKey
		 , DimICPeerGroupKey = REF.DimICPeerGroupKey
		 --IC funded (only update if funded)
		 , DimICEmployeeFundedKey = REF.DimICEmployeeFundedKey
		 , DimICEmployeeMgmtFundedKey = REF.DimICEmployeeMgmtFundedKey
		 , DimICTeamMemberFundedKey = REF.DimICTeamMemberFundedKey
		 , DimICPeerGroupFundedKey = REF.DimICPeerGroupFundedKey
		 , DimOSPEmployeeKey =  REF.DimOSPEmployeeKey
		 , DimOSPEmployeeMgmtKey = REF.DimOSPEmployeeMgmtKey
		 , DimOSPTeamMemberKey = REF.DimOSPTeamMemberKey
		 --OSP funded
		 , DimOSPEmployeeFundedKey = REF.DimOSPEmployeeFundedKey
		 , DimOSPEmployeeMgmtFundedKey = REF.DimOSPEmployeeMgmtFundedKey
		 , DimOSPTeamMemberFundedKey = REF.DimOSPTeamMemberFundedKey
		 --Assigned To
		 , DimAssignedToEmployeeKey = REF.DimAssignedToEmployeeKey
		 , DimAssignedToEmployeeMgmtKey = REF.DimAssignedToEmployeeMgmtKey
		 , DimAssignedToTeamMemberKey = REF.DimAssignedToTeamMemberKey
		 --Assigned To funded
		 , DimAssignedToEmployeeFundedKey = REF.DimAssignedToEmployeeFundedKey
		 , DimAssignedToEmployeeMgmtFundedKey = REF.DimAssignedToEmployeeMgmtFundedKey
		 , DimAssignedToTeamMemberFundedKey = REF.DimAssignedToTeamMemberFundedKey
		 , DimReferralDateKey = REF.DimReferralDateKey
		 , DimClientTradingDateKey = REF.DimClientTradingDateKey
		 --Facts
		 , ReferralCount = REF.ReferralCount
		 , SubmittedReferralCount = REF.SubmittedReferralCount
		 , FundedReferralCount = REF.FundedReferralCount
		 , ClientAlreadyReferred12Mos = REF.ClientAlreadyReferred12Mos
		 , SalesMeeting12Mos = REF.SalesMeeting12Mos
		 , ClientDevelopmentMeeting12Mos = REF.ClientDevelopmentMeeting12Mos
		 , MaxReferralCount = REF.MaxReferralCount
		 , FundingDaysLag = REF.FundingDaysLag
		 , CompletedEvent = REF.CompletedEvent
		 , RowHash = REF.RowHash
		 --Data Warehouse
		 , DWUpdatedDateTime = @DWUpdatedDateTime
		 , ETLJobProcessRunId = @ETLJobProcessRunId
		 , ETLJobSystemRunId = @ETLJobSystemRunId
	  FROM #FactClientReferral CR 
	  JOIN #Stg_ReferralKeys as REF
		ON CR.ReferralID = REF.ReferralID
	   AND CR.SourceIndicator = REF.SourceIndicator
	 WHERE CR.RowHash <> REF.RowHash

	--OPTION (Label = '#FactClientReferral-Update-Query')

	--  EXEC MDR.spGetRowCountByQueryLabel '#FactClientReferral-Update-Query', @UpdateCount OUT

	--   SET @EndTime = GETDATE()
	--   SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--  EXEC MDR.spProcessTaskLogUpdateRowCount
	--	   @ETLJobProcessRunId 
	--     , @ComponentName
	--     , @Source 
	--     , @Target 
	--     , @UpdateCount	 
	--     , @DurationInSeconds



-- INSERT RECORDS FOR NEW
SET @StartTime = GETDATE()

	INSERT 
	  INTO #FactClientReferral (
		   DimDateKey 
		 , ReferralID 
		 , DimReferralTypeKey 
		 -- Referred Client
		 , DimReferredContactKey 
		 , DimReferredClientKey
		 , DimReferredClientTenureKey
		 , DimReferredClientAgeGroupKey
		 , DimReferredClientAssetsKey 
		 --Referring Client		
		 , DimReferringClientKey 
		 , DimReferringClientTenureKey 
		 , DimReferringClientAgeGroupKey 
		 , DimReferringClientAssetsKey 
		 --IC
		 , DimICEmployeeKey
		 , DimICEmployeeMgmtKey 
		 , DimICTeamMemberKey
		 , DimICPeerGroupKey
		 , DimICEmployeeFundedKey 
		 , DimICEmployeeMgmtFundedKey 
		 , DimICTeamMemberFundedKey 
		 , DimICPeerGroupFundedKey 
		 --OSP
		 , DimOSPEmployeeKey
		 , DimOSPEmployeeMgmtKey
		 , DimOSPTeamMemberKey
		 , DimOSPEmployeeFundedKey
		 , DimOSPEmployeeMgmtFundedKey 
		 , DimOSPTeamMemberFundedKey 
		 --Assigned To
		 , DimAssignedToEmployeeKey 
		 , DimAssignedToEmployeeMgmtKey
		 , DimAssignedToTeamMemberKey
		 , DimAssignedToEmployeeFundedKey 
		 , DimAssignedToEmployeeMgmtFundedKey  
		 , DimAssignedToTeamMemberFundedKey
		 --Days
		 , DimReferralDateKey 
		 , DimClientTradingDateKey
		 --Facts
		 , ReferralCount
		 , SubmittedReferralCount
		 , FundedReferralCount
		 , ClientAlreadyReferred12Mos 
		 , SalesMeeting12Mos
		 , ClientDevelopmentMeeting12Mos
		 , MaxReferralCount
		 , FundingDaysLag
		 , CompletedEvent
		 , SourceIndicator
		 , RowHash
		 --Data Warehouse
		 , DWCreatedDateTime
		 , DWUpdatedDateTime
		 , ETLJobProcessRunId
		 , ETLJobSystemRunId

	)

	SELECT REF.DimDateKey
		 , REF.ReferralID
		 , REF.DimReferralTypeKey
		 --Referred Client
		 , REF.DimReferredContactKey
		 , REF.DimReferredClientKey
		 , REF.DimReferredClientTenureKey
		 , REF.DimReferredClientAgeGroupKey
		 , REF.DimReferredClientAssetsKey
		 --Referring Client
		 , REF.DimReferringClientKey
		 , REF.DimReferringClientTenureKey
		 , REF.DimReferringClientAgeGroupKey
		 , REF.DimReferringClientAssetsKey
		 --IC (only update if submitted)
		 , REF.DimICEmployeeKey
		 , REF.DimICEmployeeMgmtKey
		 , REF.DimICTeamMemberKey
		 , REF.DimICPeerGroupKey
		 --IC funded (only update if funded)
		 , REF.DimICEmployeeFundedKey
		 , REF.DimICEmployeeMgmtFundedKey
		 , REF.DimICTeamMemberFundedKey
		 , REF.DimICPeerGroupFundedKey
		 , REF.DimOSPEmployeeKey
		 , REF.DimOSPEmployeeMgmtKey
		 , REF.DimOSPTeamMemberKey
		 --OSP funded
		 , REF.DimOSPEmployeeFundedKey
		 , REF.DimOSPEmployeeMgmtFundedKey
		 , REF.DimOSPTeamMemberFundedKey
		 --Assigned To
		 , REF.DimAssignedToEmployeeKey
		 , REF.DimAssignedToEmployeeMgmtKey
		 , REF.DimAssignedToTeamMemberKey
		 --Assigned To funded
		 , REF.DimAssignedToEmployeeFundedKey
		 , REF.DimAssignedToEmployeeMgmtFundedKey
		 , REF.DimAssignedToTeamMemberFundedKey
		 , REF.DimReferralDateKey
		 , REF.DimClientTradingDateKey
		 --Facts
		 , REF.ReferralCount
		 , REF.SubmittedReferralCount
		 , REF.FundedReferralCount
		 , REF.ClientAlreadyReferred12Mos
		 , REF.SalesMeeting12Mos
		 , REF.ClientDevelopmentMeeting12Mos
		 , REF.MaxReferralCount
		 , REF.FundingDaysLag
		 , REF.CompletedEvent
		 , REF.SourceIndicator
		 , REF.RowHash
		 --Data Warehouse
		 , @DWUpdatedDateTime
		 , @DWUpdatedDateTime
		 , @ETLJobProcessRunId
		 , @ETLJobSystemRunId
	  FROM #Stg_ReferralKeys as REF
	  LEFT 
	  JOIN #FactClientReferral AS TGT
		ON REF.ReferralID = TGT.ReferralID
	   AND REF.SourceIndicator = TGT.SourceIndicator
	 WHERE TGT.ReferralID IS NULL

	--OPTION (Label = '#FactClientReferral-Insert-Query')

 --     EXEC MDR.spGetRowCountByQueryLabel '#FactClientReferral-Insert-Query', @InsertCount OUT

 --      SET @EndTime = GETDATE()
 --      SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

 --     EXEC MDR.spProcessTaskLogInsertRowCount
	--	   @ETLJobProcessRunId 
	--	 , @ComponentName
	--	 , @Source 
	--	 , @Target 
	--	 , @InsertCount	 
 --        , @DurationInSeconds


COMMIT TRANSACTION -- Transaction scope for Commit

END TRY

BEGIN CATCH 

	-- An error occurred; simply rollback the transaction started at the beginning the of the procedure.
	ROLLBACK TRANSACTION;

	SET @Status = 0
	SET @ErrorMessage = CONCAT('#FactClientReferral:', ERROR_MESSAGE())

END CATCH 

----Drop temp tables to free up Temp DB space
--IF OBJECT_ID ('TEMPDB..#Stg_Ext_Referral') IS NOT NULL DROP TABLE #Stg_Ext_Referral
--IF OBJECT_ID ('TEMPDB..#Stg_ReferralKeys') IS NOT NULL DROP TABLE #Stg_ReferralKeys
--IF OBJECT_ID('tempdb..#ClientMeetings', 'U') IS NOT NULL DROP TABLE #ClientMeetings
--IF OBJECT_ID('tempdb..#MultipleReferrals', 'U') IS NOT NULL DROP TABLE #MultipleReferrals

--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END
--GO

--64506
SELECT *
  FROM #MetricAdjustmentHistoryReferral
 WHERE CurrentRecord = 1 

--
SELECT *
  FROM #FactClientReferral
  order by 1 desc
