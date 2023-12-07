--TRUNCATE TABLE #HistoricalAccountAttributes
--CREATE TABLE #HistoricalAccountAttributes (
--	[FinAccountNumber] [nvarchar](25) NULL,
--	[HouseholdUID] [NVARCHAR](4000) NULL,
--	[HouseholdId] [NVARCHAR](4000) NULL,
--	[ClientId] [uniqueidentifier] NULL,
--	[ClientNumber] [int] NULL,
--	[RowHash] [varbinary](8000) NULL,
--	[EffectiveStartDate] [datetime] NULL,
--	[EffectiveEndDate] [datetime] NULL,
--	[CurrentRecord] [bit] NULL,
--	[DWCreatedDateTime] [datetime] NULL,
--	[DWUpdatedDateTime] [datetime] NULL,
--	[ETLJobProcessRunId] [uniqueidentifier] NULL,
--	[ETLJobSystemRunId] [uniqueidentifier] NULL
--)
--WITH
--(
--	DISTRIBUTION = HASH ( [FinAccountNumber] ),
--	CLUSTERED COLUMNSTORE INDEX
--)
--GO

DECLARE @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
DECLARE @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()


--CREATE PROC [REF].[spUpsertHistoricalAccountAttributes] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
--BEGIN
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
      , @DataSourceGroupName NVARCHAR(100) 
      , @DataSourceMemberName NVARCHAR(100)
      , @NextDataProcessStageName NVARCHAR(50) 
      , @DataProcessLogId UNIQUEIDENTIFIER
      
DECLARE @InsertCount BIGINT
DECLARE @UpdateCount BIGINT

DECLARE @Today DATE = convert(date,getdate() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')
DECLARE @Yesterday DATE = DATEADD(DAY,-1,@Today)
       
SET @DWUpdatedDatetime = GETDATE()
SET @Status = 1
SET @Rows = 0

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


IF OBJECT_ID ('TEMPDB..#AccountHistory_Temp') IS NOT NULL DROP TABLE #AccountHistory_Temp

CREATE TABLE #AccountHistory_Temp (
	  FinAccountNumber NVARCHAR(100)
	, HouseholdUID NVARCHAR(4000)
	, HouseholdId NVARCHAR(4000)
	, ClientId UNIQUEIDENTIFIER
	, ClientNumber INT
	, RowHash VARBINARY(8000)
	, RecordType NVARCHAR(100)
)
WITH (DISTRIBUTION = HASH(FinAccountNumber), HEAP) 


--BEGIN BACKFILL RECS IF NO RECORDS EXIST IN REFERENCE TABLE
IF NOT EXISTS (SELECT 1 FROM #HistoricalAccountAttributes) 
BEGIN 

	--BEGIN TRY

	--BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
	---- IF any batch fail, it will be caught IN the CATCH BLOCK AND will be rolled back.

	--	SET @Source = '[{"SourceTable":"Iris.fi_fi_financialaccountauditlogBase"}]'
	--	SET @Target = '[{"TargetTable":"#HistoricalAccountAttributes"}]'
	--	SET @StartTime = GETDATE()

		;WITH AccountInfo AS ( 

			SELECT AL.fi_id_Search AS FinancialAccountAuditlogId 
				 , AL.fi_finaccountnumber AS FinAccountNumber 
				 , AL.fi_FinancialAccountId AS AccountId
				 , AL.fi_contactid AS ClientId
				 , CB.fi_Id AS ClientNumber
				 , ROW_NUMBER() OVER (PARTITION BY AL.fi_finaccountnumber, CONVERT(DATE, AL.CreatedOn) ORDER BY AL.CreatedOn DESC, AL.fi_Id_Search DESC) AS DailyRowNum		 
				 , AL.CreatedOn

			  FROM Iris.fi_fi_financialaccountauditlogBase AS AL

			  --JOIN TO FI_FINANCIALACCOUNTBASE ON FIN AS EMPLOYEES WILL FAT FINGER INCORRECT FINS AND THEN LATER CORRECT THEM
			  JOIN Iris.fi_financialaccountBase AS FA
				ON AL.fi_finaccountnumber = FA.fi_FINAccountNumber

			  JOIN Iris.ContactBase AS CB
				ON AL.fi_contactid = CB.ContactId 

			  LEFT 
			  JOIN BitRpt.ONYX_TEST_OIDS OTO --Excluding Test OIDS 
				ON OTO.TEST_OIDs = CB.fi_Id

			 WHERE ISNULL(AL.fi_combinedaccountcode, 1) <> 157610000 --NOT A COMBINED ACCOUNT (C)
			   AND AL.fi_statuscode <> 157610016  --NOT REREGISTERED
			   AND AL.fi_finaccountnumber IS NOT NULL --FIN MUST EXIST
			   AND AL.CreatedOn < CONVERT(DATE, GETDATE())  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST
			   AND OTO.TEST_OIDs IS NULL --REMOVE TEST RECORDS
  
		  )

		, LastDailyChange AS ( 

			SELECT FinancialAccountAuditlogId 
				 , FinAccountNumber 
				 , AccountId
				 , ClientId
				 , ClientNumber
				 , CreatedOn
				 , HASHBYTES('SHA2_256', CONCAT(FinAccountNumber, '|', ClientId)) AS RowHash
				 , ROW_NUMBER() OVER(PARTITION BY FinAccountNumber ORDER BY CreatedOn, FinancialAccountAuditlogId) AS RowNum
			  FROM AccountInfo
			 WHERE DailyRowNum = 1 --IF RECORDS CHANGED NUMEROUS TIMES WITHIN THE DAY, GRAB THE LAST CHANGE PER DAY ONLY

		) 

		, DetectChanges AS ( 

			SELECT FinancialAccountAuditlogId 
				 , FinAccountNumber 
				 , AccountId
				 , ClientId
				 , ClientNumber
				 , CreatedOn
				 , RowHash
				 , LAG(RowHash, 1, HASHBYTES('SHA2_256','')) OVER (PARTITION BY FinAccountNumber ORDER BY RowNum) AS PrevRowHash     
			  FROM LastDailyChange

		)

		, ChangesOnly AS ( 

			SELECT FinAccountNumber 
				 , AccountId
				 , ClientId
				 , ClientNumber
				 , RowHash
				 , DC.CreatedOn
				 , ROW_NUMBER() OVER (PARTITION BY FinAccountNumber ORDER BY DC.CreatedOn) AS RowNum	  
			  FROM DetectChanges AS DC   
			 WHERE RowHash <> PrevRowHash

		 )

		 /*
				MODIFYING EFFECTIVESTARTDATE IN BELOW QUERY TO TAKE THE FIRST CREATEDON RECORD FROM 
				THE DATE THE ACCOUNT WAS CREATED IN THE BASE TABLE (Iris.fi_financialaccountBase) AS THE 
				VERY FIRST EFFECTIVESTARTDATE. 

				THIS WAS DONE BECAUSE THERE WAS FUNDING HAPPENING PRIOR TO THE FIRST CREATED ON RECORD IN THE AUDIT LOG (Iris.fi_fi_financialaccountauditlogBase).

				EXAMPLE FIN: 147637 - NUMEROUS TRADE DATES OF 4/14/2010 BUT THE MIN CREATED DATE IN THE AUDIT LOG IS 4/15
		 */

		 , FinalDataset AS ( 

			 SELECT CO.FinAccountNumber 
				  , REF.HouseholdUID
				  , REF.HouseholdId
				  , CO.ClientId
				  , CO.ClientNumber
				  , CO.RowHash
				  , CASE 
						WHEN CO.RowNum = 1 
						THEN CONVERT(DATETIME, DATEDIFF(DAY, 0, MIN(FA.CreatedOn) OVER (PARTITION BY FA.fi_FINAccountNumber)))
						ELSE CONVERT(DATETIME, DATEDIFF(DAY, 0, CO.CreatedOn))
					END AS EffectiveStartDate
				  , CONVERT(DATETIME, DATEDIFF(DAY, 0, LEAD (CO.CreatedOn, 1, @MaxDateValue) OVER (PARTITION BY CO.FinAccountNumber  ORDER BY CO.CreatedOn))) AS EffectiveEndDate
				  , CASE 
						WHEN LEAD (CO.CreatedOn, 1, @MaxDateValue) OVER (PARTITION BY CO.FinAccountNumber  ORDER BY CO.CreatedOn) = @MaxDateValue
						THEN 1 
						ELSE 0 
					END AS CurrentRecord 

			   FROM ChangesOnly AS CO 

			   JOIN FinM.account_info_ai AS AI --NEEDS TO PULL FROM FINM AS FINM INCLUDES FIN AND GWP ACCOUNTS
				 ON CO.FinAccountNumber = AI.ai_advisor_acct_no 

			   LEFT
			   JOIN Iris.fi_financialaccountBase AS FA
				 ON CO.AccountId = FA.fi_financialaccountId

			   LEFT
			   JOIN REF.CRMClientMapping AS REF
				 ON CO.ClientId = REF.ClientId_Iris
        
			  WHERE AI.ai_Selection_Field2 = '2' --PCG Account 
				AND AI.ai_Selection_Field4 in ('I','P') --Individual or Participant Account

		)


		 INSERT 
		   INTO #HistoricalAccountAttributes (  
				[FinAccountNumber]
			  , [HouseholdUID]
			  , [HouseholdId]
			  , [ClientId]
			  , [ClientNumber]
			  , [RowHash]
			  , [EffectiveStartDate]
			  , [EffectiveEndDate]
			  , [CurrentRecord]
			  , [DWCreatedDateTime]
			  , [DWUpdatedDateTime]
			  , [ETLJobProcessRunId]
			  , [ETLJobSystemRunId]
		 )

		 SELECT FinAccountNumber
			  , HouseholdUID
			  , HouseholdId
			  , ClientId
			  , ClientNumber
			  , RowHash
			  , EffectiveStartDate
			  , EffectiveEndDate
			  , CurrentRecord 
			  , @DWUpdatedDateTime AS DWCreatedDateTime
			  , @DWUpdatedDateTime AS DWUpdatedDateTime
			  , @ETLJobProcessRunId AS ETLJobProcessRunId
			  , @ETLJobSystemRunId AS ETLJobSystemRunId  
		   FROM FinalDataset   
--		 OPTION (Label = '#HistoricalAccountAttributes-Backfill-Query')


--		EXEC MDR.spGetRowCountByQueryLabel '#HistoricalAccountAttributes-Backfill-Query', @InsertCount OUT


--		SET @EndTime = GETDATE()
--		SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)


--		EXEC MDR.spProcessTaskLogInsertRowCount
--				  @ETLJobProcessRunId 
--				, @ComponentName
--				, @Source 
--				, @Target 
--				, @InsertCount	 
--				, @DurationInSeconds

--	COMMIT TRANSACTION -- Transaction scope for Commit

--	END TRY


--	BEGIN CATCH 

--		ROLLBACK TRANSACTION;
--		SET @Status = 0
--		SET @ErrorMessage = CONCAT('#HistoricalAccountAttributes Backfill Error', ': ', ERROR_MESSAGE())	

--	END CATCH 


END
--END BACKFILL RECS IF NO RECORDS EXIST IN REFERENCE TABLE


--/*
--	TYPE 1 UPDATE FOR HOUSEHOLDUID/HOUSEHOLDID
--*/

--BEGIN TRY

--BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
---- IF any batch fail, it will be caught IN the CATCH BLOCK AND will be rolled back.

----First, update HistoricalAccountAttributes for any IDs that have been newly mapped between CRMs
--SET @Source = '{"SourceTable":"REF.CRMClientMapping"}'
--SET @Target = '{"TargetTable":"#HistoricalAccountAttributes"}'
--SET @StartTime = GETDATE()

;WITH Households as (

	SELECT DISTINCT 
		   REF.HouseholdUID
		 , REF.HouseholdID
		 , REF.ClientID_IRIS
		 , REF.ClientNumber_IRIS
	  FROM REF.CRMClientMapping AS REF 
	  LEFT 
	  JOIN #HistoricalAccountAttributes AS HAA
	    ON REF.HouseholdUID = HAA.HouseholdUID
	 WHERE HAA.HouseholdUID IS NULL
	   AND REF.HouseholdUID IS NOT NULL

)

	UPDATE #HistoricalAccountAttributes
       SET HouseholdUID = Src.HouseholdUID
         , HouseholdId = Src.HouseholdId
		 , DWUpdatedDateTime  = @DWUpdatedDateTime 
		 , ETLJobProcessRunId = @ETLJobProcessRunId
		 , ETLJobSystemRunId  = @ETLJobSystemRunId 
      FROM #HistoricalAccountAttributes AS TGT  
      JOIN Households SRC 
	    ON SRC.ClientNumber_Iris = CONVERT(NVARCHAR(30),TGT.ClientNumber)
     WHERE TGT.HouseholdUID = @UnknownTextValue 
	    OR TGT.HouseholdId = @UnknownTextValue
--    OPTION (Label = '#HistoricalAccountAttributes-HHUpdate')

--	  EXEC MDR.spGetRowCountByQueryLabel '#HistoricalAccountAttributes-HHUpdate', @InsertCount OUT
 
--	  SET @EndTime = GETDATE()
--	  SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--	 EXEC MDR.spProcessTaskLogUpdateRowCount
--		  @ETLJobProcessRunId 
--		, @ComponentName
--		, @Source 
--  		, @Target 
--		, @UpdateCount	 
--		, @DurationInSeconds

--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY


--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#HistoricalAccountAttributes Household ID Update', ': ', ERROR_MESSAGE())	

--END CATCH 



--/*
--	ACTIVE SFDC FIN --> HOUSEHOLD ASSIGNMENTS
--*/

--BEGIN TRY

--BEGIN TRANSACTION

--	SET @Source = '[{"SourceTable":"Iris.fi_fi_financialaccountauditlogBase"}]'
--	SET @Target = '[{"TargetTable":"#HistoricalAccountAttributes"}]'
--	SET @StartTime = GETDATE()

;WITH FinAccounts_SFDC AS (

	SELECT DISTINCT --USING DISTINCT AS WE ONLY CARE ABOUT FIN TO HOUSEHOLD ASSIGNMENTS
	       FA.FIN_Account_Number__c AS FinAccountNumber
		 , FA.FinServ__Household__c AS HouseholdUID
		 , A.[Name] AS HouseholdName
         , FA.FinServ__Status__c AS AccountStatus
		 , FA.Contract_Date__c AS ContractDate
	  FROM PcgSf.FinServ__FinancialAccount__c AS FA
	  LEFT
	  JOIN PCGSF.Account AS A 
	    ON FA.FinServ__Household__c = A.Id
	 WHERE FA.FIN_Account_Number__c IS NOT NULL
	   AND FA.Contract_Date__c IS NOT NULL
	   AND FA.IsDeleted = 0 --NOT DELETED
	   AND FA.FinServ__Status__c NOT IN ('Canceled', 'Reregistered')
	   AND FA.FinServ__Managed__c = 1 --FI MANAGED ACCOUNT

) 

, DistinctFINs AS ( 

	SELECT DISTINCT FinAccountNumber
	  FROM FinAccounts_SFDC
)

, DupedFINAccounts AS ( 

	SELECT FinAccountNumber
	  FROM FinAccounts_SFDC
	 GROUP
	    BY FinAccountNumber
	HAVING COUNT(1) > 1 

 )

 , DeDupedFINs AS ( 

	 SELECT FA.FinAccountNumber	
		  , FA.HouseholdUID	
		  , FA.HouseholdName	
		  , FA.AccountStatus
	   FROM FinAccounts_SFDC AS FA
	   JOIN DupedFINAccounts AS DFA
		 ON FA.FinAccountNumber = DFA.FinAccountNumber
	  WHERE FA.AccountStatus IN ('Trading - Fully Completed') --ALL DUPES HAVE AT LEAST ONE EXISTING ACCOUNT IN STATUS = Trading - Fully Completed
	 
	  UNION

	 SELECT FA.FinAccountNumber	
		  , FA.HouseholdUID	
		  , FA.HouseholdName	
		  , FA.AccountStatus
	   FROM FinAccounts_SFDC AS FA
	  WHERE NOT EXISTS (SELECT 1 FROM DupedFINAccounts AS DFA WHERE FA.FinAccountNumber = DFA.FinAccountNumber)

) 

, DupesFinsNotInTradingCompleteStatus AS ( 

	SELECT FA.FinAccountNumber
		 , FA.HouseholdUID
		 , FA.HouseholdName
         , FA.AccountStatus
		 , FA.ContractDate
		 , ROW_NUMBER() OVER (PARTITION BY FA.FinAccountNumber ORDER BY FA.ContractDate DESC, FA.HouseholdUID COLLATE Latin1_General_100_BIN2_UTF8 DESC) AS RowNum 
	  FROM FinAccounts_SFDC AS FA
	  LEFT
	  JOIN DeDupedFINs AS DDF
	    ON FA.FinAccountNumber = DDF.FinAccountNumber
	 WHERE DDF.FinAccountNumber IS NULL

)

, FinalDataset AS ( 

	 SELECT FinAccountNumber	
		  , HouseholdUID	
		  , HouseholdName	
		  , AccountStatus
	   FROM DeDupedFINs 
	   
	  UNION

	 SELECT FinAccountNumber	
		  , HouseholdUID	
		  , HouseholdName	
		  , AccountStatus
	   FROM DupesFinsNotInTradingCompleteStatus
	  WHERE RowNum = 1 --IF WE STILL HAVE DUPES, TAKE THE FIN WITH THE MOST RECENT CONTRACT DATE

) 

, ActiveRecs_HAA AS ( 

	SELECT FinAccountNumber	
		 , HouseholdUID
		 , ClientId
		 , ClientNumber
		 , RowHash
		 , EffectiveStartDate
		 , EffectiveEndDate
	  FROM #HistoricalAccountAttributes
	 WHERE CurrentRecord = 1 

)

    INSERT
	  INTO #AccountHistory_Temp (
		   FinAccountNumber	
		 , HouseholdUID
		 , HouseholdId
		 , ClientId
		 , ClientNumber
		 , RowHash
		 , RecordType
	)

	SELECT FD.FinAccountNumber	
		 , FD.HouseholdUID
		 , CRM.HouseholdId
		 , CRM.ClientId_Iris
		 , CRM.ClientNumber_Iris
		 , HASHBYTES('SHA2_256', CONCAT(FD.FinAccountNumber, '|', FD.HouseholdUID)) AS RowHash
		 , CASE 
			 WHEN HAA.FinAccountNumber IS NULL THEN 'NEW'
			 WHEN FD.HouseholdUID <> HAA.HouseholdUID THEN 'MODIFIED' --NOT LOOKING AT ROW HASH SINCE SOME IRIS RECS WERE HASHED WITH CLIENTID
			 WHEN FD.HouseholdUID = HAA.HouseholdUID THEN 'NO CHANGE'
			 ELSE @UnknownTextValue
	       END AS RecordType
	  FROM FinalDataset AS FD
	  LEFT
	  JOIN REF.CRMClientMapping AS CRM
	    ON FD.HouseholdUID = CRM.HouseholdUID
	  LEFT
	  JOIN ActiveRecs_HAA AS HAA
	    ON FD.FinAccountNumber = HAA.FinAccountNumber
    


/*
	START - RUN NEW SFDC FIN RECS THROUGH IRIS BACKFILL SO THAT WE PULL ANY HISTORY THAT WE CAN FIRST
*/

	;WITH AccountInfo AS ( 

		SELECT AL.fi_id_Search AS FinancialAccountAuditlogId 
			 , AL.fi_finaccountnumber AS FinAccountNumber 
			 , AL.fi_FinancialAccountId AS AccountId
			 , AL.fi_contactid AS ClientId
			 , CB.fi_Id AS ClientNumber
			 , ROW_NUMBER() OVER (PARTITION BY AL.fi_finaccountnumber, CONVERT(DATE, AL.CreatedOn) ORDER BY AL.CreatedOn DESC, AL.fi_Id_Search DESC) AS DailyRowNum		 
			 , AL.CreatedOn

		  FROM #AccountHistory_Temp AS AH
		  
		  JOIN Iris.fi_fi_financialaccountauditlogBase AS AL
		    ON AL.fi_finaccountnumber = AH.FinAccountNumber

		  --JOIN TO FI_FINANCIALACCOUNTBASE ON FIN AS EMPLOYEES WILL FAT FINGER INCORRECT FINS AND THEN LATER CORRECT THEM
		  JOIN Iris.fi_financialaccountBase AS FA
			ON AL.fi_finaccountnumber = FA.fi_FINAccountNumber

		  JOIN Iris.ContactBase AS CB
			ON AL.fi_contactid = CB.ContactId 

		  LEFT 
		  JOIN BitRpt.ONYX_TEST_OIDS OTO --Excluding Test OIDS 
			ON OTO.TEST_OIDs = CB.fi_Id

		 WHERE ISNULL(AL.fi_combinedaccountcode, 1) <> 157610000 --NOT A COMBINED ACCOUNT (C)
		   AND AL.fi_statuscode <> 157610016  --NOT REREGISTERED
		   AND AL.fi_finaccountnumber IS NOT NULL --FIN MUST EXIST
		   AND AL.CreatedOn < CONVERT(DATE, GETDATE())  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST
		   AND OTO.TEST_OIDs IS NULL --REMOVE TEST RECORDS
		   AND AH.RecordType = 'NEW' --ONLY DO THIS FOR NEW FINS THAT DON'T ALREADY EXIST IN #HistoricalAccountAttributes
	  )

	, LastDailyChange AS ( 

		SELECT FinancialAccountAuditlogId 
			 , FinAccountNumber 
			 , AccountId
			 , ClientId
			 , ClientNumber
			 , CreatedOn
			 , HASHBYTES('SHA2_256', CONCAT(FinAccountNumber, '|', ClientId)) AS RowHash
			 , ROW_NUMBER() OVER(PARTITION BY FinAccountNumber ORDER BY CreatedOn, FinancialAccountAuditlogId) AS RowNum
		  FROM AccountInfo
		 WHERE DailyRowNum = 1 --IF RECORDS CHANGED NUMEROUS TIMES WITHIN THE DAY, GRAB THE LAST CHANGE PER DAY ONLY

	) 

	, DetectChanges AS ( 

		SELECT FinancialAccountAuditlogId 
			 , FinAccountNumber 
			 , AccountId
			 , ClientId
			 , ClientNumber
			 , CreatedOn
			 , RowHash
			 , LAG(RowHash, 1, HASHBYTES('SHA2_256','')) OVER (PARTITION BY FinAccountNumber ORDER BY RowNum) AS PrevRowHash     
		  FROM LastDailyChange

	)

	, ChangesOnly AS ( 

		SELECT FinAccountNumber 
			 , AccountId
			 , ClientId
			 , ClientNumber
			 , RowHash
			 , DC.CreatedOn
			 , ROW_NUMBER() OVER (PARTITION BY FinAccountNumber ORDER BY DC.CreatedOn) AS RowNum	  
		  FROM DetectChanges AS DC   
		 WHERE RowHash <> PrevRowHash

	 )

	 /*
			MODIFYING EFFECTIVESTARTDATE IN BELOW QUERY TO TAKE THE FIRST CREATEDON RECORD FROM 
			THE DATE THE ACCOUNT WAS CREATED IN THE BASE TABLE (Iris.fi_financialaccountBase) AS THE 
			VERY FIRST EFFECTIVESTARTDATE. 

			THIS WAS DONE BECAUSE THERE WAS FUNDING HAPPENING PRIOR TO THE FIRST CREATED ON RECORD IN THE AUDIT LOG (Iris.fi_fi_financialaccountauditlogBase).

			EXAMPLE FIN: 147637 - NUMEROUS TRADE DATES OF 4/14/2010 BUT THE MIN CREATED DATE IN THE AUDIT LOG IS 4/15
	 */

	 , FinalDataset AS ( 

		 SELECT CO.FinAccountNumber 
			  , REF.HouseholdUID
			  , REF.HouseholdId
			  , CO.ClientId
			  , CO.ClientNumber
			  , CO.RowHash
			  , CASE 
					WHEN CO.RowNum = 1 
					THEN CONVERT(DATETIME, DATEDIFF(DAY, 0, MIN(FA.CreatedOn) OVER (PARTITION BY FA.fi_FINAccountNumber)))
					ELSE CONVERT(DATETIME, DATEDIFF(DAY, 0, CO.CreatedOn))
				END AS EffectiveStartDate
			  , CONVERT(DATETIME, DATEDIFF(DAY, 0, LEAD (CO.CreatedOn, 1, @MaxDateValue) OVER (PARTITION BY CO.FinAccountNumber  ORDER BY CO.CreatedOn))) AS EffectiveEndDate
			  , CASE 
					WHEN LEAD (CO.CreatedOn, 1, @MaxDateValue) OVER (PARTITION BY CO.FinAccountNumber  ORDER BY CO.CreatedOn) = @MaxDateValue
					THEN 1 
					ELSE 0 
				END AS CurrentRecord 

		   FROM ChangesOnly AS CO 

		   JOIN FinM.account_info_ai AS AI --NEEDS TO PULL FROM FINM AS FINM INCLUDES FIN AND GWP ACCOUNTS
			 ON CO.FinAccountNumber = AI.ai_advisor_acct_no 

		   LEFT
		   JOIN Iris.fi_financialaccountBase AS FA
			 ON CO.AccountId = FA.fi_financialaccountId

		   LEFT
		   JOIN REF.CRMClientMapping AS REF
			 ON CO.ClientId = REF.ClientId_Iris
        
		  WHERE AI.ai_Selection_Field2 = '2' --PCG Account 
			AND AI.ai_Selection_Field4 in ('I','P') --Individual or Participant Account

	)


	 INSERT 
	   INTO #HistoricalAccountAttributes (  
			[FinAccountNumber]
		  , [HouseholdUID]
		  , [HouseholdId]
		  , [ClientId]
		  , [ClientNumber]
		  , [RowHash]
		  , [EffectiveStartDate]
		  , [EffectiveEndDate]
		  , [CurrentRecord]
		  , [DWCreatedDateTime]
		  , [DWUpdatedDateTime]
		  , [ETLJobProcessRunId]
		  , [ETLJobSystemRunId]
	 )

	 SELECT FD.FinAccountNumber
		  , FD.HouseholdUID
		  , FD.HouseholdId
		  , FD.ClientId
		  , FD.ClientNumber
		  , FD.RowHash
		  , FD.EffectiveStartDate
		  , FD.EffectiveEndDate
		  , FD.CurrentRecord 
		  , @DWUpdatedDateTime AS DWCreatedDateTime
		  , @DWUpdatedDateTime AS DWUpdatedDateTime
		  , @ETLJobProcessRunId AS ETLJobProcessRunId
		  , @ETLJobSystemRunId AS ETLJobSystemRunId  
	   FROM FinalDataset AS FD
	   LEFT
	   JOIN #HistoricalAccountAttributes AS HAA
	     ON FD.FinAccountNumber = HAA.FinAccountNumber
		AND FD.HouseholdUID = HAA.HouseholdUID
		AND FD.EffectiveStartDate = HAA.EffectiveStartDate
	  WHERE HAA.FinAccountNumber IS NULL
--	 OPTION (Label = '#HistoricalAccountAttributes-NewRecBackfill-Query')

--	EXEC MDR.spGetRowCountByQueryLabel '#HistoricalAccountAttributes-NewRecBackfill-Query', @InsertCount OUT

--	SET @EndTime = GETDATE()
--	SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)


--	EXEC MDR.spProcessTaskLogInsertRowCount
--			  @ETLJobProcessRunId 
--			, @ComponentName
--			, @Source 
--			, @Target 
--			, @InsertCount	 
--			, @DurationInSeconds

--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY


--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#HistoricalAccountAttributes NewRecBackfill Error', ': ', ERROR_MESSAGE())	

--END CATCH 


--/*
--	END - RUN NEW SFDC FIN RECS THROUGH IRIS BACKFILL SO THAT WE PULL ANY HISTORY THAT WE CAN FIRST
--*/




--/*
--	START - UPSERT FINS IN #HistoricalAccountAttributes THAT HAVE BEEN ASSIGNED TO A NEW CLIENT/HOUSEHOLD SINCE LAST ETL
--*/

--BEGIN TRY

--BEGIN TRANSACTION

--	SET @Source = '[{"SourceTable":"Iris.fi_fi_financialaccountauditlogBase"}]'
--	SET @Target = '[{"TargetTable":"#HistoricalAccountAttributes"}]'
--	SET @StartTime = GETDATE()

	UPDATE #HistoricalAccountAttributes
	   SET CurrentRecord = 0 
         , EffectiveEndDate = @Yesterday
         , DWUpdatedDateTime = @DWUpdatedDateTime 
         , ETLJobProcessRunId = @ETLJobProcessRunId
         , ETLJobSystemRunId = @ETLJobSystemRunId 
	  FROM #AccountHistory_Temp AS SRC
	  JOIN #HistoricalAccountAttributes AS TGT
	    ON SRC.FinAccountNumber = TGT.FinAccountNumber 
	   AND TGT.CurrentRecord = 1
	 WHERE SRC.RecordType IN ('MODIFIED', 'NEW') --WE RAN NEW RECS THROUGH IRIS BACKFILL SO WE NEED TO INCLUDE THEM HERE TOO
  --  OPTION (Label = '#HistoricalAccountAttributes-Update')

	 -- EXEC MDR.spGetRowCountByQueryLabel '#HistoricalAccountAttributes-Update', @InsertCount OUT
 
	 -- SET @EndTime = GETDATE()
	 -- SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	 --EXEC MDR.spProcessTaskLogUpdateRowCount
		--  @ETLJobProcessRunId 
		--, @ComponentName
		--, @Source 
  --		, @Target 
		--, @UpdateCount	 
		--, @DurationInSeconds


	 INSERT 
	   INTO #HistoricalAccountAttributes (  
			[FinAccountNumber]
		  , [HouseholdUID]
		  , [HouseholdId]
		  , [ClientId]
		  , [ClientNumber]
		  , [RowHash]
		  , [EffectiveStartDate]
		  , [EffectiveEndDate]
		  , [CurrentRecord]
		  , [DWCreatedDateTime]
		  , [DWUpdatedDateTime]
		  , [ETLJobProcessRunId]
		  , [ETLJobSystemRunId]
	 )

	 SELECT FinAccountNumber
		  , HouseholdUID
		  , HouseholdId
		  , ClientId
		  , ClientNumber
		  , HASHBYTES('SHA2_256', CONCAT(FinAccountNumber, '|', HouseholdUID)) AS RowHash
		  , @Yesterday AS EffectiveStartDate
		  , @MaxDateValue AS EffectiveEndDate
		  , 1 AS CurrentRecord 
		  , @DWUpdatedDateTime AS DWCreatedDateTime
		  , @DWUpdatedDateTime AS DWUpdatedDateTime
		  , @ETLJobProcessRunId AS ETLJobProcessRunId
		  , @ETLJobSystemRunId AS ETLJobSystemRunId  
	   FROM #AccountHistory_Temp AS SRC
	  WHERE SRC.RecordType IN ('MODIFIED', 'NEW')
--	 OPTION (Label = '#HistoricalAccountAttributes-Insert-Query')

--	EXEC MDR.spGetRowCountByQueryLabel '#HistoricalAccountAttributes-Insert-Query', @InsertCount OUT

--	SET @EndTime = GETDATE()
--	SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)


--	EXEC MDR.spProcessTaskLogInsertRowCount
--			  @ETLJobProcessRunId 
--			, @ComponentName
--			, @Source 
--			, @Target 
--			, @InsertCount	 
--			, @DurationInSeconds




--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY


--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#HistoricalAccountAttributes Household ID Update', ': ', ERROR_MESSAGE())	

--END CATCH 

--/*
--	END - UPSERT FINS IN #HistoricalAccountAttributes THAT HAVE BEEN ASSIGNED TO A NEW CLIENT/HOUSEHOLD SINCE LAST ETL
--*/

---- DROP TEMP TABLES 															       
--IF OBJECT_ID ('TEMPDB..#AccountHistory_Temp') IS NOT NULL DROP TABLE #AccountHistory_Temp

--SELECT @Status AS [Status] , @ErrorMessage AS ErrorMessage

--END
--GO


/******************************************************************************
                           VALIDATION SCRIPT
******************************************************************************/

--Check for duplicate active finaccountnumbers records: PASS
   select finaccountnumber
       , count(*)
    from #HistoricalAccountAttributes
    where CurrentRecord = 1 
    group by finaccountnumber
    having count(*) > 1

--Check for duplicate client/start date records: PASS
   select finaccountnumber
        , EffectiveStartDate
        , count(*)
    from #HistoricalAccountAttributes
    where CurrentRecord = 1 
    group by finaccountnumber
        , EffectiveStartDate
    having count(*) > 1


--Check for Start/End dates that don't line up: PASS
select * 
  from ( 
            select 
                   finaccountnumber
                 , EffectiveEndDate
                 , LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY finaccountnumber ORDER BY EffectiveStartDate) AS NextStartDate
                 , CASE 
                    WHEN EffectiveEndDate = LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY finaccountnumber ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END EndDateMatchesNextStartDate
              from #HistoricalAccountAttributes) as a 
    where a.EndDateMatchesNextStartDate = 0 

--Check for duplicate RowHashs: PASS
select * 
  from ( 
            select 
                   finaccountnumber
                 , RowHash
                 , LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY finaccountnumber ORDER BY EffectiveStartDate) AS NextRowHash
                 , EffectiveStartDate
				 , EffectiveEndDate
				 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 0
                    WHEN RowHash = LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY finaccountnumber ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END MatchesNextRowHash
              from #HistoricalAccountAttributes) as a 
    where a.MatchesNextRowHash = 1
