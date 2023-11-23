
/*
    STEP 1: USE BELOW CODE STARTING AT 'START HERE' COMMENT TO RECREATE FNF IN PROD AND LOAD INTO TEMPORARY TABLE
*/

/*
    STEP 2: DUPE CHECK TEMP TABLE
    EXPECTED RESULT: 0 RECORDS 
    ACTUAL RESULT: 0 RECORDS
    TEST RESULT: PASS
*/
    SELECT ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
         , SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
         , FlowCount 
         , SystemOfRecord
         , COUNT(1)
      FROM #FactNetFlows
     GROUP 
        BY ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
         , SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
         , FlowCount   
         , SystemOfRecord
    HAVING COUNT(1) > 1 

/*
    STEP 3: CHECK FOR RECORDS IN EXISTING FACT THAT DON'T EXIST IN NEW TEMP FACT TABLE
    EXPECTED RESULT: 0 RECORDS 
    ACTUAL RESULT: 5 DISTINCT FINANCIAL ACCOUNT KEYS (FINS)
    TEST RESULT: PASS

      - 501843
      - 502050
      - 501810
      - 501825
      - 501826

    AFTER LOOKING INTO THESE FINS, THEY DON'T EXIST WITHIN IRIS OR SFDC. WE MUST HAVE CREATED THEM AND THEN DELETED THEM AT SOME POINT WITHIN OUR SYSTEM(S).
*/
    SELECT DISTINCT TGT.DimFinancialAccountKey
      FROM FDW.FactNetFlows AS TGT
      LEFT
      JOIN #FactNetFlows AS SRC
        ON SRC.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = TGT.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
       AND SRC.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = TGT.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
       AND SRC.FlowCount = TGT.FlowCount
     WHERE SRC.ReferenceNumber IS NULL
      

--START HERE
--IF OBJECT_ID('TEMPDB..#FactNetFlows') IS NOT NULL DROP TABLE #FactNetFlows

--CREATE TABLE #FactNetFlows (
--	[DimTransactionDateKey] [int] NOT NULL,
--	[DimCancellationDateKey] [int]  NOT NULL,
--	[DimFinancialAccountKey] [int] NOT NULL, 
--	[DimClientKey] [int] NOT NULL,
--	[DimEmployeeKey] [int] NOT NULL, 
--	[DimTeamMemberKey] [int] NOT NULL, 
--	[DimFlowTypeKey] [int] NOT NULL, 
--	[DimClientTenureKey] [int] NOT NULL, 
--    [DimClientAssetsKey] [int] NOT NULL,
--	[DimEmployeeMgmtKey] [int] NOT NULL, 
--	[DimAgeGroupKey] [int] NOT NULL,
--	[DimPeerGroupKey] [int]  NOT NULL,
--	[FlowAmount] [decimal](18,2), 
--	[FlowAmountOriginalCurrency] [decimal](18,2),
--	[FlowCount] [int] NULL,	
--	[RecurringTransactionDaysLag] [int] NULL, --Day Lag between the flow and a previous flow with the same quantity, symbol and account
--	[ReferenceNumber] [nvarchar](25), --Durable business key
--	[SubReferenceNumber] [nvarchar](25), --Durable business key
--	[CreatedDate] [datetime],
--	[UpdatedDate] [datetime],
--	[SystemOfRecord] [nvarchar](100),
--	[DWCreatedDateTime] [datetime] NULL,  
--	[DWUpdatedDateTime] [datetime] NULL,  
--	[ETLJobProcessRunId] [uniqueidentifier] NULL,
--	[ETLJobSystemRunId] [uniqueidentifier] NULL
--)
--WITH
--(
--	DISTRIBUTION = HASH ([ReferenceNumber]), 
--	CLUSTERED COLUMNSTORE INDEX						
--)


--CREATE PROC [FDW].[spUpsertFactNetFlows] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
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
      , @CurrentStatusName NVARCHAR(50)
      , @DataSourceGroupName NVARCHAR(100)
      , @DataSourceMemberName NVARCHAR(100) 
      , @NextDataProcessStageName NVARCHAR(50)
      , @BaseTableName NVARCHAR(100) 
	  , @MaxFactLoadDate DATE 

       
DECLARE @InsertCount BIGINT, @InsertOperation NVARCHAR(20) 
DECLARE @UpdateCount BIGINT, @UpdateOperation NVARCHAR(20) 

SET @InsertOperation = 'INSERT'
SET @UpdateOperation = 'UPDATE'

       
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

IF OBJECT_ID('tempdb..#Stg_Fact_NetFlows_Temp', 'U') IS NOT NULL
    DROP TABLE #Stg_Fact_NetFlows_Temp

CREATE TABLE #Stg_Fact_NetFlows_Temp
(	
	[DimTransactionDateKey ] [int] NULL,
	[DimCancellationDateKey] [int] NULL,
	[DimDateKey] [int] NULL,
	[DimFinancialAccountKey] [int] NULL,
	[DimClientKey] [int] NULL,
	[DimEmployeeKey] [int] NULL,
	[DimTeamMemberKey] [int] NULL,
	[DimFlowTypeKey] [int] NULL, 
	[DimClientTenureKey] [int] NULL, 
	[DimClientAssetsKey] [int] NULL, 
	[DimEmployeeMgmtKey] [int] NULL, 
	[DimAgeGroupKey] [int] NULL, 
	[DimPeerGroupKey] [int] NULL,
	[FlowAmount] [decimal](18, 2) NULL,
	[FlowAmountOriginalCurrency] [decimal](18,2) NULL, 
	[FlowCount] [int] NULL,
	[RecurringTransactionDaysLag] [int] NULL,
	[StgRecordType] [nvarchar](25) NULL,
	[ReferenceNumber] [nvarchar](25) NULL,
	[SubReferenceNumber] [nvarchar](25) NULL,
	[CreatedDate] [datetime] NULL,
	[UpdatedDate] [datetime] NULL,
	[SystemOfRecord] [nvarchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ([ReferenceNumber]),
	HEAP
)

IF OBJECT_ID('tempdb..#NetFlows_Temp', 'U') IS NOT NULL
    DROP TABLE #NetFlows_Temp

CREATE TABLE #NetFlows_Temp (
	[ReferenceNumber] [nvarchar](25) NULL,
	[SubReferenceNumber] [nvarchar](25) NULL,
	[Accttype] [nvarchar] (2) NULL,
	[AccountNumberInternal] [int] NULL,	 
	[FinAccountNumber] [nvarchar](100) NULL,	 
	[TransactionCode] [nvarchar](5) NULL, 
	[CurrencyCode] [nvarchar](4) NULL,
	[ExchangeRate] [decimal](19,13) NULL, 
	[TransactionAmount] [decimal](18,2) NULL, 
	[CancellationDate] [datetime] NULL,
	[InternalTransfer] [nvarchar](25) NULL,
	[TradeDate] [datetime] NULL,
	[RecurringTransactionDaysLag] [int] NULL,
	[StgRecordType] [nvarchar](25) NULL,
	[CreatedDate] [datetime] NULL,
	[UpdatedDate] [datetime] NULL,
	[SystemOfRecord] [nvarchar](100) NULL
)
WITH
(
	DISTRIBUTION = HASH ([ReferenceNumber]),
	HEAP
)

/*
	FIND LAST DATE OF FACT TABLE LOAD 
*/

      SELECT @MaxFactLoadDate = MAX(UpdatedDate) --FIN IS IN PST NO NEED TO CONVERT BACK TO PST
	    FROM #FactNetFlows


/*
	CTAS TEMP TABLE WITH INCREMENTAL CHANGES FROM vFiTransFndng SINCE MOST RECENT FACT LOAD
*/

IF OBJECT_ID('tempdb..#NetFlowsIncremental') IS NOT NULL 
	DROP TABLE #NetFlowsIncremental

CREATE TABLE #NetFlowsIncremental (
           ReferenceNumber NVARCHAR(25)
		 , SubReferenceNumber NVARCHAR(25)
		 , AcctType NVARCHAR(25)
		 , AccountNumberInternal INT
		 , FinAccountNumber NVARCHAR(25)
		 , TransactionCode NVARCHAR(25)		
		 , AssetTypeCode NVARCHAR(25)
		 , CurrencyCode NVARCHAR(25)
		 , ExchangeRate FLOAT
		 , TransactionAmount FLOAT
		 , CancellationDate DATETIME
		 , InternalTransfer NVARCHAR(25)
		 , TradeDate DATETIME
		 , CreatedDate DATETIME
		 , UpdatedDate DATETIME 
		 , SystemOfRecord NVARCHAR(25)
)	
WITH 
(
	DISTRIBUTION = HASH(FinAccountNumber), 
	HEAP
) 


    /*
		TEMP TABLE IS LOADED WITH INCREMENTAL TRANSACTIONS SOURCED FROM FIN ONLY.
		WE'LL LOAD GWP TRANSACTIONS IN THE SUBSEQUENT STEP - LOGIC TO IDENTIFY NEW/MODIFIED RECORDS IS DIFFERENT IN GWP.
    */

	INSERT
	  INTO #NetFlowsIncremental (
           ReferenceNumber
		 , SubReferenceNumber
		 , AcctType
		 , AccountNumberInternal
		 , FinAccountNumber
		 , TransactionCode		
		 , AssetTypeCode
		 , CurrencyCode
		 , ExchangeRate
		 , TransactionAmount
		 , CancellationDate
		 , InternalTransfer
		 , TradeDate
		 , CreatedDate
		 , UpdatedDate		
		 , SystemOfRecord	
	)	

    /*
		TEMP TABLE IS LOADED WITH INCREMENTAL TRANSACTIONS SOURCED FROM FIN ONLY.
		WE'LL LOAD GWP TRANSACTIONS IN THE SUBSEQUENT STEP - LOGIC TO IDENTIFY NEW/MODIFIED RECORDS IS DIFFERENT IN GWP.
    */

    SELECT Src.TransRefNbr AS ReferenceNumber
		 , Src.TransSubRefNbr AS SubReferenceNumber
		 , Src.TransAcctTypCd AS AcctType
		 , AI.ai_fin_acct_no AS AccountNumberInternal
		 , AI.ai_advisor_acct_no AS FinAccountNumber
		 , Src.TransTypAcrnym AS TransactionCode		
		 , Src.AssetTypCd AS AssetTypeCode
		 , Src.TransCurrCd AS CurrencyCode
		 , Src.TransFxRate AS ExchangeRate
		 , ABS(Src.TransAmtNet) AS TransactionAmount
		 , Src.TransDteCxl AS CancellationDate
		 , Src.TransIntrnlXferCd AS InternalTransfer
		 , Src.TransTrdDte AS TradeDate
		 , Src.TransDteCre AS CreatedDate
		 , Src.TransDlc AS UpdatedDate
		 , 'FIN' AS SystemOfRecord

	  FROM Fin.vFiTransFndng AS Src

	  JOIN Fin.account_info_ai AS AI
	    ON Src.AcctId = AI.ai_advisor_acct_no

	  --BELOW JOIN IS USED TO EXCLUDE TRANSACTIONS IN GWP 
	  LEFT 
      JOIN FinM.PortfoliosCutover AS GWP 
        ON Src.AcctId = GWP.Portfolio_Code 
       AND Src.TransTrdDte >= GWP.Cutover_Date

	 WHERE AI.ai_Selection_Field2 = '2'
	   AND AI.ai_Selection_Field4 in ('I','P') 
	   AND Src.TransIntrnlXferCd <> 'Y' --TRANSACTION IS NOT A TRANSFER BETWEEN EXISTING FI ACCOUNTS
	   AND Src.TransTrdDte < CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') --WE HAVE FUTURE DATED FLOWS - IGNORE THOSE
	   AND CONVERT(DATE, Src.TransDlc) >= DATEADD(D, -1, ISNULL(@MaxFactLoadDate, @MinDateValue)) --LIMIT RECORDSET TO NEW RECORDS ADDED OR MODIFIED SINCE LAST FACT LOAD. GOING BACK ONE ADDITIONAL DAY JUST TO BE SAFE
	   AND GWP.Portfolio_Code IS NULL --EXCLUDE TRANSACTIONS THAT EXIST IN GWP TOO

    /*
		LOAD TEMP TABLE GWP TRANSACTIONS 

		TWO STEP PROCESS:
			
			1) PICK UP ANY NEW RECORDS CREATED/MODIFIED SINCE LAST FACT LOAD
			2) PICK UP ANY COMBINATION OR REFERENCE NUMBERS AND SUB REFERENCE NUMBERS THAT DON'T ALREADY EXIST IN THE FACT TABLE.
			   WE'VE SEEN INSTANCES WHERE THEY ARE ENTERING IN BACK-DATED CREATED AND MODIFIED DATES IN THE FIN M GWP TABLE. IF THAT HAPPENS,
			   STEP 1 WILL NEVER PICK UP THAT TRANSACTION.

    */


	--STEP 1
	INSERT
	  INTO #NetFlowsIncremental (
           ReferenceNumber
		 , SubReferenceNumber
		 , AcctType
		 , AccountNumberInternal
		 , FinAccountNumber
		 , TransactionCode		
		 , AssetTypeCode
		 , CurrencyCode
		 , ExchangeRate
		 , TransactionAmount
		 , CancellationDate
		 , InternalTransfer
		 , TradeDate
		 , CreatedDate
		 , UpdatedDate		
		 , SystemOfRecord	
	)

	SELECT Src.TransRefNbr AS ReferenceNumber
		 , Src.TransSubRefNbr AS SubReferenceNumber
		 , Src.TransAcctTypCd AS AcctType
		 , AI.ai_fin_acct_no AS AccountNumberInternal
		 , AI.ai_advisor_acct_no AS FinAccountNumber
		 , Src.TransTypAcrnym AS TransactionCode		
		 , Src.AssetTypCd AS AssetTypeCode
		 , Src.TransCurrCd AS CurrencyCode
		 , Src.TransFxRate AS ExchangeRate
		 , ABS(Src.TransAmtNet) AS TransactionAmount
		 , Src.TransDteCxl AS CancellationDate
		 , Src.TransIntrnlXferCd AS InternalTransfer
		 , Src.TransTrdDte AS TradeDate
		 , Src.TransDteCre AS CreatedDate
		 , Src.TransDlc AS UpdatedDate
		 , 'GWP' AS SystemOfRecord		 

	  FROM FinM.FiTransFndng_gwp AS Src

	  JOIN FinM.account_info_ai AS AI
	    ON Src.AcctId = AI.ai_advisor_acct_no

      JOIN FinM.PortfoliosCutover AS GWP 
        ON Src.AcctId = GWP.Portfolio_Code 
       AND Src.TransTrdDte >= GWP.Cutover_Date

	 WHERE AI.ai_Selection_Field2 = '2'
	   AND AI.ai_Selection_Field4 in ('I','P') 
	   AND Src.TransIntrnlXferCd <> 'Y' --TRANSACTION IS NOT A TRANSFER BETWEEN EXISTING FI ACCOUNTS
	   AND Src.TransTrdDte < CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') --WE HAVE FUTURE DATED FLOWS - IGNORE THOSE
	   --LIMIT RECORDSET TO NEW RECORDS ADDED SINCE LAST FACT LOAD. GOING BACK ONE ADDITIONAL DAY JUST TO BE SAFE. CONFIRMED IN SOURCE THAT TransDlc IS NEVER NULL
	   AND CONVERT(DATE, Src.TransDlc) >= DATEADD(D, -1, ISNULL(@MaxFactLoadDate, @MinDateValue)) 



	--STEP 2
	INSERT
	  INTO #NetFlowsIncremental (
           ReferenceNumber
		 , SubReferenceNumber
		 , AcctType
		 , AccountNumberInternal
		 , FinAccountNumber
		 , TransactionCode		
		 , AssetTypeCode
		 , CurrencyCode
		 , ExchangeRate
		 , TransactionAmount
		 , CancellationDate
		 , InternalTransfer
		 , TradeDate
		 , CreatedDate
		 , UpdatedDate		
		 , SystemOfRecord			 	
	)

	SELECT Src.TransRefNbr AS ReferenceNumber
		 , Src.TransSubRefNbr AS SubReferenceNumber
		 , Src.TransAcctTypCd AS AcctType
		 , AI.ai_fin_acct_no AS AccountNumberInternal
		 , AI.ai_advisor_acct_no AS FinAccountNumber
		 , Src.TransTypAcrnym AS TransactionCode		
		 , Src.AssetTypCd AS AssetTypeCode
		 , Src.TransCurrCd AS CurrencyCode
		 , Src.TransFxRate AS ExchangeRate
		 , ABS(Src.TransAmtNet) AS TransactionAmount
		 , Src.TransDteCxl AS CancellationDate
		 , Src.TransIntrnlXferCd AS InternalTransfer
		 , Src.TransTrdDte AS TradeDate
		 , Src.TransDteCre AS CreatedDate
		 , Src.TransDlc AS UpdatedDate
		 , 'GWP' AS SystemOfRecord			 

	  FROM FinM.FiTransFndng_gwp AS Src

	  JOIN FinM.account_info_ai AS AI
	    ON Src.AcctId = AI.ai_advisor_acct_no

      JOIN FinM.PortfoliosCutover AS GWP 
        ON Src.AcctId = GWP.Portfolio_Code 
       AND Src.TransTrdDte >= GWP.Cutover_Date

	  LEFT
	  JOIN #NetFlowsIncremental AS NFI
	    ON SRC.TransRefNbr = NFI.ReferenceNumber
	   AND SRC.TransSubRefNbr = NFI.SubReferenceNumber

	  LEFT
	  JOIN #FactNetFlows AS TGT
	    ON SRC.TransRefNbr = TGT.ReferenceNumber
	   AND SRC.TransSubRefNbr = TGT.SubReferenceNumber

	 WHERE AI.ai_Selection_Field2 = '2'
	   AND AI.ai_Selection_Field4 in ('I','P') 
	   AND Src.TransIntrnlXferCd <> 'Y' --TRANSACTION IS NOT A TRANSFER BETWEEN EXISTING FI ACCOUNTS
	   AND Src.TransTrdDte < CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') --WE HAVE FUTURE DATED FLOWS - IGNORE THOSE
	   AND NFI.ReferenceNumber IS NULL --REFERENCE NUMBER + SUB REF NUMBER DOES NOT EXIST WITHIN TEMP TABLE LOADED IN STEP 1 ABOVE
	   AND TGT.ReferenceNumber IS NULL --REFERENCE NUMBER + SUB REF NUMBER DOES NOT EXIST WITHIN FACT TABLE 
	       


--Figuring out recurring transactions based on the ccurrency, position, Quantity and transaction type. This is so we can separate recurring widthrawals from the rest. 
IF OBJECT_ID('tempdb..#RecurringTransactions') IS NOT NULL DROP TABLE #RecurringTransactions
CREATE TABLE #RecurringTransactions with (distribution = hash(FinAccountNumber), HEAP) 
AS  

/*
	RECURRING TRANSACTIONS ARE ONLY CONSIDERED RECURRING IF WE FIND MATCHING TRANSACTIONS WITH THE BELOW CRITERIA
		- TRANSACTION AMOUNT
		- TRANSACTION TYPE 
		- CURRENCY CODE
	
	IN ADDITION, THE TRANSACTION MUST ALSO BE SOURCED FROM THE SAME ASSET TYPE. IN OTHER WORDS, IF I HAVE 100K IFI (INITIAL FUNDS IN) USD IN THE FORM OF CASH AND 
	100K IFI (INITIAL FUNDS IN) USD IN THE FORM OF STOCK, THESE ARE NOT RECURRING TRANSACTIONS. 
*/

    SELECT MAX(Src.TransTrdDte) AS PreviousTradeDate
		 , CT.TradeDate
		 , CT.FinAccountNumber
		 , CT.TransactionAmount
		 , CT.TransactionCode
		 , CT.CurrencyCode
	  FROM FinM.FiTransFndng AS Src 
	  JOIN FinM.account_info_ai AS AI
        ON Src.AcctId = AI.ai_advisor_acct_no
	  JOIN #NetFlowsIncremental CT
	    ON AI.ai_advisor_acct_no = CT.FinAccountNumber
	   AND ABS(Src.TransAmtNet) = CT.TransactionAmount
	   AND RIGHT(TRIM(Src.TransTypAcrnym), 2) = RIGHT(TRIM(CT.TransactionCode), 2) --ONLY LOOKING AT LAST TWO CHARACTERS | IFI AND FI SHOULD STILL COUNT AS RECURRING TRANSACTIONS
	   AND Src.TransCurrCd = CT.CurrencyCode
	   AND CT.TransactionCode IN ('ISI', 'SI', 'ISO', 'SO')
	 WHERE AI.ai_Selection_Field2 = '2'
	   AND AI.ai_Selection_Field4 in ('I','P') 
	   AND Src.TransIntrnlXferCd <> 'Y' --TRANSACTION IS NOT A TRANSFER BETWEEN EXISTING FI ACCOUNTS
	   AND Src.TransTrdDte < CT.TradeDate
	   AND Src.TransTrdDte < CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')  --WE HAVE FUTURE DATED FLOWS - IGNORE THOSE
	   AND Src.TransTypAcrnym IN ('ISI', 'SI', 'ISO', 'SO')
	 GROUP 
	    BY CT.TradeDate
		 , CT.FinAccountNumber
		 , CT.TransactionAmount
		 , CT.TransactionCode
		 , CT.CurrencyCode

     UNION

    SELECT MAX(Src.TransTrdDte) AS PreviousTradeDate
		 , CT.TradeDate
		 , CT.FinAccountNumber
		 , CT.TransactionAmount
		 , CT.TransactionCode
		 , CT.CurrencyCode
	  FROM FinM.FiTransFndng AS Src 
	  JOIN FinM.account_info_ai AS AI
        ON Src.AcctId = AI.ai_advisor_acct_no
	  JOIN #NetFlowsIncremental CT
	    ON AI.ai_advisor_acct_no = CT.FinAccountNumber
	   AND ABS(Src.TransAmtNet) = CT.TransactionAmount
	   AND RIGHT(TRIM(Src.TransTypAcrnym), 2) = RIGHT(TRIM(CT.TransactionCode), 2) --ONLY LOOKING AT LAST TWO CHARACTERS | IFI AND FI SHOULD STILL COUNT AS RECURRING TRANSACTIONS
	   AND Src.TransCurrCd = CT.CurrencyCode
	   AND CT.TransactionCode IN ('IFI', 'FI', 'IFO', 'FO')
	 WHERE AI.ai_Selection_Field2 = '2'
	   AND AI.ai_Selection_Field4 in ('I','P') 
	   AND Src.TransIntrnlXferCd <> 'Y' --TRANSACTION IS NOT A TRANSFER BETWEEN EXISTING FI ACCOUNTS
	   AND Src.TransTrdDte < CT.TradeDate
	   AND Src.TransTrdDte < CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time')  --WE HAVE FUTURE DATED FLOWS - IGNORE THOSE
	   AND Src.TransTypAcrnym IN ('IFI', 'FI', 'IFO', 'FO')
	 GROUP 
	    BY CT.TradeDate
		 , CT.FinAccountNumber
		 , CT.TransactionAmount
		 , CT.TransactionCode
		 , CT.CurrencyCode




   INSERT 
     INTO #NetFlows_Temp 
     
	SELECT 
		   CT.ReferenceNumber
		 , CT.SubReferenceNumber
		 , CT.AcctType
		 , CT.AccountNumberInternal
		 , CT.FinAccountNumber
		 , CT.TransactionCode
		 , CT.CurrencyCode
		 , CT.ExchangeRate
		 , CT.TransactionAmount
		 , NULL AS CancellationDate
		 , CT.InternalTransfer
		 , CT.TradeDate	
		 , DATEDIFF(DAY, RT.PreviousTradeDate ,CT.TradeDate) AS RecurringTransactionDaysLag --Filling this one later once we reduce the recordset
         , 'New Record' AS StgRecordType	
		 , CT.CreatedDate
		 , CT.UpdatedDate
		 , CT.SystemOfRecord
	  FROM #NetFlowsIncremental CT
	  LEFT JOIN #RecurringTransactions RT
		ON CT.FinAccountNumber = RT.FinAccountNumber
		AND CT.CurrencyCode = RT.CurrencyCode
		AND CT.TransactionAmount = RT.TransactionAmount
		AND CT.TransactionCode = RT.TransactionCode
		AND CT.TradeDate = RT.TradeDate

	 UNION
	   ALL 

	SELECT 
		   CT.ReferenceNumber
		 , CT.SubReferenceNumber
		 , CT.AcctType
		 , CT.AccountNumberInternal
		 , CT.FinAccountNumber
		 , CT.TransactionCode
		 , CT.CurrencyCode
		 , CT.ExchangeRate
		 , CT.TransactionAmount
		 , CT.CancellationDate
		 , CT.InternalTransfer
		 , CT.TradeDate
		 , DATEDIFF(DAY, RT.PreviousTradeDate ,CT.TradeDate) AS RecurringTransactionDaysLag --Filling this one later once we reduce the recordset
         , 'Cancelled Record' AS StgRecordType	
		 , CT.CreatedDate
		 , CT.UpdatedDate
		 , CT.SystemOfRecord
	  FROM #NetFlowsIncremental CT
	  LEFT JOIN #RecurringTransactions RT
		ON CT.FinAccountNumber = RT.FinAccountNumber
		AND CT.CurrencyCode = RT.CurrencyCode
		AND CT.TransactionAmount = RT.TransactionAmount
		AND CT.TransactionCode = RT.TransactionCode
		AND CT.TradeDate = RT.TradeDate
	 WHERE CT.CancellationDate IS NOT NULL


; With StockTransactionswithClientInfo as (

SELECT 
           ST.ReferenceNumber
		 , ST.SubReferenceNumber
		 , ST.AcctType
		 , ST.AccountNumberInternal
		 , ST.FinAccountNumber
		 , ST.TransactionCode
		 , ST.CurrencyCode
		 , ST.ExchangeRate
		 , ST.TransactionAmount
		 , ST.CancellationDate
		 , ST.InternalTransfer
		 , ST.TradeDate	
		 , ST.RecurringTransactionDaysLag
         , ST.StgRecordType	
		 , ST.CreatedDate
		 , ST.UpdatedDate
		 , H.ClientID
		 , ST.SystemOfRecord
FROM #NetFlows_Temp ST
INNER JOIN REF.HistoricalAccountAttributes AS H
	   ON ST.FinAccountNumber = H.FinAccountNumber 
	  AND H.EffectiveStartDate <= ST.TradeDate
	  AND H.EffectiveEndDate > ST.TradeDate
)

, FinalDataset as (

SELECT 
           FD.ReferenceNumber
		 , FD.SubReferenceNumber
		 , FD.AcctType
		 , FD.AccountNumberInternal
		 , FD.FinAccountNumber
		 , FD.TransactionCode
		 , FD.CurrencyCode
		 , FD.ExchangeRate
		 , FD.TransactionAmount
		 , FD.CancellationDate
		 , FD.InternalTransfer
		 , FD.TradeDate		
		 , FD.RecurringTransactionDaysLag
		 , FD.CreatedDate
		 , FD.UpdatedDate
		 , FD.ClientID
		 , FD.StgRecordType	
		 , isnull(STG.DimClientAssetsKey,@UnknownNumberValue) DimClientAssetsKey
		 , isnull(STG.DimTenureKey,@UnknownNumberValue) DimClientTenureKey
		 , FD.SystemOfRecord
FROM StockTransactionswithClientInfo FD
LEFT JOIN STG.DailyClientAssetsandTenureKeys STG ON FD.ClientID = STG.ClientID AND STG.CalendarDate = FD.TradeDate 

)



   INSERT 
     INTO #Stg_Fact_NetFlows_Temp (     
		  DimTransactionDateKey 
		, DimCancellationDateKey
		, DimFinancialAccountKey 
		, DimClientKey 
		, DimEmployeeKey 
		, DimTeamMemberKey 
		, DimFlowTypeKey
		, DimClientTenureKey 
		, DimClientAssetsKey
	    , DimEmployeeMgmtKey
	    , DimAgeGroupKey 
		, DimPeerGroupKey
		, FlowAmount
		, FlowAmountOriginalCurrency
		, FlowCount
		, RecurringTransactionDaysLag
		, StgRecordType
	    , ReferenceNumber 
		, SubReferenceNumber 
		, CreatedDate 
		, UpdatedDate 
		, SystemOfRecord
 )

    SELECT 
		  ISNULL(DtTrd.DimDateKey, @UnknownNumberValue) AS DimTransactionDateKey
		, ISNULL(DtCxl.DimDateKey, @UnknownNumberValue) AS DimCancellationDateKey
		, ISNULL(Act.DimFinancialAccountKey, @UnknownNumberValue) AS DimFinancialAccountKey
		, ISNULL(C.DimClientKey, @UnknownNumberValue) AS DimClientKey
		, ISNULL(E.DimEmployeeKey, @UnknownNumberValue) AS DimEmployeeKey
		, ISNULL(TM.DimTeamMemberKey, @UnknownNumberValue) AS DimTeamMemberKey		
		, ISNULL(FT.DimFlowTypeKey, @UnknownNumberValue) AS DimFlowTypeKey	
		, SRC.DimClientTenureKey
		, Src.DimClientAssetsKey
		, ISNULL(EM.DimEmployeeMgmtKey, @UnknownNumberValue) AS DimEmployeeMgmtKey
		, ISNULL(AG.DimAgeGroupKey, @UnknownNumberValue) AS DimAgeGroupKey
		, ISNULL(PGIC.DimPeerGroupKey, @UnknownNumberValue) AS DimPeerGroupKey
		, CASE WHEN Src.CancellationDate IS NULL
			   THEN CONVERT(DECIMAL(18,2), (Src.TransactionAmount * (CASE WHEN Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 1 ELSE -1 END))/ISNULL(CE.ExchangeRate, Src.ExchangeRate))
			   ELSE -1 * CONVERT(DECIMAL(18,2), (Src.TransactionAmount * (CASE WHEN Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 1 ELSE -1 END))/ISNULL(CE.ExchangeRate, Src.ExchangeRate))
			   END AS FlowAmount
		, CASE WHEN Src.CancellationDate IS NULL 
			   THEN CONVERT(DECIMAL(18,2), (Src.TransactionAmount * (CASE WHEN Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 1 ELSE -1 END)))
			   ELSE -1 * CONVERT(DECIMAL(18,2), (Src.TransactionAmount * (CASE WHEN Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 1 ELSE -1 END)))
			   END AS FlowAmountOriginalCurrency
		, CASE WHEN Src.CancellationDate IS NULL THEN 1 ELSE -1 END AS FlowCount
		, Src.RecurringTransactionDaysLag
		, Src.StgRecordType
		, Src.ReferenceNumber
		, Src.SubReferenceNumber
		, Src.CreatedDate
		, Src.UpdatedDate
		, Src.SystemOfRecord

     FROM FinalDataset AS Src 

     JOIN FDW.DimDate DtTrd
       ON DtTrd.CalendarDate = Src.TradeDate

     LEFT
	 JOIN FDW.DimDate DtCxl
       ON DtCxl.CalendarDate = Src.CancellationDate

	/*
		USING THE BELOW REFERENCE TABLE IS LIKELY OVERKILL HERE SINCE WE ARE DEALING WITH INCREMENTAL 'LIVE' DATA AND THE SITUATION THAT 
		I'M ABOUT TO EXPLAIN TO YOU IS LIKELY AN EDGE CASE.
		
		HOWEVER... WITH THAT SAID... IMAGINE A SCENARIO WHERE OUR PIPELINES FAIL ON A FRIDAY NIGHT. THE FAILED PIPELINE IS NOT ADDRESSED UNTIL THE FOLLOWING MONDAY 
		MORNING. COINCIDENTALLY, A FIN, 12345, HAPPENED TO CHANGE FROM CID 100 TO CID 200 ON FRIDAY AFTERNOON. GIVEN THAT DIMFINANCIALACCOUNT IS A TYPE 1 DIM
		AND THE FACT THAT OUR DIMS PROCESS PRIOR TO THE FACTS, WHEN WE FIX THE FAILED PIPELINE ON MONDAY, DIMFINANCIALACCOUNT WOULD REFLECT FIN 12345 BEING 
		FLIPPED TO CID 200. NOW ALSO IMAGINE FIN 12345 HAVING A HANDFUL OF TRANSACTIONS THAT CAME IN ON FRIDAY PRIOR TO FLIPPING TO CID 200. 
		IF WE JOINED DIRECTLY TO DIMFINANCIALACCOUNT AND FROM THAT TO DIMCLIENT, WE WOULD BE REPORTING THE INCORRECT DIMCLIENTKEY FOR THOSE TRANSACTIONS.
		
	*/

	 LEFT
	 JOIN FDW.DimFinancialAccount Act
       ON Act.FinAccountNumber = src.FinAccountNumber

	 LEFT
	 JOIN FDW.DimClient C
	   ON C.ClientId = src.ClientId
	  AND C.EffectiveStartDate <= Src.TradeDate
	  AND C.EffectiveEndDate > Src.TradeDate

     LEFT
	 JOIN REF.vwRelationshipManagementAssignmentWindow AS IC 
	   ON IC.ClientId = C.ClientId 
	  AND IC.AssignmentWindowStartDate <= Src.TradeDate
	  AND IC.AssignmentWindowEndDate > Src.TradeDate

	 LEFT
	 JOIN FDW.DimTeamMember AS TM 
	   ON TM.TeamMemberGUID = IC.AssignedToGUID
	  AND TM.EffectiveStartDate <= Src.TradeDate
	  AND TM.EffectiveEndDate > Src.TradeDate	
	  
	  --Some ADUserIDs are reused amongst EmployeeIDs, generally when someone is hired  as fulltime from contractor.
	  --In order to combat this, we exclude employee records where they were terminated.
	  LEFT
	  JOIN [FDW].[DimEmployee]AS E 																				   
	   ON E.ActiveDirectoryUserIdWithDomain = TM.TeamMemberActiveDirectoryUserIdWithDomain
	  AND E.EffectiveStartDate <= Src.TradeDate
	  AND E.EffectiveEndDate > Src.TradeDate
	  AND TerminationRecord = 'No'

     LEFT
	 JOIN REF.CurrencyExchangeUSD AS CE
       ON Src.CurrencyCode = CE.BaseCurrency
      AND Src.TradeDate = CE.EffectiveDate

	 LEFT
	 JOIN FDW.DimFlowType AS FT
	   ON Src.TransactionAmount >= FT.FlowAmountLowerBound
      AND Src.TransactionAmount < FT.FlowAmountUpperBound
	  AND CASE WHEN Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI')  THEN 1 ELSE -1 END = FT.FlowTypeIdentifier
	  AND CASE 
			WHEN isnull(C.ClientType,@unknowntextvalue) = 'Client - Non Trading' AND Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 'Initial Funds In'
			WHEN isnull(C.ClientType,@unknowntextvalue) = 'Client - Trading' AND Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') 
			 AND Src.TradeDate >= C.ClientTradingDate AND Src.TradeDate <= DATEADD(DAY, 90, C.ClientTradingDate) THEN 'Initial Funds In'
			WHEN isnull(C.ClientType,@unknowntextvalue) <> 'Client - Non Trading' AND Src.TransactionCode IN ('FI', 'IFI', 'SI', 'ISI') THEN 'Funds In'
			
			WHEN isnull(C.ClientType,@unknowntextvalue) = 'Client - Non Trading' AND Src.TransactionCode NOT IN ('FI', 'IFI', 'SI', 'ISI') THEN 'Initial Funds Out'
			WHEN isnull(C.ClientType,@unknowntextvalue) = 'Client - Trading' AND Src.TransactionCode NOT IN ('FI', 'IFI', 'SI', 'ISI')  
			 AND Src.TradeDate >= C.ClientTradingDate AND Src.TradeDate <= DATEADD(DAY, 90, C.ClientTradingDate) THEN 'Initial Funds Out'
			WHEN isnull(C.ClientType,@unknowntextvalue) <> 'Client - Non Trading' AND Src.TransactionCode NOT IN ('FI', 'IFI', 'SI', 'ISI') THEN 'Funds Out'
		  END = FT.FundingType
	  AND CASE WHEN AcctType = '2' THEN 'Unmanaged' 
			   WHEN AcctType in ('0','1') THEN 'Managed' 
			   ELSE @UNKNOWNTEXTVALUE END =  FT.TransactionAccountType
	  AND FT.AssetType = CASE 
							WHEN TransactionCode IN ('SI', 'ISI', 'SO', 'ISO') THEN 'Stock' 
							WHEN TransactionCode IN ('FI', 'IFI', 'FO', 'IFO') THEN 'Cash' 
						 END
	  AND FT.FlowOriginalCurrency = Src.CurrencyCode

	 LEFT
	 JOIN (
             SELECT
                 ContactId as ClientID
               , fi_Id_search AS ClientNumber
               , BirthDate as DateofBirth
             FROM [Iris].[ContactBase]
             WHERE BirthDate IS NOT NULL
           ) AS Bdays on Bdays.ClientID  = src.ClientId 

     LEFT 
     JOIN [FDW].[DimAgeGroup] AS AG
       ON ISNULL(FLOOR(DATEDIFF(Day, Bdays.[DateOfBirth], Src.TradeDate)/365.25), @UnknownNumberValue) >= AG.[StartAge] 
      AND ISNULL(FLOOR(DATEDIFF(Day, Bdays.[DateOfBirth], Src.TradeDate)/365.25), @UnknownNumberValue) < AG.[EndAge]

	 LEFT
	 JOIN [FDW].[DimEmployeeMgmt] AS EM
	   ON E.EmployeeID = EM.EmployeeID
 	  AND Src.TradeDate >= EM.EffectiveStartDate
	  AND Src.TradeDate <= EM.EffectiveEndDate

	 LEFT 
	 JOIN FDW.DimPeerGroupIC PGIC
	   ON PGIC.TeamMemberSpecialty = TM.TeamMemberSpecialty
	  AND PGIC.TenureInMonths = DATEDIFF(M, E.RoleStartDate, Src.TradeDate)	

--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

---- Begin of Transaction scope. Transaction will be committed after each batch. 
--BEGIN TRANSACTION 

---- If any batch fail, it will be caught in the CATCH block and will be rolled back.
--BEGIN TRY

--SET @Source = '[{"SourceTable":"#Stg_Fact_NetFlows_Temp"}]'
--SET @Target = '[{"DestinationTable":"FDW.FactNetFlows"}]'
--SET @StartTime = GETDATE()
--SET @DWUpdatedDatetime = GETDATE()

INSERT INTO #FactNetFlows ( 
	   DimTransactionDateKey 
	 , DimCancellationDateKey
     , DimFinancialAccountKey
     , DimClientKey
     , DimEmployeeKey
     , DimTeamMemberKey
     , DimFlowTypeKey
	 , DimClientTenureKey
	 , DimClientAssetsKey
	 , DimEmployeeMgmtKey
	 , DimAgeGroupKey
	 , DimPeerGroupKey
     , FlowAmount
	 , FlowAmountOriginalCurrency
	 , FlowCount
	 , RecurringTransactionDaysLag
     , ReferenceNumber
     , SubReferenceNumber
     , CreatedDate
     , UpdatedDate
	 , SystemOfRecord
     --, DWCreatedDateTime
     --, DWUpdatedDateTime
     --, ETLJobProcessRunId
     --, ETLJobSystemRunId
)

SELECT  
       Src.DimTransactionDateKey
	 , Src.DimCancellationDateKey
     , Src.DimFinancialAccountKey
     , Src.DimClientKey
     , Src.DimEmployeeKey
     , Src.DimTeamMemberKey
     , Src.DimFlowTypeKey
	 , Src.DimClientTenureKey
	 , Src.DimClientAssetsKey
	 , Src.DimEmployeeMgmtKey
	 , Src.DimAgeGroupKey
	 , Src.DimPeerGroupKey
     , Src.FlowAmount
	 , Src.FlowAmountOriginalCurrency
	 , Src.FlowCount
	 , Src.RecurringTransactionDaysLag
     , Src.ReferenceNumber
     , Src.SubReferenceNumber
     , Src.CreatedDate
     , Src.UpdatedDate
	 , Src.SystemOfRecord
     --, @DWUpdatedDateTime AS DWCreatedDateTime
     --, @DWUpdatedDateTime AS DWUpdatedDateTime
     --, @ETLJobProcessRunId AS ETLJobProcessRunId
     --, @ETLJobSystemRunId AS ETLJobSystemRunId
  FROM #Stg_Fact_NetFlows_Temp Src
  LEFT
  JOIN #FactNetFlows Tgt
    ON Src.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.FlowCount = Tgt.FlowCount --Inserts Cancelled records that otherwise are identical to the real records
   AND Src.SystemOfRecord = Tgt.SystemOfRecord --WE HAVE IDENTICAL REFERENCE NUMBERS IN FIN AND GWP - NEED TO USE THIS TO INSERT 
 WHERE Tgt.ReferenceNumber IS NULL

--OPTION (Label = 'FDW.FactNetFlows-Insert')

--EXEC MDR.spGetRowCountByQueryLabel 'FDW.FactNetFlows-Insert', @InsertCount OUT

--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--EXEC MDR.spProcessTaskLogInsertRowCount
--	   @ETLJobProcessRunId 
--     , @ComponentName
--	 , @Source 
--	 , @Target 
--	 , @InsertCount	 
--     , @DurationInSeconds


--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY

--BEGIN CATCH 
	
--	-- Transaction started in procedure per batch. Roll back transaction.
--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT(@Source,'-',@Target,':', ERROR_MESSAGE())

--END CATCH 


----Drop temp tables to free up Temp DB space
--IF OBJECT_ID ('TEMPDB..#Stg_Fact_NetFlows_Temp') IS NOT NULL DROP TABLE #Stg_Fact_NetFlows_Temp
--IF OBJECT_ID ('TEMPDB..#NetFlows_Temp') IS NOT NULL DROP TABLE #NetFlows_Temp
--IF OBJECT_ID('tempdb..#RecurringTransactions') IS NOT NULL DROP TABLE #RecurringTransactions
--IF OBJECT_ID('tempdb..#NetFlowsIncremental') IS NOT NULL DROP TABLE #NetFlowsIncremental


--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END
--GO
