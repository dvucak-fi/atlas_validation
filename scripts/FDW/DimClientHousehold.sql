--DIM CLIENT HOUSEHOLD TEMP TABLE
IF OBJECT_ID('tempdb..#DimClientHousehold', 'U') IS NOT NULL
    DROP TABLE #DimClientHousehold

CREATE TABLE #DimClientHousehold (
	[DimClientHouseholdKey] INT IDENTITY(1,1) NOT NULL,
	[HouseholdUID] NVARCHAR(4000) NULL,
	[HouseholdId] NVARCHAR(4000) NULL,
    [ClientId_Iris] NVARCHAR(4000) NULL,
	[ClientNumber_Iris] NVARCHAR(4000) NULL,	
	[ClientFirstName] NVARCHAR(100) NULL,
	[ClientLastName] NVARCHAR(100) NULL,
	[ClientFullName] NVARCHAR(200) NULL,
	[ServiceProduct] NVARCHAR(255) NULL,
	[ClientType] NVARCHAR(100) NULL,		
	[ClientSubType] NVARCHAR(100) NULL,	
	[ClientClearanceDate]	DATETIME NULL, 
	[ClientTradingDate]	DATETIME NULL, 
	[StrengthCode] NVARCHAR(100) NULL,
	[DimStrengthCodeKey] INT NULL,
	[ContactFrequency] NVARCHAR(255) NULL,	
	[Gender] NVARCHAR(255) NULL,
	[MaritalStatus]	NVARCHAR(255) NULL,
	[RetirementStatus] NVARCHAR(100) NULL,
	[EmploymentStatus] NVARCHAR(255) NULL,
	[Industry] NVARCHAR(255) NULL,
	[Occupation] NVARCHAR(200) NULL,	
	[ResidenceCountry] NVARCHAR(100) NULL,
	[ContractCountry] NVARCHAR(100) NULL,
	[ContractEntity] NVARCHAR(100) NULL,
	[CurrencyCode] NVARCHAR(10) NULL,
	[SystemOfRecord] NVARCHAR(100) NULL,
	[RowHash] [varbinary](8000) NULL,
	[EffectiveStartDate] DATETIME NULL,
	[EffectiveEndDate] DATETIME NULL,
	[CurrentRecord] [bit] NULL,
	[DWCreatedDateTime] DATETIME NULL,
	[DWUpdatedDateTime] DATETIME NULL,
	[ETLJobProcessRunId] UNIQUEIDENTIFIER NULL,
	[ETLJobSystemRunId] UNIQUEIDENTIFIER NULL
)
WITH (DISTRIBUTION = HASH(HouseholdUID), CLUSTERED COLUMNSTORE INDEX) 
GO


DECLARE @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
DECLARE @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()

--CREATE PROC [FDW].[spUpsertDimClientHouseholdBackfill] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
--BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.

DECLARE @DWUpdatedDateTime DATETIME
      , @Rows INT
      , @StartTime DATETIME
      , @EndTime DATETIME
      , @DurationInSeconds INT
      , @Source NVARCHAR(255)
      , @Target NVARCHAR(255)
      , @Status INT
      , @ErrorMessage NVARCHAR(512)
       
DECLARE @InsertCount BIGINT
DECLARE @UpdateCount BIGINT

SET @DWUpdatedDateTime = GETDATE()
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


--SFDC TO IRIS CLINT MAPPING 
IF OBJECT_ID('tempdb..#Iris_SFDC_ClientMapping_Temp', 'U') IS NOT NULL
    DROP TABLE #Iris_SFDC_ClientMapping_Temp

CREATE TABLE #Iris_SFDC_ClientMapping_Temp (
	[HouseholdUID] NVARCHAR(4000) NULL,
	[HouseholdId] NVARCHAR(4000) NULL,
	[ClientId_Iris] UNIQUEIDENTIFIER NULL,	
	[ClientNumber_Iris] NVARCHAR(4000) NULL,	
	[AccountRecordType] NVARCHAR(4000) NULL,
	[SFDC_MigrationDate] DATETIME NULL
)
WITH (DISTRIBUTION = HASH(HouseholdUID), CLUSTERED COLUMNSTORE INDEX) 

--DIM CLIENT HOUSEHOLD TEMP TABLE
IF OBJECT_ID('tempdb..#DimClientHousehold_Temp', 'U') IS NOT NULL
    DROP TABLE #DimClientHousehold_Temp

CREATE TABLE #DimClientHousehold_Temp (
	[HouseholdUID] NVARCHAR(4000) NULL,
	[HouseholdId] NVARCHAR(4000) NULL,
    [ClientId_Iris] NVARCHAR(4000) NULL,
	[ClientNumber_Iris] NVARCHAR(4000) NULL,	
	[ClientFirstName] NVARCHAR(100) NULL,
	[ClientLastName] NVARCHAR(100) NULL,
	[ClientFullName] NVARCHAR(200) NULL,
	[ServiceProduct] NVARCHAR(255) NULL,
	[ClientType] NVARCHAR(100) NULL,		
	[ClientSubType] NVARCHAR(100) NULL,	
	[ClientClearanceDate] DATETIME NULL, 
	[ClientTradingDate]	DATETIME NULL, 
	[StrengthCode] NVARCHAR(100) NULL,
	[PreviousStrengthCode] NVARCHAR(100) NULL,
	[ContactFrequency] NVARCHAR(255) NULL,	
	[Gender] NVARCHAR(255) NULL,
	[MaritalStatus]	NVARCHAR(255) NULL,
	[RetirementStatus] NVARCHAR(100) NULL,
	[EmploymentStatus] NVARCHAR(255) NULL,
	[Industry] NVARCHAR(255) NULL,
	[Occupation] NVARCHAR(200) NULL,	
	[ResidenceCountry] NVARCHAR(100) NULL,
	[ContractCountry] NVARCHAR(100) NULL,
	[ContractEntity] NVARCHAR(100) NULL,
	[CurrencyCode] NVARCHAR(10) NULL,
	[SystemOfRecord] NVARCHAR(100) NULL,
	[RecordType] NVARCHAR(100) NULL,
	[RowHash] VARBINARY(8000) NULL,
	[CreatedDate] DATETIME,
	[EffectiveStartDate] DATETIME,
	[EffectiveEndDate] DATETIME
)
WITH (DISTRIBUTION = HASH(HouseholdUID), CLUSTERED COLUMNSTORE INDEX) 


 --Insert Unknown Member Record
--IF NOT EXISTS (SELECT 1 FROM #DimClientHousehold WHERE DimClientHouseholdKey = @UnknownNumberValue)

--BEGIN

--    SET IDENTITY_INSERT #DimClientHousehold ON
          
--    INSERT INTO #DimClientHousehold (
--		    [DimClientHouseholdKey] 
--		  , [HouseholdUID] 
--		  , [HouseholdId] 
--		  , [ClientId_Iris] 
--		  , [ClientNumber_Iris] 
--		  , [ClientFirstName]		  
--		  , [ClientLastName]
--		  , [ClientFullName] 
--		  , [ServiceProduct] 
--		  , [ClientType] 		
--		  , [ClientSubType] 	
--		  , [ClientClearanceDate]	
--		  , [ClientTradingDate]	 
--		  , [StrengthCode] 
--		  , [DimStrengthCodeKey] 
--		  , [ContactFrequency] 	
--		  , [Gender] 
--		  , [MaritalStatus]	
--		  , [RetirementStatus] 
--		  , [EmploymentStatus] 
--		  , [Industry] 
--		  , [Occupation] 	
--		  , [ResidenceCountry] 
--		  , [ContractCountry] 
--		  , [ContractEntity] 
--		  , [CurrencyCode] 
--		  , [SystemOfRecord] 
--		  , [RowHash] 
--		  , [EffectiveStartDate] 
--		  , [EffectiveEndDate] 
--		  , [CurrentRecord] 
--		  , [DWCreatedDateTime]
--		  , [DWUpdatedDateTime] 
--		  , [ETLJobProcessRunId] 
--		  , [ETLJobSystemRunId] 
--    )
    
--    SELECT -- Unknown member record
--            @UnknownNumberValue AS DimClientHouseholdKey 
--		  , @UnknownNumberValue AS HouseholdUID 
--		  , @UnknownNumberValue AS HouseholdId
--          , @UnknownGuid AS ClientId_Iris 
--          , @UnknownNumberValue AS ClientNumber_Iris
--          , @UnknownTextValue AS ClientFirstName
--          , @UnknownTextValue AS ClientLastName		  
--		  , @UnknownTextValue AS ClientFullName
--          , @UnknownTextValue AS ServiceProduct
--          , @UnknownTextValue AS ClientType 
--          , @UnknownTextValue AS ClientSubType
--		  , NULL AS ClientClearanceDate
--          , NULL AS ClientTradingDate
--          , @UnknownTextValue AS StrengthCode
--          , @UnknownNumberValue AS DimStrengthCodeKey
--          , @UnknownTextValue AS ContactFrequency 
--          , @UnknownTextValue AS Gender 
--          , @UnknownTextValue AS MaritalStatus
--          , @UnknownTextValue AS RetirementStatus
--          , @UnknownTextValue AS EmploymentStatus
--          , @UnknownTextValue AS Industry
--          , @UnknownTextValue AS Occupation
--          , @UnknownTextValue AS ResidenceCountry 
--          , @UnknownTextValue AS ContractCountry 
--          , @UnknownTextValue AS ContractEntity 
--          , @UnknownTextValueAbbreviated AS CurrencyCode
--          , @UnknownTextValue AS SystemOfRecord 
--          , NULL AS RowHash
--          , @MinDateValue AS EffectiveStartDate
--          , @MaxDateValue AS EffectiveEndDate
--          , 1 AS CurrentRecord
--          , @DWUpdatedDateTime AS DWCreatedDateTime
--          , @DWUpdatedDateTime AS DWUpdatedDateTime
--          , @UnknownGuId AS ETLJobProcessRunId
--          , @UnknownGuId AS ETLJobSystemRunId


--    SET IDENTITY_INSERT #DimClientHousehold OFF

--END


--BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
---- If any batch fail, it will be caught in the CATCH block and will be rolled back.
--BEGIN TRY

SET @Source = '{"SourceTable":"Iris.fi_contactauditlogBase"}'
SET @Target = '{"TargetTable":"#DimClientHousehold"}'
SET @StartTime = GETDATE()
SET @DWUpdatedDateTime = GETDATE()


--INSERT SFDC TO IRIS CLIENT MAPPINGS INTO TEMP TABLE 
    INSERT  
	  INTO #Iris_SFDC_ClientMapping_Temp (HouseholdUID, HouseholdId, ClientId_Iris, ClientNumber_Iris, AccountRecordType, SFDC_MigrationDate)

	SELECT A.Id AS HouseholdUID
         , A.Household_ID__c AS HouseholdId
		 , CB.ContactId AS ClientId_Iris
		 , CB.fi_Id AS ClientNumber_Iris
		 , RT.[Name] AS AccountRecordType
		 , CONVERT(DATE, A.CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS SFDC_MigrationDate
	  FROM PCGSF.Account AS A 
	  JOIN PCGSF.AccountContactRelation AS ACR
	    ON A.Id = ACR.AccountId 
	  JOIN PCGSF.RecordType AS RT
	    ON A.RecordTypeId = RT.Id 
	   AND RT.sObjectType = 'Account'
	   AND RT.IsActive = 1 
	  JOIN Iris.ContactBase AS CB 
	    ON TRIM(A.Legacy_ID__c) = CB.fi_Id
	 WHERE ACR.FinServ__Primary__c = 1 --PRIMARY ACCOUNT HOLDER 
	   AND ACR.IsDeleted = 0 --IGNORE DELTED RECORDS 
	   AND TRIM(A.Legacy_ID__c) IS NOT NULL	
   

;WITH IrisHistory AS ( 

  SELECT SFDC.HouseholdUID
       , SFDC.HouseholdId 
	   , CA.[fi_contactid] AS ClientId_Iris 
       , CB.fi_Id_Search AS ClientNumber_Iris 
	   , CA.fi_FirstName AS ClientFirstName	   
	   , CA.fi_LastName AS ClientLastName	   
	   , CA.fi_fullname AS ClientFullName
	   , SP.[fi_Name] AS ServiceProduct
	   , CType.[Value] AS ClientType
	   , CSType.[Value] AS ClientSubType
	   , CONVERT(DATETIME, CONVERT(DATE,CA.[fi_RelationshipClearanceDate])) AS ClientClearanceDate
	   , SCode.[Value] AS StrengthCode
	   , @UnknownTextValue AS ContactFrequency
	   , Gend.[Value] AS Gender 
	   , MSC.[Value] AS MaritalStatus
	   , CASE
			WHEN CA.[fi_IsRetired] = 1 THEN 'Retired' 
			WHEN CA.[fi_IsRetired] = 0 THEN 'Not Retired' 
			ELSE NULL 
		 END AS RetirementStatus
	   , @UnknownTextValue AS EmploymentStatus
	   , Ind.[value] AS Industry
	   , Occ.[Value] AS Occupation
	   , Cou.fi_Code AS ResidenceCountry
         --IRIS/SFDC MAPPING OUTLINED IN JIRA STORY PDDTI-39 
	   , CASE 
            WHEN CONT.vchValue  = 'Spain'
			THEN 'ES'
            WHEN CONT.vchValue  = 'United Kingdom'
			THEN 'GB'	
            WHEN CONT.vchEntityID IS NOT NULL 
            THEN CONT.vchValue
			ELSE 'US' --US CLIENTS DO NOT EXIST WITH THIS TABLE 		
         END AS ContractCountry
	     --IRIS/SFDC MAPPING OUTLINED IN JIRA STORY PDDTI-39 
       , CASE             
			WHEN CType.[Value] = 'Prospect' AND ENT.[vchValue] = 'FI' THEN 'FIE'
            WHEN CType.[Value] <> 'Prospect' AND ENT.[vchValue] = 'FI' THEN 'FAM'            
			WHEN ENT.[vchValue] = 'FI!!FIL' THEN 'FIL'
            WHEN ENT.[vchValue] = 'FI,FIL' THEN 'FIL'
            WHEN ENT.[vchValue] = 'FIA' THEN 'FIA'
            WHEN ENT.[vchValue] = 'FIE' THEN 'FIE'
            WHEN ENT.[vchValue] = 'FII' THEN 'FII'
            WHEN ENT.[vchValue] = 'FIL' THEN 'FIL'			
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect') AND CONT.vchValue = 'AU' THEN 'FIA'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect') AND CONT.vchValue = 'CA' THEN 'FAM'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] = 'Client - Trading' AND CONT.vchValue IN ('GB', 'United Kingdom') THEN 'FAM'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect', 'Client - Trading') AND CONT.vchValue IN ('GB', 'United Kingdom')  THEN 'FIE'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect') AND CONT.vchValue IN ('IE', 'IT', 'ES', 'Spain')  THEN 'FII'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect') AND CONT.vchValue IN ('BE', 'DK', 'FR', 'NL', 'NO', 'SE')  THEN 'FIL'
			WHEN ENT.[vchValue] IS NULL AND CType.[Value] NOT IN ('Client Contact', 'Former Prospect') AND CONT.vchValue IN ('US')  THEN 'FAM'
            ELSE @UnknownTextValue
         END AS ContractEntity
	   , Cur.ISOCurrencyCode AS CurrencyCode
	   , 'Iris' AS SystemOfRecord
	   , CONVERT(DATETIME, CA.CreatedOn AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedOn  --CONVERT DATES TO PST
	   , ROW_NUMBER() OVER (PARTITION BY CA.[fi_contactid], CONVERT(DATE, CA.CreatedOn) ORDER BY CA.fi_Id DESC, CA.CreatedOn DESC) AS DayRowNum
	   , ISNULL(SFDC.SFDC_MigrationDate, @MaxDateValue) AS SFDC_MigrationDate

	FROM Iris.fi_contactauditlogBase AS CA	
	
	JOIN Iris.ContactBase AS CB
	  ON CA.fi_ContactId = CB.ContactId 

	LEFT
	JOIN #Iris_SFDC_ClientMapping_Temp AS SFDC 
	  ON CB.fi_Id = SFDC.ClientNumber_Iris

	--INNER JOIN TO RM ENTITY TO ONLY INCLUDE CLIENTS HAVE HAVE BEEN WITIN RELATIONSHIP MANAGEMENT
	JOIN Iris.fi_relationshipmanagementBase AS RM
	  ON CB.ContactId = RM.fi_ContactId

    LEFT
    JOIN Iris.fi_serviceproductBase AS SP
      ON SP.fi_serviceproductId = CA.fi_serviceproductid

    LEFT
    JOIN Iris.StringMapBase SCode
      ON SCode.AttributeValue = CA.fi_strengthcode
     AND SCode.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND SCode.AttributeName = 'fi_strengthcode'

    LEFT
    JOIN Iris.StringMapBase CType
      ON CType.AttributeValue = CA.fi_customertypecode
     AND CType.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND CType.AttributeName = 'fi_customertypecode'

    LEFT
    JOIN Iris.StringMapBase CSType
      ON CSType.AttributeValue = CA.fi_CustomerSubTypeCode
     AND CSType.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND CSType.AttributeName = 'fi_CustomerSubTypeCode'	

    LEFT
    JOIN Iris.StringMapBase Gend
      ON Gend.AttributeValue = CA.fi_gendercode
     AND Gend.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND Gend.AttributeName = 'fi_gendercode'

    LEFT
    JOIN Iris.StringMapBase MSC
      ON MSC.AttributeValue = CA.fi_familystatuscode
     AND MSC.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND MSC.AttributeName = 'fi_familystatuscode'
   
    LEFT
    JOIN Iris.StringMapBase Ind
      ON Ind.AttributeValue = CA.fi_industrycode
     AND Ind.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND Ind.AttributeName = 'fi_industrycode'
  
    LEFT
    JOIN Iris.StringMapBase Occ
      ON Occ.AttributeValue = CA.fi_occupationcode
     AND Occ.ObjectTypeCode = 10079 --fi_contactauditlogBase
     AND Occ.AttributeName = 'fi_occupationcode'
   
    LEFT
    JOIN Iris.fi_countryBase Cou
      ON Cou.fi_countryId = CA.fi_residencecountryid

    LEFT
    JOIN Iris.businessunitBase CServ
      ON CServ.BusinessUnitId = CA.fi_servicingcountrybusinessunitid
   
    LEFT
    JOIN Iris.TransactionCurrencyBase Cur
      ON Cur.TransactionCurrencyId = CA.TransactionCurrencyId	  

    LEFT
    JOIN FICAD.CAD_ENTITY_ATTRIBUTES CONT 
      ON CB.fi_Id_Search = CONVERT(INT, CONT.vchEntityID)
     AND CONT.iAttrID = 27 --REPORTING COUNTRY     

    LEFT
    JOIN FICAD.CAD_ENTITY_ATTRIBUTES ENT
      ON CB.fi_Id_Search = CONVERT(INT, ENT.vchEntityID)
     AND ENT.iAttrID = 41 --SUBSIDIARY
     AND CONVERT(DATE, CA.CreatedOn) >= CONVERT(DATE, ENT.dtCreatedDatetime)

) 

, ClientTradingDateAuditLog AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , ClientType
	   , ClientSubType
	   , ClientClearanceDate
	   , CASE 
            WHEN ClientType = 'Client - Trading'
		     AND LAG (ClientType, 1, @UnknownTextValue) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) <> 'Client - Trading'
            THEN CONVERT(DATETIME, CONVERT(DATE, CreatedOn))
            ELSE NULL
         END AS ClientTradingDate
	   , StrengthCode
	   , ContactFrequency
	   , Gender 
	   , MaritalStatus
	   , RetirementStatus
	   , EmploymentStatus
	   , Industry
	   , Occupation
	   , ResidenceCountry
	   , ContractCountry
	   , ContractEntity
	   , CurrencyCode
	   , SystemOfRecord
	   , CreatedOn
	FROM IrisHistory  
   WHERE DayRowNum = 1 --LAST CHANGE IN DAY   
     AND CONVERT(DATE, CreatedOn) <= SFDC_MigrationDate --ONLY INCLUDE RECORDS BEFORE SFDC MIGRATION. IF CLIENT IS NOT IN SFDC 1/1/1900 IS USED

) 

, AttributeGroups AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , COUNT(ServiceProduct) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpServiceProduct
	   , ClientType
	   , COUNT(ClientType) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpClientType
	   , ClientSubType
	   , COUNT(ClientSubType) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpClientSubType
	   , ClientClearanceDate
	   , COUNT(ClientClearanceDate) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpClientClearanceDate
	   , ClientTradingDate
	   , COUNT(ClientTradingDate) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpClientTradingDate
	   , StrengthCode
	   , COUNT(StrengthCode) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpStrengthCode
	   , ContactFrequency
	   , Gender 
	   , COUNT(Gender) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpGender
	   , MaritalStatus
	   , COUNT(MaritalStatus) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpMaritalStatus
	   , RetirementStatus
	   , COUNT(RetirementStatus) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpRetirementStatus
	   , EmploymentStatus
	   , Industry
	   , COUNT(Industry) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpIndustry
	   , Occupation
	   , COUNT(Occupation) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpOccupation
	   , ResidenceCountry
	   , COUNT(ResidenceCountry) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpResidenceCountry
	   , ContractCountry
	   , COUNT(ContractCountry) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpContractCountry
	   , ContractEntity
       , COUNT(ContractEntity) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpContractEntity
	   , CurrencyCode
	   , COUNT(CurrencyCode) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS GrpCurrencyCode
	   , SystemOfRecord
	   , CreatedOn
	FROM ClientTradingDateAuditLog  

) 

, ClientHistory AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , FIRST_VALUE(ServiceProduct) OVER (PARTITION BY ClientId_Iris, GrpServiceProduct ORDER BY CreatedOn) AS ServiceProduct
	   , FIRST_VALUE(ClientType) OVER (PARTITION BY ClientId_Iris, GrpClientType ORDER BY CreatedOn) AS ClientType
	   , FIRST_VALUE(ClientSubType) OVER (PARTITION BY ClientId_Iris, GrpClientSubType ORDER BY CreatedOn) AS ClientSubType
	   , FIRST_VALUE(ClientClearanceDate) OVER (PARTITION BY ClientId_Iris, GrpClientClearanceDate ORDER BY CreatedOn) AS ClientClearanceDate
	   , FIRST_VALUE(ClientTradingDate) OVER (PARTITION BY ClientId_Iris, GrpClientTradingDate ORDER BY CreatedOn) AS ClientTradingDate
	   , FIRST_VALUE(StrengthCode) OVER (PARTITION BY ClientId_Iris, GrpStrengthCode ORDER BY CreatedOn) AS StrengthCode
	   , ContactFrequency --NEW SFDC FILED - NO IRIS MAPPING 
	   , FIRST_VALUE(Gender) OVER (PARTITION BY ClientId_Iris, GrpGender ORDER BY CreatedOn) AS Gender
	   , FIRST_VALUE(MaritalStatus) OVER (PARTITION BY ClientId_Iris, GrpMaritalStatus ORDER BY CreatedOn) AS MaritalStatus
	   , FIRST_VALUE(RetirementStatus) OVER (PARTITION BY ClientId_Iris, GrpRetirementStatus ORDER BY CreatedOn) AS RetirementStatus
	   , EmploymentStatus --NEW SFDC FILED - NO IRIS MAPPING 
	   , FIRST_VALUE(Industry) OVER (PARTITION BY ClientId_Iris, GrpIndustry ORDER BY CreatedOn) AS Industry
	   , FIRST_VALUE(Occupation) OVER (PARTITION BY ClientId_Iris, GrpOccupation ORDER BY CreatedOn) AS Occupation
	   , FIRST_VALUE(ResidenceCountry) OVER (PARTITION BY ClientId_Iris, GrpResidenceCountry ORDER BY CreatedOn) AS ResidenceCountry
	   , FIRST_VALUE(ContractCountry) OVER (PARTITION BY ClientId_Iris, GrpContractCountry ORDER BY CreatedOn) AS ContractCountry
	   , FIRST_VALUE(ContractEntity) OVER (PARTITION BY ClientId_Iris, GrpContractEntity ORDER BY CreatedOn) AS ContractEntity
	   , FIRST_VALUE(CurrencyCode) OVER (PARTITION BY ClientId_Iris, GrpCurrencyCode ORDER BY CreatedOn) AS CurrencyCode
	   , SystemOfRecord
	   , CreatedOn
    FROM AttributeGroups

)

, ChangeTracking AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , ClientType
	   , ClientSubType
	   , ClientClearanceDate
	   , ClientTradingDate
	   , StrengthCode
	   , ContactFrequency
	   , Gender
	   , MaritalStatus
	   , RetirementStatus
	   , EmploymentStatus
	   , Industry
	   , Occupation
	   , ResidenceCountry
	   , ContractCountry
	   , ContractEntity
	   , CurrencyCode
	   , SystemOfRecord
	   , CreatedOn
       , HASHBYTES('SHA2_256', CONCAT(ClientFirstName, '|', ClientLastName, '|', ClientFullName, '|'
			, ServiceProduct, '|', ClientType, '|', ClientSubType, '|'
            , ClientClearanceDate, '|', ClientTradingDate, '|', StrengthCode, '|', ContactFrequency, '|', Gender, '|'
            , MaritalStatus, '|', RetirementStatus, '|', Industry, '|'
            , Occupation, '|', ResidenceCountry, '|', ContractEntity, '|', CurrencyCode)) AS RowHash 
  
    FROM ClientHistory 

)

, PriorRowHash AS (

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , ClientType
	   , ClientSubType
	   , ClientClearanceDate
	   , ClientTradingDate
	   , StrengthCode
	   , ContactFrequency
	   , Gender
	   , MaritalStatus
	   , RetirementStatus
	   , EmploymentStatus
	   , Industry
	   , Occupation
	   , ResidenceCountry
	   , ContractCountry
	   , ContractEntity
	   , CurrencyCode
	   , SystemOfRecord
	   , CreatedOn
       , RowHash 
	   , LAG (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS PriorRowHash
    FROM ChangeTracking

)

, DistinctChanges AS ( 

  SELECT HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ISNULL(ServiceProduct, @UnknownTextValue) AS ServiceProduct
	   , ISNULL(ClientType, @UnknownTextValue) AS ClientType
	   , ISNULL(ClientSubType, @UnknownTextValue) AS ClientSubType
	   , ClientClearanceDate
	   , ClientTradingDate
	   , ISNULL(StrengthCode, @UnknownTextValue) AS StrengthCode
       , LAG (ISNULL(StrengthCode, @UnknownTextValue), 1, @UnknownTextValue) OVER (PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS PreviousStrengthCode
	   , ISNULL(ContactFrequency, @UnknownTextValue) AS ContactFrequency
	   , ISNULL(Gender, @UnknownTextValue) AS Gender
	   , ISNULL(MaritalStatus, @UnknownTextValue) AS MaritalStatus
	   , ISNULL(CONVERT(NVARCHAR(100), RetirementStatus), @UnknownTextValue) AS RetirementStatus
	   , ISNULL(EmploymentStatus, @UnknownTextValue) AS EmploymentStatus
	   , ISNULL(Industry, @UnknownTextValue) AS Industry
	   , ISNULL(Occupation, @UnknownTextValue) AS Occupation
	   , ISNULL(CONVERT(NVARCHAR(100), ResidenceCountry), @UnknownTextValue) AS ResidenceCountry
	   , ISNULL(CONVERT(NVARCHAR(100), ContractCountry), @UnknownTextValue) AS ContractCountry
	   , ISNULL(ContractEntity, @UnknownTextValue) AS ContractEntity
	   , ISNULL(CONVERT(NVARCHAR(100), CurrencyCode), @UnknownTextValue) AS CurrencyCode
	   , SystemOfRecord
       , RowHash 
	   , CreatedOn AS EffectiveStartDate
       , LEAD(CreatedOn, 1, @MaxDateValue) OVER(PARTITION BY ClientId_Iris ORDER BY CreatedOn) AS EffectiveEndDate
       , CASE WHEN LEAD(CreatedOn, 1, @MaxDateValue) OVER(PARTITION BY ClientId_Iris ORDER BY CreatedOn) = @MaxDateValue THEN 1 ELSE 0 END AS CurrentRecord 
    FROM PriorRowHash AS PRH
   WHERE RowHash <> PriorRowHash

)

  INSERT
    INTO #DimClientHousehold (
         HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , ClientType
	   , ClientSubType
	   , ClientClearanceDate
	   , ClientTradingDate
	   , StrengthCode
       , DimStrengthCodeKey
	   , ContactFrequency
	   , Gender
	   , MaritalStatus
	   , RetirementStatus
	   , EmploymentStatus
	   , Industry
	   , Occupation
	   , ResidenceCountry
	   , ContractCountry
	   , ContractEntity
	   , CurrencyCode
	   , SystemOfRecord
       , RowHash 
	   , EffectiveStartDate
       , EffectiveEndDate
       , CurrentRecord 
       , DWCreatedDateTime
       , DWUpdatedDateTime
       , ETLJobProcessRunId
       , ETLJobSystemRunId

  )


  SELECT C.HouseholdUID
       , C.HouseholdId
	   , C.ClientId_Iris 
	   , C.ClientNumber_Iris
	   , C.ClientFirstName
	   , C.ClientLastName
	   , C.ClientFullName
	   , C.ServiceProduct
	   , C.ClientType
	   , C.ClientSubType
	   , C.ClientClearanceDate
	   , C.ClientTradingDate
	   , C.StrengthCode
       , DSC.DimStrengthCodeKey
	   , C.ContactFrequency
	   , C.Gender
	   , C.MaritalStatus
	   , C.RetirementStatus
	   , C.EmploymentStatus
	   , C.Industry
	   , C.Occupation
	   , C.ResidenceCountry
	   , C.ContractCountry
	   , C.ContractEntity
	   , C.CurrencyCode
	   , C.SystemOfRecord
       --DOING A NEW ROW HASH HERE SINCE WE REPLACED NULLS WITH UNKNOWN VALUES - WE'LL DO THE SAME THING IN THE UPSERT SO WE WANT TO MAKE SURE THE HASH IS BEING APPLIED THE EXACT SAME WAY
       , HASHBYTES('SHA2_256', CONCAT(C.ClientFirstName, '|', C.ClientLastName, '|', C.ClientFullName, '|'
			, C.ServiceProduct, '|', C.ClientType, '|', C.ClientSubType, '|'
            , C.ClientClearanceDate, '|', C.ClientTradingDate, '|', C.StrengthCode, '|', C.ContactFrequency, '|', C.Gender, '|'
            , C.MaritalStatus, '|', C.RetirementStatus, '|', C.Industry, '|'
            , C.Occupation, '|', C.ResidenceCountry, '|', C.ContractEntity, '|', C.CurrencyCode)) AS RowHash 
	   , C.EffectiveStartDate
       , C.EffectiveEndDate
       , C.CurrentRecord 
       , @DWUpdatedDateTime AS DWCreatedDateTime
       , @DWUpdatedDateTime AS DWUpdatedDateTime
       , @ETLJobProcessRunId AS ETLJobProcessRunId
       , @ETLJobSystemRunId AS ETLJobSystemRunId

    FROM DistinctChanges AS C

    LEFT 
    JOIN FDW.DimStrengthCode AS DSC
      ON C.PreviousStrengthCode = DSC.PreviousStrengthCodeName
     AND C.StrengthCode = DSC.StrengthCodeName    
    
	LEFT
    JOIN #DimClientHousehold DCH
      ON DCH.ClientId_Iris = C.ClientId_Iris
     AND DCH.EffectiveStartDate = C.EffectiveStartDate
   
   WHERE DCH.ClientId_Iris IS NULL

 -- OPTION (Label = '#DimClientHouseholdBackfill-Insert')
 --   EXEC MDR.spGetRowCountByQueryLabel '#DimClientHouseholdBackfill-Insert', @InsertCount OUT

	--SET @EndTime = GETDATE()
	--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

 --  EXEC MDR.spProcessTaskLogInsertRowCount
	--	@ETLJobProcessRunId 
	--  , @ComponentName
	--  , @Source 
 -- 	  , @Target 
	--  , @InsertCount	 
	--  , @DurationInSeconds



/*
	START SFDC BACKFILL
*/

;WITH DimClientActiveRecord AS ( 

	SELECT HouseholdUID
	     , HouseholdId
		 , ClientId_Iris
		 , ClientNumber_Iris
		 , ClientType
		 , ClientTradingDate
		 , StrengthCode
		 , RowHash
		 , EffectiveStartDate
		 , EffectiveEndDate
	  FROM #DimClientHousehold
	 WHERE CurrentRecord = 1 

)

, ClientRecords_SFDC AS (

    SELECT A.Id AS HouseholdUID
	     , A.Household_ID__c AS HouseholdId 
		 , CM.ClientId_Iris
		 , CM.ClientNumber_Iris
	     , C.FirstName AS ClientFirstName 
	     , C.LastName AS ClientLastName 
	     , C.[Name] AS ClientFullName 
	     , ISNULL(A.Service_fromIRIS__c, @UnknownTextValue) AS ServiceProduct
	     , ISNULL(A.Client_Type_HH__c, @UnknownTextValue) AS ClientType 
	     , ISNULL(A.Household_Sub_Type__c, @UnknownTextValue) AS ClientSubType
	     , CONVERT(DATETIME, CONVERT(DATE, A.Household_Clearance_Date__c)) AS ClientClearanceDate
	     , ISNULL(A.Strength_CodeHH__c, @UnknownTextValue) AS StrengthCode   
	     , ISNULL(A.Preferred_Contact_Frequency__c, @UnknownTextValue) AS ContactFrequency
	     , ISNULL(C.FinServ__Gender__c, @UnknownTextValue) AS Gender
	     , ISNULL(C.FinServ__MaritalStatus__c, @UnknownTextValue) AS MaritalStatus
	     , CASE		
			WHEN C.Employment_Status__c = 'Retired' THEN 'Retired' 
			WHEN C.Employment_Status__c IS NOT NULL THEN 'Not Retired'
			ELSE @UnknownTextValue
		   END AS RetirementStatus
	     , ISNULL(C.Employment_Status__c, @UnknownTextValue) AS EmploymentStatus
	     , ISNULL(A.Industry, @UnknownTextValue) AS Industry
	     , ISNULL(C.FinServ__Occupation__c, @UnknownTextValue) AS Occupation 
	     , ISNULL(C.Residence_Country_1__c, @UnknownTextValue) AS ResidenceCountry
	     , ISNULL(A.Contract_Country__c, @UnknownTextValue) AS ContractCountry
	     , ISNULL(A.Contract_Entity__c, @UnknownTextValue) AS ContractEntity
	   	 , ISNULL(A.CurrencyIsoCode, @UnknownTextValue) AS CurrencyCode 
	     , 'SFDC' AS SystemOfRecord 
		 , CONVERT(DATETIME, A.CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') AS CreatedDate  --CONVERT DATES TO PST

      FROM PCGSF.Account AS A

	  JOIN PCGSF.AccountContactRelation AS ACR 
	    ON A.Id = ACR.AccountId

	  JOIN PCGSF.Contact AS C 
	    ON C.Id = ACR.ContactId
      
	  LEFT
	  JOIN #Iris_SFDC_ClientMapping_Temp AS CM
	    ON CM.HouseholdUID = A.Id 

     WHERE ACR.FinServ__Primary__c = 1 --PRIMARY MEMBER OF HOUSEHOLD GROUP   
	   AND ACR.IsDeleted = 0 --ACTIVE RECORD 
	   AND A.IsDeleted = 0 --ACTIVE RECORD 
	   AND C.IsDeleted = 0 --ACTIVE RECORD 
		  			
)	

, ClientTradingEvaluation AS ( 

    SELECT SFDC.HouseholdUID
         , SFDC.HouseholdId
	     , SFDC.ClientId_Iris 
	     , SFDC.ClientNumber_Iris
	     , SFDC.ClientFirstName
	     , SFDC.ClientLastName
	     , SFDC.ClientFullName
	     , SFDC.ServiceProduct
	     , SFDC.ClientType
	     , SFDC.ClientSubType
	     , SFDC.ClientClearanceDate
	     , CASE 
              WHEN SFDC.ClientType = 'Client - Trading'
		       AND ISNULL(DCA.ClientType, @UnknownTextValue) <> 'Client - Trading'
              THEN CONVERT(DATETIME, CONVERT(DATE, SFDC.ClientClearanceDate))
              ELSE DCA.ClientTradingDate
           END AS ClientTradingDate
	     , SFDC.StrengthCode
         , ISNULL(DCA.StrengthCode, @UnknownTextValue) AS PreviousStrengthCode
	     , SFDC.ContactFrequency
	     , SFDC.Gender
	     , SFDC.MaritalStatus
	     , SFDC.RetirementStatus
	     , SFDC.EmploymentStatus
	     , SFDC.Industry
	     , SFDC.Occupation
	     , SFDC.ResidenceCountry
	     , SFDC.ContractCountry
	     , SFDC.ContractEntity
	     , SFDC.CurrencyCode
	     , SFDC.SystemOfRecord
		 , SFDC.CreatedDate
		 , DCA.RowHash AS DimClientRowHash
	  FROM ClientRecords_SFDC AS SFDC
	  LEFT
	  JOIN DimClientActiveRecord AS DCA
	    ON SFDC.HouseholdUID = DCA.HouseholdUID
	  
) 

, ChangeTracking AS ( 

    SELECT HouseholdUID
         , HouseholdId
	     , ClientId_Iris 
	     , ClientNumber_Iris
	     , ClientFirstName
	     , ClientLastName
	     , ClientFullName
	     , ServiceProduct
	     , ClientType
	     , ClientSubType
	     , ClientClearanceDate
	     , ClientTradingDate
	     , StrengthCode
		 , PreviousStrengthCode
	     , ContactFrequency
	     , Gender
	     , MaritalStatus
	     , RetirementStatus
	     , EmploymentStatus
	     , Industry
	     , Occupation
	     , ResidenceCountry
	     , ContractCountry
	     , ContractEntity
	     , CurrencyCode
	     , SystemOfRecord
	     , CreatedDate
         , HASHBYTES('SHA2_256', CONCAT(ClientFirstName, '|', ClientLastName, '|', ClientFullName, '|'
			  , ServiceProduct, '|', ClientType, '|', ClientSubType, '|'
              , ClientClearanceDate, '|', ClientTradingDate, '|', StrengthCode, '|', ContactFrequency, '|', Gender, '|'
              , MaritalStatus, '|', RetirementStatus, '|', Industry, '|'
              , Occupation, '|', ResidenceCountry, '|', ContractEntity, '|', CurrencyCode)) AS RowHash 
	     , DimClientRowHash 
		 
      FROM ClientTradingEvaluation 

)

, IdentifyRecordTypes AS (

    SELECT HouseholdUID
         , HouseholdId
	     , ClientId_Iris 
	     , ClientNumber_Iris
	     , ClientFirstName
	     , ClientLastName
	     , ClientFullName
	     , ServiceProduct
	     , ClientType
	     , ClientSubType
	     , ClientClearanceDate
	     , ClientTradingDate
	     , StrengthCode
		 , PreviousStrengthCode
	     , ContactFrequency
	     , Gender
	     , MaritalStatus
	     , RetirementStatus
	     , EmploymentStatus
	     , Industry
	     , Occupation
	     , ResidenceCountry
	     , ContractCountry
	     , ContractEntity
	     , CurrencyCode
	     , SystemOfRecord
	     , CreatedDate
         , RowHash 
	     , DimClientRowHash 
		 , CASE 
			WHEN DimClientRowHash IS NULL THEN 'New'
		    WHEN DimClientRowHash IS NOT NULL AND DimClientRowHash <> RowHash THEN 'Modified'
			WHEN DimClientRowHash = RowHash THEN 'Unchanged'
		   END AS RecordType
	  FROM ChangeTracking

)

	INSERT
	  INTO #DimClientHousehold_Temp ( 
           HouseholdUID
         , HouseholdId
	     , ClientId_Iris 
	     , ClientNumber_Iris
	     , ClientFirstName
	     , ClientLastName
	     , ClientFullName
	     , ServiceProduct
	     , ClientType
	     , ClientSubType
	     , ClientClearanceDate
	     , ClientTradingDate
	     , StrengthCode
         , PreviousStrengthCode
	     , ContactFrequency
	     , Gender
	     , MaritalStatus
	     , RetirementStatus
	     , EmploymentStatus
	     , Industry
	     , Occupation
	     , ResidenceCountry
	     , ContractCountry
	     , ContractEntity
	     , CurrencyCode
	     , SystemOfRecord
         , RowHash 
		 , RecordType
		 , CreatedDate
	)

    SELECT HouseholdUID
         , HouseholdId
	     , ClientId_Iris 
	     , ClientNumber_Iris
	     , ClientFirstName
	     , ClientLastName
	     , ClientFullName
	     , ServiceProduct
	     , ClientType
	     , ClientSubType
	     , ClientClearanceDate
	     , ClientTradingDate
	     , StrengthCode
         , PreviousStrengthCode
	     , ContactFrequency
	     , Gender
	     , MaritalStatus
	     , RetirementStatus
	     , EmploymentStatus
	     , Industry
	     , Occupation
	     , ResidenceCountry
	     , ContractCountry
	     , ContractEntity
	     , CurrencyCode
	     , SystemOfRecord
         , RowHash 
		 , RecordType
		 , CreatedDate
	  FROM IdentifyRecordTypes 
	 WHERE RecordType IN ('New', 'Modified') --IGNORE RECORDS THAT ARE NOT NEW OR WERE NOT MODIFIED

--SET @Source = '{"SourceTable":"PCGSF.Account"}'
--SET @Target = '{"TargetTable":"#DimClientHousehold"}'
--SET @StartTime = GETDATE()

UPDATE #DimClientHousehold
   SET [CurrentRecord] = 0 --Deactive existing record 
     , [ContractCountry] = SRC.ContractCountry --TYPE ONE UPDATE 
     , [EffectiveEndDate] = SRC.CreatedDate --Set EndDate to new incoming start date
     , [DWUpdatedDateTime] = @DWUpdatedDateTime
     , [ETLJobProcessRunId] = @ETLJobProcessRunId
     , [ETLJobSystemRunId] = @ETLJobSystemRunId           
  FROM #DimClientHousehold AS TGT
  JOIN #DimClientHousehold_Temp AS SRC
    ON TGT.HouseholdUID = SRC.HouseholdUID
 WHERE TGT.CurrentRecord = 1 
   AND SRC.RecordType = 'Modified'
   
--OPTION (Label = '#DimClientHousehold-SFDCBackfillUpdate-Query')
--  EXEC MDR.spGetRowCountByQueryLabel '#DimClientHousehold-SFDCBackfillUpdate-Query', @UpdateCount OUT

--EXEC MDR.spProcessTaskLogUpdateRowCount
--     @ETLJobProcessRunId 
--   , @ComponentName
--   , @Source 
--   , @Target 
--   , @UpdateCount         
--   , @DurationInSeconds


/*
	SCD-II INSERT: INSERTS NEW RECORDS IDENTIFIED FOR EACH CLIENT IN INCOMNIG SOURCE DATA
*/

--SET @Source = '{"SourceTable":"PCGSF.Account"}'
--SET @Target = '{"TargetTable":"#DimClientHousehold"}'
--SET @StartTime = GETDATE()  

  INSERT
    INTO #DimClientHousehold (
         HouseholdUID
       , HouseholdId
	   , ClientId_Iris 
	   , ClientNumber_Iris
	   , ClientFirstName
	   , ClientLastName
	   , ClientFullName
	   , ServiceProduct
	   , ClientType
	   , ClientSubType
	   , ClientClearanceDate
	   , ClientTradingDate
	   , StrengthCode
       , DimStrengthCodeKey
	   , ContactFrequency
	   , Gender
	   , MaritalStatus
	   , RetirementStatus
	   , EmploymentStatus
	   , Industry
	   , Occupation
	   , ResidenceCountry
	   , ContractCountry
	   , ContractEntity
	   , CurrencyCode
	   , SystemOfRecord
       , RowHash 
	   , EffectiveStartDate
       , EffectiveEndDate
       , CurrentRecord 
       , DWCreatedDateTime
       , DWUpdatedDateTime
       , ETLJobProcessRunId
       , ETLJobSystemRunId
  )

  SELECT SRC.HouseholdUID
       , SRC.HouseholdId
	   , SRC.ClientId_Iris 
	   , SRC.ClientNumber_Iris
	   , SRC.ClientFirstName
	   , SRC.ClientLastName
	   , SRC.ClientFullName
	   , SRC.ServiceProduct
	   , SRC.ClientType
	   , SRC.ClientSubType
	   , SRC.ClientClearanceDate
	   , SRC.ClientTradingDate
	   , SRC.StrengthCode
       , DSC.DimStrengthCodeKey
	   , SRC.ContactFrequency
	   , SRC.Gender
	   , SRC.MaritalStatus
	   , SRC.RetirementStatus
	   , SRC.EmploymentStatus
	   , SRC.Industry
	   , SRC.Occupation
	   , SRC.ResidenceCountry
	   , SRC.ContractCountry
	   , SRC.ContractEntity
	   , SRC.CurrencyCode
	   , SRC.SystemOfRecord
       , SRC.RowHash 
	   , SRC.CreatedDate AS EffectiveStartDate
       , @MaxDateValue AS EffectiveEndDate
       , 1 AS CurrentRecord
       , @DWUpdatedDateTime
       , @DWUpdatedDateTime
       , @ETLJobProcessRunId
	   , @ETLJobSystemRunId

    FROM #DimClientHousehold_Temp AS SRC

    LEFT
    JOIN #DimClientHousehold AS TGT
      ON SRC.HouseholdUID = TGT.HouseholdUID
	 AND TGT.CurrentRecord = 1 
	 AND SRC.EffectiveStartDate = TGT.EffectiveStartDate
	 AND SRC.RowHash = TGT.RowHash 

    LEFT 
    JOIN FDW.DimStrengthCode AS DSC
      ON SRC.PreviousStrengthCode = DSC.PreviousStrengthCodeName
     AND SRC.StrengthCode = DSC.StrengthCodeName 
  
   WHERE SRC.SystemOfRecord = 'SFDC'
     AND SRC.RecordType IN ('New', 'Modified') --INSERT ANY NEW OR MODIFIED RECORDS
     AND TGT.HouseholdUID IS NULL

 -- OPTION (Label = '#DimClientHousehold-SFDCBackfillInsert-Query')
 --   EXEC MDR.spGetRowCountByQueryLabel '#DimClientHousehold-SFDCBackfillInsert-Query', @InsertCount OUT

	-- SET @EndTime = GETDATE()
	-- SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--EXEC MDR.spProcessTaskLogInsertRowCount
	--	 @ETLJobProcessRunId 
	--   , @ComponentName
	--   , @Source 
	--   , @Target 
	--   , @InsertCount         
	--   , @DurationInSeconds



--SET @EndTime = GETDATE()
--SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY

--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#DimClientHouseholdBackfill', ': ', ERROR_MESSAGE())

--END CATCH 

--IF OBJECT_ID('tempdb..#Iris_SFDC_ClientMapping_Temp', 'U') IS NOT NULL DROP TABLE #Iris_SFDC_ClientMapping_Temp 
--IF OBJECT_ID('tempdb..#DimClientHousehold_Temp', 'U') IS NOT NULL DROP TABLE #DimClientHousehold_Temp

--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END
--GO


