
--run proc numerous times to ensure we're not inserting dupes
	--Run1: 10796856
	--Run2: 10796856
	--Run3: 10796856
SELECT COUNT(1)
  FROM #DimContact


--Check for Iris and SFDC recs
select SystemOfRecord
    , count(1)
from #DimContact
group by SystemOfRecord

--Check for dupe HH	
select HouseholdUID
    , count(1)
from #DimContact
where HouseholdUID <> '[Unknown]'
group by HouseholdUID
having count(1) > 1

--check for dupe client numbers	
select ContactNumber
    , count(1)
from #DimContact
where ContactNumber <> -1
group by ContactNumber
having count(1) > 1


--CREATE TABLE #DimContact (
--    [DimContactKey] INT IDENTITY (1,1), 
--	[HouseholdUID] NVARCHAR(4000) NULL,
--	[HouseholdId] NVARCHAR(4000) NULL,
--    [ContactId] UNIQUEIDENTIFIER NULL,
--	[ContactNumber] INT NULL,	
--	[ContactFirstName] NVARCHAR(100) NULL,
--	[ContactLastName] NVARCHAR(100) NULL,
--	[ContactFullName] NVARCHAR(200) NULL,
--	[ServiceProduct] NVARCHAR(255) NULL,
--	[ContactType] NVARCHAR(100) NULL,		
--	[ContactSubType] NVARCHAR(100) NULL,	
--	[ClearanceDate]	DATETIME NULL, 
--	[StrengthCode] NVARCHAR(100) NULL,
--	[ContactFrequency] NVARCHAR(255) NULL,	
--	[Gender] NVARCHAR(255) NULL,
--	[MaritalStatus]	NVARCHAR(255) NULL,
--	[RetirementStatus] NVARCHAR(100) NULL,
--	[EmploymentStatus] NVARCHAR(255) NULL,
--	[Industry] NVARCHAR(255) NULL,
--	[Occupation] NVARCHAR(200) NULL,	
--	[ResidenceCountry] NVARCHAR(100) NULL,
--	[ContractCountry] NVARCHAR(100) NULL,
--	[ContractEntity] NVARCHAR(100) NULL,
--	[CurrencyCode] NVARCHAR(10) NULL,
--	[IsDeleted] BIT NULL,
--	[RowHash] VARBINARY(8000) NULL, 
--	[SystemOfRecord] NVARCHAR(25) NULL,
--	[DWCreatedDateTime] DATETIME NULL,
--	[DWUpdatedDateTime] DATETIME NULL,
--	[ETLJobProcessRunId] UNIQUEIDENTIFIER NULL,
--	[ETLJobSystemRunId] UNIQUEIDENTIFIER NULL
--)
--WITH (DISTRIBUTION = HASH(ContactNumber), CLUSTERED COLUMNSTORE INDEX) 
--GO


--alter PROC [FDW].[spUpsertDimContact] @ETLJobSystemRunId [UNIQUEIDENTIFIER],@ETLJobProcessRunId [UNIQUEIDENTIFIER],@ComponentName [NVARCHAR](255) AS
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
      , @DataSourceGroupName NVARCHAR(100)
      , @DataSourceMemberName NVARCHAR(100)
      , @NextDataProcessStageName NVARCHAR(50) 
      , @BaseTableName NVARCHAR(100) 
       
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



/* 
	CHECK IF UNKNOWN MEMBER VALUES EXISTS, IF NOT, ADD IT TO THE DIMENSION 	
*/

--EXEC [FDW].[spInsertUnknownDimensionRow] '#DimContact', @ETLJobSystemRunId, @ETLJobProcessRunId, @ComponentName 



IF OBJECT_ID ('TEMPDB..#DimContact_Temp') IS NOT NULL DROP TABLE #DimContact_Temp
CREATE TABLE #DimContact_Temp (           
    [HouseholdUID] NVARCHAR(4000) NULL,
	[HouseholdId] NVARCHAR(4000) NULL,
	--[Legacy_ID__c] INT NULL,
    [ContactId] UNIQUEIDENTIFIER NULL,
	[ContactNumber] INT NULL,	
	[ContactFirstName] NVARCHAR(100) NULL,
	[ContactLastName] NVARCHAR(100) NULL,
	[ContactFullName] NVARCHAR(200) NULL,
	[ServiceProduct] NVARCHAR(255) NULL,
	[ContactType] NVARCHAR(100) NULL,		
	[ContactSubType] NVARCHAR(100) NULL,	
	[ClearanceDate]	DATETIME NULL, 
	[StrengthCode] NVARCHAR(100) NULL,
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
	[IsDeleted] BIT NULL,
	[RowHash] VARBINARY(8000) NULL, 
	[SystemOfRecord] NVARCHAR(25) NULL, 
	[RecordType] NVARCHAR(25) NULL
)
WITH (DISTRIBUTION = HASH(ContactNumber), CLUSTERED COLUMNSTORE INDEX) 




/*
	START: BACKFILL DIM CONTACT WITH IRIS HISTORY IF NO RECORDS EXIST
*/

IF NOT EXISTS (SELECT TOP 1 * FROM #DimContact WHERE DimContactKey <> @UnknownNumberValue) 

BEGIN 

;WITH IrisContacts AS ( 
	  
	  SELECT ISNULL(SFDC.HouseholdUID, @UnknownTextValue) HouseholdUID
		   , ISNULL(SFDC.HouseholdId, @UnknownTextValue) HouseholdId
		   , CB.ContactId AS ContactId
		   , CB.fi_Id_Search AS ContactNumber
		   , CB.FirstName AS ContactFirstName	   
		   , CB.LastName AS ContactLastName	   
		   , CB.fullname AS ContactFullName
		   -- SFDC mapping logic outlined in PDDTI-1072
		   , CASE 
		     WHEN ISNULL(CServ.fi_ServicingCountryCode,@unknowntextvalue) = 'AV' 
		     THEN 'AV' 
		     ELSE ISNULL(SP.[fi_Name],@unknowntextvalue) 
		     END AS ServiceProduct
		   , ISNULL(CType.[Value],@unknowntextvalue) AS ContactType
		   , ISNULL(CSType.[Value],@unknowntextvalue) AS ContactSubType
		   , CONVERT(DATETIME, CONVERT(DATE,CB.[fi_RelationshipClearanceDate])) AS ClearanceDate
		   , ISNULL(SCode.[Value],@unknowntextvalue) AS StrengthCode
		   , @UnknownTextValue AS ContactFrequency
		   , ISNULL(Gend.[Value],@unknowntextvalue) AS Gender 
		   , ISNULL(MSC.[Value],@unknowntextvalue) AS MaritalStatus
		   , CASE
			   WHEN CB.[fi_IsRetired] = 1 THEN 'Retired' 
			   WHEN CB.[fi_IsRetired] = 0 THEN 'Not Retired' 
			   ELSE @unknowntextvalue
		     END AS RetirementStatus
		   , @UnknownTextValue AS EmploymentStatus
		   , ISNULL(Ind.[value],@unknowntextvalue) AS Industry
		   , ISNULL(Occ.[Value],@unknowntextvalue) AS Occupation
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
		   , ISNULL(Cur.ISOCurrencyCode,@unknowntextvalue) AS CurrencyCode
		   , CASE 
				WHEN CB.statecode = 0 THEN 0 --CB.statecode = 0 IS ACTIVE
				WHEN CB.statecode = 1 THEN 1 --CB.statecode = 1 IS INACTIVE 
				ELSE NULL
			 END AS IsDeleted
		   , 'Iris' AS SystemOfRecord

		FROM Iris.ContactBase AS CB
   
		LEFT 
		JOIN REF.CRMClientMapping AS SFDC 
		  ON CB.fi_Id_search = SFDC.ClientNumber_IRIS

		LEFT 
		JOIN Iris.fi_serviceproductBase AS SP 
		  ON SP.fi_serviceproductId = CB.fi_serviceproductid

		LEFT 
		JOIN Iris.StringMapBase AS SCode  
		  ON SCode.AttributeValue = CB.fi_strengthcode
		 AND SCode.ObjectTypeCode = 2 --CONTACTBASE
		 AND SCode.AttributeName = 'fi_strengthcode'

		LEFT 
		JOIN Iris.StringMapBase AS CType  
		  ON CType.AttributeValue = CB.fi_customertypecode
		 AND CType.ObjectTypeCode = 2 --CONTACTBASE
		 AND CType.AttributeName = 'fi_customertypecode'

		LEFT
		JOIN Iris.StringMapBase AS CSType
		  ON CSType.AttributeValue = CB.fi_CustomerSubTypeCode
		 AND CSType.ObjectTypeCode = 2 --CONTACTBASE
		 AND CSType.AttributeName = 'fi_CustomerSubTypeCode'	

		LEFT
		JOIN Iris.StringMapBase AS Gend
		  ON Gend.AttributeValue = CB.gendercode
		 AND Gend.ObjectTypeCode = 2 --CONTACTBASE
		 AND Gend.AttributeName = 'gendercode'

		LEFT
		JOIN Iris.StringMapBase AS MSC
		  ON MSC.AttributeValue = CB.familystatuscode
		 AND MSC.ObjectTypeCode = 2 --fi_contactauditlogBase
		 AND MSC.AttributeName = 'familystatuscode'
   
		LEFT
		JOIN Iris.StringMapBase AS Ind
		  ON Ind.AttributeValue = CB.fi_industrycode
		 AND Ind.ObjectTypeCode = 2 --CONTACTBASE
		 AND Ind.AttributeName = 'fi_industrycode'
  
		LEFT
		JOIN Iris.StringMapBase AS Occ
		  ON Occ.AttributeValue = CB.fi_occupationcode
		 AND Occ.ObjectTypeCode = 2 --CONTACTBASE
		 AND Occ.AttributeName = 'fi_occupationcode'
   
		LEFT
		JOIN Iris.fi_countryBase AS Cou
		  ON Cou.fi_countryId = CB.fi_residencecountryid

		LEFT
		JOIN Iris.businessunitBase AS CServ
		  ON CServ.BusinessUnitId = CB.fi_servicingcountrybusinessunitid
   
		LEFT
		JOIN Iris.TransactionCurrencyBase AS Cur
		  ON Cur.TransactionCurrencyId = CB.TransactionCurrencyId	  

		LEFT
		JOIN FICAD.CAD_ENTITY_ATTRIBUTES CONT 
		  ON CB.fi_Id_Search = CONVERT(INT, CONT.vchEntityID)
		 AND CONT.iAttrID = 27 --REPORTING COUNTRY     

		LEFT
		JOIN FICAD.CAD_ENTITY_ATTRIBUTES ENT
		  ON CB.fi_Id_Search = CONVERT(INT, ENT.vchEntityID)
		 AND ENT.iAttrID = 41 --SUBSIDIARY

)

    INSERT 
      INTO #DimContact (      
 	       HouseholdUID
         , HouseholdId
	     , ContactId 
	     , ContactNumber
	     , ContactFirstName
	     , ContactLastName
	     , ContactFullName
	     , ServiceProduct
	     , ContactType
	     , ContactSubType
		 , ClearanceDate
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
		 , IsDeleted
		 , RowHash
		 , SystemOfRecord
         , DWCreatedDateTime
	     , DWUpdatedDateTime
	     --, ETLJobProcessRunId
	     --, ETLJobSystemRunId
	)


 	SELECT HouseholdUID
         , HouseholdId
	     , ContactId 
	     , ContactNumber
	     , ContactFirstName
	     , ContactLastName
	     , ContactFullName
	     , ServiceProduct
	     , ContactType
	     , ContactSubType   
		 , ClearanceDate
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
		 , IsDeleted
		 , HASHBYTES('SHA2_256', CONCAT(HouseholdUID
									  , '|', HouseholdId
									  , '|', ContactId
									  , '|', ContactFirstName
								      , '|', ContactLastName
								      , '|', ContactFullName
								      , '|', ServiceProduct
								      , '|', ContactType
								      , '|', ContactSubType
								      , '|', ClearanceDate
								      , '|', StrengthCode
								      , '|', ContactFrequency
								      , '|', Gender
								      , '|', MaritalStatus
								      , '|', RetirementStatus
								      , '|', EmploymentStatus
								      , '|', Industry
								      , '|', Occupation
								      , '|', ResidenceCountry
								      , '|', ContractCountry
								      , '|', ContractEntity
								      , '|', CurrencyCode
								      , '|', IsDeleted)) AS RowHash
		 , 'Iris' AS SystemOfRecord 
		 , @DWUpdatedDateTime  
		 , @DWUpdatedDateTime  
		 --, @ETLJobProcessRunId 
		 --, @ETLJobSystemRunId   
	  FROM IrisContacts 

END 


/*
	END: BACKFILL DIM CONTACT WITH IRIS HISTORY IF NO RECORDS EXIST
*/


/*
	UPDATE HOUSEHOLD UIDS
*/

--BEGIN TRY

--BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
---- IF any batch fail, it will be caught IN the CATCH BLOCK AND will be rolled back.


--First, update DimContact for any IDs that have been newly mapped between CRMs
SET @Source = '{"SourceTable":"REF.CRMClientMapping"}'
SET @Target = '{"TargetTable":"#DimContact"}'
SET @StartTime = GETDATE()


/* 
	Get households that are not yet in DimContact
*/

; WITH Households as (

	SELECT DISTINCT 
		   REF.HouseholdUID
		 , REF.HouseholdID
		 , REF.ClientID_IRIS
		 , REF.ClientNumber_IRIS
	  FROM REF.CRMClientMapping REF 
	  LEFT 
	  JOIN #DimContact AS DC  
	    ON REF.HouseholdUID = DC.HouseholdUID
	 WHERE DC.HouseholdUID IS NULL
	   AND REF.HouseholdUID IS NOT NULL
	   AND REF.ClientNumber_Iris IS NOT NULL
)


	UPDATE #DimContact
	   SET HouseholdUID = Src.HouseholdUID
		 , HouseholdId = ISNULL(Src.HouseholdId,@UnknownTextValue)
		 , DWUpdatedDateTime = @DWUpdatedDateTime 
		 --, ETLJobProcessRunId = @ETLJobProcessRunId
		 --, ETLJobSystemRunId = @ETLJobSystemRunId 
	  FROM #DimContact AS TGT  
	  JOIN Households AS SRC 
		ON SRC.ClientNumber_Iris = CONVERT(NVARCHAR(30),TGT.ContactNumber)
	 WHERE TGT.HouseholdUID = @UnknownTextValue  --Update only records that have unknown SFDC IDs and have incoming known IDs
		OR TGT.HouseholdId = @UnknownTextValue --Update only records that have unknown SFDC IDs and have incoming known IDs
	--OPTION (Label = '#DimContact-HHUpdate')



; WITH LegacyIDs as (

	SELECT DISTINCT 
		   REF.HouseholdUID
		 , REF.HouseholdID
		 , REF.ClientID_IRIS AS ContactId
		 , REF.ClientNumber_IRIS AS ContactNumber
	  FROM REF.CRMClientMapping REF 
	  LEFT 
	  JOIN #DimContact AS DC  
	    ON REF.ClientNumber_IRIS = DC.ContactNumber
	 WHERE DC.ContactNumber IS NULL
	   AND REF.ClientNumber_IRIS IS NOT NULL
	   AND REF.HouseholdUID IS NOT NULL

)


	UPDATE #DimContact
	   SET ContactNumber = Src.ContactNumber
		 , ContactId = ISNULL(Src.ContactId,@UnknownGuid)
		 , DWUpdatedDateTime = @DWUpdatedDateTime 
		 --, ETLJobProcessRunId = @ETLJobProcessRunId
		 --, ETLJobSystemRunId = @ETLJobSystemRunId 
	  FROM #DimContact AS TGT  
	  JOIN LegacyIDs AS SRC 
		ON SRC.HouseholdUID = TGT.HouseholdUID
	 WHERE TGT.ContactNumber = @UnknownNumberValue  --Update only records that have unknown client numbers
--	OPTION (Label = '#DimContact-HHUpdate')

--	  EXEC MDR.spGetRowCountByQueryLabel '#DimContact-HHUpdate', @InsertCount OUT

--	   SET @EndTime = GETDATE()
--	   SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--	  EXEC MDR.spProcessTaskLogUpdateRowCount
--			@ETLJobProcessRunId 
--		  , @ComponentName
--		  , @Source 
--  		  , @Target 
--		  , @UpdateCount	 
--		  , @DurationInSeconds


--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY

--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#DimContact HH Update', ': ', ERROR_MESSAGE())	

--END CATCH 



/*
	START: SFDC DIM CONTACT UPSERT
*/

SET @Source = '{"SourceTable":["PCGSF.Account", "PCGSF.Contact", "PCGSF.AccountContactRelation"]}'
SET @Target = '{"TargetTable":"#DimContact"}'
SET @StartTime = GETDATE()

--declare @UnknownTextValue nvarchar(100) = '[Unknown]'
--declare @UnknownGuid uniqueidentifier = '00000000-0000-0000-0000-000000000000'
--declare @UnknownNumberValue int = -1

;WITH SFDCContacts AS (
 
    SELECT ISNULL(A.Id, @UnknownTextValue) AS HouseholdUID
	     , ISNULL(A.Household_ID__c, @UnknownTextValue) AS HouseholdId 
		 --, ISNULL(TRY_CAST(TRIM(A.Legacy_ID__c) AS INT), @UnknownNumberValue) AS Legacy_ID__c
		 , ISNULL(CM.ClientId_IRIS, @UnknownGuid) AS  ContactId
		 , ISNULL(CM.ClientNumber_IRIS, @UnknownNumberValue) AS ContactNumber
	     , C.FirstName AS ContactFirstName 
	     , C.LastName AS ContactLastName 
	     , C.[Name] AS ContactFullName 
	     , ISNULL(A.Service__c, @UnknownTextValue) AS ServiceProduct
	     , ISNULL(A.Client_Type_HH__c, @UnknownTextValue) AS ContactType 
	     , ISNULL(A.Household_Sub_Type__c, @UnknownTextValue) AS ContactSubType
	     , CONVERT(DATETIME, CONVERT(DATE, A.Household_Clearance_Date__c)) AS ClearanceDate
	     , ISNULL(A.Strength_CodeHH__c, @UnknownTextValue) AS StrengthCode   
	     , ISNULL(A.Preferred_Contact_Frequency__c, @UnknownTextValue) AS ContactFrequency
	     , ISNULL(
			CASE 
				WHEN C.FinServ__Gender__c = '1' THEN 'Male'
				WHEN C.FinServ__Gender__c = '2' THEN 'Female'
				ELSE @UnknownTextValue
			END, @UnknownTextValue) AS Gender
	     , ISNULL(
			CASE 
				WHEN C.FinServ__MaritalStatus__c = '1' THEN 'Single'
				WHEN C.FinServ__MaritalStatus__c = '2' THEN 'Married'
				WHEN C.FinServ__MaritalStatus__c = '3' THEN 'Divorced'
				WHEN C.FinServ__MaritalStatus__c = '4' THEN 'Widowed'
				ELSE @UnknownTextValue
			END, @UnknownTextValue) AS MaritalStatus
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
		 , A.IsDeleted
		 -- This should never happen in higher environments, but there are no system protections for having more than one primary contact per household
		 -- Later we'll limit this to RowNum = 1
		 , Row_Number() OVER (PARTITION BY A.Id ORDER BY C.LastModifieddate DESC) RowNum 

      FROM PCGSF.Account AS A

	  JOIN PCGSF.AccountContactRelation AS ACR 
	    ON A.Id = ACR.AccountId

	  JOIN PCGSF.Contact AS C 
	    ON C.Id = ACR.ContactId

	  JOIN PCGSF.RecordType AS RT
	    ON A.RecordTypeId = RT.Id
	  
	  LEFT 
	  JOIN REF.CRMClientMapping  CM  
	    ON CM.HouseholdUID = A.Id 

     WHERE ACR.FinServ__Primary__c = 1 --PRIMARY MEMBER OF HOUSEHOLD GROUP
	   AND ACR.IsDeleted = 0 --ACTIVE RECORD 
	   AND ACR.ISActive = 1 --ACTIVE RECORD
	   AND RT.[Name] = 'Household' 

)


, RowHash AS ( 

    SELECT HouseholdUID
         , HouseholdId
		 --, Legacy_ID__c
	     , ContactId 
	     , ContactNumber
	     , ContactFirstName
	     , ContactLastName
	     , ContactFullName
	     , ServiceProduct
	     , ContactType
	     , ContactSubType
	     , ClearanceDate
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
		 , IsDeleted
		 , HASHBYTES('SHA2_256', CONCAT(HouseholdUID
									  , '|', HouseholdId
									  , '|', ContactId
									  , '|', ContactFirstName
								      , '|', ContactLastName
								      , '|', ContactFullName
								      , '|', ServiceProduct
								      , '|', ContactType
								      , '|', ContactSubType
								      , '|', ClearanceDate
								      , '|', StrengthCode
								      , '|', ContactFrequency
								      , '|', Gender
								      , '|', MaritalStatus
								      , '|', RetirementStatus
								      , '|', EmploymentStatus
								      , '|', Industry
								      , '|', Occupation
								      , '|', ResidenceCountry
								      , '|', ContractCountry
								      , '|', ContractEntity
								      , '|', CurrencyCode
								      , '|', IsDeleted)) AS RowHash
		 , 'SFDC' AS SystemOfRecord
	  FROM SFDCContacts AS SRC
     WHERE RowNum = 1 -- Limit to one record	

)


	INSERT 
      INTO #DimContact_Temp (      
 	       HouseholdUID
         , HouseholdId
		 --, Legacy_ID__c
	     , ContactId 
	     , ContactNumber
	     , ContactFirstName
	     , ContactLastName
	     , ContactFullName
	     , ServiceProduct
	     , ContactType
	     , ContactSubType
		 , ClearanceDate
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
		 , IsDeleted
		 , RowHash
		 , SystemOfRecord
		 , RecordType
	)


    SELECT SRC.HouseholdUID
         , SRC.HouseholdId
		 --, SRC.Legacy_ID__c
	     , SRC.ContactId 
	     , SRC.ContactNumber
	     , SRC.ContactFirstName
	     , SRC.ContactLastName
	     , SRC.ContactFullName
	     , SRC.ServiceProduct
	     , SRC.ContactType
	     , SRC.ContactSubType
	     , SRC.ClearanceDate
	     , SRC.StrengthCode
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
		 , SRC.IsDeleted
		 , SRC.RowHash
		 , SRC.SystemOfRecord
		 , CASE 
			WHEN TGT.DimContactKey IS NULL THEN 'NEW'
			WHEN TGT.RowHash <> SRC.RowHash THEN 'MODIFIED'
			WHEN TGT.RowHash = SRC.RowHash THEN 'NO CHANGE' 
			ELSE @UnknownTextValue
		   END AS RecordType
	  FROM RowHash AS SRC
	  LEFT
	  JOIN #DimContact AS TGT
	    ON SRC.ContactNumber = TGT.ContactNumber
	   AND SRC.HouseholdUID = TGT.HouseholdUID


/*
	UPDATE EXISTING RECORD
*/

--BEGIN TRY

--BEGIN TRANSACTION -- Begin of Transaction scope. Transaction will be committed after each batch. 
---- IF any batch fail, it will be caught IN the CATCH BLOCK AND will be rolled back.


	UPDATE #DimContact
	   SET HouseholdUID = SRC.HouseholdUID
	     , HouseholdId = SRC.HouseholdId
		 , ContactId = SRC.ContactId
		 , ContactFirstName = SRC.ContactFirstName
		 , ContactLastName = SRC.ContactLastName
		 , ContactFullName = SRC.ContactFullName
		 , ServiceProduct = SRC.ServiceProduct
		 , ContactType = SRC.ContactType
		 , ContactSubType = SRC.ContactSubType
		 , ClearanceDate = SRC.ClearanceDate
		 , StrengthCode = SRC.StrengthCode
		 , ContactFrequency = SRC.ContactFrequency
		 , Gender = SRC.Gender
		 , MaritalStatus = SRC.MaritalStatus
		 , RetirementStatus = SRC.RetirementStatus
		 , EmploymentStatus = SRC.EmploymentStatus
		 , Industry = SRC.Industry
		 , Occupation = SRC.Occupation
		 , ResidenceCountry = SRC.ResidenceCountry
		 , ContractCountry = SRC.ContractCountry
		 , ContractEntity = SRC.ContractEntity
		 , CurrencyCode = SRC.CurrencyCode
		 , IsDeleted = SRC.IsDeleted
		 , RowHash = SRC.RowHash
		 , SystemOfRecord = SRC.SystemOfRecord
		 , DWUpdatedDateTime = @DWUpdatedDateTime  
		 --, ETLJobProcessRunId = @ETLJobProcessRunId 
		 --, ETLJobSystemRunId = @ETLJobSystemRunId  
	  FROM #DimContact_Temp AS SRC
	  JOIN #DimContact AS TGT
	    ON SRC.ContactNumber = TGT.ContactNumber 
	   AND SRC.HouseholdUID = TGT.HouseholdUID
	 WHERE SRC.RecordType = 'MODIFIED' --ONLY UPDATE RECORDS THAT HAVE BEEN MODIFIED 

	--OPTION (Label = '#DimContact-Update')

	--  EXEC MDR.spGetRowCountByQueryLabel '#DimContact-Update', @InsertCount OUT

	--   SET @EndTime = GETDATE()
	--   SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

	--  EXEC MDR.spProcessTaskLogUpdateRowCount
	--		@ETLJobProcessRunId 
	--	  , @ComponentName
	--	  , @Source 
 -- 		  , @Target 
	--	  , @UpdateCount	 
	--	  , @DurationInSeconds




/*
	INSERT NEW RECORDS 
*/

    INSERT 
      INTO #DimContact (      
 	       HouseholdUID
         , HouseholdId
	     , ContactId 
	     , ContactNumber
	     , ContactFirstName
	     , ContactLastName
	     , ContactFullName
	     , ServiceProduct
	     , ContactType
	     , ContactSubType
		 , ClearanceDate
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
		 , IsDeleted
		 , RowHash
		 , SystemOfRecord
         , DWCreatedDateTime
	     , DWUpdatedDateTime
	     --, ETLJobProcessRunId
	     --, ETLJobSystemRunId
	)


 	SELECT SRC.HouseholdUID
         , SRC.HouseholdId
	     , SRC.ContactId 
	     , SRC.ContactNumber
	     , SRC.ContactFirstName
	     , SRC.ContactLastName
	     , SRC.ContactFullName
	     , SRC.ServiceProduct
	     , SRC.ContactType
	     , SRC.ContactSubType   
		 , SRC.ClearanceDate
	     , SRC.StrengthCode
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
		 , SRC.IsDeleted
		 , SRC.RowHash
		 , SRC.SystemOfRecord 
		 , @DWUpdatedDateTime  
		 , @DWUpdatedDateTime  
		 --, @ETLJobProcessRunId 
		 --, @ETLJobSystemRunId   
	  FROM #DimContact_Temp AS SRC
	  LEFT
	  JOIN #DimContact AS TGT
	    ON SRC.ContactNumber = TGT.ContactNumber
       AND SRC.HouseholdUID = TGT.HouseholdUID
	 WHERE SRC.RecordType = 'NEW'
	   AND TGT.ContactNumber IS NULL

--	OPTION (Label = '#DimContact-Insert')

--	  EXEC MDR.spGetRowCountByQueryLabel '#DimContact-Insert', @InsertCount OUT

--	   SET @EndTime = GETDATE()
--	   SET @DurationInSeconds = DATEDIFF(SECOND, @StartTime, @EndTime)

--	  EXEC MDR.spProcessTaskLogInsertRowCount
--			@ETLJobProcessRunId 
--		  , @ComponentName
--		  , @Source 
--  		  , @Target 
--		  , @UpdateCount	 
--		  , @DurationInSeconds



--COMMIT TRANSACTION -- Transaction scope for Commit

--END TRY

--BEGIN CATCH 

--	ROLLBACK TRANSACTION;
--	SET @Status = 0
--	SET @ErrorMessage = CONCAT('#DimContact Upsert', ': ', ERROR_MESSAGE())	

--END CATCH 

--IF OBJECT_ID ('TEMPDB..#DimContact_Temp') IS NOT NULL DROP TABLE #DimContact_Temp

--SELECT @Status AS Status , @ErrorMessage AS ErrorMessage

--END

