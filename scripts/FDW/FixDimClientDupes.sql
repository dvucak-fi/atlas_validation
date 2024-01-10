
IF OBJECT_ID('TEMPDB..#DupeClientKeys') IS NOT NULL 
	DROP TABLE #DupeClientKeys

 CREATE TABLE #DupeClientKeys (
        DimClientKey INT
	  , ClientNumber INT
	  , EffectiveStartDate DATETIME
 )
WITH (DISTRIBUTION = HASH(DimClientKey),HEAP);



DECLARE @MaxDate DATE = '9999-12-31'

;WITH DupeClients AS ( 

	 SELECT ClientID
		  , ClientNumber
		  , COUNT(HouseholdUID) AS C
	   FROM fdw.dimclient
	  WHERE CurrentRecord = 1 
		AND ClientId IS NOT NULL
	  GROUP BY ClientID, ClientNUmber
	 HAVING COUNT(HouseholdUID) > 1 

)

     INSERT 
       INTO #DupeClientKeys ( 
            DimClientKey 
	      , ClientNumber 
	      , EffectiveStartDate
	 )

	 SELECT DC.DimClientKey 
	      , DC.ClientNumber
		  , DC.EffectiveStartDate
	   FROM FDW.DimClient AS DC
	   JOIN DupeClients AS D
	     ON DC.ClientId = D.ClientId




/*
	FIX DIM CLIENT 
	CTAS DIM CLIENT INTO DIM CLIENT PDDTI-1380. THIS WILL CORRECT THE DUPLICATES WE HAVE WITHIN DIM CLIENT CURRENTLY.
*/


CREATE TABLE FDW.DimClient_PDDTI1380
WITH (DISTRIBUTION = HASH(ClientID), CLUSTERED COLUMNSTORE INDEX) 
AS
WITH DupeClientFix AS ( 

	SELECT DC.DimClientKey
		 , A.Id AS HouseholdUID
		 , A.Household_ID__c AS HouseholdId
		 , DC.ClientNumber
		 , CASE 
			WHEN DC.ClientTradingDate IS NULL 
			THEN LAG (DC.ClientTradingDate, 1, NULL) OVER (PARTITION BY DC.ClientNumber ORDER BY DC.EffectiveStartDate) 
			ELSE DC.ClientTradingDate
		   END AS ClientTradingDate
		 , DC.EffectiveStartDate
		 , LEAD (DC.EffectiveStartDate, 1, @MaxDate) OVER (PARTITION BY DC.ClientNumber ORDER BY DC.EffectiveStartDate) AS EffectiveEndDate
		 , CASE WHEN LEAD (DC.EffectiveStartDate, 1, @MaxDate) OVER (PARTITION BY DC.ClientNumber ORDER BY DC.EffectiveStartDate) = @MaxDate THEN 1 ELSE 0 END AS CurrentRecord
	  FROM FDW.DimClient AS DC
	  JOIN (SELECT DISTINCT ClientNumber FROM #DupeClientKeys) AS D
		ON DC.ClientNumber = D.ClientNumber
	  JOIN PCGSF.Account AS A	 
		ON A.Legacy_ID__c = DC.ClientNumber 	 

)

    SELECT DC.DimClientKey	
	     , CASE WHEN DCF.DimClientKey IS NOT NULL THEN DCF.HouseholdUID ELSE DC.HouseholdUID END AS HouseholdUID
	     , CASE WHEN DCF.DimClientKey IS NOT NULL THEN DCF.HouseholdId ELSE DC.HouseholdId END AS HouseholdId
	     , DC.ClientID	
	     , DC.ClientNumber	
	     , DC.ClientFirstName	
	     , DC.ClientLastName	
	     , DC.ClientFullName	
	     , DC.ServiceProduct	
	     , DC.ClientType	
	     , DC.ClientSubType	
	     , DC.ClientClearanceDate	
	     , CASE WHEN DCF.DimClientKey IS NOT NULL THEN DCF.ClientTradingDate ELSE DC.ClientTradingDate END AS ClientTradingDate
	     , DC.StrengthCode	
	     , DC.DimStrengthCodeKey	
	     , DC.ContactFrequency	
	     , DC.Gender	
	     , DC.MaritalStatus	
	     , DC.RetirementStatus	
	     , DC.EmploymentStatus	
	     , DC.Industry	
	     , DC.Occupation	
	     , DC.ResidenceCountry	
	     , DC.ContractCountry	
	     , DC.ContractEntity	
	     , DC.CurrencyCode	
	     , DC.SystemOfRecord	
	     , DC.RowHash	
	     , DC.EffectiveStartDate	
	     , CASE WHEN DCF.DimClientKey IS NOT NULL THEN DCF.EffectiveEndDate ELSE DC.EffectiveEndDate END AS EffectiveEndDate
	     , CASE WHEN DCF.DimClientKey IS NOT NULL THEN DCF.CurrentRecord ELSE DC.CurrentRecord END AS CurrentRecord
	     , DC.DWCreatedDateTime	
	     , DC.DWUpdatedDateTime	
	     , DC.ETLJobProcessRunId	
	     , DC.ETLJobSystemRunId
	  FROM FDW.DimClient AS DC
	  LEFT
	  JOIN DupeClientFix AS DCF
	    ON DC.DimClientKey = DCF.DimClientKey 






/*
	FACT TABLE: FactCACCaseMilestones
	RECORD COUNT: 815

	DUPES START ON 11/29/2023 - DELETE ALL RECORS ON/AFTER 11/29/2023

*/
SELECT *
  FROM FDW.FACTCASESNAPSHOTDAILY 
 WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
 order by 1, casenumber

 SELECT DimDateKey	
      , DimClientKey
      , CaseNumber
	  , COUNT(1) AS RECCOUNT
   FROM FDW.FACTCASESNAPSHOTDAILY
  WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
  GROUP 
     BY DimDateKey	
      , DimClientKey
      , CaseNumber
 HAVING COUNT(1) > 1
  ORDER BY 1




/*
	FACT TABLE: FactClientSnapShotDaily
	RECORD COUNT: 7097 
	RECORD DELETE: 117 (DUPE QUERY BELOW) 

	DUPED CLIENT KEYS START ON 11/29/2023 - DELETE ALL RECORDS ON/AFTER 11/29/2023

*/
SELECT *
  FROM FDW.FactClientSnapShotDaily 
 WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
 order by 1


 SELECT DimDateKey	
      , ClientNumber
	  , COUNT(1) AS RECCOUNT
   FROM FDW.FactClientSnapShotDaily
  WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
  GROUP 
     BY DimDateKey	
      , ClientNumber
 HAVING COUNT(1) > 1
  ORDER BY 1



/*
	FACT TABLE: FactFinancialAccountClearance
	RECORD COUNT: 0 
	RECORD DELETE: 0 
*/

SELECT *
  FROM FDW.FactFinancialAccountClearance 
 WHERE DimCreatedClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
    OR DimClearanceClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
 order by 1




/*
	FACT TABLE: FactInteractionActivity
	RECORD COUNT: 0 
	RECORD DELETE: 0 

	TRUNCATE TABLE ENTIRELY. NEED TO RUN IRIS PROC FIRST THEN SFDC PROC
*/

SELECT *
  FROM FDW.FactInteractionActivity 
 WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
 order by 1


/*
	FACT TABLE: FactNetFlows
	RECORD COUNT: 0 
	RECORD DELETE: 0 

	TRUNCATE TABLE ENTIRELY. 
*/

SELECT *
  FROM FDW.FactNetFlows 
 WHERE DimClientKey IN (SELECT DimClientKey FROM #DupeClientKeys)
 order by UpdatedDate
