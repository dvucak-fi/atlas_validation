select *
  from ref.clientonboarding
  where clientnumber = 18780211


SELECT O.[Id] OpportunityID
      ,O.[IsDeleted]
      ,REF.HouseholdUID
	  ,REF.HouseholdID
	  ,REF.ClientNumber_IRIS ClientNumber
	  ,REF.CLIENTID_IRIS ClientID
      ,O.[RecordTypeId]
	  ,R.Name
      ,O.[StageName]
      ,O.[CurrencyIsoCode]
      ,O.[Target_Assets__c] TargetAmountBase
      ,O.[CCA_Amount__c] SignedAmountBase
      ,O.[CCA_Date__c] SignedDate

  FROM [PcgSf].[Opportunity] O
  INNER JOIN PCGSF.RecordType R on R.ID = O.RecordTypeID
  INNER JOIN REF.CRMClientMapping REF ON REF.HouseholdUID = COALESCE(O.[FinServ__Household__c],O.[AccountId])
  WHERE CCA_Date__c IS NOT NULL
    AND Is_Test_Record__c = 0
	AND R.Name = 'FI Opportunity'
	and REF.ClientNumber_IRIS = 18780211



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

, DupeClientFix AS ( 

	SELECT DC.DimClientKey
		 , A.Id AS HouseholdUID
		 , A.Household_ID__c AS HouseholdId
		 , DC.ClientNumber
		 , DC.EffectiveStartDate
		 , LEAD (DC.EffectiveStartDate, 1, @MaxDate) OVER (PARTITION BY DC.ClientNumber ORDER BY DC.EffectiveStartDate) AS EffectiveEndDate
		 , CASE WHEN LEAD (DC.EffectiveStartDate, 1, @MaxDate) OVER (PARTITION BY DC.ClientNumber ORDER BY DC.EffectiveStartDate) = @MaxDate THEN 1 ELSE 0 END AS CurrentRecord
	  FROM FDW.DimClient AS DC
	  JOIN DupeClients AS D
		ON DC.ClientNumber = D.ClientNumber
	  JOIN PCGSF.Account AS A	 
		ON A.Legacy_ID__c = DC.ClientNumber 	 

)

--874749
--2288289

    SELECT DC.DimClientKey
	     , DC.ClientNumber
		 , DC.HouseholdUID
		 , DCF.HouseholdUID AS HouseholdUID_NEW
		 , DC.HouseholdId
		 , DCF.HouseholdId AS HouseholdId_NEW
		 , DC.EffectiveStartDate
		 , DC.EffectiveEndDate
		 , DC.CurrentRecord
		 , DCF.EffectiveStartDate AS EffectiveStartDate_NEW
		 , DCF.EffectiveEndDate AS EffectiveEndDate_NEW
		 , DCF.CurrentRecord AS CurrentRecord_NEW
      FROM DupeClientFix AS DCF
	  JOIN FDW.DimClient AS DC
	    ON DCF.DimClientKey = DC.DimClientKey 
	 ORDER BY ClientNumber, EffectiveStartDate


	 select * 
	   from fdw.factclientsnapshotdaily
	   where clientnumber = 17921361
	   order by 1 desc


	   SELECT *
	     FROM REF.CRMClientMapping
		 where clientnumber_Iris in (7746160, 17921361, 17925769)



/*
	UPDATE STATEMENT TO CORRECT DUPLICATES IN DIM CLIENT
*/

--UPDATE FDW.DimClient
--   SET HouseholdUID = DCF.HouseholdUID
--     , HouseholdId = DCF.HouseholdId
--	 , EffectiveEndDate = DCF.EffectiveEndDate
--	 , CurrentRecord = DCF.CurrentRecord 
--  FROM DupeClientFix AS DCF
--  JOIN FDW.DimClient AS DC
--    ON DC.DimClientKey = DCF.DimClientKey



SELECT DimDateKey
     , ClientNumber
	 , COUNT(1) 
  FROM FDW.FactClientSnapshotDaily
 GROUP BY DimDateKey, ClientNumber
 HAVING COUNT(1) > 1 




 SELECT *
   FROM INFORMATION_SCHEMA.COLUMNS AS T
  WHERE TABLE_SCHEMA = 'FDW'
    AND LEFT(TABLE_NAME, 4) = 'Fact'
	AND COLUMN_NAME LIKE '%ClientKey%'
  ORDER BY 3


  SELECT * 
    FROM FDW.FACTCLIENTTERMINATION
