SELECT COUNT(1) AS RecCount
  FROM FDW.FactClientServiceIncident
  
--RecCount: 39,239

/*
  TEST 1: CHECK TO SEE IF ANY CSI HAS DUPLICATE CURRENT RECORD FLAGS
*/

SELECT IncidentId
     , COUNT(1)
  FROM fdw.factclientserviceincident
 GROUP
    BY IncidentId
HAVING COUNT(1) > 1

--TEST PASSED 


/*
  TEST 2: UPDATE SOURCE RECORDSET TO SHOW AS IF THE CSI WAS RESOLVED THEN RUN FACT UPSERT 
  
  EXPECTED OUTCOME: DimResolutionDateKey and MinutesToResolution are populated within fact table, DimCSIResolutionDateKey updated to 27 (resolved = yes)
*/

--DimResolutionDateKey = NULL
--Minutes to Resolution = NULL
--DimCSIResolutionDateKey = 87 (NO)
 SELECT * 
   FROM FDW.FactClientServiceIncident
  WHERE IncidentId = 28122

UPDATE [BAS].[CSI_IncidentMainTable]
   SET dtResolutionDate = GETDATE()
 WHERE IID = 28122

 
DECLARE @A UNIQUEIDENTIFIER = NEWID() 
   EXEC [FDW].[spUpsertFactClientServiceIncident] @A, @A, 'Transform and Load Fact Tables'
  
	--DimResolutionDateKey = 20230201
	--Minutes to Resolution = 5706474
	--DimCSIResolutionDateKey = 27 (YES)
	 SELECT * 
	   FROM FDW.FactClientServiceIncident
	  WHERE IncidentId = 28122
  
--TEST PASSED


/*
  TEST 3: RUN FACT UPSERT AGAIN WITH NO CHANGE
  
  EXPECTED OUTCOME: NO CHANGE
*/

 --39,239
SELECT COUNT(1) AS RecCount
  FROM FDW.FactClientServiceIncident 


DECLARE @A UNIQUEIDENTIFIER = NEWID() 
   EXEC [FDW].[spUpsertFactClientServiceIncident] @A, @A, 'Transform and Load Fact Tables'


 --39,239
SELECT COUNT(1) AS RecCount
  FROM FDW.FactClientServiceIncident 
  
--TEST PASSED
