
/*
  TEST 1 - INSERT ONE RECORD INTO SOURCE OBJECT WITH THREE DIFFERENT CSI REASONS, ONE REASON IS NEW AND DOES NOT YET EXIST WITHIN DIM REASON
*/


  --INSERT RECORD INTO SOURCE OBJECT
  DECLARE @DateTime DATETIME = GETDATE()

  INSERT INTO BAS.CSI_IncidentMainTable 
  (iID, vchLOSIDescriptionA1, vchLOSIDescriptionB1, vchLOSIDescriptionA2, vchLOSIDescriptionB2, dtDateSubmitted, vchSubmittedBy, vchCACID, vchICID, vchCOPID)
  VALUES (2, 'ABCD', 'Transfer Error', NULL, 'Data Entry Error', @DateTime, 'mrowe', 'mrowe', 'mromasanta', NULL)

  --EXPECTED OUTCOME: ONE ADDITIONAL RECORD ADDED TO DIM CLIENT SERVICE INCIDENT FOR NEW INSERTED SOURCE RECORD (IID = 2)
  
  --RUN UPSERT FOR Dim Incident (Bridge Table 1) 
  DECLARE @A UNIQUEIDENTIFIER = NEWID()
  EXEC [FDW].[spInsertDimClientServiceIncident] @A, @A, 'Transform and Load Dimension Tables'

  --Initial Count = 42,926

  --After Dim Client Service Incident Insert = 42,927
	SELECT * 
      FROM FDW.DimClientServiceIncident
     ORDER BY IncidentId

/************************************
*******  TEST RESULT: PASSED  *******
*************************************/



/* 

	RUN UPSERT FOR Dim Reason (Bridge Table 2) 

*/
	--Record Count: 72
  	SELECT * 
      FROM FDW.DimCSIReason     

	DECLARE @A UNIQUEIDENTIFIER = NEWID()
	EXEC [FDW].[spInsertDimCSIReason] @A, @A, 'Transform and Load Dimension Tables'

  --EXPECTED OUTCOME: ONE ADDITIONAL RECORD ADDED TO DIMCSIREASON FOR NEW INSERTED REASON 'ABCD'

  --After Dim CSI Reason Insert = 73
  	SELECT * 
      FROM FDW.DimCSIReason     
	  ORDER BY 2

/************************************
*******  TEST RESULT: PASSED  *******
*************************************/



/* 

	RUN UPSERT FOR DIM CS INCIDENT CS REASON BRIDGE (BRIDGE TABLE THAT LINKS INCIDENT TO REASON TO MAP M:M RELATIONSHIP BETWEEN INCIDENT AND REASON) 

*/
	--Record Count: 16,824
  	SELECT * 
      FROM FDW.BridgeCSIncidentCSReason      

	DECLARE @A UNIQUEIDENTIFIER = NEWID()
	EXEC [FDW].[spInsertBridgeCSIncidentCSReason] @A, @A, 'Transform and Load Bridge Tables'

  --EXPECTED OUTCOME: THREE ADDITIONAL RECORDS ADDED TO BridgeCSIncidentCSReason FOR NEW INCIDENT THAT WAS CREATED WITH THREE REASONS 
		--DimClientServiceIncidentKey | DimCSIReasonKey
		--17149 | 54
		--17149 | 68
		--17149 | 4

	SELECT * 
	  FROM FDW.DimClientServiceIncident 
	  WHERE IncidentId = 2

  	SELECT * 
      FROM FDW.DimCSIReason    
	  WHERE IncidentReason IN ('ABCD', 'Transfer Error', 'Data Entry Error')

  	SELECT * 
      FROM FDW.BridgeCSIncidentCSReason 
	  where DimClientServiceIncidentKey = 17149



/************************************
*******  TEST RESULT: PASSED  *******
*************************************/
