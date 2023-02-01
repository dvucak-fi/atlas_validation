
/*
  TEST 1 - INSERT ONE RECORD INTO SOURCE OBJECT WITH THREE DIFFERENT CSI REASONS, ONE REASON IS NEW AND DOES NOT YET EXIST WITHIN DIM REASON
*/

--Initial Record Count: 39,237
select * 
  from BAS.CSI_IncidentMainTable 


  --INSERT RECORD INTO SOURCE OBJECT
  DECLARE @DateTime DATETIME = GETDATE()

  INSERT INTO BAS.CSI_IncidentMainTable 
  (iID, vchLOSIDescriptionA1, vchLOSIDescriptionB1, vchLOSIDescriptionA2, vchLOSIDescriptionB2, dtDateSubmitted, vchSubmittedBy, vchCACID, vchICID, vchCOPID, vchPersonResponsible1, vchPersonResponsible2)
  VALUES (4, 'ABCD', 'Transfer Error', NULL, 'Data Entry Error', @DateTime, 'mrowe', 'mrowe', 'mromasanta', NULL, 'Engineer', 'Mini Architect')

  --EXPECTED OUTCOME: ONE ADDITIONAL RECORD ADDED TO SOURCE AND DIM CLIENT SERVICE INCIDENT FOR NEW INSERTED SOURCE RECORD (INCIDENTID = 4)
  
  --RUN UPSERT FOR Dim Incident (Bridge Table 1) 
  DECLARE @A UNIQUEIDENTIFIER = NEWID()
  EXEC [FDW].[spInsertDimClientServiceIncident] @A, @A, 'Transform and Load Dimension Tables'

  --Record Count: 39,238
	select * 
	  from BAS.CSI_IncidentMainTable 

  --Initial Count = 42,927

  --After Dim Client Service Incident Insert = 42,928
	SELECT * 
      FROM FDW.DimClientServiceIncident
     ORDER BY IncidentId

/************************************
*******  TEST RESULT: PASSED  *******
*************************************/



/* 

	RUN UPSERT FOR Dim Role Responsible (Bridge Table 1) 

*/
	--Record Count: 55
  	SELECT * 
      FROM FDW.DimCSIRoleResponsible     

	DECLARE @A UNIQUEIDENTIFIER = NEWID()
	EXEC [FDW].[spInsertDimCSIRoleResponsible] @A, @A, 'Transform and Load Dimension Tables'

  --EXPECTED OUTCOME: TWO ADDITIONAL RECORD ADDED TO DIM ROLE RESPONSIBLE FOR ROLES ENGINEER AND MINI ARCHITECT

	--Record Count: 57
  	SELECT * 
      FROM FDW.DimCSIRoleResponsible 

/************************************
*******  TEST RESULT: PASSED  *******
*************************************/



/* 

	RUN UPSERT FOR DIM CS INCIDENT CS REASON BRIDGE (BRIDGE TABLE THAT LINKS INCIDENT TO REASON TO MAP M:M RELATIONSHIP BETWEEN INCIDENT AND REASON) 

*/
	--Record Count: 14,869
  	SELECT * 
      FROM FDW.BridgeCSIncidentCSRoleResponsible      

	DECLARE @A UNIQUEIDENTIFIER = NEWID()
	EXEC [FDW].[spInsertBridgeCSIncidentCSRoleResponsible] @A, @A, 'Transform and Load Bridge Tables'

  --EXPECTED OUTCOME: TWO ADDITIONAL RECORDS ADDED TO BridgeCSIncidentCSRoleResponsible FOR NEW INCIDENT THAT WAS CREATED WITH TWO NEW ROLES
		--DimClientServiceIncidentKey | DimCSIRoleResponsibleKey
		--42381 | 143
		--42381 | 50

	--Record Count: 14,871
  	SELECT * 
      FROM FDW.BridgeCSIncidentCSRoleResponsible   

	SELECT * 
	  FROM FDW.DimClientServiceIncident 
	  WHERE IncidentId = 4

  	SELECT * 
      FROM FDW.DimCSIRoleResponsible     
	  WHERE RoleResponsible IN ('engineer', 'mini architect')

  	SELECT * 
      FROM FDW.BridgeCSIncidentCSRoleResponsible
	  where DimClientServiceIncidentKey = 42381



/************************************
*******  TEST RESULT: PASSED  *******
*************************************/
