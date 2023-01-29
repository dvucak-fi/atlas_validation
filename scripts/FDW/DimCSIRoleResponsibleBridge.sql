/*
  INITIAL EVALUATION 
*/

SELECT * 
  FROM FDW.DimCSIRoleResponsibleBridge
  order by 1

  --Max Surrogate Key = 15863
  --Record Count = 16,823

  select * from fdw.dimcsiroleresponsible

SELECT iID, vchPersonResponsible1, vchPersonResponsible2
  FROM BAS.CSI_IncidentMainTable 
order by 1

/*
  TEST 1 - INSERT ONE RECORD INTO SOURCE OBJECT WITH THREE CSI RoleResponsibleS  
*/

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchPersonResponsible1, vchPersonResponsible2)
  VALUES (1, 'CAC', 'Marketing')

  --EXPECTED OUTCOME: TWO ADDITIONAL RECORDS ARE ADDED TO BRDIGE TABLE AND MAX SURROGATE KEY IS INCREMENTED BY 1 TO 15864
  
    --Max Surrogate Key = 15864
    --Record Count = 16,825
  
  --TEST PASSED 


/*
  TEST 2- INSERT TWO RECORDS INTO SOURCE OBJECT WITH ONE ONE ROLE RESPONSIBLE THE OTHER WITH TWO NULL VALUES
*/

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchPersonResponsible1, vchPersonResponsible2)
  VALUES (2, 'Custodian', NULL)

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchPersonResponsible1, vchPersonResponsible2)
  VALUES (3, NULL, NULL)

  --EXPECTED OUTCOME: RECORD COUNT INCREMENTED TO 16,826 AND MAX SURROGATE KEY INCREMENTED TO 15865
  
    --Max Surrogate Key = 15865
    --Record Count = 16,826
  
  --TEST PASSED 
