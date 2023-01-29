/*
  INITIAL EVALUATION 
*/

SELECT * 
  FROM FDW.DimCSIReasonBridge
  order by 1

  --Max Surrogate Key = 14319
  --Record Count = 19655


/*
  TEST 1 - INSERT ONE RECORD INTO SOURCE OBJECT WITH THREE CSI REASONS  
*/

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchLOSIDescriptionA1, vchLOSIDescriptionB1, vchLOSIDescriptionA2, vchLOSIDescriptionB2)
  VALUES (1, 'Process Error', 'Transfer Error', NULL, 'Data Entry Error')

  --EXPECTED OUTCOME: THREE ADDITIONAL RECORDS ARE ADDED TO BRDIGE TABLE AND MAX SURROGATE KEY IS INCREMENTED BY 1 TO 14320
  
    --Max Surrogate Key = 14320
    --Record Count = 19658
  
  --TEST PASSED 


/*
  TEST 2- INSERT TWO RECORDS INTO SOURCE OBJECT WITH ONE CSI REASON FOR ONE AND TWO FOR THE OTHER 
*/

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchLOSIDescriptionA1, vchLOSIDescriptionB1, vchLOSIDescriptionA2, vchLOSIDescriptionB2)
  VALUES (2, NULL, 'Transfer Error', NULL, 'Data Entry Error')

  INSERT INTO BAS.CSI_IncidentMainTable (iID, vchLOSIDescriptionA1, vchLOSIDescriptionB1, vchLOSIDescriptionA2, vchLOSIDescriptionB2)
  VALUES (3, 'Process Error', NULL, NULL, NULL)

  --EXPECTED OUTCOME: THREE ADDITIONAL RECORDS ARE ADDED TO BRDIGE TABLE AND TWO NEW SURROGATE KEYS ARE CREATED, 14321 AND 14322.
  
    --Max Surrogate Key = 14322
    --Record Count = 19661
  
  --TEST PASSED 
