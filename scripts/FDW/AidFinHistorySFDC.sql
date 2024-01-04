

WITH SFDC_AuditHistory_AID AS ( 

    SELECT AH.Id AS AuditLogId
	     , FA.Id AS FinancialAccountId
         , AH.OldValue AS OldAID
         , AH.NewValue AS NewAID
         , AH.CreatedDate
         , ROW_NUMBER() OVER (PARTITION BY AH.ParentId ORDER BY AH.CreatedDate, AH.Id COLLATE Latin1_General_100_BIN2_UTF8) AS RowNum
		 , FA.CreatedDate AS FinancialAccountCreatedDate
      FROM PCGSF.FinServ__FinancialAccount__History AS AH  
	  JOIN PCGSF.FinServ__FinancialAccount__c AS FA
	    ON AH.ParentId = FA.Id
     WHERE Field = 'AID_Account_Number__c'
       --AND DataType = 'EntityId'
       --AND AH.CreatedDate < @TODAY  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST     
	   
)

, FullAuditHistoryAID AS ( 

    SELECT AuditLogId
	     , FinancialAccountId
         , OldAID AS AccountNumber
         , FinancialAccountCreatedDate AS CreatedDate
      FROM SFDC_AuditHistory_AID AS AH
     WHERE RowNum = 1

     UNION 

    SELECT AuditLogId
	     , FinancialAccountId
         , NewAID AS AccountNumber
         , CreatedDate
      FROM SFDC_AuditHistory_AID

     UNION
    
    /*
        SOME HOUSEHOLDS HAVE AIDs BUT NO HISTORY - UNION THOSE IN TOO
    */

    SELECT '1' AS AuditLogId
	     , FA.Id AS FinancialAccountId
         , FA.AID_Account_Number__c AS AccountNumber
         , FA.CreatedDate
      FROM PCGSF.FinServ__FinancialAccount__c AS FA 
      LEFT
      JOIN (SELECT DISTINCT FinancialAccountId FROM SFDC_AuditHistory_AID) AS AH
        ON FA.Id = AH.FinancialAccountId
     WHERE FA.AID_Account_Number__c IS NOT NULL
       AND AH.FinancialAccountId IS NULL    
 
 )

 , SFDC_AuditHistory_FIN AS ( 

    SELECT AH.Id AS AuditLogId
	     , FA.Id AS FinancialAccountId
         , AH.OldValue AS OldFin
         , AH.NewValue AS NewFin
         , AH.CreatedDate
         , ROW_NUMBER() OVER (PARTITION BY AH.ParentId ORDER BY AH.CreatedDate, AH.Id COLLATE Latin1_General_100_BIN2_UTF8) AS RowNum
		 , FA.CreatedDate AS FinancialAccountCreatedDate
      FROM PCGSF.FinServ__FinancialAccount__History AS AH  
	  JOIN PCGSF.FinServ__FinancialAccount__c AS FA
	    ON AH.ParentId = FA.Id
     WHERE Field = 'FIN_Account_Number__c'
       --AND DataType = 'EntityId'
       --AND AH.CreatedDate < @TODAY  --AVOID PULLING A SUBSET OF DAILY CHANGES FROM TODAY SO LIMIT TO PRIOR DAY CHANGES TO PULL IN COMPLETE LIST     
	   

)

, FullAuditHistoryFIN AS ( 

    SELECT AuditLogId
	     , FinancialAccountId
         , OldFin AS FinAccountNumber
         , FinancialAccountCreatedDate AS CreatedDate
      FROM SFDC_AuditHistory_FIN AS AH
     WHERE RowNum = 1

     UNION 

    SELECT AuditLogId
	     , FinancialAccountId
         , NewFin AS FinAccountNumber
         , CreatedDate
      FROM SFDC_AuditHistory_FIN

     UNION
    
    /*
        SOME HOUSEHOLDS HAVE A FIN ASSIGNED BUT NO HISTORY - UNION THOSE IN TOO
    */

    SELECT '1' AS AuditLogId
	     , FA.Id AS FinancialAccountId
         , FA.FIN_Account_Number__c AS FinAccountNumber
         , FA.CreatedDate
      FROM PCGSF.FinServ__FinancialAccount__c AS FA 
      LEFT
      JOIN (SELECT DISTINCT FinancialAccountId FROM SFDC_AuditHistory_FIN) AS AH
        ON FA.Id = AH.FinancialAccountId
     WHERE FA.FIN_Account_Number__c IS NOT NULL
       AND AH.FinancialAccountId IS NULL    
 
 )

 , UnionAidFinChanges AS ( 

	 SELECT AuditLogId	
		  , FinancialAccountId	
		  , AccountNumber
		  , NULL AS FinAccountNumber
		  , CreatedDate
	   FROM FullAuditHistoryAID

	  UNION

	 SELECT AuditLogId	
		  , FinancialAccountId	
		  , NULL AS AccountNumber
		  , FinAccountNumber	
		  , CreatedDate
	   FROM FullAuditHistoryFIN


)

, GroupAidFin AS ( 

	SELECT AuditLogId	
		 , FinancialAccountId	
		 , AccountNumber	
		 , COUNT(AccountNumber) OVER (PARTITION BY FinancialAccountId ORDER BY CreatedDate) AS GrpAccountNumber
		 , FinAccountNumber	
		 , COUNT(FinAccountNumber) OVER (PARTITION BY FinancialAccountId ORDER BY CreatedDate) AS GrpFinAccountNumber
		 , CreatedDate
	  FROM UnionAidFinChanges

)

, FillValues AS ( 

	SELECT AuditLogId	
		 , FinancialAccountId	
		 --, AccountNumber	
		 , GrpAccountNumber
		 , FIRST_VALUE(AccountNumber) OVER (PARTITION BY FinancialAccountId, GrpAccountNumber ORDER BY CreatedDate) AS AccountNumber
		 --, FinAccountNumber	
		 , GrpFinAccountNumber
		 , FIRST_VALUE(FinAccountNumber) OVER (PARTITION BY FinancialAccountId, GrpFinAccountNumber ORDER BY CreatedDate) AS FinAccountNumber
		 , CreatedDate
	  FROM GroupAidFin	

) 


    select DISTINCT 
           FinancialAccountId	
         , AccountNumber	
         , FinAccountNumber	
         , CreatedDate
      from FillValues
  	  WHERE FinancialAccountId = 'a0F8W00000N5FlKUAV'
 order by createddate
