/*	STEP 1 REF.ClientOnboarding IS THE OBJECT THAT DRIVES ALL CLEARANCE RECORDS 
	RECORD COUNT BEFORE TRUNCATE: 
		- TOTAL: 78,965
		- CLEARED: 64,528
		- RESELL: 11,649

	TRUNCATE REF.ClientOnboarding	
*/

			SELECT COUNT(1)
			  FROM REF.ClientOnboarding

			SELECT COUNT(1)
			  FROM REF.ClientOnboarding
			 WHERE ClearanceDate IS NOT NULL

			SELECT COUNT(1)
			  FROM REF.ClientOnboarding
			 WHERE reselldate IS NOT NULL

			TRUNCATE TABLE REF.ClientOnboarding


/*	
	STEP 2 - RELOAD REF.ClientOnboarding
	
	RECORD COUNT: 
		- TOTAL: 78,965
		- CLEARED: 68,528
		- RESELL: 11,649

*/

			declare @a uniqueidentifier = newid() 
			exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Fact Tables'

			--78965
			SELECT COUNT(1)
			  FROM REF.ClientOnboarding


			--64528
			SELECT COUNT(1)
			  FROM REF.ClientOnboarding
			 WHERE ClearanceDate IS NOT NULL


			--11649
			SELECT COUNT(1)
			  FROM REF.ClientOnboarding
			 WHERE RESELLDATE IS NOT NULL


/*
	STEP 3 - RELOAD FactServiceClearanceMilestone

	EXPECTED RESULT: THE TOTAL, CLEARED, AND RESELL COUNTS SHOULD MATCH BETWEEN REF.ClientOnboarding and FactServiceClearanceMilestone

			RECORD COUNT FROM REF.ClientOnboarding: 
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

*/
			--TRUNCATE OBJECT
			TRUNCATE TABLE FDW.FactServiceClearanceMilestone

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].[spUpsertFactServiceClearanceMilestone] @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   SUM(CASE WHEN DimClearanceMilestoneKey = 19 THEN 1 ELSE 0 END) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceMilestoneKey = 79 THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimClearanceMilestoneKey = 3 THEN 1 ELSE 0 END) AS ResellCount
			  FROM FDW.FactServiceClearanceMilestone

/*

			RECORD COUNT FROM FDW.FactServiceClearanceMilestone: 
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

			RECORD COUNTS SHOULD MATCH WHAT IS IN THE CLIENT ONBAORDING REF TABLE 

		    TEST RESULT: EXACT MATCH - PASS

*/



/*
	STEP 4 - RELOAD FactServiceClearanceAccumulatingSnapshot			
*/

			--TRUNCATE OBJECT
			TRUNCATE TABLE FDW.FactServiceClearanceAccumulatingSnapshot

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceClearanceAccumulatingSnapshotBackfill @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM FDW.FactServiceClearanceAccumulatingSnapshot
			 WHERE CurrentRecord = 1 

/*

			RECORD COUNT FROM FDW.FactServiceClearanceAccumulatingSnapshot AFTER BACKFILL
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

			RECORD COUNTS SHOULD MATCH WHAT IS IN THE CLIENT ONBAORDING REF TABLE 

		    TEST RESULT: EXACT MATCH - PASS

*/


/*
	STEP 5 - REPEAT THE SAME PROCESS FOR FactServiceClearanceAccumulatingSnapshot BUT WITH THE STANDARD UPSERT
*/

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceClearanceAccumulatingSnapshot @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM FDW.FactServiceClearanceAccumulatingSnapshot
			 WHERE CurrentRecord = 1 


/*

			RECORD COUNT FROM FDW.FactServiceClearanceAccumulatingSnapshot AFTER UPSERT:
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

			RECORD COUNTS SHOULD MATCH WHAT IS IN THE CLIENT ONBAORDING REF TABLE 

		    TEST RESULT: EXACT MATCH - PASS

*/


/*
	STEP 6 - RELOAD FactServiceAssignment 
*/

			--TRUNCATE OBJECT
			TRUNCATE TABLE FDW.FactServiceAssignment

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceAssignmentBackfill @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM ( 
						SELECT ClientNumber
							 , DimInitialAccountSetupDateKey
							 , MAX(DimClearanceDateKey) AS DimClearanceDateKey
							 , MAX(DimResellDateKey) AS DimResellDateKey
						  FROM FDW.FactServiceAssignment
						 GROUP BY ClientNumber, DimInitialAccountSetupDateKey
				   ) AS A
				  
/*

			RECORD COUNT FROM FDW.FactServiceAssignment AFTER BACKFILL:
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

			RECORD COUNTS SHOULD MATCH WHAT IS IN THE CLIENT ONBAORDING REF TABLE 

		    TEST RESULT: EXACT MATCH - PASS

*/


/*
	STEP 7 - REPEAT SAME PROCESS FOR FactServiceAssignment BUT WITH DAILY UPSERT
*/

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceAssignment @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM ( 
						SELECT ClientNumber
							 , DimInitialAccountSetupDateKey
							 , MAX(DimClearanceDateKey) AS DimClearanceDateKey
							 , MAX(DimResellDateKey) AS DimResellDateKey
						  FROM FDW.FactServiceAssignment
						 GROUP BY ClientNumber, DimInitialAccountSetupDateKey
				   ) AS A


/*

			RECORD COUNT FROM FDW.FactServiceAssignment AFTER UPSERT:
			- TOTAL: 78,965
			- CLEARED: 64,528
			- RESELL: 11,649

			RECORD COUNTS SHOULD MATCH WHAT IS IN THE CLIENT ONBAORDING REF TABLE 

		    TEST RESULT: EXACT MATCH - PASS

*/


/*
	STEP 8 - IDENTIFY FINS THAT HAVE YET TO CLEAR AND MANUALLY UPDATE THEM TO HAVE A CLEARANCE DATE SO WE CAN TEST THE CLIENT ONBOARDING UPSERT 
*/
   			SELECT DISTINCT 
				   fi_ContactId AS ClientId 
				 , fi_FINAccountNumber AS FinAccountNumber
				 , fi_ClearanceDate AS ClearanceDate
			  FROM Iris.fi_financialaccountBase AS FAB   
			  JOIN REF.HistoricalAccountAttributes AS HAA
			    ON FAB.fi_FINAccountNumber = HAA.FinAccountNumber
			  JOIN REF.ClientOnboarding AS CO
			    ON FAB.fi_ContactId = CO.ClientId 			  
			 WHERE FAB.fi_ClearanceDate IS NULL
			   AND FAB.fi_FINAccountNumber IS NOT NULL
			   AND FAB.fi_managedcode = 157610000  --MANAGED
			   AND CO.ClearanceDate IS NULL
			   AND CO.ResellDate IS NULL

 /*
	STEP 9 - CHECK RECORD IN REF TABLE TO ENSURE CLEARANCE DATE IS MISSING 
 */
			SELECT * 
			  FROM REF.ClientOnboarding
			 WHERE ClientId = '8FDBCE5D-E153-E611-940F-0025B50B0059'

			SELECT fi_FINAccountNumber  
				 , fi_ClearanceDate
			  FROM Iris.fi_financialaccountBase
			 WHERE fi_ContactId = '8FDBCE5D-E153-E611-940F-0025B50B0059'

	 


 /* 
	STEP 10 - UPDATE FIN TO HAVE A CLEARANCE DATE
 */

			UPDATE Iris.fi_financialaccountBase
			   SET fi_ClearanceDate = GETDATE()
			 WHERE fi_FINAccountNumber = '156323'


/* 
	STEP 11 - RUN REF CLIENT ONBOARDING TABLE UPSERT NOW THAT A NEW CLIENT "CLEARED"
		
		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,528
		- RESELL: 11,649
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,529
		- RESELL: 11,649
		EXPECTED OUTCOME: WE UPDATED A NEW FINANCIAL ACCOUNT RECORD TO MAKE IT SEEM LIKE IT CLEARED SO THE TOTAL RECORD COUNT AND 
						  RESELL RECORD COUNT SHOULD STAY THE SAME BUT THE CLEARED RECORD COUNT SHOULD INCREMENT BY ONE.
		
		TEST RESULT: PASS
 */


	declare @a uniqueidentifier = newid() 
	exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Reference Tables'

	--
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding

	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL

	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ResellDate IS NOT NULL


/* 
	STEP 12 - RUN THE FactServiceClearanceMilestone UPSERT NOW THAT A NEW CLIENT "CLEARED"
		
		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,528
		- RESELL: 11,649
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,529
		- RESELL: 11,649
		EXPECTED OUTCOME: WE UPDATED A NEW FINANCIAL ACCOUNT RECORD TO MAKE IT SEEM LIKE IT CLEARED SO THE TOTAL RECORD COUNT AND 
						  RESELL RECORD COUNT SHOULD STAY THE SAME BUT THE CLEARED RECORD COUNT SHOULD INCREMENT BY ONE.
		
		TEST RESULT: PASS
 */


			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].[spUpsertFactServiceClearanceMilestone] @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   SUM(CASE WHEN DimClearanceMilestoneKey = 19 THEN 1 ELSE 0 END) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceMilestoneKey = 79 THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimClearanceMilestoneKey = 3 THEN 1 ELSE 0 END) AS ResellCount
			  FROM FDW.FactServiceClearanceMilestone



			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceClearanceAccumulatingSnapshot @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM FDW.FactServiceClearanceAccumulatingSnapshot
			 WHERE CurrentRecord = 1 



/*
	STEP 13 - REPEAT SAME PROCESS FOR FactServiceAssignment NOW THAT A NEW CLIENT "CLEARED"

		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,528
		- FUNDED: 11,649
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,529
		- FUNDED: 11,649
		EXPECTED OUTCOME: WE UPDATED A NEW FINANCIAL ACCOUNT RECORD TO MAKE IT SEEM LIKE IT CLEARED SO THE TOTAL RECORD COUNT AND 
						  RESELL RECORD COUNT SHOULD STAY THE SAME BUT THE CLEARED RECORD COUNT SHOULD INCREMENT BY ONE.
		
		TEST RESULT: PASS

*/

			--RUN UPSERT
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceAssignment @a, @a, 'Transform and Load Fact Tables'

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM ( 
						SELECT ClientNumber
							 , DimInitialAccountSetupDateKey
							 , MAX(DimClearanceDateKey) AS DimClearanceDateKey
							 , MAX(DimResellDateKey) AS DimResellDateKey
						  FROM FDW.FactServiceAssignment
						 GROUP BY ClientNumber, DimInitialAccountSetupDateKey
				   ) AS A



/*
	STEP 13 - ADD A NEW IC ASSIGNMENT TO AN EXISTING OPEN OPPORTUNITY 

	EXPECTED RESULT: 
		DISTINCT INITIAL ACCOUNT SETUP, CLEARED, AND RESELL RECORD COUNTS SHOULD NOT CHANGE
		TOTAL RECORD COUNT WITHIN FDW.FactServiceAssignmentShould INCREMENT BY ONE

		RECORD COUNT BEFORE UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,529
		- FUNDED: 11,649

		TOTAL RECORD COUNT BEFORE UPSERT:: 87,893

*/

	        --CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM ( 
						SELECT ClientNumber
							 , DimInitialAccountSetupDateKey
							 , MAX(DimClearanceDateKey) AS DimClearanceDateKey
							 , MAX(DimResellDateKey) AS DimResellDateKey
						  FROM FDW.FactServiceAssignment
						 GROUP BY ClientNumber, DimInitialAccountSetupDateKey
				   ) AS A

			SELECT COUNT(1) FROM FDW.FactServiceAssignment

			--SELECT *
			--  FROM REF.ClientOnboarding 
			--  WHERE CLEARANCEDATE IS NULL AND RESELLDATE IS NULL 


			--SELECT * 
			--  FROM FDW.FactServiceAssignment 
			-- WHERE ClientNumber = 3908396

			SELECT * 
			  FROM REF.RelationshipManagementAssignment 
			 WHERE ClientNumber = 3908396
			 order by AssignmentStartDate		

			SELECT * 
			  FROM [REF].[vwRelationshipManagementAssignmentWindow]
			 WHERE ClientNumber = 3908396
			 order by AssignmentwINDOWStartDate		

			 INSERT 
			   INTO REF.RelationshipManagementAssignment (
					ClientId	
					,ClientNumber	
					,RelationshipManagementTypeCode	
					,RelationshipManagementTypeCodeName	
					,AssignedToGUID	
					,AssignedToUserId	
					,AssignedToActiveDirectoryUserIdWithDomain	
					,AssignedToFullName	
					,AssignmentStartDate	
					,AssignmentEndDate	
					,IsCanceledAssignment	
					,IsCompletedAssignment	
					,IsEndOfDayAssignment	
					,DWCreatedDateTime	
					,DWUpdatedDateTime	
					,ETLJobProcessRunId	
					,ETLJobSystemRunId
			)

			SELECT top 1 
			       '7D402DC6-636C-E411-940A-0025B50A007D'	
			       ,3908396	
				   ,RelationshipManagementTypeCode	
				   ,RelationshipManagementTypeCodeName	
				   ,AssignedToGUID	
				   ,AssignedToUserId	
				   ,AssignedToActiveDirectoryUserIdWithDomain	
				   ,AssignedToFullName	
				   ,'2017-10-31 11:42:23.000'	
				   ,'9999-12-31 00:00:00.000'	
				   ,IsCanceledAssignment	
				   ,IsCompletedAssignment	
				   ,1	
				   ,DWCreatedDateTime	
				   ,DWUpdatedDateTime	
				   ,ETLJobProcessRunId	
				   ,ETLJobSystemRunId

			  FROM REF.RelationshipManagementAssignment 
			 WHERE AssignedToUserId = 'vpham'

			--UPDATE REF.RelationshipManagementAssignment 
			--   SET AssignmentEndDate = '2017-10-31 11:42:23.000'
			-- WHERE ClientNumber = 3908396
			--   and AssignmentStartDate = '2017-09-25 11:42:23.000'



		    --RUN UPSERT NOW THAT WE HAVE ADDED A NEW "ASSIGNMENT" 
			declare @a uniqueidentifier = newid() 
			exec [FDW].spUpsertFactServiceAssignment @a, @a, 'Transform and Load Fact Tables'


			SELECT * 
			  FROM FDW.FactServiceAssignment 
			 WHERE ClientNumber = 3908396

			SELECT * 
			  FROM REF.RelationshipManagementAssignment 
			 WHERE ClientNumber = 3908396
			 order by AssignmentStartDate


			--CHECK RECORD COUNTS
			SELECT 
				   COUNT(1) AS IASCount			   
				 , SUM(CASE WHEN DimClearanceDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ClearanceCount
				 , SUM(CASE WHEN DimResellDateKey IS NOT NULL THEN 1 ELSE 0 END) AS ResellCount 
			  FROM ( 
						SELECT ClientNumber
							 , DimInitialAccountSetupDateKey
							 , MAX(DimClearanceDateKey) AS DimClearanceDateKey
							 , MAX(DimResellDateKey) AS DimResellDateKey
						  FROM FDW.FactServiceAssignment
						 GROUP BY ClientNumber, DimInitialAccountSetupDateKey
				   ) AS A

			SELECT COUNT(1) FROM FDW.FactServiceAssignment

/*

		RECORD COUNT BEFORE UPSERT: 
		- TOTAL: 78,965 --> 78,965
		- CLEARED: 64,529 --> 64,529
		- FUNDED: 11,649 --> 11,649

		TOTAL RECORD COUNT BEFORE UPSERT: 87,893 --> 87,894

		TEST RESULT: PASS

*/



 /* 
	STEP 14 - RESET DEFAULT VALUES 
 */

			UPDATE Iris.fi_financialaccountBase
			   SET fi_ClearanceDate = NULL
			 WHERE fi_FINAccountNumber = '156323'

			DELETE 
			  FROM REF.RelationshipManagementAssignment 
			 WHERE ClientNumber = 3908396
			  AND AssignedToUserId = 'vpham'




--CHECK TO SEE IF THERE ARE ANY CLIENTS WITH DUPLICATE INITIAL ACCOUNT SETUP INCIDENTS. SHOULD RETURN 0 RECORDS. 
  select ClientNumber
       , DimInitialAccountSetupDateKey
	   , count(1) 
    from FDW.FactServiceClearanceAccumulatingSnapshot
	where currentrecord = 1
	group by ClientNumber
       , DimInitialAccountSetupDateKey
	having count(1) > 1  

--WE SHOULD NEVER HAVE MORE CLEARANCES AND FUNDED RECORDS THAN INITIAL ACCOUNT SETUP RECORDS
WITH RecordCounts AS ( 

	SELECT CLIENTNUMBER
		 , sum(case when DimClearanceMilestoneKey = 19 then 1 else 0 end) as InitialAccountSetupRecordCount
		 , sum(case when DimClearanceMilestoneKey = 79 then 1 else 0 end) as ClearanceRecordCount
		 , sum(case when DimClearanceMilestoneKey = 139 then 1 else 0 end) as FundedRecordCount
	  FROM FDW.FactServiceClearanceMilestone
	 --WHERE CLIENTNUMBER = 4993589
	 group by CLIENTNUMBER

)

SELECT * 
  FROM RecordCounts
 WHERE ClearanceRecordCount > InitialAccountSetupRecordCount
    OR FundedRecordCount > InitialAccountSetupRecordCount

--CHECK TO SEE IF THERE'S ANY RECORDS WITH THE SAME IC ASSIGNMENT. SHOULD NOT RETURN ANY RECORDS 
SELECT ClientNUmber
     , INitialAccountSetupDate
	 , AssignmentStartDate
	 , AssignedToUserId 
	 , COUNT(1)
  FROM FDW.FactServiceAssignment 
 GROUP BY ClientNUmber
     , INitialAccountSetupDate
	 , AssignmentStartDate
	 , AssignedToUserId 
HAVING COUNT(1) > 1


SELECT * 
  FROM FDW.FACTSERVICEASSIGNMENT 
  WHERE ClientNUmber = 6263462
