

/*	STEP 1 
	RECORD COUNT BEFORE TRUNCATE: 
		- TOTAL: 83,158
		- CLEARED: 68,522
		- FUNDED: 68,211

	TRUNCATE REF.ClientOnboarding	
*/
		--83158
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding


	--68522
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL


	--68211
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE FundedDate IS NOT NULL


    TRUNCATE TABLE REF.ClientOnboarding


/*	STEP 2
	REF.ClientOnboarding UPSERT
	
	RECORD COUNT: 
		- TOTAL: 83,158
		- CLEARED: 68,522
		- FUNDED: 68,211

*/

	declare @a uniqueidentifier = newid() 
	exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Fact Tables'

	--83158
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding


	--68522
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL


	--68211
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE FundedDate IS NOT NULL

/*
	STEP 3 
	IDENTIFY FINS THAT HAVE YET TO CLEAR AND MANUALLY UPDATE THEM TO HAVE A CLEARANCE DATE SO WE CAN TEST THE CLIENT ONBOARDING UPSERT 
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



 /*
	STEP 4 - CHECK RECORD IN REF TABLE TO ENSURE CLEARANCE DATE IS MISSING 
 */
	SELECT * 
	  FROM REF.ClientOnboarding
	 WHERE ClientId = '14EE3B1A-646C-E411-940A-0025B50A007D'


 /* 
	STEP 5 - UPDATE FIN TO HAVE A CLEARANCE DATE
 */

	UPDATE Iris.fi_financialaccountBase
	   SET fi_ClearanceDate = '2023-02-14'
	 WHERE fi_FINAccountNumber = '179131'



 /* 
	STEP 5 - RE-RUN UPSERT 

		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 83,158
		- CLEARED: 68,522
		- FUNDED: 68,211

		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 83,158
		- CLEARED: 68,523
		- FUNDED: 68,211

		OUTCOME: WE ADDED A CLEARANCE DATE FOR ONE CLIENT (WITHOUT A FUNDED DATE) SO TOTAL RECORD COUNT AND FUNDED RECORD COUNT STAYS THE SAME BUT THE
				 CLEARED RECORD COUNT INCREMENTS BY ONE.

		TEST RESULT: PASS


 */


	declare @a uniqueidentifier = newid() 
	exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Reference Tables'

	--83158
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding


	--68523
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL


	--68211
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE FundedDate IS NOT NULL



 /* 
	STEP 6 - RE-RUN UPSERT JUST TO SEE IF ANYTHING CHANGES

		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 83,158
		- CLEARED: 68,523
		- FUNDED: 68,211

		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 83,158
		- CLEARED: 68,523
		- FUNDED: 68,211

		OUTCOME: NOTHING CHANGED SO WE DON'T EXPECT TO SEE ANY CHANGES TO THE EXISTING RECORD COUNTS

		TEST RESULT: PASS


 */


 	declare @a uniqueidentifier = newid() 
	exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Reference Tables'


	--83158
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding


	--68523
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL


	--68211
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE FundedDate IS NOT NULL



 /*
	STEP 7 - CHECK RECORD IN REF TABLE TO ENSURE CLEARANCE DATE IS NOW POPULATED 

	TEST RESULT: PASS

 */
	SELECT * 
	  FROM REF.ClientOnboarding
	 WHERE ClientId = '14EE3B1A-646C-E411-940A-0025B50A007D'
