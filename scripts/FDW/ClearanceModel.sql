
/*	STEP 1 
	TRUNCATE FDW.FactServiceClearanceMilestone
	RECORD COUNT BEFORE TRUNCATE: 219,271
*/
	SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone
    TRUNCATE TABLE FDW.FactServiceClearanceMilestone


/*	STEP 2
	RUN SERVICE CLEARNCE MILESTONE UPSERT
	RECORD COUNT: 219,271
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceMilestone] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone


/*	STEP 3
	TRUNCATE FDW.FactServiceClearanceAccumulatingSnapshot
	RECORD COUNT BEFORE TRUNCATE: 219,271
*/
  SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot
  TRUNCATE TABLE FDW.FactServiceClearanceAccumulatingSnapshot
  

/*	STEP 4
	RUN ACCUMULATING SNAPSHOT BACKFILL
	RECORD COUNT: 219,271
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshotBackfill] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot

/*	STEP 5
	RUN ACCUMULATING SNAPSHOT UPSERT
	RECORD COUNT: 219,271
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshot] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot

SELECT * FROM FDW.FactServiceClearanceAccumulatingSnapshot ORDER BY DWCreatedDateTime DESC

SELECT * FROM FDW.FactServiceClearanceAccumulatingSnapshot WHERE CLIENTNUMBER = 1021942 
ORDER BY 2, 3, 4

SELECT * FROM REF.ClientOnboarding WHERE ClientNumber = 1021942
SELECT * FROM FDW.FactServiceClearanceMilestone WHERE ClientNumber = 1021942 ORDER BY 1


SELECT *
  FROM FDW.FactServiceClearanceMilestone
 WHERE CLIENTNUMBER = 4993589

SELECT *
  FROM FDW.FactServiceClearanceAccumulatingSnapshot
 WHERE CLIENTNUMBER = 4993589

 SELECT ClientNumber
      , DimInitialAccountSetupDateKey 
	  , COUNT(1) 
   FROM FDW.FactServiceClearanceAccumulatingSnapshot
  WHERE CurrentRecord = 1 
 GROUP BY ClientNumber
      , DimInitialAccountSetupDateKey 
	HAVING COUNT(1) > 1


/*
	STEP 6 
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
			   AND CO.ResellDate IS NULL


 /*
	STEP 7 - CHECK RECORD IN REF TABLE TO ENSURE CLEARANCE DATE IS MISSING 
 */
	SELECT * 
	  FROM REF.ClientOnboarding
	 WHERE ClientId = '26BE0F0B-646C-E411-940A-0025B50A007D'


 /* 
	STEP 8 - UPDATE FIN TO HAVE A CLEARANCE DATE
 */

	UPDATE Iris.fi_financialaccountBase
	   SET fi_ClearanceDate = GETDATE()
	 WHERE fi_FINAccountNumber = '362989'




 /* 
	STEP 9 - RUN REF CLIENT ONBOARDING TABLE UPSERT NOW THAT A NEW CLIENT "CLEARED"
		
		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,527
		- FUNDED: 64,135
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 78,965
		- CLEARED: 64,528
		- FUNDED: 64,135

		EXPECTED OUTCOME: WE UPDATED A NEW FINANCIAL ACCOUNT RECORD TO MAKE IT SEEM LIKE IT CLEARED SO THE TOTAL RECORD COUNT AND 
						  FUNDED RECORD COUNT SHOULD STAY THE SAME BUT THE CLEARED RECORD COUNT SHOULD INCREMENT BY ONE.
		
		TEST RESULT: PASS
 */


	declare @a uniqueidentifier = newid() 
	exec [REF].[spUpsertClientOnboarding] @a, @a, 'Transform and Load Reference Tables'

	--
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding


	--
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE ClearanceDate IS NOT NULL


	--
	SELECT COUNT(1)
	  FROM REF.ClientOnboarding
	 WHERE FundedDate IS NOT NULL



 /* 
	STEP 10 - NEXT WE NEED TO MAKE SURE THIS RECORD MAKE IT TO THE FACT SERVICE CLEARANCE MILESTONE TABLE 

	EXPECTED OUTCOME: RECORD COUNT INCREASES BY 1 TO ACCOUNT FOR NEW CLEARED ACCOUNT

		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 219,271
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 219,272

	TEST RESULT: PASS

*/

	SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone

	declare @a uniqueidentifier = newid() 
	exec [FDW].[spUpsertFactServiceClearanceMilestone] @a, @a, 'Transform and Load Fact Tables'


/*	STEP 11
	RUN ACCUMULATING SNAPSHOT UPSERT AGAIN TO MAKE SURE THE NEWLY "CLEARED" ACCOUNT IS FLOWING THROUGH TO THE ACCUMULATING SNAPSHOT
	
		RECORD COUNT PRIOR TO RUNNING UPSERT: 
		- TOTAL: 219,271
		- CLEARED: 128,662
		- FUNDED: 64,135
		
		RECORD COUNT AFTER UPSERT: 
		- TOTAL: 219,272
		- CLEARED: 128,663
		- FUNDED: 64,135


*/

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot
SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot WHERE DimClearanceDateKey IS NOT NULL
SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot WHERE DimFundedDateKey IS NOT NULL


declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshot] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot
SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot WHERE DimClearanceDateKey IS NOT NULL
SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot WHERE DimFundedDateKey IS NOT NULL





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


-- DUPES CAN EXIST HERE - THAT JUST MEANS THAT WE HAVE CLIENT THAT HAVE MULTIPLE INITIAL ACCOUNT SETUP INCIDENTS. THE MOST WE'VE SEEN IS 3 THUS FAR
with recs as ( 
SELECT *
  FROM FDW.FactServiceClearanceAccumulatingSnapshot
 WHERE CurrentRecord = 1
)

select clientid
     , count(1)
  from recs 
  group by clientid 
  having count(1) > 1
  order by count(1) desc

SELECT *
  FROM FDW.FactServiceClearanceAccumulatingSnapshot
 WHERE CurrentRecord = 1
   and clientid = 'C3F526F6-636C-E411-940A-0025B50A007D'

--CHECK TO SEE IF THERE ARE ANY CLIENTS WITH DUPLICATE INITIAL ACCOUNT SETUP INCIDENTS. SHOULD RETURN 0 RECORDS. 
  select ClientNumber
       , DimInitialAccountSetupDateKey
	   , count(1) 
    from FDW.FactServiceClearanceAccumulatingSnapshot
	where currentrecord = 1
	group by ClientNumber
       , DimInitialAccountSetupDateKey
	having count(1) > 1  


SELECT * 
  FROM  FDW.FactServiceClearanceAccumulatingSnapshot
  WHERE CLIENTNUMBER = 5821449
