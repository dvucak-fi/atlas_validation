/*
	STEP 1: CREATE BACKUP OF EXISITNG FCSSD (NO CALL CYCLE FIELDS)
	
		--> FDW.FactClientSnapshotDaily_08222023
*/

/*
	STEP 2: DROP EXISTING TABLE. I'VE ALREADY MADE THE REQUIRED CHANGES IN DEV MANUALLY. WE'LL SCRAP THAT AND RE-RUN EVERYTHING AS IT WOULD IN AN ACTUAL DEPLOYMENT
*/

	DROP TABLE FDW.FactClientSnapshotDaily

/*
	STEP 3: CTAS FDW.FactClientSnapshotDaily_08222023 INTO FDW.FactClientSnapshotDaily WITHOUT THE NEW CHANGES I MANUALLY ADDED
*/


CREATE TABLE FDW.FactClientSnapshotDaily
WITH
(
	DISTRIBUTION = HASH ( [DimClientKey] ),
	CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT * 
  FROM FDW.FactClientSnapshotDaily_08222023



/*
	STEP 4: RUN POST DEPLOYMENT SCRIPT TO ADD DimCallCycle FK and New CallCycleContact Measure
*/

			CREATE TABLE FDW.FactClientSnapShotDaily_NEW
			WITH
			(
				DISTRIBUTION = HASH ( [DimClientKey] ),
				CLUSTERED COLUMNSTORE INDEX
			)
			AS
			SELECT DimDateKey
				 , DimClientKey
				 , DimTeamMemberKey
				 , DimClientAssetsKey
				 , DimClientTenureKey
				 , DimEmployeeKey
				 , DimEmployeeMgmtKey
				 , DimAgeGroupKey
				 , DimKYCStatusKey
				 , DimPeerGroupKey
				 , -1 AS DimContactFrequencyKey
				 , ClientId
				 , ClientNumber
				 , AssetsUnderManagementUSD
				 , NetLiquidAssets
				 , TotalLiquidAssets
				 , TotalLiabilities
				 , NetWorthUSD
				 , ContactLast90
				 , BioReviewLast365
				 , NetworkReviewLast365
				 , SuitWizReviewLast365
	             , DaysSinceLastContact
	             , DaysSinceLastAttempt
	             , DaysSinceLastVirtualMeeting
	             , DaysSinceLastInPersonMeeting
				 , NULL AS ContactFrequencyViolation
				 , ClientCount
				 , DWCreatedDateTime
				 , DWUpdatedDateTime
				 , ETLJobProcessRunId
				 , ETLJobSystemRunId
			  FROM FDW.FactClientSnapShotDaily

			--CHECK TO MAKE SURE RECORD COUNTS MATCH BEFORE DROPPING TABLE
			IF (SELECT COUNT(1) FROM FDW.FactClientSnapShotDaily) = (SELECT COUNT(1) FROM FDW.FactClientSnapShotDaily_NEW)
			BEGIN
				DROP TABLE FDW.FactClientSnapShotDaily
			END 

			--RECREATE FCSSD
			CREATE TABLE FDW.FactClientSnapShotDaily
			WITH
			(
				DISTRIBUTION = HASH ( [DimClientKey] ),
				CLUSTERED COLUMNSTORE INDEX
			)
			AS
			SELECT DimDateKey
				 , DimClientKey
				 , DimTeamMemberKey
				 , DimClientAssetsKey
				 , DimClientTenureKey
				 , DimEmployeeKey
				 , DimEmployeeMgmtKey
				 , DimAgeGroupKey
				 , DimKYCStatusKey
				 , DimPeerGroupKey
				 , DimContactFrequencyKey
				 , ClientId
				 , ClientNumber
				 , AssetsUnderManagementUSD
				 , NetLiquidAssets
				 , TotalLiquidAssets
				 , TotalLiabilities
				 , NetWorthUSD
				 , ContactLast90
				 , BioReviewLast365
				 , NetworkReviewLast365
				 , SuitWizReviewLast365
	             , DaysSinceLastContact
	             , DaysSinceLastAttempt
	             , DaysSinceLastVirtualMeeting
	             , DaysSinceLastInPersonMeeting
				 , ContactFrequencyViolation
				 , ClientCount
				 , DWCreatedDateTime
				 , DWUpdatedDateTime
				 , ETLJobProcessRunId
				 , ETLJobSystemRunId
			  FROM FDW.FactClientSnapShotDaily_NEW

			--DROP FCSSD TEMP OBJECT
			DROP TABLE FDW.FactClientSnapShotDaily_NEW


/*
	STEP 5: CHECK CALL CYCLE COUNTS. ALL DimCallCycleKey FKs SHOULD BE -1
*/

	SELECT F.DimContactFrequencyKey
		 , CF.ContactFrequencyInDays
		 , COUNT(1) AS RecCount
	  FROM FDW.FactClientSnapshotDaily F
	  LEFT
	  JOIN FDW.DimContactFrequency AS CF 
		ON F.DimContactFrequencyKey = CF.DimContactFrequencyKey
	 WHERE F.DimDateKey = 20230821
	 GROUP 
	    BY F.DimContactFrequencyKey
         , CF.ContactFrequencyInDays
	 ORDER 
	    BY COUNT(1) DESC


/*
	STEP 6: RUN ADF SCRIPT TO UPDATE DimCallCycleKeys and CallCycleContact Measure in Fact Table
*/

	--> RUN: FDW Post Deployment Synapse Pipeline
	--> RUN TIME: 28m 56s


/*
	STEP 7:  CHECK CALL CYCLE COUNTS - SHOULD NOT HAVE -1 DimCallCycleKeys ANY LONGER
			 CHECK TOTAL RECORD COUNT (229,203,419)
*/


	SELECT F.DimContactFrequencyKey
		 , CF.ContactFrequencyInDays
		 , COUNT(1) AS RecCount
	  FROM FDW.FactClientSnapshotDaily F
	  LEFT
	  JOIN FDW.DimContactFrequency AS CF 
		ON F.DimContactFrequencyKey = CF.DimContactFrequencyKey
	 WHERE F.DimDateKey = 20230821
	 GROUP 
	    BY F.DimContactFrequencyKey
         , CF.ContactFrequencyInDays
	 ORDER 
	    BY COUNT(1) DESC

	
	SELECT COUNT(1) FROM FDW.FactClientSnapshotDaily


/*
	STEP 8:  RUN DAILY UPSERT AND ENSURE RECORD COUNTS MATCH WHAT THEY WERE IN STEP 7 
			 CHECK TOTAL RECORD COUNT (229,203,419)
*/

	DECLARE @A UNIQUEIDENTIFIER = NEWID() 
	EXEC [FDW].[spUpsertFactClientSnapshotDaily] @A, @A, 'Transform and Load Fact Tables'

	SELECT COUNT(1) FROM FDW.FactClientSnapshotDaily


/*
	STEP 9: FACT TABLE GRAIN VALIDATION - ONE RECORD PER CLIENT PER DAY - QUERY SHOULD NOT RETURN ANY RECORDS 
*/

	SELECT ClientNumber
	     , DimDateKey
		 , COUNT(1) 
	  FROM FDW.FactClientSnapshotDaily
	 GROUP 
	    BY ClientNumber
	     , DimDateKey
	HAVING COUNT(1) > 1 


/*
	STEP 10: BACKFILL VALIDATION 
			 WE NEED TO VALIDATE THE BACKFILL IN THE EVENT THAT WE NEED TO RUN IT AT SOME POINT 

			 CHECK CURRENT FACT TABLE RECORD COUNT: 229,203,419
			 TRUNCATE TABLE: TRUNCATE TABLE FDW.FactClientSnapshotDaily
			 RUN BACKFILL: DECLARE @A UNIQUEIDENTIFIER = NEWID() 
						   EXEC [FDW].[spUpsertFactClientSnapshotDailyBackfill] @A, @A, 'Transform and Load Fact Tables'
			 RUN TIME: 39m 25s
			 CHECK NEW RECORD COUNT: 229,203,419

*/

	TRUNCATE TABLE FDW.FactClientSnapshotDaily

	DECLARE @A UNIQUEIDENTIFIER = NEWID() 
	EXEC [FDW].[spUpsertFactClientSnapshotDailyBackfill] @A, @A, 'Transform and Load Fact Tables'

	SELECT COUNT(1) FROM FDW.FactClientSnapshotDaily
