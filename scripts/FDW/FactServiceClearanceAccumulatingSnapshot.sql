
/*	STEP 1 
	TRUNCATE FDW.FactServiceClearanceMilestone
	RECORD COUNT BEFORE TRUNCATE: 162501
*/
	SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone
  TRUNCATE TABLE FDW.FactServiceClearanceMilestone

/*	STEP 2
	RUN SERVICE CLEARNCE MILESTONE BACKFILL
	RECORD COUNT: 158283

*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceMilestoneBackfill] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone

/*	STEP 3
	RUN SERVICE CLEARNCE MILESTONE UPSERT
	RECORD COUNT: 162501
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceMilestone] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceMilestone




/*	STEP 4
	TRUNCATE FDW.FactServiceClearanceAccumulatingSnapshot
	RECORD COUNT BEFORE TRUNCATE: 162501
*/
  SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot
  TRUNCATE TABLE FDW.FactServiceClearanceAccumulatingSnapshot
  
/*	STEP 5
	RUN ACCUMULATING SNAPSHOT BACKFILL
	RECORD COUNT: 162501
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshotBackfill] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot

/*	STEP 6
	RUN ACCUMULATING SNAPSHOT UPSERT
	RECORD COUNT: 162501
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshot] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot
	
SELECT *
  FROM FDW.FactServiceClearanceMilestone
 WHERE CLIENTNUMBER = 4993589

SELECT *
  FROM FDW.FactServiceClearanceAccumulatingSnapshot
 WHERE CLIENTNUMBER = 4993589



/*	STEP 4
	ADD TEST RECORDS TO FACT SERVICE CLEARANCE MILESTONE
	INSERT COUNT: 2 (ONE FOR CLEARED, ONE FOR FUNDED) 
	RECORD COUNT: 
*/

DECLARE @GetDate DATETIME = GETDATE()

INSERT 
  INTO FDW.FactServiceClearanceMilestone (
	   DimDateKey	
	 , DimClientKey	
	 , ClientId	
	 , ClientNumber	
	 , DimClientAssetsKey	
	 , DimClientTenureKey	
     , DimAgeGroupKey	
     , DimEmployeeKey	
     , DimEmployeeMgmtKey	
     , DimTeamMemberKey	
     , DimPeerGroupKey	
     , DimClearanceMilestoneKey	
     , MilestoneCount	
     , SignedAmountUSD	
     , TargetAssetsUSD	
     , DWCreatedDateTime	
     , DWUpdatedDateTime	
     , ETLJobProcessRunId	
     , ETLJobSystemRunId
)
--select DimClearanceMilestoneKey from fdw.dimclearancemilestone where Milestone = 'Clearance'
--select DimEmployeeKey from fdw.dimemployee where ActiveDirectoryUserId = 'vpham' order by effectivestartdate
VALUES (20160815
		,328740	
		,'48498741-646C-E411-940A-0025B50A007D'
		,4993589	
		,-1	
		,-1	
		,-1
		,2768 --employee key belongs to vu pham	
		,-1	
		,-1	
		,-1	
		,79 --clearance
		,1	
		,null
		,null
		,@GetDate	
		,@GetDate
		,'A1BA9BB9-B3C5-49E5-B2C7-F594A8A1373D'
		,'A1BA9BB9-B3C5-49E5-B2C7-F594A8A1373D')

DECLARE @GetDate2 DATETIME = GETDATE()

INSERT 
  INTO FDW.FactServiceClearanceMilestone (
	   DimDateKey	
	 , DimClientKey	
	 , ClientId	
	 , ClientNumber	
	 , DimClientAssetsKey	
	 , DimClientTenureKey	
     , DimAgeGroupKey	
     , DimEmployeeKey	
     , DimEmployeeMgmtKey	
     , DimTeamMemberKey	
     , DimPeerGroupKey	
     , DimClearanceMilestoneKey	
     , MilestoneCount	
     , SignedAmountUSD	
     , TargetAssetsUSD		
     , DWCreatedDateTime	
     , DWUpdatedDateTime	
     , ETLJobProcessRunId	
     , ETLJobSystemRunId
)

VALUES (20161103
		,328740	
		,'48498741-646C-E411-940A-0025B50A007D'
		,4993589	
		,-1	
		,-1	
		,-1	
		,7669  --employee key belongs to brian crawford
		,-1	
		,-1	
		,-1	
		,139 --funded
		,1	
		,null
		,null
		,@GetDate2
		,@GetDate2
		,'A1BA9BB9-B3C5-49E5-B2C7-F594A8A1373D'
		,'A1BA9BB9-B3C5-49E5-B2C7-F594A8A1373D')


/*	STEP 7
	RUN ACCUMULATING SNAPSHOT UPSERT AGAIN AFTER ADDING 2 NEW TEST RECORDS TO FACT SERVICE CLEARANCE MILESTONE

	EXPECTED OUTCOME: RECORD COUNT SHOULD INCREASE BY 2 SINCE WE ADDED TWO NEW RECORDS TO THE 

	RECORD COUNT BEFORE: 162501
	RECORD COUNT AFTER: 162503
*/

declare @a uniqueidentifier = newid() 
exec [FDW].[spUpsertFactServiceClearanceAccumulatingSnapshot] @a, @a, 'Transform and Load Fact Tables'

SELECT COUNT(1) FROM FDW.FactServiceClearanceAccumulatingSnapshot

SELECT *
  FROM FDW.FactServiceClearanceMilestone
 WHERE CLIENTNUMBER = 4993589

SELECT *
  FROM FDW.FactServiceClearanceAccumulatingSnapshot
 WHERE CLIENTNUMBER = 4993589
 order by 3



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
