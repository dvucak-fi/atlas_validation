
   
/****
    REF.AssignmentHistoryIC Validaiton 
***/



--Check for duplicate active client records: PASS
   select ClientId_Iris
       , count(*)
    from REF.AssignmentHistoryIC
    where CurrentRecord = 1 
    group by ClientId_Iris
    having count(*) > 1

--Check for duplicate assignment start date records: PASS
   select ClientId_Iris
        , AssignmentStartDate
        , count(*)
    from REF.AssignmentHistoryIC
    where CurrentRecord = 1 
    group by ClientId_Iris
        , AssignmentStartDate
    having count(*) > 1


--Check for Start/End dates that don't line up: PASS
select * 
  from ( 
            select 
                   ClientId_Iris
                 , AssignmentEndDate
                 , LEAD (AssignmentStartDate, 1, '9999-12-31') OVER (PARTITION BY ClientId_Iris ORDER BY AssignmentStartDate) AS NextStartDate
                 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 1 
                    WHEN AssignmentEndDate = LEAD (AssignmentStartDate, 1, '9999-12-31') OVER (PARTITION BY ClientId_Iris ORDER BY AssignmentStartDate)
                    THEN 1 
                    ELSE 0 
                   END EndDateMatchesNextStartDate
              from REF.AssignmentHistoryIC) as a 
    where a.EndDateMatchesNextStartDate = 0 

--Check for duplicate RowHashs: PASS
select * 
  from ( 
            select 
                   ClientId_Iris
                 , RowHash
                 , LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId_Iris ORDER BY AssignmentStartDate) AS NextRowHash
                 , AssignmentStartDate
				 , AssignmentEndDate
				 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 0
                    WHEN RowHash = LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId_Iris ORDER BY AssignmentStartDate)
                    THEN 1 
                    ELSE 0 
                   END MatchesNextRowHash
              from REF.AssignmentHistoryIC) as a 
    where a.MatchesNextRowHash = 1

/*
	TEST INCREMENTAL CHANGES BY 
*/

select * from ref.assignmenthistoryic where clientnumber_iris = '22' ORDER BY ASSIGNMENTSTARTDATE

UPDATE Iris.fi_relationshipmanagementBase
SET fi_AssignedToUserId = 'C69AA14A-BB6A-E411-93FD-0025B50B0088' --Seth Groener 
WHERE FI_CONTACTID = '33CFBDB4-636C-E411-940A-0025B50A007D'

UPDATE Iris.fi_relationshipmanagementBase
SET fi_AssignedToUserId = '2F934267-9224-E711-9415-0025B50B0059' --Cosmin Bugnar
WHERE FI_CONTACTID = '33CFBDB4-636C-E411-940A-0025B50A007D'

DECLARE @A UNIQUEIDENTIFIER = NEWID()
EXEC [REF].[spUpsertAssignmentHistoryICIris] @A, @A, 'Tranform and Load Reference Tables'

select * from ref.assignmenthistoryic where clientnumber_iris = '22' ORDER BY ASSIGNMENTSTARTDATE
