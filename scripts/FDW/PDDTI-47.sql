/*

	CHECK RECORD COUNTS 
	EXPECTED RESULT: MATCH

*/
DECLARE @OrigRecCount INT 
      , @NewRecCount INT

SELECT @OrigRecCount = COUNT(1) FROM FDW.FactClientSnapshotDaily_08222023
SELECT @NewRecCount = COUNT(1) FROM FDW.FactClientSnapshotDaily

SELECT CASE WHEN @OrigRecCount = @NewRecCount THEN 'MATCH' ELSE 'ERROR' END 

/*

	CHECK FOR GRAIN VIOLATION 
	EXPECTED RESULT: NO RECORDS

*/

  SELECT ClientNumber
       , DimDateKey
	   , COUNT(1) AS RecCount
    FROM FDW.FactClientSnapshotDaily
   GROUP BY ClientNumber
       , DimDateKey
  HAVING COUNT(1) > 1
