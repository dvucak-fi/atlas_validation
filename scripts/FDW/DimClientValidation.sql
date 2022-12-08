
   
/****
    FDW.DimClient Validaiton 
***/


--Check for unknown member row: PASS
SELECT * 
  FROM FDW.DimClient
  where DimClientKey = -1


--Check for active client count: 94,211 (not sure if this is correct)
    --Daily AUM Summary US PCG: 91,220     
   SELECT COUNT(*) 
     FROM FDW.DimClient
    WHERE CurrentRecord = 1 
      AND ClientTypeCode = 101510
      AND FirstTradeDate IS NOT NULL

--Check for duplicate active client records: PASS
   select ClientNumber
       , count(*)
    from FDW.DimClient
    where CurrentRecord = 1 
    group by ClientNumber
    having count(*) > 1

--Check for duplicate client/start date records: PASS
   select ClientNumber
        , EffectiveStartDate
        , count(*)
    from FDW.DimClient
    where CurrentRecord = 1 
    group by ClientNumber
        , EffectiveStartDate
    having count(*) > 1


--Check for Start/End dates that don't line up: PASS
select * 
  from ( 
            select 
                   ClientNumber
                 , EffectiveEndDate
                 , LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY ClientId ORDER BY EffectiveStartDate) AS NextStartDate
                 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 1 
                    WHEN EffectiveEndDate = LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY ClientId ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END EndDateMatchesNextStartDate
              from FDW.DimClient) as a 
    where a.EndDateMatchesNextStartDate = 0 

--Check for duplicate RowHashs: PASS
select * 
  from ( 
            select 
                   ClientNumber
                 , RowHash
                 , LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId ORDER BY EffectiveStartDate) AS NextRowHash
                 , EffectiveStartDate
				 , EffectiveEndDate
				 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 0
                    WHEN RowHash = LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY ClientId ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END MatchesNextRowHash
              from FDW.DimClient) as a 
    where a.MatchesNextRowHash = 1


