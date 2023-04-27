
   
/****
    REF.HouseholdFinMapping Validaiton 
***/



--Check for duplicate active client records: PASS
   select FinAccountNumber
       , count(*)
    from REF.HouseholdFinMapping
    where CurrentRecord = 1 
    group by FinAccountNumber
    having count(*) > 1



--Check for duplicate client/start date records: PASS
   select FinAccountNumber
        , EffectiveStartDate
        , count(*)
    from REF.HouseholdFinMapping
    where CurrentRecord = 1 
    group by FinAccountNumber
        , EffectiveStartDate
    having count(*) > 1


--Check for Start/End dates that don't line up: PASS
select * 
  from ( 
            select 
                   FinAccountNumber
                 , EffectiveEndDate
                 , LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY FinAccountNumber ORDER BY EffectiveStartDate) AS NextStartDate
                 , CASE 
                    WHEN EffectiveEndDate = LEAD (EffectiveStartDate, 1, '9999-12-31') OVER (PARTITION BY FinAccountNumber ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END EndDateMatchesNextStartDate
              from REF.HouseholdFinMapping) as a 
    where a.EndDateMatchesNextStartDate = 0 



--Check for duplicate RowHashs: PASS
select * 
  from ( 
            select 
                   FinAccountNumber
                 , RowHash
                 , LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY FinAccountNumber ORDER BY EffectiveStartDate) AS NextRowHash
                 , EffectiveStartDate
				 , EffectiveEndDate
				 , CASE 
                    --WHEN DimCLientKey = -1 
                    --THEN 0
                    WHEN RowHash = LEAD (RowHash, 1, HASHBYTES('SHA2_256', '')) OVER (PARTITION BY FinAccountNumber ORDER BY EffectiveStartDate)
                    THEN 1 
                    ELSE 0 
                   END MatchesNextRowHash
              from REF.HouseholdFinMapping) as a 
    where a.MatchesNextRowHash = 1
