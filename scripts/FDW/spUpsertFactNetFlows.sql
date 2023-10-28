DECLARE @RecordCount_FNF INT
DECLARE @RecordCount_FNF_REWRITE INT

SELECT @RecordCount_FNF = count(1)
  FROM FDW.FactNetFlows

SELECT @RecordCount_FNF_REWRITE = count(1)
  FROM FDW.FactNetFlows_REWRITE

--CHECK TO SEE IF RECORDS COUNTS MATCH 
SELECT CASE WHEN @RecordCount_FNF_REWRITE = @RecordCount_FNF THEN 'TRUE' ELSE 'FALSE' END AS RecordCountMatch

--ALTHOUGH RECORDS COUNTS DO MATCH, WE COULD STILL THEORETICALLY HAVE SPECIFIC TRANSACTIONS THAT ARE MISSING. LOOK FOR THOSE IF THEY EXIST. 
SELECT SRC.*
  FROM FDW.FactNetFlows_REWRITE AS SRC
  LEFT
  JOIN FDW.FactNetFlows AS TGT
    ON Src.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.FlowCount = Tgt.FlowCount --Inserts Cancelled records that otherwise are identical to the real records
 WHERE Tgt.ReferenceNumber IS NULL
