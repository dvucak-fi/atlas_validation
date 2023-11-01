DECLARE @RecordCount_FNF INT
DECLARE @RecordCount_FNF_REWRITE INT

SELECT @RecordCount_FNF = count(1)
  FROM FDW.FactNetFlows_11012023

SELECT @RecordCount_FNF_REWRITE = count(1)
  FROM FDW.FactNetFlows

--CHECK TO SEE IF RECORDS COUNTS MATCH 
SELECT CASE WHEN @RecordCount_FNF_REWRITE = @RecordCount_FNF THEN 'TRUE' ELSE 'FALSE' END AS RecordCountMatch

--ALTHOUGH RECORDS COUNTS DO MATCH, WE COULD STILL THEORETICALLY HAVE SPECIFIC TRANSACTIONS THAT ARE MISSING. LOOK FOR THOSE IF THEY EXIST. 
SELECT SRC.*
  FROM FDW.FactNetFlows_11012023 AS SRC
  LEFT
  JOIN FDW.FactNetFlows AS TGT
    ON Src.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
   AND Src.FlowCount = Tgt.FlowCount --Inserts Cancelled records that otherwise are identical to the real records
 WHERE Tgt.ReferenceNumber IS NULL


--CHECK TO SEE IF ALL KEYS AND MEASURES MATCH 
 SELECT SUM(CASE WHEN SRC.DimTransactionDateKey <> TGT.DimTransactionDateKey THEN 1 ELSE 0 END) AS DimTransactionDateKey
      , SUM(CASE WHEN SRC.DimCancellationDateKey <> TGT.DimCancellationDateKey THEN 1 ELSE 0 END) AS DimCancellationDateKey
      , SUM(CASE WHEN SRC.DimFinancialAccountKey <> TGT.DimFinancialAccountKey THEN 1 ELSE 0 END) AS DimFinancialAccountKey
      , SUM(CASE WHEN SRC.DimClientKey <> TGT.DimClientKey THEN 1 ELSE 0 END) AS DimClientKey
      , SUM(CASE WHEN SRC.DimEmployeeKey <> TGT.DimEmployeeKey THEN 1 ELSE 0 END) AS DimEmployeeKey
      , SUM(CASE WHEN SRC.DimTeamMemberKey <> TGT.DimTeamMemberKey THEN 1 ELSE 0 END) AS DimTeamMemberKey
      , SUM(CASE WHEN SRC.DimFlowTypeKey <> TGT.DimFlowTypeKey THEN 1 ELSE 0 END) AS DimFlowTypeKey
      , SUM(CASE WHEN SRC.DimClientTenureKey <> TGT.DimClientTenureKey THEN 1 ELSE 0 END) AS DimClientTenureKey
      , SUM(CASE WHEN SRC.DimClientAssetsKey <> TGT.DimClientAssetsKey THEN 1 ELSE 0 END) AS DimClientAssetsKey
      , SUM(CASE WHEN SRC.DimEmployeeMgmtKey <> TGT.DimEmployeeMgmtKey THEN 1 ELSE 0 END) AS DimEmployeeMgmtKey
      , SUM(CASE WHEN SRC.DimAgeGroupKey <> TGT.DimAgeGroupKey THEN 1 ELSE 0 END) AS DimAgeGroupKey
      , SUM(CASE WHEN SRC.DimPeerGroupKey <> TGT.DimPeerGroupKey THEN 1 ELSE 0 END) AS DimPeerGroupKey
      , SUM(CASE WHEN SRC.FlowAmount <> TGT.FlowAmount THEN 1 ELSE 0 END) AS FlowAmount
      , SUM(CASE WHEN SRC.FlowAmountOriginalCurrency <> TGT.FlowAmountOriginalCurrency THEN 1 ELSE 0 END) AS FlowAmountOriginalCurrency
      , SUM(CASE WHEN SRC.FlowCount <> TGT.FlowCount THEN 1 ELSE 0 END) AS FlowCount
      , SUM(CASE WHEN SRC.RecurringTransactionDaysLag <> TGT.RecurringTransactionDaysLag THEN 1 ELSE 0 END) AS RecurringTransactionDaysLag
      , SUM(CASE WHEN SRC.ReferenceNumber <> TGT.ReferenceNumber THEN 1 ELSE 0 END) AS ReferenceNumber
      , SUM(CASE WHEN SRC.SubReferenceNumber <> TGT.SubReferenceNumber THEN 1 ELSE 0 END) AS SubReferenceNumber
      , SUM(CASE WHEN SRC.CreatedDate <> TGT.CreatedDate THEN 1 ELSE 0 END) AS CreatedDate
      , SUM(CASE WHEN SRC.UpdatedDate <> TGT.UpdatedDate THEN 1 ELSE 0 END) AS UpdatedDate
   FROM FDW.FactNetFlows_11012023 AS SRC
   LEFT
   JOIN FDW.FactNetFlows AS TGT
     ON Src.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.ReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
    AND Src.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS = Tgt.SubReferenceNumber COLLATE SQL_Latin1_General_CP1_CS_AS
    AND Src.FlowCount = Tgt.FlowCount --Inserts Cancelled records that otherwise are identical to the real records

