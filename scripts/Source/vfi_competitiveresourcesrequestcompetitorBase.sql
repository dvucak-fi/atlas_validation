USE [AZURE_FICRMMSCRM]

IF EXISTS (
		SELECT 1
		  FROM SYS.views AS V
		  JOIN SYS.schemas AS S
			ON V.schema_id = S.schema_id
		 WHERE V.name = 'vfi_competitiveresourcesrequestcompetitorBase'
		   AND S.name = 'edwreader'
)
DROP VIEW edwreader.vfi_competitiveresourcesrequestcompetitorBase
GO

CREATE VIEW [edwreader].[vfi_competitiveresourcesrequestcompetitorBase]
AS
SELECT [fi_competitiveresourcesrequestcompetitorId]
      ,[CreatedOn]
      ,[CreatedBy]
      ,[ModifiedOn]
      ,[ModifiedBy]
      ,[CreatedOnBehalfBy]
      ,[ModifiedOnBehalfBy]
      ,[OwnerId]
      ,[OwnerIdType]
      ,[OwningBusinessUnit]
      ,[statecode]
      ,[statuscode]
      ,[VersionNumber]
      ,[ImportSequenceNumber]
      ,[OverriddenCreatedOn]
      ,[TimeZoneRuleVersionNumber]
      ,[UTCConversionTimeZoneCode]
      ,[fi_name]
      ,[fi_CompetitiveResourcesRequestId]
      ,[fi_AccountId]
  FROM [FICRM_MSCRM].[dbo].[fi_competitiveresourcesrequestcompetitorBase] (NOLOCK)
