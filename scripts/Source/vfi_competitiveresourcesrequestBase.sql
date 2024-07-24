USE [AZURE_FICRMMSCRM]

IF EXISTS (
		SELECT 1
		  FROM SYS.views AS V
		  JOIN SYS.schemas AS S
			ON V.schema_id = S.schema_id
		 WHERE V.name = 'vfi_competitiveresourcesrequestBase'
		   AND S.name = 'edwreader'
)
DROP VIEW edwreader.vfi_competitiveresourcesrequestBase
GO

CREATE VIEW [edwreader].[vfi_competitiveresourcesrequestBase]
AS
SELECT [fi_competitiveresourcesrequestId]
     , [CreatedOn]
     , [CreatedBy]
     , [ModifiedOn]
     , [ModifiedBy]
     , [CreatedOnBehalfBy]
     , [ModifiedOnBehalfBy]
     , [OwnerId]
     , [OwnerIdType]
     , [OwningBusinessUnit]
     , [statecode]
     , [statuscode]
     , [VersionNumber]
     , [ImportSequenceNumber]
     , [OverriddenCreatedOn]
     , [TimeZoneRuleVersionNumber]
     , [UTCConversionTimeZoneCode]
     , [fi_name]
     , [fi_ServiceRequested]
     , [fi_Requestor]
     , [fi_RequestDate]
     , [fi_MeetingCallDate]
     , [fi_DueDate]
     , [fi_Competitors]
     , [fi_ExpecutedAUM]
     , [TransactionCurrencyId]
     , [ExchangeRate]
     , [fi_expecutedaum_Base]
     , [fi_FullStatementonFile]
     , [fi_DuplicateRequest]
     , [fi_MappingRequested]
     , [fi_MappingAnticipatedDate]
     , [fi_MappingCompleted]
     , [fi_PeerReviewCompletedBy]
     , [fi_PriorAssignedAnalyst]
     , [fi_AssignedToUserId]
     , [fi_ContactId]
     , [fi_Email]
     , [fi_CalledClient]
     , [fi_MetClient]
     , [fi_CompletedOn]
     , [fi_CompetitorReview]
     , [fi_StatementReview]
     , [fi_RecommendationReview]
     , [fi_ProductReview]
     , [fi_Other]
     , [fi_LOATOI]
     , [fi_RequestorTitle]
     , [fi_AdditionalRequestDetails]
     , [fi_IsPCGI]
     , [fi_TimeofMeetingCall]
     , [fi_VerbaltoRequestor]
     , [fi_TermThreatOption]
     , [fi_CDVP]
     , [fi_Consideringacompetitor]
     , [fi_DelinkTransferout]
     , [fi_Wishtoterminateverballyorviaemail]
  FROM [FICRM_MSCRM].[dbo].[fi_competitiveresourcesrequestBase] (NOLOCK)
