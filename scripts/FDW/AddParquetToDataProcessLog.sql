

/*
	===========================================
	==	    CHANGE THE BELOW PARAMETERS      ==
	==	    FOR YOUR DATA SOURCE MEMBER      ==
	===========================================
*/

--ADD DATA SOURCE MEMBER AND DATA SOURCE GROUP NAMES
DECLARE @DataSourceMemberName NVARCHAR(100) = 'Account' 
DECLARE @DataSourceGroupName NVARCHAR(100) = 'PCG_SFDC'  

--FILE NAME FROM NOTEBOOK
DECLARE @NewFileName NVARCHAR(200) = 'PCG_SFDC/sObject/Account/2023/05/20/03:13:44/part-00000-eead594c-87bf-4971-b7e8-45dd29e850cd-c000.snappy.parquet'   



/*
	============================================================
	==	                  LEAVE BELOW AS IS                   ==
	==       PRESS F5 TO ADD RECORD TO DATA PROCESS LOG       ==
	============================================================
*/


DECLARE @DWCreatedDateTime DATETIME = GETDATE()
      , @DataProcessLogId UNIQUEIDENTIFIER
	  , @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
	  , @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()
	  , @ExtractStartDateTime DATETIME = DATEADD(DAY, -1, CONVERT(DATETIME, CONVERT(DATE, GETDATE())))
	  , @ExtractEndDateTime DATETIME = CONVERT(DATETIME, CONVERT(DATE, GETDATE()))
	  , @DataSourceMemberId INT
	  , @FileNameFromDataLake NVARCHAR(100)
	  , @FullFilePath NVARCHAR(200) 
	  , @DataLakeFilePath NVARCHAR(100)

DECLARE @Today DATE = 
	CASE 
		WHEN CONVERT(DATE, GETDATE()) > CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') 
		THEN CONVERT(DATE, GETDATE()) 
		ELSE CONVERT(DATE, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time') 
	END
DECLARE @CurrentDate NVARCHAR(100) = CONCAT(YEAR(@Today), '/', FORMAT(MONTH(@Today), '00'), '/', DAY(@Today))
DECLARE @RelativeFolderPathFromRoot NVARCHAR(100) = CONCAT('/', LEFT(@NewFileName, (CHARINDEX(@CurrentDate , @NewFileName) -1) + LEN(@CurrentDate)))
DECLARE @FileNameParsed NVARCHAR(255) = SUBSTRING(@NewFileName, (LEN(@RelativeFolderPathFromRoot) + 1), LEN(@NewFileName)-LEN(@RelativeFolderPathFromRoot) + 1)

SELECT @FullFilePath =  CONCAT('https://a00001datalakestadev3.dfs.core.windows.net/raw/', @NewFileName)

IF OBJECT_ID ('TEMPDB..#Stg_Temp') IS NOT NULL DROP TABLE #Stg_Temp

DECLARE @BulkLoadStatement NVARCHAR(2000) 

 			   SET @BulkLoadStatement = 
					CONCAT('COPY INTO #Stg_Temp',  
						  ' FROM ''', @FullFilePath,
						  ''' WITH
 						   ( FILE_TYPE = ''PARQUET''
							, MAXERRORS = 0
							, CREDENTIAL = ( IDENTITY = ''Managed Identity'' )
							, AUTO_CREATE_TABLE = ''ON''
						   )'
						 )

				EXEC sp_executesql @BulkLoadStatement  --Execute COPY INTO Statement to Load Data from Raw Zone into Staging Table				
	  

SET @DataProcessLogId = NEWID() --@@IDENTITY

--FIND DATA SOURCE MEMBER ID FROM THE METADATA FOR THE SPECIFIED OBJECT 
SELECT @DataSourceMemberId = DM.DataSourceMemberId
  FROM MDR.DataSourceMember AS DM
  JOIN MDR.DataSourceGroup AS DG
    ON DM.DataSourceGroupId = DG.DataSourceGroupId
 WHERE DM.DataSourceMemberName = @DataSourceMemberName
   AND DG.DataSourceGroupName = @DataSourceGroupName
	  
;WITH InsertPrep AS ( 

	 SELECT @DataProcessLogId AS DataProcessLogId
		  , @DataSourceMemberId AS DataSourceMemberId
		  , @FileNameParsed AS [FileName]
		  , 'a00001datalakestadev3.dfs.core.windows.net' AS StorageAccountName
		  , 'raw' AS PrimaryFolderName
		  , @RelativeFolderPathFromRoot AS RelativeFolderPathFromRoot
		  , concat('abfss://raw@a00001datalakestadev3.dfs.core.windows.net/', @NewFileName) AS FilePathAbsolute
		  , 'abfss://' AS StorageSystemPrefix
		  , NULL AS FileDateStampSuffix
		  , @ExtractStartDateTime AS ExtractStartDateTime
		  , @ExtractEndDateTime AS ExtractEndDateTime
		  , 60 AS DataFileProcessStageId
		  , 60 AS DataProcessStageId
		  , 1 AS RecordCount
		  , NULL AS ExtractQuery
		  , @DWCreatedDateTime AS DWLoadStartDateTime 
		  , @DWCreatedDateTime AS DWLoadEndDateTime 
		  , @ETLJobProcessRunId AS ETLJobProcessRunId
		  , @ETLJobSystemRunId AS ETLJobSystemRunId

)

     INSERT 
       INTO [MDR].[DataProcessLog] ( 
			[DataProcessLogId]
		  , [DataSourceMemberId]
		  , [FileName]
		  , [StorageAccountName]
		  , [PrimaryFolderName]
		  , [RelativeFolderPathFromRoot]
		  , [FilePathAbsolute]
		  , [StorageSystemPrefix]
		  , [FileDateStampSuffix]
		  , [ExtractStartDateTime]
		  , [ExtractEndDateTime]
		  , [DataFileProcessStageId]
		  , [DataProcessStageId]
		  , [RecordCount]
		  , [ExtractQuery]
		  , [DWLoadStartDateTime]
		  , [DWLoadEndDateTime]
		  , [ETLJobProcessRunId]
		  , [ETLJobSystemRunId]
      )

	 SELECT SRC.DataProcessLogId
		  , SRC.DataSourceMemberId
		  , SRC.[FileName]
		  , SRC.StorageAccountName
		  , SRC.PrimaryFolderName
		  , SRC.RelativeFolderPathFromRoot
		  , SRC.FilePathAbsolute
		  , SRC.StorageSystemPrefix
		  , SRC.FileDateStampSuffix
		  , SRC.ExtractStartDateTime
		  , SRC.ExtractEndDateTime
		  , SRC.DataFileProcessStageId
		  , SRC.DataProcessStageId
		  , SRC.RecordCount
		  , SRC.ExtractQuery
		  , SRC.DWLoadStartDateTime
		  , SRC.DWLoadEndDateTime
		  , SRC.ETLJobProcessRunId
		  , SRC.ETLJobSystemRunId
	   FROM InsertPrep AS SRC
	   LEFT
	   JOIN MDR.DataProcessLog AS TGT
	     ON SRC.DataSourceMemberId = TGT.DataSourceMemberId
        AND SRC.[FileName] = TGT.[FileName]
	  WHERE TGT.DataSourceMemberId IS NULL
	  
	  DECLARE @DSG NVARCHAR(100) = CASE WHEN @DataSourceGroupName = 'PCG_SFDC' THEN 'PcgSf' ELSE @DataSourceGroupName END 
	  DECLARE @OdsProcCall NVARCHAR(1000) = CONCAT('DECLARE @a UNIQUEIDENTIFIER = NEWID() EXEC [ODS].[spUpsert_', @DSG, '_', @DataSourceMemberName, '] @a, @a, ''Transform and Load Operational Tables''')


	  --ods upsert
	  EXEC sp_executesql @OdsProcCall
	  
