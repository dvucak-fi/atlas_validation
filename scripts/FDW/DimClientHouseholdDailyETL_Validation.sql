SELECT * 
  FROM FDW.DimClientHousehold 
 WHERE ClientNumber_Iris = 99999

	--SELECT Id, Strength_CodeHH__c, ETLJobSystemRunId FROM PCGSF.ACCOUNT WHERE Legacy_ID__c = '99999'


/*
	ETLJobSystemRunId
	d1793110-05f4-4f45-a8d7-b41d022c6413
*/

	DECLARE @IrisCID NVARCHAR(100) = '99999'
		  , @DataSourceGroupName NVARCHAR(100) = 'PCG_SFDC'
		  , @DataSourceMemberId INT
		  , @DataSourceMemberName NVARCHAR(100) = 'Account'
		  , @ObjectSchema NVARCHAR(100) = 'PCGSF'
		  , @ETLJobSystemRunId UNIQUEIDENTIFIER  
		  , @DataLakeFileName NVARCHAR(1000)
		  , @TempTableName NVARCHAR(100) 
		  , @BulkLoadStatement NVARCHAR(1000) 


	--FIND DATA SOURCE MEMBER ID FROM THE METADATA FOR THE SPECIFIED OBJECT 
	SELECT @DataSourceMemberId = DataSourceMemberId
	  FROM MDR.DataSourceMember
	 WHERE DataSourceMemberName = @DataSourceMemberName
	   AND AppSpecificFolderName = @DataSourceGroupName


	--FIND ETL JOB RUN ID THAT CREATED THE DimClientHousehold RECORD WE'RE TESTING
	SELECT @ETLJobSystemRunId = ETLJobSystemRunId 
	  FROM PCGSF.Account 
	 WHERE Id IN ( 
			SELECT HouseholdUID
			  FROM FDW.DimClientHousehold
			 WHERE ClientNumber_Iris = @IrisCID
			   AND CurrentRecord = 1 
	)


	--USING THE ETL JOB RUN ID, FIND THE DATA LAKE FILE NAME AND STORE IT IN A VARAIBLE 
	SELECT @DataLakeFileName = CONCAT('https://', StorageAccountName, '/', PrimaryFolderName, RelativeFolderPathFromRoot, '/', FileName)
	  FROM MDR.DataProcessLog 
	 WHERE ETLJobSystemRunId = @ETLJobSystemRunId --JOB SYSTEM RUN ID THAT CREATED THE FILE INITITALLY
	   AND DataSourceMemberId IN (
			SELECT DataSourceMemberId
			  FROM MDR.DataSourceMember
			 WHERE DataSourceMemberName = @DataSourceMemberName
			   AND AppSpecificFolderName = @DataSourceGroupName
	)
 


		IF OBJECT_ID ('TEMPDB..#Account_Temp')  IS NOT NULL DROP TABLE #Account_Temp

		SET @TempTableName = CONCAT('#', @DataSourceMemberName, '_Temp')

		--READ THE DATA LAKE FILE INTO A TEMP TABLE 
		   --WE NEED TO GRAB THE 
 			   SET @BulkLoadStatement = 
					CONCAT('COPY INTO ', @TempTableName, 
						  ' FROM ''', @DataLakeFileName,
						  ''' WITH
 						   ( FILE_TYPE = ''PARQUET''
							, MAXERRORS = 0
							, CREDENTIAL = ( IDENTITY = ''Managed Identity'' )
							, AUTO_CREATE_TABLE = ''ON''
						   )'
						 )

			EXEC sp_executesql @BulkLoadStatement  --Execute COPY INTO Statement to Load Data from Raw Zone into Staging Table

			--CHECK TO MAKE SURE THE DATA LAKE FILE AND ODS MATCH 
			select Id, Strength_CodeHH__c, ETLJobSystemRunId from #Account_Temp where id = '0011k00000xMbEOAA0'
			SELECT Id, Strength_CodeHH__c, ETLJobSystemRunId FROM PCGSF.ACCOUNT WHERE Legacy_ID__c = '99999'

			--GET THE DATE OF WHEN THE FILE WAS LOADED INTO THE DATA LAKE - WE'LL USE THIS IN THE SPARK CLUSTER
			SELECT CONVERT(DATE, DWCreatedDateTime) FROM #Account_Temp where id = '0011k00000xMbEOAA0'




/*
	SWITCH OVER TO SYNAPSE NOTEBOOK 
*/




--ENTER NEW DATA LAKE FILE PATH 
DECLARE @NewFileName NVARCHAR(200) = 'PCG_SFDC/sObject/Account/2023/04/14/part-00000-d2df61d4-a1d8-491d-ad0b-bb92caa0c63a-c000.snappy.parquet'
      , @FullFilePath NVARCHAR(200) 

SELECT @FullFilePath =  CONCAT('https://a00001datalakestadev3.dfs.core.windows.net/raw/', @NewFileName)


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
				



DECLARE @DWCreatedDateTime DATETIME = GETDATE()
      , @DataProcessLogId UNIQUEIDENTIFIER
	  , @ETLJobProcessRunId UNIQUEIDENTIFIER = NEWID()
	  , @ETLJobSystemRunId UNIQUEIDENTIFIER = NEWID()
	  , @ExtractStartDateTime DATETIME = DATEADD(DAY, -1, CONVERT(DATETIME, CONVERT(DATE, GETDATE())))
	  , @ExtractEndDateTime DATETIME = CONVERT(DATETIME, CONVERT(DATE, GETDATE()))
	  , @DataSourceMemberId INT
	  , @DataSourceMemberName NVARCHAR(100) = 'Account'
	  , @DataSourceGroupName NVARCHAR(100) = 'PCG_SFDC'
	  

SET @DataProcessLogId = NEWID() --@@IDENTITY

--FIND DATA SOURCE MEMBER ID FROM THE METADATA FOR THE SPECIFIED OBJECT 
SELECT @DataSourceMemberId = DataSourceMemberId
  FROM MDR.DataSourceMember
 WHERE DataSourceMemberName = @DataSourceMemberName
   AND AppSpecificFolderName = @DataSourceGroupName

INSERT INTO [MDR].[DataProcessLog]
    (
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
	  
SELECT 
        @DataProcessLogId
      , @DataSourceMemberId
      , REPLACE(@NewFileName, 'PCG_SFDC/sObject/Account/2023/04/14/', '') AS FileNameFromDataLake 
      , 'a00001datalakestadev3.dfs.core.windows.net'
      , 'raw'
      , '/PCG_SFDC/sObject/Account/2023/04/14' AS FilePathInDataLake--CHANGE
      , concat('abfss://raw@a00001datalakestadev3.dfs.core.windows.net/', @NewFileName)
      , 'abfss://'
      , NULL
      , @ExtractStartDateTime
      , @ExtractEndDateTime
      , 60 
      , 60
	  , 1
      , NULL
      , @DWCreatedDateTime AS DWLoadStartDateTime 
      , @DWCreatedDateTime AS DWLoadStartDateTime 
      , @ETLJobProcessRunId
      , @ETLJobSystemRunId


select * 
from mdr.dataprocesslog 
  where datasourcememberid = 636

  

--RUN ODS UPSERT 
DECLARE @A UNIQUEIDENTIFIER = NEWID()
EXEC [ODS].[spUpsert_PcgSf_Account] @A, @A, 'Transform and Load Operational Tables'

--CHECK DIM TO SEE WHAT IT LOOKS LIKE BEFORE RUNNING UPSERT
select * 
  from fdw.dimclienthousehold
  where clientnumber_iris = 99999

--RUN UPSERT
DECLARE @A UNIQUEIDENTIFIER = NEWID()
EXEC [FDW].[spUpsertDimClientHousehold] @A, @A, 'Transform and Load Dimension Tables'

--CHECK DIM TO SEE WHAT IT LOOKS LIKE AFTER RUNNING UPSERT
select * 
  from fdw.dimclienthousehold
  where clientnumber_iris = 99999

