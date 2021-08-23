/******************************************************************************
 * Copyright of this product 2013-2023,
 * MACHBASE Corporation(or Inc.) or its subsidiaries.
 * All Rights reserved.
 ******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <machbase_sqlcli.h>

#define MACHBASE_PORT_NO    5666
#define ERROR_CHECK_COUNT   100

#define RC_SUCCESS          0
#define RC_FAILURE          -1

#define UNUSED(aVar) do { (void)(aVar); } while(0)

#define CHECK_APPEND_RESULT(aRC, aEnv, aCon, aSTMT)             \
    if( !SQL_SUCCEEDED(aRC) )                                   \
    {                                                           \
        if( checkAppendError(aEnv, aCon, aSTMT) == RC_FAILURE ) \
        {                                                       \
            return RC_FAILURE;                                  \
        }                                                       \
    }                                                           \


#define CHECK_STMT_RESULT(aRC, aSTMT, aMsg)     \
    if( sRC != SQL_SUCCESS )                    \
    {                                           \
        printError(gEnv, gCon, aSTMT, aMsg);    \
        goto error;                             \
    }



SQLHENV     gEnv;
SQLHDBC     gCon;

void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg);
time_t getTimeStamp();
int connectDB();
void disconnectDB();
int executeDirectSQL(const char *aSQL, int aErrIgnore);
int createTable();
int appendOpen(SQLHSTMT aStmt);
int appendData(SQLHSTMT aStmt);
SQLBIGINT appendClose(SQLHSTMT aStmt);
int selectData();


void printError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt, char *aMsg)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( aMsg != NULL )
    {
        printf("%s\n", aMsg);
    }

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) == SQL_SUCCESS )
    {
        printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);
    }
}

int checkAppendError(SQLHENV aEnv, SQLHDBC aCon, SQLHSTMT aStmt)
{
    SQLINTEGER      sNativeError;
    SQLCHAR         sErrorMsg[SQL_MAX_MESSAGE_LENGTH + 1];
    SQLCHAR         sSqlState[SQL_SQLSTATE_SIZE + 1];
    SQLSMALLINT     sMsgLength;

    if( SQLError(aEnv, aCon, aStmt, sSqlState, &sNativeError,
        sErrorMsg, SQL_MAX_MESSAGE_LENGTH, &sMsgLength) != SQL_SUCCESS )
    {
        return RC_FAILURE;
    }

    printf("SQLSTATE-[%s], Machbase-[%d][%s]\n", sSqlState, sNativeError, sErrorMsg);

    if( sNativeError != 9604 &&
        sNativeError != 9605 &&
        sNativeError != 9606 )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void appendDumpError(SQLHSTMT    aStmt,
                     SQLINTEGER  aErrorCode,
                     SQLPOINTER  aErrorMessage,
                     SQLLEN      aErrorBufLen,
                     SQLPOINTER  aRowBuf,
                     SQLLEN      aRowBufLen)
{
    char       sErrMsg[1024] = {0, };
    char       sRowMsg[32 * 1024] = {0, };

    UNUSED(aStmt);

    if (aErrorMessage != NULL)
    {
        strncpy(sErrMsg, (char *)aErrorMessage, aErrorBufLen);
    }

    if (aRowBuf != NULL)
    {
        strncpy(sRowMsg, (char *)aRowBuf, aRowBufLen);
    }

    fprintf(stdout, "Append Error : [%d][%s]\n[%s]\n\n", aErrorCode, sErrMsg, sRowMsg);
}

time_t getTimeStamp()
{
    struct timeval sTimeVal;
    int            sRet;

    sRet = gettimeofday(&sTimeVal, NULL);

    if (sRet == 0)
    {
        return (time_t)(sTimeVal.tv_sec * 1000000 + sTimeVal.tv_usec);
    }
    else
    {
        return 0;
    }
}

int connectDB()
{
    char sConnStr[1024];

    if( SQLAllocEnv(&gEnv) != SQL_SUCCESS )
    {
        printf("SQLAllocEnv error\n");
        return RC_FAILURE;
    }

    if( SQLAllocConnect(gEnv, &gCon) != SQL_SUCCESS )
    {
        printf("SQLAllocConnect error\n");

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    sprintf(sConnStr,"SERVER=127.0.0.1;UID=SYS;PWD=MANAGER;CONNTYPE=1;PORT_NO=%d", MACHBASE_PORT_NO);

    if( SQLDriverConnect( gCon, NULL,
                          (SQLCHAR *)sConnStr,
                          SQL_NTS,
                          NULL, 0, NULL,
                          SQL_DRIVER_NOPROMPT ) != SQL_SUCCESS
      )
    {

        printError(gEnv, gCon, NULL, "SQLDriverConnect error");

        SQLFreeConnect(gCon);
        gCon = SQL_NULL_HDBC;

        SQLFreeEnv(gEnv);
        gEnv = SQL_NULL_HENV;

        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

void disconnectDB()
{
    if( SQLDisconnect(gCon) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, NULL, "SQLDisconnect error");
    }

    SQLFreeConnect(gCon);
    gCon = SQL_NULL_HDBC;

    SQLFreeEnv(gEnv);
    gEnv = SQL_NULL_HENV;
}

int executeDirectSQL(const char *aSQL, int aErrIgnore)
{
    SQLHSTMT sStmt = SQL_NULL_HSTMT;

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        if( aErrIgnore == 0 )
        {
            printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
            return RC_FAILURE;
        }
    }

    if( SQLExecDirect(sStmt, (SQLCHAR *)aSQL, SQL_NTS) != SQL_SUCCESS )
    {

        if( aErrIgnore == 0 )
        {
            printError(gEnv, gCon, sStmt, "SQLExecDirect Error");

            SQLFreeStmt(sStmt,SQL_DROP);
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        if (aErrIgnore == 0)
        {
            printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
            sStmt = SQL_NULL_HSTMT;
            return RC_FAILURE;
        }
    }
    sStmt = SQL_NULL_HSTMT;

    return RC_SUCCESS;
}

int createTable()
{
    int sRC;

    sRC = executeDirectSQL("DROP TABLE BINARY_SAMPLE", 1);
    if( sRC != RC_SUCCESS )
    {
        return RC_FAILURE;
    }

    sRC = executeDirectSQL("CREATE TABLE BINARY_SAMPLE(filename varchar(100), rawdata binary)", 0);
    if( sRC != RC_SUCCESS )
    {
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int appendOpen(SQLHSTMT aStmt)
{
    const char *sTableName = "BINARY_SAMPLE";

    if( SQLAppendOpen(aStmt, (SQLCHAR *)sTableName, ERROR_CHECK_COUNT) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendOpen Error");
        return RC_FAILURE;
    }

    return RC_SUCCESS;
}

int appendData(SQLHSTMT aStmt)
{
    SQL_APPEND_PARAM sParam[2];
    SQLRETURN        sRC;


	char 			sVarchar[128] = {0,};

	struct stat		sStatBuf;
	FILE			*sFp          = NULL;

	int				sFileSize     = 0; 
	char			sFileName[40] = "extract_file.png";
	char 			*sBinaryData  = NULL;


	sFp = fopen(sFileName, "rb");
	if( sFp == NULL )
	{
		printf("File Open Error : %s\n", sFileName );
        return RC_FAILURE;
	}
	stat(sFileName, &sStatBuf);
	sFileSize = sStatBuf.st_size;

	sBinaryData = (char *)malloc( sizeof(char)  * sFileSize);

	fread(sBinaryData, sStatBuf.st_size, 1, sFp);

	fclose(sFp);


    memset(sParam, 0, sizeof(sParam));

    strcpy(sVarchar, "raw_data.bin");
    sParam[0].mVar.mLength = strlen(sVarchar);
    sParam[0].mVar.mData   = sVarchar;

	sParam[1].mBlob.mLength = sFileSize;
	sParam[1].mBlob.mData   = sBinaryData;
	

	sRC = SQLAppendDataV2(aStmt, sParam);
    CHECK_APPEND_RESULT(sRC, gEnv, gCon, aStmt);

	if( sBinaryData )
	{
		free(sBinaryData);
		sBinaryData = NULL;
	}

    return RC_SUCCESS;
}

int selectData()
{

    const char *sSQL = "SELECT filename, rawdata FROM BINARY_SAMPLE";

    SQLHSTMT    sStmt = SQL_NULL_HSTMT;
    SQLRETURN   sRC   = SQL_ERROR;
	
	SQLLEN		sBinaryLimitLength = (64 * 1024 * 1024); // 64MB
	SQLLEN      sFileNameLen    = 0;
	SQLLEN      sBinaryLen     	= 0;

	FILE		*sFp 			= NULL;

    char        sFileName[128] 	= {0,};
    char        sNewFile[256] 	= {0,};
    char        *sBinaryData 	= NULL;


	sBinaryData = (char *)malloc( sizeof(char) * sBinaryLimitLength);

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( SQLPrepare(sStmt, (SQLCHAR *)sSQL, SQL_NTS) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQPrepare Error");
        goto error;
    }

    if( SQLExecute(sStmt) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLExecute Error");
        goto error;
    }

    sRC = SQLBindCol(sStmt, 1, SQL_C_CHAR, sFileName, sizeof(sFileName), &sFileNameLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 1 Error");

    sRC = SQLBindCol(sStmt, 2, SQL_C_BINARY, sBinaryData, sBinaryLimitLength, &sBinaryLen);
    CHECK_STMT_RESULT(sRC, sStmt, "SQLBindCol 2 Error");

    while( SQLFetch(sStmt) == SQL_SUCCESS )
    {
		printf("DB filename : %s, filesize : %ld\n", sFileName, sBinaryLen);

		sprintf(sNewFile, "%s2", sFileName);

		sFp = fopen(sNewFile, "wb");
		if( sFp != NULL )
		{
			fwrite(sBinaryData, 1, sBinaryLen, sFp);
			fclose(sFp);	
		}

		printf("new filename : %s\n", sNewFile);
	}

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;


	if( sBinaryData )
	{
		free(sBinaryData);
		sBinaryData = NULL;
	}

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

	if( sBinaryData )
	{
		free(sBinaryData);
		sBinaryData = NULL;
	}

    return RC_FAILURE;
}


SQLBIGINT appendClose(SQLHSTMT aStmt)
{
    SQLBIGINT sSuccessCount = 0;
    SQLBIGINT sFailureCount = 0;

    if( SQLAppendClose(aStmt, &sSuccessCount, &sFailureCount) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, aStmt, "SQLAppendClose Error");
        return RC_FAILURE;
    }

    printf("success : %ld, failure : %ld\n", sSuccessCount, sFailureCount);

    return sSuccessCount;
}

int main()
{
    SQLHSTMT    sStmt = SQL_NULL_HSTMT;

    if( connectDB() == RC_SUCCESS )
    {
        printf("connectDB success\n");
    }
    else
    {
        printf("connectDB failure\n");
        goto error;
    }

    if( createTable() == RC_SUCCESS )
    {
        printf("createTable success\n");
    }
    else
    {
        printf("createTable failure\n");
        goto error;
    }

    if( SQLAllocStmt(gCon, &sStmt) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAllocStmt Error");
        goto error;
    }

    if( appendOpen(sStmt) == RC_SUCCESS )
    {
        printf("appendOpen success\n");
    }
    else
    {
        printf("appendOpen failure\n");
        goto error;
    }

    if( SQLAppendSetErrorCallback(sStmt, appendDumpError) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLAppendSetErrorCallback Error");
        goto error;
    }

    appendData(sStmt);

    appendClose(sStmt);

    if( SQLFreeStmt(sStmt, SQL_DROP) != SQL_SUCCESS )
    {
        printError(gEnv, gCon, sStmt, "SQLFreeStmt Error");
        goto error;
    }
    sStmt = SQL_NULL_HSTMT;

	selectData();

    disconnectDB();

    return RC_SUCCESS;

error:
    if( sStmt != SQL_NULL_HSTMT )
    {
        SQLFreeStmt(sStmt, SQL_DROP);
        sStmt = SQL_NULL_HSTMT;
    }

    if( gCon != SQL_NULL_HDBC )
    {
        disconnectDB();
    }

    return RC_FAILURE;
}
