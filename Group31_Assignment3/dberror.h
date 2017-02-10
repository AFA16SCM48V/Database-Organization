#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096
# define STORAGE 40
/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_FILE_CREATION_FAILED 5
#define RC_FILE_ALREADY_PRESENT 6
#define RC_FILE_NOT_DESTROYED 8
#define RC_WRONG_PAGE_NUMBER 9
#define RC_FSEEK_FAILED 10
#define RC_ERROR 12
#define RC_PAGE_HANDLE_NOT_INIT 13
#define RC_PAGE_NUMBER_OUT_OF_BOUNDRY 14


#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205
#define RC_RECORD_REMOVED 210

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 
#define RC_NOMEM  304
#define RC_FILE_NOT_OPENED 305
#define RC_FILE_NOT_DELETED 306
#define RC_FILE_SEEK_ERROR 307
#define RC_FILE_CUR_POS_ERROR 308
#define RC_NO_PAGES_ERROR 309
#define RC_BUFFER_CREATE_ERROR 310
#define RC_READ_FAILED 311
#define RC_BUFFER_DEL_ERROR 312
#define RC_DUMMY_PAGE_CREATE_ERROR 313
#define RC_BUFFER_DIRTY 314
#define RC_BUFFER_UNDEFINED_STRATEGY 315
#define RC_UNPIN_ERROR 316
#define RC_BUFFER_EXCEEDED 317
#define RC_PAGE_NOT_FOUND 318
#define RC_FORCE_FLUSH_FAILED 319
#define RC_NO_FRAME_INIT 321
#define RC_INVALID_PAGENUMBER 322
#define RC_FUNC_INVALIDARG  320
#define RC_FORCE_FAILED 323
#define RC_WRONG_NUMBER_OF_FRAMES 325
#define RC_POOL_IN_USE 326
#define RC_FILE_NAME_NOT_PROVIDED 327
#define RC_NO_FRAME_WAIT 328
#define RC_UNPIN_FAIL 500
#define RC_BUFFER_FULL_ERROR  501
#define RC_PAGE_NOT_FOUND_IN_BUFFER  502
#define RC_NULL_LIST 329
#define RC_NO_SUCH_LINK 330
#define RC_NO_FRAME_AVAILABLE 331
#define RC_INVALID_REPLACEMENT_STRATEGY 332
#define RC_PAGE_REPLACE_FAIL 333
#define RC_CANNOT_REPLACE_FAIL 334
#define RC_CANNOT_REPLACE_PAGE 335
#define RC_PAGE_REPLACE_PAGE 336
#define RC_NO_FREE_BUFFER_ERROR 337
#define RC_NO_SUCH_PAGE_IN_BUFF 338

//ADDITIONAL RETURN CODES DEFINED
#define RC_FILE_POINTER_IS_NULL 5 //extra
#define RC_FILE_OFFSET_FAILED 6 //extra
#define RC_MEM_ALLOCATION_FAIL 7//extra
#define RC_FILE_NOT_CLOSED 8//extra
#define RC_FILE_EXIST 9//extra
#define RC_FILE_CREATE_ERROR 10 // extra
#define RC_FILE_REMOVE_ERROR 11//extra
#define RC_BUFFER_MGR_NOT_INIT 500//extra
#define RC_BUFFERPOOL_ALREADY_AVAILABE_FOR_THIS_PAGEFILE 501//extra
#define RC_BUFFERPOOL_SHUTDOWN_ERROR_PAGES_STILL_PINNED 502//extra
#define RC_POOL_NOT_FOUND 503//extra
#define RC_PAGENUM_NOT_IN_BUFFERPOOL 504//extra
#define RC_POOL_IS_FULL 505//extra
#define RC_POOL_REMOVE_ERROR 506//extra
#define RC_INVALID_SCHEMA	601//extra
#define  RC_NO_MEMORY 602//extra
#define RC_INVALID_HANDLE 603//extra
#define RC_INVALID_SCAN_PARAM 604//extra


/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
