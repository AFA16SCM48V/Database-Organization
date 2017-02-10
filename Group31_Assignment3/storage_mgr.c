#include "dberror.h"
#include "test_helper.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <math.h>

FILE *smPointer;

// prototypes of additional methods
extern RC checkForFileHandle(SM_FileHandle *smHandler);
extern RC checkNonExistingPage(int pageNum, SM_FileHandle *smHandler);
extern RC checkForFilePointer(SM_FileHandle *smHandler);
extern RC isFileReadyToRead(int pageNum, SM_FileHandle *smHandler);
//end of declaration

/*********************************************************************************
 * Function Name: initStorageManager
 *
 * Description: initializes the storage manager
 *
 * Parameters: ---
 *
 * Return: ---
 *
 *********************************************************************************/
void initStorageManager(void) {
  printf("\n Initialized storage manager");
}

/*********************************************************************************
 * Function Name : createPageFile
 *
 * Description: creates a new PageFile with the given fileName with initial file
 * 				size as one page and also fills the single page with '\0' bytes
 * 				Writes the details of file Handle in the first block of the file
 *
 * Parameters: *fileName - pointer to the name of the file to be created
 *
 * Return: RC - return code based on the status of the action done by method
 * 				RC_OK - if file was created successfully
 * 				RC_MEM_ALLOCATION_FAIL  - if memory allocation for file failed
 * 				RC_FILE_CREATE_ERROR	- if error occured while creating file
 *
 *********************************************************************************/

RC createPageFile(char *fileName)
{
  SM_FileHandle smHandle;
  int fileSize,counter;
  char fillerData[PAGE_SIZE];
  SM_PageHandle pageHandle;
  pageHandle = (SM_PageHandle) malloc(PAGE_SIZE);
  if (pageHandle == NULL) {
	  return RC_MEM_ALLOCATION_FAIL;
  }

  for (counter = 0; counter < PAGE_SIZE; counter++)
	  pageHandle[counter] = 0;

  smPointer = fopen(fileName, "w+");
  if (smPointer != NULL) {
	//  printf("\n File created");

	  	  // Set file details
	      smHandle.fileName = fileName;
	      smHandle.totalNumPages = 1;
	      smHandle.curPagePos = 0;

	    //  printf("\n Initially No of pages %d", smHandle.totalNumPages);

	      // FILE HANDLE DETAILS STORED IN FIRST BLOCK OF THE FILE
	    //  printf("\n The file details are stored in the first block of the file");

	      fwrite(&smHandle, sizeof(smHandle), sizeof(smHandle), smPointer);

	      // THE REMAINING SPACE IN FIRST BLOCK FILLED WITH NULL VALUE
	      fileSize = PAGE_SIZE - (sizeof(smHandle)*(sizeof(SM_FileHandle)));
	      memset(fillerData, '\0', fileSize);
	      fwrite(fillerData,fileSize, 1, smPointer);
	      fwrite(pageHandle,PAGE_SIZE,1, smPointer);

	     // printf("\n No of pages %d", smHandle.totalNumPages);

	      // Close the file
	      fclose(smPointer);
	      fflush(stdout);
	      return RC_OK;
  } else  {
   // printf("\n Error occurred while creating a file !! ");
    return RC_FILE_CREATE_ERROR;
  }
}

//OPENPAGEFILE FUNCTION STARTS
/***********************************
This function is used to open the 
specified file.

fopen function is used to open 
the file and the file header is 
updated in this function.
 
 ************************************/ 

RC openPageFile(char *fileName,SM_FileHandle *fHandle)
{
	// Open file for both reading and writing
		//printf("\n Opening the Page file");
	  smPointer = fopen(fileName, "r+");
	  if (smPointer == NULL)
	    return RC_FILE_NOT_FOUND;
	  else {
		// Read and store file contents
	    fread(fHandle, sizeof(fHandle), 2, smPointer);
	    fclose(smPointer);
	   // printf("\n closing file");
	    //fflush(stdout);
	    return RC_OK;
	  }
}

/*********************************************************************************
 * Function Name: closePageFile
 *
 * Description: Close an open page file
 *
 * Parameters: *fHandle - file handle of the file to be closed
 *
 * Return: Return code based on the status of the action done by method
 * 			RC_OK 			   		- if file close is successful
 * 			RC_FILE_NOT_CLOSED 		- if file is not closed
 * 			RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 *
 *********************************************************************************/
RC closePageFile(SM_FileHandle *fHandle) {
  if(checkForFileHandle(fHandle) == RC_OK) {
	  smPointer = fopen(fHandle->fileName, "r+");
	  if (smPointer != NULL) {
	      int closeRes = fclose(smPointer);
	      if(closeRes == 0) {
	      	//printf("\n Closed file :  %s", fHandle->fileName);
	      //	fflush(stdout);
	      	return RC_OK;
	      } else {
	      	//printf("\n Error occurred while closing the file");
	      //	fflush(stdout);
	      	return RC_FILE_NOT_CLOSED;

	      }
	  }
	  return RC_OK;
  }
  return RC_FILE_HANDLE_NOT_INIT;
}

/*********************************************************************************
 * Function Name: destroyPageFile
 *
 * Description: Destroy an open page file
 *
 * Parameters: *fHandle - file handle of the file to be destroyed
 *
 * Return: Return code based on the status of the action done by method
 * 			RC_OK 			     - if file destroy is successful
 * 			RC_FILE_REMOVE_ERROR - if file is not destroyed
 *
 *********************************************************************************/
RC destroyPageFile(char *fileName) {
	//printf("\n Removing file : %s", fileName);
		int file_rem = remove(fileName);
		if (file_rem == 0) {
			//printf("\n Removed the file successfully");
			return RC_OK;
		}
		else {
			//printf("\n Error while removing file");
			return RC_FILE_REMOVE_ERROR;
		}
}

/*********************************************************************************
 * Function Name : readBlock
 *
 * Description: reads the pageNumth block from a file and stores its
 *              content in the memory pointed to by the memPage page handle
 *
 * Parameters:
 *			pageNum  - the position of the block which has to be read
 *			*fHandle - the file handle
 *			*memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: RC - return code representing the status of the action done by method
 * 				RC_OK 					  - if a block is read successfully
 * 				RC_FILE_HANDLE_NOT_INIT   - if file handle is not initialized
 * 				RC_READ_NON_EXISTING_PAGE - if page number is not valid
 * 				RC_FILE_POINTER_IS_NULL   - if file pointer is NULL
 *
 *********************************************************************************/
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
		//printf("\n In readBlock function**");
		int fileStatus = isFileReadyToRead(pageNum, fHandle);
		//printf("\n File Name %s", fHandle->fileName);
		//printf("\n No of pages %d", fHandle->totalNumPages);
		//fflush(stdout);
			if(fileStatus==RC_OK)
			{
				int noElements= PAGE_SIZE * (pageNum+1);
				smPointer = fopen(fHandle->fileName, "r+");
				if(smPointer != NULL) {
					fseek(smPointer, noElements, SEEK_SET);
						fread(memPage, PAGE_SIZE, sizeof(char),smPointer);
						fHandle->curPagePos = (pageNum+1) * PAGE_SIZE;
						//printf("\n reading pageNum: %d ,  content read: %s", pageNum, memPage);
						fclose(smPointer);
						return RC_OK;
				}
				return RC_FILE_POINTER_IS_NULL;
			}
			else
				return fileStatus;
}

/*********************************************************************************
 * Function Name : getBlockPos
 *
 * Description: Returns the current page position in a file
 *
 * Parameters: *fHandle - the file handle
 *
 * Return: 		returns the current page position or RC_FILE_HANDLE_NOT_INIT
 * 				if file handle is null
 *
 *********************************************************************************/
int getBlockPos(SM_FileHandle *fHandle) {
	if(checkForFileHandle(fHandle) == RC_OK) {
		return (fHandle->curPagePos)/PAGE_SIZE;
	}
	return RC_FILE_HANDLE_NOT_INIT;
}

/*********************************************************************************
 * Function Name : readFirstBlock
 *
 * Description: Reads the first page in a file
 *
 * Parameters:
 * 			*fHandle - the file handle
 *		    *memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: return code representing the status of action done by the method
 * 			RC_OK 					- if first page is read successfully
 * 			RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 *
 *********************************************************************************/
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int handleCheck = checkForFileHandle(fHandle);
	if(handleCheck == RC_OK) {
		return readBlock(1, fHandle, memPage);
	} else
		return handleCheck;
}

/*********************************************************************************
 * Function Name : readPreviousBlock
 *
 * Description: Reads the previous page relative to the current Page Position of the file
 *
 * Parameters:
 * 		*fHandle - the file handle
		*memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: return code representing the status of action done by the method
 * 			RC_OK 					  - if previous block is read successfully
 * 			RC_FILE_HANDLE_NOT_INIT   - if file handle is not initialized
 * 			RC_READ_NON_EXISTING_PAGE - if there is no previous block to read
 *
 *********************************************************************************/
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int handleCheck = checkForFileHandle(fHandle);
	if(handleCheck == RC_OK) {
		int curPos;
		curPos = getBlockPos(fHandle);
		if(curPos > 0) {
			return readBlock(curPos-1, fHandle, memPage);
		}
		return RC_READ_NON_EXISTING_PAGE;
	}
	return handleCheck;
}

/*********************************************************************************
 * Function Name : readCurrentBlock
 *
 * Description: Reads the current page of the file
 *
 * Parameters:
 * 			*fHandle - the file handle
		    *memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: return code representing the status of action done by the method
 * 			RC_OK 					  - if previous block is read successfully
 * 			RC_FILE_HANDLE_NOT_INIT   - if file handle is not initialized
 *
 *********************************************************************************/
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int handleCheck = checkForFileHandle(fHandle);
	if(handleCheck == RC_OK) {
		int curPos;
		curPos = getBlockPos(fHandle);
		if((curPos >= 0) && (curPos <= fHandle->totalNumPages)) {
			return readBlock(curPos, fHandle, memPage);
		}
		return RC_READ_NON_EXISTING_PAGE;
	}
	return handleCheck;
}

/*********************************************************************************
 * Function Name : readNextBlock
 *
 * Description: Reads the next page relative to the curPagePos of the file
 *
 * Parameters:
 * 			*fHandle - the file handle
			*memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: return code representing the status of action done by the method
 * 			RC_OK 					  - if next block is read successfully
 * 			RC_FILE_HANDLE_NOT_INIT   - if file handle is not initialized
 * 			RC_READ_NON_EXISTING_PAGE - if there is no next block to read
 *
 *********************************************************************************/
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int handleCheck = checkForFileHandle(fHandle);
		if(handleCheck == RC_OK) {
			int curPos;
			curPos = getBlockPos(fHandle);
			if(curPos < fHandle->totalNumPages) {
				return readBlock(curPos+1, fHandle, memPage);
			}
			return RC_READ_NON_EXISTING_PAGE;
		}
		return handleCheck;
}

/*********************************************************************************
 * Function Name : readLastBlock
 *
 * Description: Reads the last page in a file
 *
 * Parameters:
 *			*fHandle - the file handle
			*memPage - the page handle (pointer to memory where data is to be stored)
 *
 * Return: return code representing the status of action done by the method
 * 				RC_OK 					- if the last block is read successfully
 * 				RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 *
 *********************************************************************************/
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {

	int handleCheck = checkForFileHandle(fHandle);
	if(handleCheck == RC_OK) {
		return readBlock(fHandle->totalNumPages, fHandle, memPage);
	}
	return handleCheck;
}

//WRITEBLOCK FUNCTION STARTS

/************************************
This function is used to write 
data to the specified block. 

writeCurrentBlock function is used 
to write the data.

**************************************/

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//printf("\n In WriteBlock Function**");
		int writtenBytes=0;
		//Check if page file handle is initialized
		if(checkForFileHandle(fHandle) == RC_OK)
		{
			if (fHandle->totalNumPages == 1)
			    fHandle->curPagePos = PAGE_SIZE;

				int fileSize = (pageNum + 1) * PAGE_SIZE;
				smPointer = fopen(fHandle->fileName, "r+");
				if(smPointer==NULL)
				{
					return RC_FILE_NOT_FOUND; // Error while opening a file
				}
				int resOffset=fseek(smPointer,fileSize,SEEK_SET);
				if(resOffset!=0)
				{
					return RC_FILE_OFFSET_FAILED;
				}
				//printf("\n Data to be written on page %d,  %s",  pageNum, memPage);
				if(memPage!=NULL)
				{
				writtenBytes = fwrite(memPage, 1, PAGE_SIZE, smPointer);
				if (writtenBytes != PAGE_SIZE)
				{
					fclose(smPointer);
					return RC_WRITE_FAILED; // Unable to write data to disk
				}
				}

				//increment the file pointer and block number
				fHandle->totalNumPages++;
				fHandle->curPagePos = ftell(smPointer);
				//close file
				fclose(smPointer);
				return RC_OK;
		}
		/*else
			printf("\n Error while opening the file");
		printf("Done with writing block");*/
		return RC_FILE_HANDLE_NOT_INIT;
}

/*********************************************************************************
 * Function Name : writeCurrentBlock
 *
 * Description: writes the current page of the file to the disk
 *
 * Parameters:
 * 				*fHandle - the file handle
 * 				memPage  - Page which holds the data which needs to be written
 *
 * Return: Return code based on the action taken by the method
 * 			RC_OK					- if write is successful
 * 			RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 * 			RC_FILE_NOT_FOUND		- if there is an error while opening file
 * 			RC_FILE_OFFSET_FAILED	- if seeking the current offset failed
 * 			RC_WRITE_FAILED			- if unable to write data to disk
 *
 *********************************************************************************/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	int writtenBytes = 0;
		//Check if page file handle is initialized
		if(checkForFileHandle(fHandle) == RC_OK) {
			if (fHandle->totalNumPages == 1)
			    fHandle->curPagePos = PAGE_SIZE;

				smPointer = fopen(fHandle->fileName, "r+");
				if(smPointer==NULL)
				{
					return RC_FILE_NOT_FOUND; // Error while opening a file
				}
				int resOffset=fseek(smPointer,fHandle->curPagePos,SEEK_SET);
				if(resOffset!=0)
				{
					return RC_FILE_OFFSET_FAILED;
				}

				writtenBytes = fwrite(memPage, 1, PAGE_SIZE, smPointer);
				if (writtenBytes)
				{
						fclose(smPointer);
						return RC_WRITE_FAILED; // Unable to write data to disk
				}

				//close file
				fclose(smPointer);
				fflush(stdout);
				return RC_OK;
		}
		return RC_FILE_HANDLE_NOT_INIT;
}

/*********************************************************************************
 * Function Name : appendEmptyBlock
 *
 * Description:	Increases the number of pages in the file by one. The new last page
 * 				will be filled by zero bytes
 *
 * Parameters:		*fHandle - the file handle
 *
 * Return:
 * 				RC_OK 					- if empty block appended successfully
 * 				RC_FILE_NOT_FOUND 		- if error while opening file
 * 				RC_FILE_HANDLE_NOT_INIT - if file handle not initialized
 *
 *********************************************************************************/
RC appendEmptyBlock(SM_FileHandle *fHandle) {
	char empty_Data[PAGE_SIZE];

	  if(checkForFileHandle(fHandle) == RC_OK) {
		  smPointer = fopen(fHandle->fileName, "a");
		    if(smPointer==NULL)
		  	{
		  		return RC_FILE_NOT_FOUND; 					// Error while opening a file
		  	}

		    //Append zero bytes in remaining space
		    memset(empty_Data, 0, PAGE_SIZE);
		    fwrite(empty_Data, PAGE_SIZE, 1, smPointer);

		    //increment number of pages in the file
		    fHandle->totalNumPages++;
		    fclose(smPointer);
		    fflush(stdout);

		    return RC_OK;
	  }
	  return RC_FILE_HANDLE_NOT_INIT;  			// "Page file handle not initialized")
}

/*********************************************************************************
 * Function Name : ensureCapacity
 *
 * Description: If the file has less than numberOfPages pages then
 * 				increase the size to numberOfPages
 *
 * Parameters:
 * 				numberOfPages - the size to which the capacity has to be increased
 * 				*fHandle 	  - file Handle
 *
 * Return: the total number of pages in the file after increasing capacity
 * 			 RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 *
 *********************************************************************************/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
	 int smCount;
	  if(checkForFileHandle(fHandle) == RC_OK) {
		  if (numberOfPages > fHandle->totalNumPages) {
		      for (smCount = fHandle->totalNumPages; smCount < numberOfPages; smCount++) {
		        appendEmptyBlock(fHandle);
		      }
		    }
		    fflush(stdout);
		    return fHandle->totalNumPages;
	  }
	  return RC_FILE_HANDLE_NOT_INIT;  			// "Page file handle not initialized")
}

/*********************************************************************************
 * Function Name : checkForFileHandle
 *
 * Description: check if the file handle is initialized or not
 *
 * Parameters:		*smHandler - the file handle
 *
 * Return:
 * 				RC_OK 					- if the file handle is not null
 * 				RC_FILE_HANDLE_NOT_INIT - if file handle is not initialized
 *
 *********************************************************************************/
extern RC checkForFileHandle(SM_FileHandle *smHandler) {
	//printf("\n Checking file handle");
	if(smHandler != NULL)
	{
		//printf("\n File handle is NOT NUll ");
		return RC_OK;
	} else
	{
		//printf("\n File handle is NUll ");
		return RC_FILE_HANDLE_NOT_INIT;
	}
}

/*********************************************************************************
 * Function Name : checkNonExistingPage
 *
 * Description: checks if the given page number exists or not
 *
 * Parameters:
 * 				pageNum 	- the page number to be checked
 * 				*smHandler  - the file handle
 *
 * Return:
 * 				RC_OK 					  - if the given page number is valid
 * 				RC_READ_NON_EXISTING_PAGE - if the page number is not valid
 *
 *********************************************************************************/
extern RC checkForFilePointer(SM_FileHandle *smHandler) {
	//printf("\n Checking file pointer");
	if(smHandler->mgmtInfo != NULL)
		return RC_OK;
	else
		return RC_FILE_POINTER_IS_NULL;
}

/*********************************************************************************
 * Function Name : checkForFilePointer
 *
 * Description: checks if the file pointer is null or not
 *
 * Parameters:		*smHandler - the file handle
 *
 * Return:
 * 			RC_OK 					- if the file pointer is not null
 * 			RC_FILE_POINTER_IS_NULL - if the file pointer is null
 *
 *********************************************************************************/
extern RC checkNonExistingPage(int pageNum, SM_FileHandle *smHandler) {
	//printf("\n Checking non existing page");
	if((pageNum >= 0) && (pageNum <= smHandler->totalNumPages))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}

/*********************************************************************************
 * Function Name :	isFileReadyToRead
 *
 * Description: checks if the file is ready and available to be read
 *
 * Parameters:
 * 				pageNum    - page number to be read
 * 				*smHandler - the file handle
 *
 * Return: RC - return code representing the status of the action done by method
 * 				RC_OK 					  - if the file is ready to be read
 * 				RC_FILE_HANDLE_NOT_INIT   - if file handle is not initialized
 * 				RC_READ_NON_EXISTING_PAGE - if page number is not valid
 * 				RC_FILE_POINTER_IS_NULL   - if file pointer is NULL
 *
 *********************************************************************************/
extern RC isFileReadyToRead(int pageNum, SM_FileHandle *smHandler) {
	//printf("\n Checking if file is ready to read.");
	if(checkForFileHandle(smHandler) == RC_OK) {
		if(checkNonExistingPage(pageNum, smHandler) == RC_OK) {
			return RC_OK;
		} else {
			return RC_READ_NON_EXISTING_PAGE;
		}
	} else {
		return RC_FILE_HANDLE_NOT_INIT;
	}
}
