#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

/*
 * struct PageFrame is the template for each pageframe present inside a bufferpool
 */
typedef struct PageFrame {
	BM_PageHandle *ph;
	int fixcount;
	int dirty;
	int pageOrder;
	int isEmpty;
	int accessOrder;
	int cb;
}PageFrame;

/*
 * struct PoolList is the template for a single bufferPool.
 * It has all details about a bufferPool like - page details, collection of pageframes
 * and a pointer to the next bufferPool
 */
typedef struct PoolList {
	BM_BufferPool *pool;
	struct PageFrame *frame;
	struct PoolList *next;
}PoolList;

/*
 *struct Buffermanager is template of buffer manager. There is only one buffer manager to manage all the bufferpools
 *BufferManager contains pointer to the head node of a list of buffer pools
 */
typedef struct BufferManager {
	struct PoolList *head;
}BufferManager;

SM_FileHandle *fh;
BufferManager manager;
int readCount, writeCount;
int mostRecentlyUsedPageCountInLRU = 0;   // used in LRU to track most recently used page
int *lurRecord;

/*
 * This method is to identify if a particular pagefile has a bufferpool associated already
 */
RC findPageFileInPool(const char *const pageFileName) {
	if(manager.head != NULL) {
		PoolList *currentPool = manager.head;     // create a temporary pointer to first pool in manager
		while(currentPool->next != NULL) {									//loop till last element
			if(strcmp(pageFileName, currentPool->pool->pageFile) == 0) {
				return RC_BUFFERPOOL_ALREADY_AVAILABE_FOR_THIS_PAGEFILE;	//if file name found, return found
			}
			currentPool = currentPool->next;
	}
		return RC_OK;	//if filename not found or if buffer manager is not yet initialized, return OK
	}
}

/*
 * This method returns all details of the pool associated with the pagefile
 */
PoolList * getPoolDetails(BM_BufferPool *const bm) {
	if(manager.head != NULL) {
		PoolList *found = NULL, *currentPool = manager.head;
		while(currentPool != NULL) {									//loop till last element

			if(strcmp(bm->pageFile, currentPool->pool->pageFile) == 0) {

				found = currentPool; //if file name found, return found
				break;
			}
			currentPool = currentPool->next;
		}
		return found;
	} else {
		return NULL;
	}
}

/*
 * Once we know which bufferpool has to be removed from the pool, this method removes it from the list of pools
 */
RC removePoolFromPoolList(BM_BufferPool *const bm) {

		PoolList *previousPool ,*currentPool = manager.head;
		if(currentPool != NULL) {
			if(strcmp(bm->pageFile, currentPool->pool->pageFile) == 0) {		//if this is the pool of the file
				if(currentPool->next != NULL) {
					manager.head = currentPool->next;
				} else {
					manager.head = NULL;
				}
				return RC_OK;
			} else {
				previousPool = currentPool;
				currentPool = currentPool->next;
				while(currentPool != NULL) {
					if(strcmp(bm->pageFile, currentPool->pool->pageFile) == 0) {		//if pool of the file found
						previousPool->next = currentPool->next;
						currentPool->next = NULL;
						return RC_OK;
					}
					previousPool = currentPool;
					currentPool = currentPool->next;
				}
			}
		}
		return RC_POOL_REMOVE_ERROR;
}

int findMaxPageOrder(PageFrame *frame, BM_BufferPool *bm) {
	int i=0, max = -1;
	while(i<bm->numPages) {
		if(frame[i].pageOrder > max) {
			max = frame[i].pageOrder;
		}
		i++;
	}
	return max;
}

int findMaxAccessOrder(PageFrame *frame, BM_BufferPool *bm) {
	int i=0, max = -1;
		while(i<bm->numPages) {
			if(frame[i].accessOrder > max) {
				max = frame[i].accessOrder;
			}
			i++;
		}
		return max;
}

int findMinPageOrder(PageFrame *frame, BM_BufferPool *bm) {
	   int page2Replace=0, min, i;
	   min = INT_MAX;
	   for(i=0; i<bm->numPages; i++) {
		   if (frame[i].pageOrder <= min && frame[i].fixcount ==0) {
			   min = frame[i].pageOrder;
			   page2Replace = i;
		   }
	   }
	   return page2Replace;
}

int findMinAccessOrder(PageFrame *frame, BM_BufferPool *bm) {
	int page2Replace=0, min, i;
		   min = INT_MAX;
		   for(i=0; i<bm->numPages; i++) {
			   if (frame[i].accessOrder <= min && frame[i].fixcount ==0) {
				   min = frame[i].accessOrder;
				   page2Replace = i;
			   }
		   }
		   return page2Replace;
}

RC FIFO_pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {

	PoolList *pool = getPoolDetails(bm);

	if(pool != NULL) {								//only if there is a pool associated with the pagefile we proceed with pinPage
		int flag = 0;								//flag to check if page already pinned

			int i=0;
			//if page is already in bufferpool, just increase its fix count and replace its content with the new content passed to this method
			while(i < bm->numPages) {
				if(pool-> frame[i].isEmpty == 0) {
					if(pageNum == pool->frame[i].ph->pageNum) {

						flag = 1;
						if(pool->frame[i].fixcount == 0) {
							readBlock(pageNum, fh, pool->frame[i].ph->data);
						}
						pool->frame[i].fixcount++;
						page->data = pool->frame[i].ph->data;
						return RC_OK;
					}
				}
				i++;
			}
		//}

		//if page not in bufferpool
		if(flag == 0) {
			int i=0, frameNotEmpty = 1, maxOrder;
			while(i < bm->numPages) {
				//if an empty frame is found

				if(pool-> frame[i].isEmpty == 1) {

					frameNotEmpty = 0;											//mark a flag that an empty frame is found

					readBlock(pageNum, fh, pool->frame[i].ph->data);			//read contents from disk into the pageframe
					page->data = pool->frame[i].ph->data;
					pool->frame[i].ph->pageNum = pageNum;
					pool->frame[i].fixcount = 1;								//set fixcount for the page to 1
					pool->frame[i].dirty = 0;									//mark the page as not dirty
					maxOrder = findMaxPageOrder(pool->frame, bm);
					maxOrder++;
					pool->frame[i].pageOrder = maxOrder;						//find the pageOrder value for this page and save it
					pool->frame[i].isEmpty = 0;									//mark that age frame is not empty
					readCount++;
					return RC_OK;
				}
				i++;
			}

			//if no pageFrame is free, then we have to use FIFO replacement strategy to pin the page
			if(frameNotEmpty == 1) {
				//check if there is any page which is the oldest and also has fixCount 0
				int index, matchFound = 0;

				index = findMinPageOrder(pool->frame, bm);
				if(index > -1) {
					matchFound = 1;
				}
				//if there is one such place which is available, replace it
				if(matchFound == 1) {
					if(pool->frame[index].dirty == 1) {
						forcePage(bm, pool->frame[index].ph);
						writeCount++;
					}
					unpinPage(bm, pool->frame[index].ph);

					readBlock(pageNum, fh, pool->frame[index].ph->data);			//read contents from disk into the pageframe
					pool->frame[index].ph->pageNum = pageNum;
					pool->frame[index].fixcount = 1;								//set fixcount for the page to 1
					pool->frame[index].dirty = 0;									//mark the page as not dirty
					maxOrder = findMaxPageOrder(pool->frame, bm);
					maxOrder++;
					pool->frame[index].pageOrder = maxOrder;						//find the pageOrder value for this page and save it
					pool->frame[index].isEmpty = 0;										//mark that age frame is not empty
					page->data = pool->frame[index].ph->data;
					readCount++;
					return RC_OK;
				} else {															//when there is not old page with fix count 0, return an error message
					return RC_POOL_IS_FULL;
				}
			}
		}
	} else {
		return RC_POOL_NOT_FOUND;													//when there is no bufferpool associated with the page file
	}
}


RC LRU_pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	int maxOrder;

	PoolList *pool = getPoolDetails(bm);
	if(pool != NULL) {								//only if there is a pool associated with the pagefile we proceed with pinPage
		int flag = 0;								//flag to check if page already pinned

		//if page is already in bufferpool, just increase its fix count and replace its content with the new content passed to this method
		if(pool->frame != NULL) {
			int i=0;
			while(i < bm->numPages) {
				if(pool-> frame[i].isEmpty == 0) {
					if(pageNum == pool->frame[i].ph->pageNum) {
						flag = 1;
						if(pool->frame[i].fixcount == 0) {
							fflush(stdout);
							readBlock(pageNum, fh, pool->frame[i].ph->data);
						}
						pool->frame[i].fixcount++;
						*(pool->frame[i].ph->data) = *(page->data);
						page->data = pool->frame[i].ph->data;
						maxOrder = findMaxAccessOrder(pool->frame, bm);
						maxOrder++;
						pool->frame[i].accessOrder = maxOrder;						//find the pageOrder value for this page and save it
						fflush(stdout);
						return RC_OK;

					}
				}
				i++;
			}
		}
		//if page not in bufferpool

		if(flag == 0) {
			int i=0, frameNotEmpty = 1 ;
			//search if there is at least one empty pageFrame where the content can be read into
			while(i < bm->numPages) {
				//if an empty frame is found
				if(pool-> frame[i].isEmpty == 1) {
					frameNotEmpty = 0;											//mark a flag that an empty frame is found

					readBlock(pageNum, fh, pool->frame[i].ph->data);			//read contents from disk into the pageframe
					fflush(stdout);
					page->data = pool->frame[i].ph->data;
					pool->frame[i].ph->pageNum = pageNum;

					pool->frame[i].fixcount = 1;								//set fixcount for the page to 1

					pool->frame[i].dirty = 0;								//mark the page as not dirty

					maxOrder = findMaxAccessOrder(pool->frame, bm);

					maxOrder++;
					pool->frame[i].accessOrder = maxOrder;						//find the pageOrder value for this page and save it
					pool->frame[i].isEmpty = 0;									//mark that age frame is not empty

					readCount++;
					return RC_OK;
				}
				i++;
			}

			//if no pageFrame is free, then we have to use FIFO replacement strategy to pin the page
			if(frameNotEmpty == 1) {
				//check if there is any page which is the oldest accessed and also has fixCount 0

				int index, matchFound = 0;
				index = findMinAccessOrder(pool->frame, bm);
				if(index > -1) {
					matchFound = 1;
				}

				//if there is one such place which is available, replace it
				if(matchFound == 1) {

					if(pool->frame[index].dirty == 1) {
						forcePage(bm, pool->frame[index].ph);
						writeCount++;
					}
					unpinPage(bm, pool->frame[index].ph);
					readBlock(pageNum, fh, pool->frame[index].ph->data);	//read contents from disk into the pageframe
					pool->frame[index].ph->pageNum = pageNum;
					pool->frame[index].fixcount = 1;								//set fixcount for the page to 1
					pool->frame[index].dirty = 0;									//mark the page as not dirty
					maxOrder = findMaxAccessOrder(pool->frame, bm);
					maxOrder++;
					pool->frame[index].accessOrder = maxOrder;						//find the pageOrder value for this page and save it
					pool->frame[index].isEmpty = 0;
					page->data = pool->frame[index].ph->data;					//mark that age frame is not empty
					readCount++;
					return RC_OK;
				} else {															//when there is not old page with fix count 0, return an error message
					return RC_POOL_IS_FULL;
				}
			}
		}
	} else {
		return RC_POOL_NOT_FOUND;													//when there is no bufferpool associated with the page file
	}
}

RC clock_pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	PoolList *pool = getPoolDetails(bm);
	if(pool != NULL) {								//only if there is a pool associated with the pagefile we proceed with pinPage
		int flag = 0;								//flag to check if page already pinned

		//if page is already in bufferpool, just increase its fix count and replace its content with the new content passed to this method
		if(pool->frame != NULL) {
			int i=0;
			while(i < bm->numPages) {
				if(pool-> frame[i].isEmpty == 0) {
					if(pageNum == pool->frame->ph->pageNum) {
						flag = 1;
						if(pool->frame[i].fixcount == 0) {
							readBlock(pageNum, fh, pool->frame[i].ph->data);
						}
						pool->frame[i].fixcount++;
						pool->frame[i].cb = 0; // used for the clock strategy
						*(pool->frame[i].ph->data) = *(page->data);
						page->data = pool->frame[i].ph->data;
						return RC_OK;
					}
				}
				i++;
			}
		}
		//if page not in bufferpool
		if(flag == 0) {
			int i=0, frameNotEmpty = 1, maxOrder;
			//search if there is at least one empty pageFrame where the content can be read into
			while(i < bm->numPages) {
				//if an empty frame is found
				fflush(stdout);
				if(pool-> frame[i].isEmpty == 1) {
					frameNotEmpty = 0;											//mark a flag that an empty frame is found

					readBlock(pageNum, fh, pool->frame[i].ph->data);			//read contents from disk into the pageframe
					page->data = pool->frame[i].ph->data;
					pool->frame[i].ph->pageNum = pageNum;
					pool->frame[i].fixcount = 1;								//set fixcount for the page to 1
					pool->frame[i].cb = 1; 										// used for the clock strategy

					pool->frame[i].dirty = 0;									//mark the page as not dirty

					maxOrder = findMaxPageOrder(pool->frame, bm);

					maxOrder++;
					pool->frame[i].pageOrder = maxOrder;						//find the pageOrder value for this page and save it

					pool->frame[i].isEmpty = 0;									//mark that age frame is not empty

					readCount++;
					return RC_OK;
				}
				i++;
			}


				//if no pageFrame is free, then we have to use clock replacement strategy to pin the page
				if(frameNotEmpty == 1)
				{
					int index, i=0, matchFound = 0;
					while(i<bm->numPages)
					{
						//check if there is any page which has the clock bit set as 0
						/* retrieve first frame with cb = 0 and set all bits to zero along the way */
					while (pool->frame[i].cb == 1)
						{
							pool->frame[i].cb = 0;

						}
					/*if (pool->frame[i] == NULL)
						{
						return RC_POOL_IS_FULL;

					}else {*/
					matchFound = 1;
					index = i;

					i++;
				}
				//if there is one such place which is available, replace it
				if(matchFound == 1)
				{
					if(pool->frame[index].dirty == 1)
					{
						forcePage(bm, pool->frame[index].ph);
						writeCount++;
					}
					unpinPage(bm, pool->frame[index].ph);
					readBlock(pageNum, fh, pool->frame[index].ph->data);			//read contents from disk into the pageframe
					pool->frame[i].ph->pageNum = pageNum;
					pool->frame[index].fixcount = 1;								//set fixcount for the page to 1
					pool->frame[index].dirty = 0;									//mark the page as not dirty
					pool->frame[index].cb = 1;
					maxOrder = findMaxPageOrder(pool->frame, bm);
					maxOrder++;
					pool->frame[index].pageOrder = maxOrder;						//find the pageOrder value for this page and save it
					pool->frame[i].isEmpty = 0;										//mark that age frame is not empty
					page->data = pool->frame[index].ph->data;
					readCount++;
					return RC_OK;
				} else {															//when there is not old page with fix count 0, return an error message
					return RC_POOL_IS_FULL;
				}
			}
		}
	} else {
		return RC_POOL_NOT_FOUND;													//when there is no bufferpool associated with the page file
	}
}

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy, void *stratData) {
   int checkDuplicatePool = findPageFileInPool(pageFileName);

	readCount = 0, writeCount = 0;
	if(checkDuplicatePool == RC_OK) {
	//if(pool == NULL) {

		fh=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));							// allocating memory for file handle

			//setting parameters passed to the fields of bufferpool
			bm->numPages = numPages;
			bm->strategy = strategy;
			bm->pageFile = (char *)pageFileName;

			openPageFile (bm->pageFile, fh);										//opening the file for read

			/*creating a bufferpool with as many pageframes as much as number of pages in the pagefile*/

			PageFrame *frame = (PageFrame *)malloc(numPages*(sizeof(PageFrame)));	// allocating memory for pageframes for the buffer pool
			int i=0;
			while(i<bm->numPages) {
				frame[i].isEmpty = 1;
				frame[i].pageOrder = -1;
				frame[i].accessOrder = -1;
				frame[i].cb = 0;
				frame[i].fixcount = 0;
				frame[i].dirty = 0;
				frame[i].ph = MAKE_PAGE_HANDLE();
				frame[i].ph->pageNum = NO_PAGE;
				frame[i].ph->data = (char *) malloc((sizeof(char))*PAGE_SIZE);
				i++;
			}

		//	PoolList *currentPool = (PoolList *) malloc((sizeof(BM_BufferPool)) + (numPages *(sizeof(PageFrame))));
			PoolList *currentPool = (PoolList *) malloc (sizeof(PoolList));

			currentPool->pool = bm;
			currentPool->frame = frame;
			currentPool->next = NULL;

			/*Adding the bufferpool to the buffer manager*/
			if(manager.head == NULL) {
				manager.head = currentPool;
			} else {
				PoolList *nextPool = manager.head;
				while(nextPool != NULL) {
					nextPool = nextPool->next;
				}
				nextPool->next = currentPool;
			}
			return RC_OK;
		}
		else
		{
			return RC_BUFFERPOOL_ALREADY_AVAILABE_FOR_THIS_PAGEFILE;
		}
	}

RC shutdownBufferPool(BM_BufferPool *const bm) {

	forceFlushPool(bm); 			//first all dirty pages have to be written to the disk
	int frameNum;

		PoolList *poolToBeShutDown = getPoolDetails(bm);							//get all details about the pool
		if(poolToBeShutDown != NULL)
		{

			/*//if the pool to be shutdown has a pagefile associated
			for(frameNum=0;frameNum < bm->numPages; frameNum++) {
				if( poolToBeShutDown->frame[frameNum].fixcount > 0) {						//check if there are any pinned pages
					printf("\n still pages are there with fxcnt > 0");
					fflush(stdout);
					return RC_BUFFERPOOL_SHUTDOWN_ERROR_PAGES_STILL_PINNED;			//return error message if pages are pinned
				}

			}*/
			int result = removePoolFromPoolList(bm);								//link the previous pool in the list of pool, to the next pool in the list

			free(poolToBeShutDown->frame->ph->data);
			free(poolToBeShutDown->frame->ph);
			free(poolToBeShutDown->frame);											//free the memory allocated for page frames in the pool
			free(poolToBeShutDown); 												// free memory allocated for this pool


			return result;
		}
	return RC_POOL_NOT_FOUND;
}

RC forceFlushPool(BM_BufferPool *const bm) {
	PoolList *flushPool = getPoolDetails(bm);										//get details about the bufferpool associated with the pagefile
	if(flushPool != NULL) {															//if bufferpool exists for the file
		int currentPageFrame = 0;
		while(currentPageFrame < bm->numPages) { 						//loop through all pageframes of the bufferpool
			if((flushPool->frame[currentPageFrame].dirty == 1)&&(flushPool->frame[currentPageFrame].fixcount == 0)) {										//if a page is dirty and its fix count is zero
				int pageNumber, returnCode;
				char *data;
				pageNumber = flushPool->frame[currentPageFrame].ph->pageNum;
				data = flushPool->frame[currentPageFrame].ph->data;
				returnCode = writeBlock(pageNumber, fh, data);						//write the contents of the page to the disk

				/*if write is successful, increase write count, mark the page as not dirty,
				close the pagefile and return RC_OK*/
				if(returnCode == RC_OK) {
					writeCount++;
					flushPool->frame[currentPageFrame].dirty = 0;
					closePageFile(fh);
				} else {															//if write not successful, return error message
					closePageFile(fh);
					return RC_WRITE_FAILED;
				}
			}
			currentPageFrame++;
		}
		return RC_OK;
	} else {
		return RC_POOL_NOT_FOUND;													//when there is no pool associated with the file name
	}
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
	PoolList *pool = getPoolDetails(bm);											//get details about the bufferpool containing that page
	if(pool != NULL) {																//if there is a pool associated with that page
		int currentPageFrame = 0;
		while(currentPageFrame < bm->numPages) {								//loop through page frames to locate the page
				if(page->pageNum == pool->frame[currentPageFrame].ph->pageNum) {							//if page is found
					pool->frame[currentPageFrame].dirty = 1;												//mark it dirty
					return RC_OK;
				}
			currentPageFrame++;
		}
		return RC_PAGENUM_NOT_IN_BUFFERPOOL;										//if page is not pinned in the bufferpool
	}
	return RC_POOL_NOT_FOUND;														//if there is no pool for this pagefile
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
	PoolList *pool = getPoolDetails(bm);
	if(pool != NULL) {
		int currentPageFrame = 0;
		while(currentPageFrame < bm->numPages) {								//loop through page frames to locate the page
			if(page->pageNum == pool->frame[currentPageFrame].ph->pageNum) {							//if page is found
				if(pool->frame[currentPageFrame].fixcount > 0) {
					int count = pool->frame[currentPageFrame].fixcount;
					count--;
					pool->frame[currentPageFrame].fixcount = count;
					forcePage(bm, page);
					return RC_OK;
				}
				if(pool->frame[currentPageFrame].fixcount == 0) {
					return RC_OK;
				}
			}
			currentPageFrame++;
		}
		return RC_PAGENUM_NOT_IN_BUFFERPOOL;										//if page is not pinned in the bufferpool
	}
	return RC_POOL_NOT_FOUND;														//if there is no pool for this pagefile
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
	PoolList *pool = getPoolDetails(bm);											//get details about the bufferpool containing that page
		if(pool != NULL) {															//if there is a pool associated with that page
			int currentPageFrame = 0;
			while(currentPageFrame < bm->numPages) {							//loop through page frames to locate the page
				if(page->pageNum == pool->frame[currentPageFrame].ph->pageNum) {						//if page is found
					int returnCode;
					returnCode = writeBlock(page->pageNum, fh, page->data);			//write the contents of the page to the disk
					if(returnCode == RC_OK) {
						return RC_OK;
					} else {
						return RC_WRITE_FAILED;										//if write not successful, return error message
					}
				}
				currentPageFrame++;
			}
			return RC_PAGENUM_NOT_IN_BUFFERPOOL;									//if page is not pinned in the bufferpool
		}
		return RC_POOL_NOT_FOUND;													//if there is no pool for this pagefile
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	page->pageNum = pageNum;
	asprintf(&page->data, "%s", "");
	//asprintf(&page->data, "%s-%i", "Page", pageNum);
	int result;
	switch(bm->strategy) {
		case RS_FIFO: 	return FIFO_pinPage(bm, page, pageNum);
		case RS_LRU: 	return LRU_pinPage(bm, page, pageNum);
	}
}

PageNumber *getFrameContents (BM_BufferPool *const bm) {
	PoolList *PoolForFrame = getPoolDetails(bm);
	int *frameCounts;
	frameCounts = (PageNumber *) malloc(bm->numPages *(sizeof(PageNumber*)+1));
	int i=0;
	if(PoolForFrame != NULL) {
		while(i< bm->numPages){
				if(PoolForFrame->frame[i].isEmpty == 0){
					frameCounts[i] = PoolForFrame->frame[i].ph->pageNum;
				}
				else {
					frameCounts[i] = -1;
				}
			i++;
		}
	}
	return frameCounts;
}


bool *getDirtyFlags (BM_BufferPool *const bm) {
	PoolList *PoolDetails = getPoolDetails(bm);
	bool *dirtyFlags;
	dirtyFlags = (bool *) malloc(sizeof(int *) * bm->numPages);

	int i = 0;
	if(PoolDetails != NULL) {
		while (i < bm->numPages) {
				if (PoolDetails->frame[i].isEmpty == 0) {
					if (PoolDetails->frame[i].dirty == 1) {
						dirtyFlags[i] = true;
					} else {
						dirtyFlags[i] = false;
					}
				} else {
					dirtyFlags[i] = false;
				}
				i++;

		}
	}

	return dirtyFlags;

}
int *getFixCounts (BM_BufferPool *const bm) {
		PoolList *pool = getPoolDetails(bm);
		int *fcounts;
		fcounts = (int *) malloc(bm->numPages *(sizeof(int*)+1));

		int i = 0;
		while(pool != NULL && i < bm->numPages)
		{
			if(pool->frame[i].isEmpty == 0){

				fcounts[i] = pool->frame[i].fixcount;
				}
				else {
					fcounts[i] =  0 ;
				}
				i++;
	    }

		return fcounts;
}

int getNumReadIO (BM_BufferPool *const bm) {

	return readCount;

}

int getNumWriteIO (BM_BufferPool *const bm) {

	return writeCount;
}
