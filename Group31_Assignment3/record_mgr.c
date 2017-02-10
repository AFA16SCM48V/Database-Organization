#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include <time.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "dt.h"
#include "tables.h"
#include "record_mgr.h"

#define LENGTH_OF_META_DATA 8

typedef struct LookUp
{
    Expr *cond;
    int currRecord;
    int numOfRecords;
    int noOfPages;
    int currentPage;
    int parsedRecords;
	int empty_block;
} LookUp;

RC initRecordManager(void* mgmtData) {
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

int updateOffset(int offset, int increment) {
	return offset+increment;
}

RC markNextAvailablePage(char *data, int pageNum) {
	int memoffset = 0;
	memcpy(data+memoffset, &pageNum, sizeof(int));
	return RC_OK;
}

void writeMaxNumOfSlots(char *data, int offset, int maxNoOfSlots, int size) {
	memcpy(data+offset, &maxNoOfSlots, size);
}

void writeSchemaLength(char *data, int offset, int schemaLength, int size) {
	memcpy(data+offset, &schemaLength, size);
}

void writeSchema(char *data, int offset, char *schema, int size) {
	memcpy(data+offset, schema, size);
}

void writePageNumber(char *data, int offset, int pageNumber, int size) {
	memcpy(data+offset, &pageNumber, size);
}

void writeNumOfSlots(char *data, int offset, int numOfSlots, int size) {
	memcpy(data+offset, &numOfSlots, size);
}

RC writeSchemaInPageFile(char *data, Schema *schema) {
	int schemaLength, memOffset = 0;
	char *schemaInString = malloc(200);
	schemaInString = convertSchemaToString (schema);
	schemaLength = strlen(schemaInString);
	int recordSize = getRecordSize(schema);
	int intSize = sizeof(int);
	int maxNoOfSlots = (PAGE_SIZE - LENGTH_OF_META_DATA) / (recordSize+intSize);
	memOffset = updateOffset(memOffset, intSize);
	writeMaxNumOfSlots(data, memOffset, maxNoOfSlots, intSize);
	memOffset = updateOffset(memOffset, intSize);
	writeSchemaLength(data, memOffset, schemaLength, intSize);
	memOffset = updateOffset(memOffset, intSize);
	writeSchema(data, memOffset, schemaInString, schemaLength);
	return RC_OK;
}

RC addNewPageToInsertRecord(char *data, int pageNumber) {
	int numOfSlots = 0, memOffset = 0;
	int intSize = sizeof(int);
	writePageNumber(data, memOffset, pageNumber, intSize);
	memOffset = updateOffset(memOffset, intSize);
	writeNumOfSlots(data, memOffset, numOfSlots, intSize);
	return RC_OK;
}

RC writeRecord(BM_PageHandle *phandle, Record *record, int slotNum, int recordPos, int recordSize, int intSize, int pageNum) {

	memcpy(phandle->data+recordPos, &slotNum, intSize);				//write the slot number
	writeNumOfSlots(phandle->data, 4, slotNum, intSize);				//update no of slots in the page

	recordPos = updateOffset(recordPos, intSize);					//move the offset
	memcpy(phandle->data+recordPos, record->data, recordSize);		//write the data

	RID *rid = (RID *) malloc (sizeof(RID));
	rid->page = pageNum;
	rid->slot = slotNum;
	//update the RID details in the record
	record->id.page = rid->page;
	record->id.slot = rid->slot;
	return RC_OK;
}

RC setValuesForFieldsInSchema(Schema* schema, char* data) {
	int i,sizeOfSchema = 0, intSize = sizeof(int), memOffset = 0;
	char* firstSplit;
	char* secondSplit;
	char *attrNamesAndDataTypes[200], *partsSplit[100];

	memOffset = updateOffset(memOffset, 2*intSize);
	memcpy(&sizeOfSchema, data+memOffset, intSize);
	char* schemaInString = (char *) malloc(sizeOfSchema);
	memOffset += sizeof (sizeOfSchema);
	memcpy(schemaInString, data + memOffset, sizeOfSchema);

	firstSplit = strtok(schemaInString, ";");
	schema->numAttr = *firstSplit-'0';

	for(i=0;firstSplit != NULL;i++) {
		partsSplit[i] = firstSplit;
		firstSplit = strtok(NULL, ";");
	}

	char key[] = "";
	strcpy(key, partsSplit[2]);
	schema->keySize = strlen(key);
	schema->keyAttrs = (int *)malloc(sizeof(sizeOfSchema) * (schema->keySize));
	int keyAttr[] = {0};
	schema->keyAttrs = keyAttr;

	char output[]="";
	char* result;
	strcpy(output, partsSplit[1]);
	result = strtok(output, ",");

	for(i=0;result != NULL;i++) {
			attrNamesAndDataTypes[i] = result;
			result = strtok(NULL, ",");
	}

	schema->attrNames = (char **) malloc(sizeof (char*) * schema->numAttr);
	schema->dataTypes = (DataType *) malloc(sizeof (DataType)*schema->numAttr);
	schema->typeLength = (int *)malloc((sizeof (sizeOfSchema) * schema->numAttr));
	char *dataType;
	char *subType = (char *) malloc(10*sizeof(char));

	for(i=0; i< (int)schema->numAttr; i++) {
		schema->attrNames[i] = strtok(attrNamesAndDataTypes[i], ":");
		fflush(stdout);
		dataType = strtok(NULL, ":");
		char c = *dataType;
		switch(c) {
		case 'I': schema->dataTypes[i] = DT_INT;
				  schema->typeLength[i] = 0;
				  break;
		case 'F': schema->dataTypes[i] = DT_FLOAT;
				  schema->typeLength[i] = 0;
				  break;
		case 'B': schema->dataTypes[i] = DT_BOOL;
				  schema->typeLength[i] = 0;
				  break;
		case 'S': schema->dataTypes[i] = DT_STRING;
				  char* size = dataType;
				  size = strtok(size, "[");
				  size = strtok(NULL, "]");
				  schema->typeLength[i] = atoi(size);
				  break;
		}
	}

	return RC_OK;
}

void setValuesForFieldsInRecord(Record *record, LookUp *data, RM_ScanHandle * scan){

	record->id.page = data->currentPage;
	record->id.slot = data->currRecord;
    getRecord(scan -> rel, record->id, record);
}

RC createTable(char* name, Schema* schema) {
	BM_BufferPool *bpool = MAKE_POOL();					//allocating memory for buffer pool for this pagefile
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();			//allocating memory for pagehandle for this pagefile
	BM_PageHandle *phandle1 = MAKE_PAGE_HANDLE();

	createPageFile(name);								//creating the underlying pagefile for the table
	initBufferPool(bpool, name, 3, RS_FIFO, NULL);		//setting up the bufferpool for the pagefile
	pinPage(bpool, phandle, 0);							//pinning page 0 to write schema details
	markNextAvailablePage(phandle->data, 1);
	writeSchemaInPageFile(phandle->data ,schema);		//writing schema details in page 0
	markDirty(bpool, phandle);
	unpinPage(bpool, phandle);

	pinPage(bpool, phandle1, 1);							//pin page1 to create first block
	addNewPageToInsertRecord(phandle1->data, 1);
	markDirty(bpool, phandle1);
	unpinPage(bpool, phandle1);
	forceFlushPool(bpool);								//forceflush the buffer pool
	shutdownBufferPool(bpool);							//shutdown the bufferpool
	free(bpool);
	free(phandle);
	free(phandle1);
	return RC_OK;
}

RC openTable(RM_TableData* rel, char* name) {
	//set the name field of rel to the table name
	rel->name = (char *) malloc (sizeof(name));
	rel->name = name;

	//set the mgmtData field of rel to the bufferPool associated with the file
	rel->mgmtData = MAKE_POOL();
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();

	initBufferPool(rel->mgmtData, name, 3, RS_FIFO, NULL);
	pinPage(rel->mgmtData, phandle, 0);

	//write the values for respective fields of schema inside rel with schema read from the file
	rel->schema = (Schema *) malloc (sizeof(Schema));
	setValuesForFieldsInSchema(rel->schema, phandle->data);
	unpinPage(rel->mgmtData, phandle);
	free(phandle);
    return RC_OK;
}

RC closeTable(RM_TableData *rel)  {
    free(rel->schema->dataTypes);
    free(rel->schema->attrNames);
    free(rel->schema->typeLength);
    free(rel->schema);
    shutdownBufferPool(rel->mgmtData);
    return RC_OK;
}

RC deleteTable(char *name) {
    destroyPageFile(name);
    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {

    int noOfTuplesInEachPage = 0, totalNoOfTuples=0;
    int intSize=sizeof(int);
    int pageNum = 0, i = 1, memOffset;
    BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
    BM_PageHandle *phandle1 = MAKE_PAGE_HANDLE();

    pinPage(rel->mgmtData, phandle, 0);
    memcpy(&pageNum, phandle->data, intSize);
    unpinPage(rel->mgmtData, phandle);

    for (i = 1; i <= pageNum; i++) {
    	memOffset = 0;
    	memOffset = updateOffset(memOffset, intSize);
        pinPage(rel->mgmtData, phandle1, i);
        memcpy(&noOfTuplesInEachPage, phandle1->data+memOffset, intSize);
        totalNoOfTuples += noOfTuplesInEachPage;
        unpinPage(rel->mgmtData, phandle1);
    }

    free(phandle);
    free(phandle1);
    return totalNoOfTuples;
}

RC insertRecord(RM_TableData* rel, Record* record) {

	int pageNum=0, maxSlots = 0, noOfSlots = 0, recordSize = 0;
	char c;
	int memOffset = 0, intSize = sizeof(int), recordPos = PAGE_SIZE;
	int schemaSize = 0; char *schema = (char *) malloc(sizeof(char));
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
	pinPage(rel->mgmtData, phandle, 0);							//pin the page where schema is written to read values

	memcpy(&pageNum, phandle->data, sizeof(int));			//read the pagenum from file and store it
	memOffset += sizeof(int);
	memcpy(&maxSlots, phandle->data+memOffset, sizeof(int));		//read the max no of slots and store it
	memOffset = updateOffset(memOffset, intSize);
	memcpy(&schemaSize, phandle->data+memOffset, intSize);
	memOffset = updateOffset(memOffset, intSize);
	memcpy(schema, phandle->data+memOffset, schemaSize);
	unpinPage(rel->mgmtData, phandle);

	memOffset = 0;
	pinPage(rel->mgmtData, phandle, pageNum);					//pin the next available page
	memcpy(&pageNum, phandle->data, intSize);					//copy its pagenum value to pagenum variable
	memOffset = updateOffset(memOffset, intSize);
	memcpy(&noOfSlots, phandle->data+memOffset, intSize);		//copy its no of slots value to noOfSlots variable

	recordSize = getRecordSize(rel->schema);					//calculate the record size
	recordPos = (LENGTH_OF_META_DATA+(noOfSlots * (recordSize + intSize)))+1;		//calculate the next available position to write record

	if(noOfSlots < maxSlots) {									//if still free slots are there to write this record
		writeRecord(phandle, record, noOfSlots+1, recordPos, recordSize, intSize, pageNum);		//write the record
		int c = -1;
		memcpy(&c, phandle->data+4, intSize);
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
		return RC_OK;
	} else {													//if not enough memory to write the record
		unpinPage(rel->mgmtData, phandle);
		pinPage(rel->mgmtData, phandle, 0);
		pageNum = pageNum+1;
		markNextAvailablePage(phandle->data, pageNum);			//update the next available page number in schema details
		memcpy(&pageNum, phandle->data, intSize);
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
		pinPage(rel->mgmtData, phandle, pageNum);				//pin the next available page

		addNewPageToInsertRecord(phandle->data, pageNum);
		recordPos = (LENGTH_OF_META_DATA+(0 * (recordSize + intSize)))+1;		//calculate the next available position to write recor
		noOfSlots = 1;
		writeRecord(phandle, record, noOfSlots, recordPos, recordSize, intSize, pageNum);	//write record
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
		return RC_OK;
	}
}

RC deleteRecord(RM_TableData *rel, RID id) {
    int markTombstone = 0 , intSize=sizeof (int), recordpos, memOffset = 0;                                    //Declare the variables
	int recordSize = getRecordSize(rel->schema);
	int noOfSlots = (id.slot);
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
	pinPage(rel->mgmtData, phandle, id.page);
	recordpos = LENGTH_OF_META_DATA+((noOfSlots-1)*(recordSize+intSize))+1;		//find out where the record is located
	memOffset = updateOffset(recordpos, intSize);
	memcpy(phandle->data + memOffset, &markTombstone,recordSize);                 //Mark the tombstone
    markDirty(rel->mgmtData, phandle);
    unpinPage(rel->mgmtData, phandle);
    forceFlushPool(rel->mgmtData);
    free(phandle);
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    int recordpos, memOffset = 0, intSize=sizeof(int);   //Variables are declared
    int recordSize = getRecordSize(rel->schema);
    int noOfSlots = record->id.slot;
    BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, phandle, record->id.page);
    recordpos = LENGTH_OF_META_DATA+((noOfSlots-1)*(recordSize+intSize))+1;		//find out where the record is located
    memOffset = updateOffset(recordpos, intSize);
    memcpy(phandle->data + memOffset, record->data,recordSize);
    markDirty(rel->mgmtData, phandle);
    unpinPage(rel->mgmtData, phandle);
    forceFlushPool(rel->mgmtData);
    free(phandle);
    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)  {
    int intSize = sizeof (int);
    int recordPos = 0, memOffset = 0;
    int recordSize = getRecordSize(rel->schema);
    char dataAtPos[recordSize];
	int noOfSlots = id.slot;
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
	pinPage(rel->mgmtData, phandle, id.page);
	recordPos = LENGTH_OF_META_DATA+((noOfSlots-1)*(recordSize+intSize))+1;		//find out where the record is located
	memOffset = updateOffset(recordPos, intSize);
	memcpy(record->data, phandle->data+memOffset, recordSize);
    record->id.slot = id.slot;
    record->id.page = id.page;
    unpinPage(rel->mgmtData, phandle);
    free(phandle);
    return RC_OK;
}

int getSize(Schema *schema, int counter){
//get size of schema according to the data type
    if (schema -> dataTypes[counter] == DT_INT)                                  // if type is int
        return sizeof(int);                                                     // assign size of int
    else if (schema -> dataTypes[counter] == DT_STRING)                         // if type is string
        return schema -> typeLength[counter];                                   // assign string
    else if (schema -> dataTypes[counter] == DT_FLOAT)                          // if type is float
        return sizeof(float);                                                   // assign float
    else if (schema -> dataTypes[counter] == DT_BOOL)                           // if type is bool
        return sizeof(bool);                                                    // asiign bool
}

int getRecordSize(Schema *schema)  {
	// Check if the Schema is not null
		if (schema == NULL) {
			return RC_INVALID_HANDLE;
		}

	    // Get size of the record according to the data type of the attribute
	    int attrCount, recordSize = 0;
	    for (attrCount = 0; attrCount < schema -> numAttr; attrCount++)
		{
	        recordSize = recordSize + getSize(schema, attrCount);                // Get the length of the attribute
	    }

		fflush(stdout);

	    return recordSize;
}

Schema *createSchema(int numAttr, char** attrNames, DataType* dataTypes, int* typeLength, int keySize, int* keys) {
	// Initialize table schema
	    Schema * newSchema;

		// Check if schema parameters are valid
		if (numAttr == 0 || attrNames == NULL || dataTypes == NULL)
		{
			printf("\n Invalid parameters to create table schema");
			exit(RC_INVALID_SCHEMA);
		}

		//  Allocate memory for schema table
	    newSchema = (Schema * ) malloc(sizeof(Schema));
		// Check if schema table exists if not throw an error
	    if (newSchema == NULL) {

			printf("\n Insufficent memory for allocation");
	        return RC_NO_MEMORY;
	    }

	    // Set all passed attributes to newly created schema
	    newSchema -> numAttr = numAttr;
	    newSchema -> attrNames = attrNames;
	    newSchema -> dataTypes = dataTypes;
	    newSchema -> typeLength = typeLength;
	    newSchema -> keySize = keySize;
	    newSchema -> keyAttrs = keys;
	    fflush(stdout);

	    return newSchema;                                                //Return the created schema
}

/*
  Frees all the resources used by schema handle
  Parameter:
  schema = schema handle to be freed
 */
RC freeSchema(Schema * schema)
{

	// Check if the schema to be freed in not Null
	if (schema == NULL || schema->attrNames == NULL || schema->dataTypes == NULL) {
		THROW(RC_INVALID_HANDLE, "Invalid Schema");
	}

    // Free schema object
    free(schema);
    return RC_OK;
}



/***************************************************
In this function a new record is created and it should
allocate enough memory to the data field values so that
it can hold the binary values for all attributes of this
record as determined by the schema.

****************************************************/
RC createRecord(Record** record, Schema* schema)                    

{
	*record = (Record *)malloc(sizeof(Record));									//allocate memory of Record sizze
	(*record)->data = (char *)malloc(sizeof(char)*getRecordSize(schema));		//allocate memory to data in record
	fflush(stdout);
	return RC_OK;
}

/*****************************************************
In this function the record and all the attributes 
associated with it is deleted from a schema. 

*******************************************************/
RC freeRecord(Record *record)
{
	// free record object
    free(record->data);                                             //Free the record data
    free(record);                                                   //Free the record
    return RC_OK;
}

RC setAttr(Record* record, Schema* schema, int attrNum, Value* value) {

	int intVal, dataSize, memOffset = 0;
	float floatVal;
	bool boolVal;
	char *charVal;
	attrOffset(schema, attrNum, &memOffset);

	switch(schema->dataTypes[attrNum]) {
		case DT_INT: 	intVal = value->v.intV;
						memcpy(record->data + memOffset, &intVal, sizeof (int));
						break;
		case DT_FLOAT:	floatVal = value->v.floatV;
						memcpy(record->data + memOffset, &floatVal, sizeof (float));
						break;
		case DT_BOOL:	boolVal = value->v.boolV;
						memcpy(record->data + memOffset, &boolVal, sizeof (bool));
						break;
		case DT_STRING: dataSize = schema->typeLength[attrNum];
						charVal = (char *) malloc(dataSize);
						charVal = value->v.stringV;
						memcpy(record->data + memOffset, charVal, dataSize);
						break;
	}

	return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    int memOffset= 0, intVal=0;
    int typeLength, attrnum;
    char *charVal;
    Value *val;
    float floatVal;
    bool boolVal;

    attrnum = schema->dataTypes[attrNum];
    typeLength = schema->typeLength[attrNum];

    attrOffset(schema, attrNum, &memOffset);

    val = (Value *) malloc(sizeof (Value));

    switch(attrnum) {
    	case DT_INT:		memcpy(&intVal, record->data+memOffset, sizeof (int));
        					val->v.intV = intVal;
        					break;
    	case DT_FLOAT:		memcpy(&floatVal, record->data+memOffset, sizeof (float));
        					val->v.floatV = floatVal;
        					break;
    	case DT_BOOL:		memcpy(&boolVal, record->data+memOffset, sizeof (bool));
        					val->v.boolV = boolVal;
        					break;
    	case DT_STRING:		charVal = record->data+memOffset;
        					val->v.stringV = (char *) malloc(typeLength + 1);
        					strncpy(val->v.stringV, charVal, typeLength);
        					val->v.stringV[schema->typeLength[attrNum]] = '\0';
    }
    val->dt = attrnum;
    *value = val;
    return RC_OK;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

	int noOfPages, intSize = sizeof(int);

	if (rel == NULL || scan == NULL)                                            //check if relation and scan exist
	        return RC_INVALID_SCAN_PARAM;

		BM_PageHandle *phandle = MAKE_PAGE_HANDLE();

	    LookUp *details  = NULL;
		details = (LookUp *) malloc(sizeof (LookUp));							 //Allocate memory for the searched variable

	   if (details == NULL)                                                       //check if scan data does not exist, if not return error
	        return RC_NO_MEMORY;


	    pinPage(rel->mgmtData, phandle, 0);
	    memcpy(&noOfPages, phandle->data, intSize);
	    unpinPage(rel->mgmtData, phandle);

	    details->noOfPages = noOfPages;
	    details->currRecord = 1;
	    details->currentPage = 1;
	    details->parsedRecords = 0;
	    details->numOfRecords = 0;
	    details->cond = cond;
	    scan->rel = rel;
	    scan->mgmtData = (void *) details;

	    free(phandle);
	    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record)  {
		int recordNum = 0, intSize = sizeof(int);
		BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
		BM_BufferPool *bPmanager;
		Value *dataValue;
		LookUp *Details = scan->mgmtData;
		Schema *Tschema = scan->rel->schema;
		Expr * condition = Details -> cond;
		bPmanager = scan->rel->mgmtData;
		int Offset = 0;
		Expr * attributeNumber, * temp;
		Details -> numOfRecords = getNumTuples(scan -> rel);

		pinPage(bPmanager,phandle,Details->currentPage);
		Offset = Offset + (1 * (intSize));
		memcpy(&recordNum, phandle->data + Offset,intSize);
		unpinPage(bPmanager, phandle);

		free(phandle);
		if (Details->currentPage < Details->noOfPages+1) {
				if (Details->currRecord <= recordNum){

					if (condition -> expr.op -> type == OP_BOOL_NOT) { 				// check for condition
								temp = condition -> expr.op -> args[0];                        // copy temp condition in attribute number
								attributeNumber = temp -> expr.op -> args[0];

								;
						} else {
								attributeNumber = condition -> expr.op -> args[1];
							 	// assign condition value to attribute number
						}

					setValuesForFieldsInRecord(record,Details,scan);
					getAttr(record, Tschema, attributeNumber->expr.attrRef, &dataValue); // To get the attribute of the table

					evalExpr(record, Tschema, condition, &dataValue);
					Details -> currRecord = Details -> currRecord + 1;
					scan->mgmtData = Details;
					if (dataValue->v.boolV != 1)
					{
						next(scan, record);
					}
					else
					{
						return RC_OK;
					}
				}
				else
				{
					if (Details -> currentPage + 1 < Details -> noOfPages) {               // check for no blocks
					Details -> currRecord = 0;                                                      // set current record
					Details -> currentPage = Details -> currentPage + 1;              // increament block
					next(scan, record);                                                                         // next record

					} else
					return RC_RM_NO_MORE_TUPLES;                                                                // error
				}

			}else
	        return RC_RM_NO_MORE_TUPLES;        // error code
}

RC closeScan(RM_ScanHandle *scan)
 {

    free(scan->mgmtData);
    return RC_OK;
}
