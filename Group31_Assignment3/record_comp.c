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

typedef struct Search
{
    Expr *cond;
    int currRecord;
    int numRecords;
    int no_blocks;
    int currentblock;
    int parsedRecords;
} Search;

RC initRecordManager(void* mgmtData) {
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}
RC markNextAvailablePage(char *data, int pageNum) {
	memcpy(data, &pageNum, sizeof(int));
	return RC_OK;
}

int updateOffset(int offset, int increment) {
	return offset+increment;
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
	schemaInString = serializeSchema1(schema);
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
	recordPos = updateOffset(recordPos, intSize);					//move the offset
	memcpy(phandle->data+recordPos, record->data, recordSize);		//write the data
	writeNumOfSlots(phandle->data, 4, slotNum+1, intSize);				//update no of slots in the page
	RID *rid = (RID *) malloc (sizeof(RID));
	rid->page = pageNum;
	rid->slot = slotNum;
	//update the RID details in the record
	record->id.page = rid->page;
	record->id.slot = rid->slot;
	return RC_OK;
}

/*char** spliStringBasedOnToken(char** result, char* input, char *delimiter) {
	char inputString[100];

	strcpy(inputString,input);
	printf("\n splitting string %s %s %s", inputString, delimiter, input);

	fflush(stdout);
	int i, count = 0;
	while(*input) if (*input++ == delimiter[0]) ++count;
	//count = count+1;
	printf("\n no of delimiters: %d", count);
	printf("\n after copy %s", inputString);
	//char result[count][100];
	result = (char **)calloc(count, sizeof(char *));
    for (i=0; i<count; i++)
         result[i] = (char *)calloc(200, sizeof(char));


	for(i=0; i<count; i++) {
		if(i == 0)
		strcpy(result[i],strtok(inputString, delimiter));
		else
			strcpy(result[i],strtok(NULL, delimiter));
		printf("\n value in index %d is: %s asdasas", i,result[i]);
		fflush(stdout);
		//inp = NULL;
	}

	return result;
}*/

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
	printf("\n keyAttr: %d", keyAttr[0]);
	fflush(stdout);

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

RC createTable(char* name, Schema* schema) {
	BM_BufferPool *bpool = MAKE_POOL();					//allocating memory for buffer pool for this pagefile
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();			//allocating memory for pagehandle for this pagefile
	BM_PageHandle *phandle1 = MAKE_PAGE_HANDLE();

	createPageFile(name);								//creating the underlying pagefile for the table
	initBufferPool(bpool, name, 3, RS_FIFO, NULL);		//setting up the bufferpool for the pagefile
	pinPage(bpool, phandle, 0);							//pinning page 0 to write schema details
	markNextAvailablePage(phandle->data, 0);
	writeSchemaInPageFile(phandle->data ,schema);		//writing schema details in page 0
	markDirty(bpool, phandle);
	unpinPage(bpool, phandle);

	pinPage(bpool, phandle1, 1);							//pin page1 to create first block
	addNewPageToInsertRecord(phandle1->data, 0);
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
	printf("\n back from set values... ");
	fflush(stdout);
	printf("\n rel->name: %s", rel->name);
	fflush(stdout);
	unpinPage(rel->mgmtData, phandle);
	free(phandle);
    return RC_OK;
}

RC closeTable(RM_TableData *rel)  {
	printf("\n inside close table.. ");
	fflush(stdout);
	/*shutdownBufferPool(rel->mgmtData);
	printf("\n bufferpool shut down");
	    fflush(stdout);*/
	printf("\n inside close, table name: %s", rel->name);
		fflush(stdout);

    free(rel->schema->dataTypes);
    printf("\n dataTypes freed ");
    	fflush(stdout);
    free(rel->schema->attrNames);
    printf("\n attrNames freed ");
    	fflush(stdout);
    /*free(rel->schema->keyAttrs);
    printf("\n keyAttrs freed ");
    	fflush(stdout);*/
    free(rel->schema->typeLength);
    printf("\n typeLength freed ");
        fflush(stdout);
    free(rel->schema);
    printf("\n schema freed ");
        fflush(stdout);
  /*  free(rel->name);
	printf("\n name freed ");
		fflush(stdout);*/
    return RC_OK;
}

RC deleteTable(char *name)
{
    destroyPageFile(name);
    return RC_OK;
}
int getNumTuples(RM_TableData *rel)
{
    int t=sizeof(int),record = 0,block = 0, i = 1, total_records=0;          //Variables are declared
    BM_PageHandle *bh = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, bh, TABLE_INFO_BLOCK);                           //Pin the Table info block to get the no of blocks
    unpinPage(rel->mgmtData, bh);

    for (i = 1; i <= block; i++)
    {
        pinPage(rel->mgmtData, bh, i);                                      //Add the total number of records in the table
        memcpy(bh->data + (2 * (t)), &record, t);
        total_records += record;
        unpinPage(rel->mgmtData, bh);
    }
    free(bh);
    return total_records;
}

RC insertRecord(RM_TableData* rel, Record* record) {

	int pageNum = 0, maxSlots = 0, noOfSlots = 0, recordSize = 0;
	int memOffset = 0, intSize = sizeof(int), recordPos = PAGE_SIZE;
	BM_PageHandle *phandle = MAKE_PAGE_HANDLE();
	pinPage(rel->mgmtData, phandle, 0);							//pin the page where schema is written to read values

	memcpy(&pageNum, phandle->data, intSize);					//read the pagenum from file and store it
	memOffset = updateOffset(memOffset, intSize);
	memcpy(&maxSlots, phandle->data+memOffset, intSize);		//read the max no of slots and store it
	unpinPage(rel->mgmtData, phandle);

	pinPage(rel->mgmtData, phandle, pageNum+1);					//pin the next available page
	memcpy(&pageNum, phandle->data, intSize);					//copy its pagenum value to pagenum variable
	memOffset = updateOffset(memOffset, intSize);
	memcpy(&noOfSlots, phandle->data+memOffset, intSize);		//copy its no of slots value to noOfSlots variable

	recordSize = getRecordSize(rel->schema);					//calculate the record size
	recordPos = (LENGTH_OF_META_DATA+(noOfSlots * (recordSize + intSize)))+1;		//calculate the next available position to write record

	if(noOfSlots <= maxSlots) {									//if still free slots are there to write this record
		writeRecord(phandle, record, noOfSlots, recordPos, recordSize, intSize, pageNum);		//write the record
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
	} else {													//if not enough memory to write the record
		unpinPage(rel->mgmtData, phandle);
		pinPage(rel->mgmtData, phandle, 0);
		pageNum = pageNum+1;
		markNextAvailablePage(phandle->data, pageNum);			//update the next available page number in schema details
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
		pinPage(rel->mgmtData, phandle, pageNum);				//pin the next available page
		addNewPageToInsertRecord(phandle->data, pageNum);
		writeRecord(phandle, record, noOfSlots, recordPos, recordSize, intSize, pageNum);	//write record
		markDirty(rel->mgmtData, phandle);
		unpinPage(rel->mgmtData, phandle);
	}
}

RC deleteRecord(RM_TableData *rel, RID id) {
    int tomb = TOMBSTONE , t=sizeof (int);                                         //Declare the variables
    BM_PageHandle *bh = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, bh, id.page + 1);
    memmove(bh->data + ((3 * t) + ((id.slot) * t)), &tomb, t);                     //Mark the tombstone
    markDirty(rel->mgmtData, bh);
    unpinPage(rel->mgmtData, bh);
    forceFlushPool(rel->mgmtData);
    free(bh);
    return RC_OK;
}

/**********************************************
This function is used to delete a record with
certain RID from the table.

The record is moved from the table to the another
variable.

************************************************/
RC updateRecord(RM_TableData *rel, Record *record)
 {
    int record_off = 0, t=sizeof(int);                                             //Variables are declared
    BM_PageHandle *bh = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, bh, record->id.page + 1);
    memmove(&record_off, bh->data + ((3 * t)+((record->id.slot) * t)), t);         //Record is updated
    memmove(bh->data + record_off, record->data,(getRecordSize(rel->schema)));
    markDirty(rel->mgmtData, bh);
    unpinPage(rel->mgmtData, bh);
    forceFlushPool(rel->mgmtData);
    free(bh);
    return RC_OK;
}

/**********************************************
This function is used to get a record with
certain RID from the table.

************************************************/
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    int t=sizeof (int), record_off = 0;                                             //Variables are declared
    BM_PageHandle *bh = MAKE_PAGE_HANDLE();
    pinPage(rel->mgmtData, bh, id.page + 1);
    memmove(&record_off, bh->data + ((3 * t) + ((id.slot) * t)), t);                //Record offset is fetched

    if (record_off == TOMBSTONE)
    {
        free(bh);
        return RC_RECORD_REMOVED;
    }
    else
    {
    memmove(record->data, bh->data + record_off, (getRecordSize(rel->schema)));     //Record if fetched
    record->id.slot = id.slot;
    record->id.page = id.page;

    unpinPage(rel->mgmtData, bh);

    free(bh);
    return RC_OK;
    }
}



/******************************************************
This function is used to return the size in bytes of
records for a given schema.

*******************************************************/

int getRecordSize(Schema *schema)
{
     int i=0, sizeofRecord=0;

    for (i = 0; i < schema->numAttr; i++)                               //Calculate the size of the record depending on the data type
    {
    if(schema->dataTypes[i]==DT_INT)
    {
        sizeofRecord += sizeof (int);
    }
    else if(schema->dataTypes[i]==DT_STRING)
    {
        sizeofRecord +=schema->typeLength[i];
    }
    else if(schema->dataTypes[i]==DT_FLOAT)
    {
        sizeofRecord += sizeof (float);
    }
    else if(schema->dataTypes[i]==DT_BOOL)
    {
        sizeofRecord += sizeof (bool);
    }
    }
    return sizeofRecord;
}

/******************************************************
This function is used to create a new schema using various
attributes like length ,datatype etc.

*******************************************************/
Schema *createSchema(int numAttr, char** attrNames, DataType* dataTypes, int* typeLength, int keySize, int* keys)
 {
    Schema *schema;
    schema = (Schema *) malloc(sizeof (Schema));                   //Create the schema and assign the values
    schema->numAttr = numAttr;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->dataTypes = dataTypes;
    schema->keyAttrs = keys;
    schema->attrNames = attrNames;
    return schema;                                                 //Return the created schema
}

/******************************************************
This function is used to delete a schema.

******************************************************/
RC freeSchema(Schema* schema)
{
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
    *record = (Record *) malloc(sizeof (Record));                   //Allocate memory to the record
    (*record)->data = (char *) malloc((getRecordSize(schema)));
    return RC_OK;
}

/*****************************************************
In this function the record and all the attributes
associated with it is deleted from a schema.

*******************************************************/
RC freeRecord(Record *record)
{
    free(record->data);                                             //Free the record data
    free(record);                                                   //Free the record
}

/********************************************************
In this function we can set the attribute values in a record.

**********************************************************/

RC setAttr(Record* record, Schema* schema, int attrNum, Value* value)
{

int off = 0, num;                                            //Variables are declared
char *buf;
attrOffset(schema, attrNum, &off);                           //Calculate the offset of the record depending on the datatype
if(schema->dataTypes[attrNum]==DT_INT)
{
    num = value->v.intV;
    memcpy(record->data + off, &num, sizeof (int));
}
else if(schema->dataTypes[attrNum]==DT_STRING)
{
    num = schema->typeLength[attrNum];
    buf = (char *) malloc(num);
    buf = value->v.stringV;
    memcpy(record->data + off, buf, num);
}
else if(schema->dataTypes[attrNum]==DT_FLOAT)
{
    float n = value->v.floatV;
    memcpy(record->data + off, &n, sizeof (float));

}
else if(schema->dataTypes[attrNum]==DT_BOOL)
{
    bool num = value->v.boolV;
    memcpy(record->data + off, &num, sizeof (bool));
}

return RC_OK;
}

/********************************************************
In this function we can get the attribute values of a record.

**********************************************************/
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    int off= 0, n=0;                                            //Variables are declared
    char *attrData;
    Value *val;
    float fnum;
    bool bnum;
    attrOffset(schema, attrNum, &off);
    val = (Value *) malloc(sizeof (Value));
    val->dt = schema->dataTypes[attrNum];
    if(schema->dataTypes[attrNum]==DT_INT)                      //Populate the value of attribute based on the data type
    {
        memcpy(&n, record->data + off, sizeof (int));
        val->v.intV = n;
    }
    else if(schema->dataTypes[attrNum]==DT_BOOL)
    {
        memcpy(&bnum, record->data + off, sizeof (bool));
        val->v.boolV = bnum;
    }
    else if(schema->dataTypes[attrNum]==DT_FLOAT)
    {
        memcpy(&fnum, record->data + off, sizeof (float));
        val->v.floatV = fnum;
    }
    else if(schema->dataTypes[attrNum]==DT_STRING)
    {
        attrData = record->data+off;
        val->v.stringV = (char *) malloc(schema->typeLength[attrNum] + 1);
        strncpy(val->v.stringV, attrData, schema->typeLength[attrNum]);
        val->v.stringV[schema->typeLength[attrNum]] = '\0';
    }

    *value = val;
    return RC_OK;
}




/***************************************************
A user can initiate a scan to search all tuples from
a table that fulfill a certain condition.
Start of a scan initializes the RM_ScanHandle data structure
passed as an argument to startScan.

*****************************************************/
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
 {

    Search *data = NULL;
    data = (Search *) malloc(sizeof (Search));                          //Allocate memory for the searched variable

    BM_PageHandle *bh = MAKE_PAGE_HANDLE();

    pinPage(rel->mgmtData, bh, TABLE_INFO_BLOCK);
    memcpy(&data->no_blocks, bh->data, sizeof (int));                   //Populate the required data to the search variable
    unpinPage(rel->mgmtData, bh);
    data->currRecord = 0;
    data->currentblock = 1;
    data->no_blocks++;
    data->parsedRecords = 0;
    data->numRecords = 0;
    data->cond = cond;
    scan->rel = rel;
    scan->mgmtData = (void *) data;

    free(bh);
    return RC_OK;
}

/******************************************************
The next method returns the next tuple that satisfy the
scan condition.
If NULL is passed in a scan method, then all tuples of
table should be returned. next should return
RC_RM_NO_MORE_TUPLES once the scan is completed .

*******************************************************/
RC next(RM_ScanHandle *scan, Record *record)
{
    int record_num = 0, t= sizeof (int);                                                 //Variable are declared
    BM_BufferPool *bm;
    Search *data;
    Schema *schema;
    Value *value;
    schema = scan->rel->schema;
    data = scan->mgmtData;
    Expr *cond = data->cond;
    Expr *attrnum, *temp;
    bm = scan->rel->mgmtData;
    data->numRecords = getNumTuples(scan->rel);

    BM_PageHandle *bh = MAKE_PAGE_HANDLE();                             //Buffer page handle is initialised

    pinPage(bm, bh, data->currentblock);
    memmove(&record_num, bh->data + (2*t),t);                               //the next record is fetched
    unpinPage(bm, bh);
    free(bh);
    if (data->currentblock < data->no_blocks + 1)
    {
        if (data->currRecord <= record_num)
        {
            if (cond->expr.op->type != OP_BOOL_NOT)
            {
                attrnum = cond->expr.op->args[1];
            }
            else
            {
                temp = cond->expr.op->args[0];
                attrnum = temp->expr.op->args[0];
            }
            record->id.slot = data->currRecord;
            record->id.page = data->currentblock - 1;

            RID rid = record->id;
            getRecord(scan->rel, rid, record);                    //To get records from the table

            getAttr(record, schema, attrnum->expr.attrRef, &value); // To get the attribute of the table

            evalExpr(record, schema, cond, &value);
            data->currRecord +=1;
            scan->mgmtData = data;
            if (value->v.boolV != 1)
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
            if (data->currentblock + 1 >= data->no_blocks)
            {
                return RC_RM_NO_MORE_TUPLES;
            }
            else
            {
                data->currRecord = 0;
                data->currentblock+= 1;
                next(scan, record);
            }
        }
    }
    else
    {

        return RC_RM_NO_MORE_TUPLES;

    }

}

/******************************************************
CloseScan indicates to record manager that all
associated resources can be cleaned .

********************************************************/
RC closeScan(RM_ScanHandle *scan)
 {

    free(scan->mgmtData);

    return RC_OK;
}
/***********************************************
This function is used to update schema of the table.

*************************************************/

RC update_table(Schema *schema, char *data, int dataBlock, TableInfoUpdate type)
{
    int off = 0,sizeInt = sizeof(int);
    int schemaSize = 0;
    char *serializedSchema = malloc(200);                               //200 bytes of memory is allocated to the variable

    if (type==0)                                                        //Check if the page is created for the first time
    {
            serializedSchema = serializeSchema1(schema);                //Serialize the schema data to string
            memcpy(data, &dataBlock, sizeInt);                          //Update the table information
            off+=sizeInt;
            schemaSize = strlen(serializedSchema);
            memcpy(data + off, &schemaSize, sizeInt);
            off+=sizeInt;
            memcpy(data + off, serializedSchema, schemaSize);

    }
    else
    {
            memcpy(data, &dataBlock, sizeInt);                          //When pagefile already exist, update table information
    }

    free(serializedSchema);
}
/***********************************************************
This function is used to update the table information to
the header.

************************************************************/

RC update_header(char *data, TableInfoUpdate tbl_info, int blk_num, int num_slots, int num_records) {
    int t=sizeof(int); //Variable declared

    if (tbl_info==0) //if the table is created for the first time then populate value inside the data variable
    {

        memcpy(data, &blk_num, t);        //when page file is created for the first time
        memcpy(data + t, &num_slots, t);
        memcpy(data + t, &num_records, t);
    }
    else if(tbl_info==1) // if table is already created then populate the number of records in the data
    {

        memcpy(data + (2 * (t)), &num_records, t);
    }
    else
    {
        memcpy(data + t, &num_slots, t);
    }
}

/***********************************************
This function is used to insert the given number
of slots to the data variable.

*************************************************/
RC slot_insert(char *data, int num_slots, int record_off)
 {
    int t=sizeof (int);
    memcpy(data +((3*t)+ (num_slots*t)) , &record_off, t);         //insert the given number of slots to the data variable
    return RC_OK;
}

/***********************************************
This function is used to obtain the expression
value depending on the type of expression.


*************************************************/
static RC getExpr(Expr *expr, Expr **result_expr)
{
    Value *v = NULL;
    Operator *opr = NULL;
    Expr *temp = NULL;

    temp = (Expr *) malloc(sizeof (Expr));                              //Allocate temporary memory for expression
    temp->type = expr->type;

    if(expr->type==EXPR_CONST)                                          //Return expression value depending on the type of the expression
    {
        v = (Value *) malloc(sizeof (Value));
        if (v != NULL)
        {


        }
else{
freeExpr(temp);
            temp = NULL;
            *result_expr = temp;
            return RC_NOMEM;
}
        *v = *(expr->expr.cons);
        temp->expr.cons = v;
    }
    else if(expr->type==EXPR_ATTRREF)
    {
        temp->expr.attrRef = expr->expr.attrRef;
    }
    else if(expr->type==EXPR_OP)
    {
        opr = (Operator *) malloc(sizeof (Operator));
        if (opr == NULL)
        {
            freeExpr(temp);
            temp = NULL;
            *result_expr = temp;
            return RC_NOMEM;
        }
        opr->args = expr->expr.op->args;
        opr->type = expr->expr.op->type;
        getExpr(expr->expr.op->args[0], &opr->args[0]);
        if (opr->args[0] != NULL && opr->type != OP_BOOL_NOT)
        {
            getExpr(expr->expr.op->args[1], &opr->args[1]);
        }
        if (opr->args[0] == NULL || (opr->type != OP_BOOL_NOT && opr->args[1] == NULL))
        {
            freeExpr(temp);
            temp = NULL;
            *result_expr = temp;
            return RC_NOMEM;
        }
    }
}
/****************************************************
This function is used to obtain the schema from the
table data.

*******************************************************/
RC ReadSchema(RM_TableData* rel, char *schemadata)
 {
    int schemaSize = 0,i;
    Schema *schema;
    char *var1, *var2, *var3, *arr[100];
    int off=0;

    rel->schema = malloc(sizeof(Schema));                                           //Allocate memory to table
    off += sizeof (schemaSize);
    memcpy(&schemaSize, schemadata + off, sizeof (schemaSize));                     //Copy the schema size to schemasize
    char* tmp = (char *) malloc(schemaSize);
    off += sizeof (schemaSize);
    memcpy(tmp, schemadata + off, schemaSize);                                      //Copy the schema-size to the temporary variable

    //atoi to convert string to integer
    //strtok - to split the string based on tokens
    rel->schema->numAttr = atoi(strtok(tmp, ";"));
    rel->schema->typeLength = malloc((rel->schema->numAttr) * sizeof (schemaSize));

    var1 = strtok(NULL, ";");                                                       //Initialise the character variable with tokens

    rel->schema->keySize = atoi(strtok(NULL, ";"));
    rel->schema->keyAttrs = malloc((rel->schema->keySize) * sizeof (schemaSize));

    var2 = strtok(var2, ",");                                                       //Store var2 tokens

    for (i=0; var2 != NULL; i++)
    {

        rel->schema->keyAttrs[i] = atoi(var2);                                      //Store the key defined in a schema
        var2 = strtok(NULL, ",");
    }

    var2 = strtok(var1, ",");

    for (i=0; var2 != NULL; i++)
    {
        arr[i] = var2;                                                              //Store the tokens in an array
        var2 = strtok(NULL, ",");
    }

    rel->schema->attrNames = (char **) malloc(rel->schema->numAttr * sizeof (char*));
    rel->schema->dataTypes = (DataType *) malloc(rel->schema->numAttr * sizeof (DataType));
    for (i = 0; i < rel->schema->numAttr; i++)
    {
        rel->schema->attrNames[i] = strtok(arr[i], ":");
        var2 = strtok(NULL, ":");
        if (strstr(var2, "INT"))                                                    //Store the data-type of the attribute
        {
            rel->schema->dataTypes[i] = DT_INT;
        }
        else if (strstr(var2, "BOOL"))
        {
            rel->schema->dataTypes[i] = DT_BOOL;
        }
        else if (strstr(var2, "STR"))
        {
            rel->schema->dataTypes[i] = DT_STRING;
            var3 = strstr(var2, "[");
            var3 = strtok(var3, "]");
            var3++;
            rel->schema->typeLength[i] = atoi(var3);
        }
        else if (strstr(var2, "FLOAT"))
        {
            rel->schema->dataTypes[i] = DT_FLOAT;
        }
    }
    free(tmp);                                                                      //Free the memory allocated
    return RC_OK;
}
