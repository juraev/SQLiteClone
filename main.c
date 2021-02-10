//
// Created by gitarist on 06/02/21.
//
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <zconf.h>
#include <errno.h>

typedef struct {
    char *buffer;
    size_t bufferLength;
    ssize_t inputLength;
} InputBuffer;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row rowToInsert;
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serializeRow(Row* source, void* destination){
    memcpy(destination + ID_OFFSET, &(source -> id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, &(source -> username), USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, &(source -> email), EMAIL_SIZE);
}

void deserializeRow(void* source, Row* destination){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void printRow(Row *row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    int fileDescriptor;
    uint32_t fileLength;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct{
    uint32_t numRows;
    Pager* pager;
} Table;

Pager* pagerOpen(const char* fileName){
    int fd = open(fileName, O_RDWR | O_CREAT | S_IWUSR | S_IRUSR);
    if(fd == -1){
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t fileLength = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i ++){
        pager->pages[i] = NULL;
    }
    return pager;
}

void pagerFlush(Pager* pager, uint32_t pageNum, uint32_t size){
    if(pager->pages[pageNum] == NULL){
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if(offset == -1){
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fileDescriptor, pager->pages[pageNum], size);

    if(bytesWritten == -1){
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

Table* dbOpen(const char* fileName){
    Pager* pager = pagerOpen(fileName);
    uint32_t numRows = pager->fileLength / ROW_SIZE;
    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->numRows = numRows;
    return table;
}

void dbClose(Table* table){
    Pager* pager = table -> pager;
    uint32_t numOfFullPages = table->numRows / ROWS_PER_PAGE;

    for(uint32_t i = 0; i < numOfFullPages; i ++){
        if(pager->pages[i] == NULL){
            continue;
        }
        pagerFlush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // There may be a partial page to write to the end of the file
    // This should not be needed after we switch to a B-tree
    uint32_t numAdditionalRows = table->numRows % ROWS_PER_PAGE;
    if(numAdditionalRows > 0){
        uint32_t pageNum = numOfFullPages;
        if(pager->pages[pageNum] != NULL){
            pagerFlush(pager, pageNum, numAdditionalRows * ROW_SIZE);
            free(pager->pages[pageNum]);
            pager->pages[pageNum] = NULL;
        }
    }

    int result = close(pager->fileDescriptor);
    if(result == -1){
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i ++){
        void* page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

//Table* newTable(){
//    Table* table = malloc(sizeof(Table));
//    table->numRows = 0;
//    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i ++){
//        table->pages[i] = NULL;
//    }
//    return table;
//}
//
//void freeTable(Table* table){
//    free(table->pages);
//    for(uint32_t i = 0; table->pages[i]; i++){
//        free(table->pages[i]);
//    }
//    free(table);
//}

void* getPage(Pager* pager, uint32_t pageNum){
    if(pageNum > TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if(pager->pages[pageNum] == NULL){
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->fileLength / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if(pager->fileLength % PAGE_SIZE){
            numPages += 1;
        }

        if(pageNum <= numPages){
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(pager->fileDescriptor, page, PAGE_SIZE);
            if(bytesRead == -1){
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[pageNum] = page;
    }

    return pager->pages[pageNum];
}

void* rowSlot(Table* table, uint32_t rowNum){
    uint32_t pageNum = rowNum / ROWS_PER_PAGE;
//    void* page = table->pages[pageNum];
//    if(page == NULL){
//        page = table->pages[pageNum] = malloc(PAGE_SIZE);
//    }
    void* page = getPage(table->pager, pageNum);

    uint32_t rowOffset = rowNum % ROWS_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}

InputBuffer *newInputBuffer() {
    InputBuffer *inputBuffer = (InputBuffer *) malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->bufferLength = 0;
    inputBuffer->inputLength = 0;

    return inputBuffer;
}

void readInput(InputBuffer *inputBuffer) {
    size_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->bufferLength), stdin);

    if (bytesRead <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    //Ignore trailing newline

    inputBuffer->inputLength = bytesRead - 1;
    inputBuffer->buffer[bytesRead - 1] = 0;
}

void closeInputBuffer(InputBuffer *buffer) {
    free(buffer->buffer);
    free(buffer);
}

MetaCommandResult doMetaCommand(InputBuffer *inputBuffer, Table* table) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
//        closeInputBuffer(inputBuffer);
        dbClose(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareInsert(InputBuffer* inputBuffer, Statement* statement){
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(inputBuffer->buffer, " ");
    char* idString = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if(idString == NULL || username == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(idString);

    if(id < 0){
        return PREPARE_NEGATIVE_ID;
    }

    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->rowToInsert.id = id;
    strcpy(statement->rowToInsert.username, username);
    strcpy(statement->rowToInsert.email, email);
    return PREPARE_SUCCESS;
}

PrepareResult prepareStatement(InputBuffer *inputBuffer, Statement *statement) {
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0) {
        return prepareInsert(inputBuffer, statement);
//        statement->type=STATEMENT_INSERT;
//        int argsAssigned = sscanf(
//                inputBuffer->buffer, "insert %d %s %s", &(statement->rowToInsert.id),
//                statement->rowToInsert.username, statement->rowToInsert.email);
//        return PREPARE_SUCCESS;
    }
    if(strcmp(inputBuffer->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult executeInsert(Statement* statement, Table* table){
    if(table->numRows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row* rowToInsert = &(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table, table->numRows));
    table->numRows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table){
    Row row;

    for (uint32_t i = 0; i < table->numRows; i ++){
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }
    return EXIT_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return executeInsert(statement, table);
        case (STATEMENT_SELECT):
            return executeSelect(statement, table);
    }
}

void printPrompt() { printf("db > "); }

int main(int argc, char *argv[]) {

//    Table* table = newTable();

    if(argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* fileName = argv[1];
    Table* table = dbOpen(fileName);

    InputBuffer *inputBuffer = newInputBuffer();

    while (true) {
        printPrompt();
        readInput(inputBuffer);

        if (inputBuffer->buffer[0] == '.') {
            switch (doMetaCommand(inputBuffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", inputBuffer->buffer);
                    continue;
            }
        }

        Statement statement;

        switch (prepareStatement(inputBuffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive. \n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement. \n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'. \n", inputBuffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed. \n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full. \n");
        }
    }
}


