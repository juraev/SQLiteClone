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
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
    NODE_INTERNAL, NODE_LEAF
} NodeType;

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

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

typedef struct {
    int fileDescriptor;
    uint32_t fileLength;
    uint32_t numPages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager *pager;
    uint32_t rootPageNum;
} Table;

typedef struct {
    Table *table;
    uint32_t pageNum;
    uint32_t cellNum;
    bool endOfTable;
} Cursor;


/*
 * Common node header layout
 */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE =
        NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf node header layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

/*
 * Leaf node body layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;


/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE +
                                           INTERNAL_NODE_NUM_KEYS_SIZE +
                                           INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Header Layout
 */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

void serializeRow(Row *source, void *destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(void *source, Row *destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void printRow(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

Pager *pagerOpen(const char *fileName) {
    int fd = open(fileName,
                  O_RDWR |      // Read/Write mode
                  O_CREAT,  // Create file if it does not exist
                  S_IWUSR |     // User write permission
                  S_IRUSR   // User read permission
    );
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }
    off_t fileLength = lseek(fd, 0, SEEK_END);

    Pager *pager = malloc(sizeof(Pager));
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;
    pager->numPages = (fileLength / PAGE_SIZE);

    if (fileLength % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pagerFlush(Pager *pager, uint32_t pageNum) {
    if (pager->pages[pageNum] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fileDescriptor, pager->pages[pageNum], PAGE_SIZE);

    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void* getPage(Pager *pager, uint32_t pageNum) {
    if (pageNum > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (pager->pages[pageNum] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void *page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->fileLength / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->fileLength % PAGE_SIZE) {
            numPages += 1;
        }

        if (pageNum <= numPages) {
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(pager->fileDescriptor, page, PAGE_SIZE);
            if (bytesRead == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[pageNum] = page;

        if (pageNum >= pager->numPages) {
            pager->numPages = pageNum + 1;
        }
    }

    return pager->pages[pageNum];
}

/*
 * Until we start recycling free pages, new pages will always
 * go onto the end of the database file
 */
uint32_t getUnusedPageNum(Pager* pager){
    return pager->numPages;
}

NodeType getNodeType(void *node) {
    uint8_t value = *((uint8_t *) (node + NODE_TYPE_OFFSET));
    return (NodeType) (value > 0);
}

void setNodeType(void *node, NodeType nodeType) {
    uint8_t value = nodeType;
    *((uint8_t *) (node + NODE_TYPE_OFFSET)) = value;
}

bool isNodeRoot(void* node){
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void setNodeRoot(void* node, bool isRoot){
    uint8_t value = isRoot;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t *leafNodeNumCells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leafNodeCell(void *node, uint32_t cellNum) {
    return node + LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t *leafNodeKey(void *node, uint32_t cellNum) {
    return leafNodeCell(node, cellNum);
}

void *leafNodeValue(void *node, uint32_t cellNum) {
    return leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}

void initializeLeafNode(void *node) {
    setNodeType(node, NODE_LEAF);
    setNodeRoot(node, false);
    *leafNodeNumCells(node) = 0;
}

uint32_t* internalNodeNumKeys(void* node){
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internalNodeRightChild(void* node){
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internalNodeCell(void* node, uint32_t cellNum){
    return node + INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internalNodeChild(void* node, uint32_t childNum){
    uint32_t numKeys = *internalNodeNumKeys(node);

    if(childNum > numKeys){
        printf("Tried to access childNum %d > numKeys %d\n", childNum, numKeys);
        exit(EXIT_FAILURE);
    } else if(childNum == numKeys){
        return internalNodeRightChild(node);
    } else {
        return internalNodeCell(node, childNum);
    }
}

uint32_t* internalNodeKey(void* node, uint32_t keyNum){
    return internalNodeCell(node, keyNum) + INTERNAL_NODE_CHILD_SIZE;
}

void initializeInternalNode(void* node){
    setNodeType(node, NODE_INTERNAL);
    setNodeRoot(node, false);
    *internalNodeNumKeys(node) = 0;
}

uint32_t getNodeMaxKey(void* node){
    switch (getNodeType(node)) {
        case NODE_INTERNAL:
            return *internalNodeKey(node, *internalNodeNumKeys(node) - 1);
        case NODE_LEAF:
            return *leafNodeKey(node, *leafNodeNumCells(node) - 1);
    }
}

void createNewRoot(Table* table, uint32_t rightChildPageNum){
    /*
     * Handle splitting the root.
     * Old root copied to new page, becomes left child.
     * Address of right child passed in.
     * Re-initialize root page to contain the new root node.
     * New root node points to two children.
     */

    void* root = getPage(table->pager, table->rootPageNum);
    void* rightChild = getPage(table->pager, rightChildPageNum);
    uint32_t leftChildPageNum = getUnusedPageNum(table->pager);
    void* leftChild = getPage(table->pager, leftChildPageNum);

    /* Leaf child has data copied from old root */
    memcpy(leftChild, root, PAGE_SIZE);
    setNodeRoot(leftChild, false);

    /* Root node is a new internal node with one key and two children */
    initializeInternalNode(root);
    setNodeRoot(root, true);
    *internalNodeNumKeys(root) = 1;
    *internalNodeChild(root, 0) = leftChildPageNum;
    uint32_t leftChildMaxKey = getNodeMaxKey(leftChild);
    *internalNodeKey(root, 0) = leftChildMaxKey;
    *internalNodeRightChild(root) = rightChildPageNum;
}

void leafNodeSplitAndInsert(Cursor* cursor, uint32_t key, Row* value){
    /*
     * Create a new node and move half the cells over.
     * Insert the new value in one of the two nodes.
     * Update parent or create a new parent.
     */
    void* oldNode = getPage(cursor->table->pager, cursor->pageNum);
    uint32_t newPageNum = getUnusedPageNum(cursor->table->pager);
    void* newNode = getPage(cursor->table->pager, newPageNum);
    initializeLeafNode(newNode);

    /*
     * All existing keys plus new key should be divided
     * evenly between old(left) and new (right) nodes.
     * Starting from the right, move each key to correct position.
     */

    for (uint32_t i = LEAF_NODE_MAX_CELLS; i <= LEAF_NODE_MAX_CELLS && i >= 0; i--) {
        void* destinationNode;
        if(i >= LEAF_NODE_LEFT_SPLIT_COUNT){
            destinationNode = newNode;
        } else {
            destinationNode = oldNode;
        }
        uint32_t indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leafNodeCell(destinationNode, indexWithinNode);

        if(i == cursor->cellNum){
            serializeRow(value, destinationNode);
        } else if(i > cursor->cellNum){
            memcpy(destination, leafNodeCell(oldNode, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leafNodeCell(oldNode, i), LEAF_NODE_CELL_SIZE);
        }
    }

    /*Update cell count on both leaf nodes*/
    *(leafNodeNumCells(oldNode)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leafNodeNumCells(newNode)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if(isNodeRoot(oldNode)){
        return createNewRoot(cursor->table, newPageNum);
    } else {
        printf("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}

void leafNodeInsert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = getPage(cursor->table->pager, cursor->pageNum);

    uint32_t numCells = *leafNodeNumCells(node);
    if (numCells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        leafNodeSplitAndInsert(cursor, key, value);
        return;
    }

    if (cursor->cellNum < numCells) {
        //make room for new cell
        for (uint32_t i = numCells; i > cursor->cellNum; i--) {
            memcpy(leafNodeCell(node, i), leafNodeCell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leafNodeNumCells(node)) += 1;
    *(leafNodeKey(node, cursor->cellNum)) = key;
    serializeRow(value, leafNodeValue(node, cursor->cellNum));
}

Cursor* leafNodeFind(Table *table, uint32_t pageNum, uint32_t key) {
    void *node = getPage(table->pager, pageNum);
    uint32_t numCells = *leafNodeNumCells(node);

    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->pageNum = pageNum;

    //Binary Search
    uint32_t minIndex = 0;
    uint32_t onePastMaxIndex = numCells;
    while (onePastMaxIndex != minIndex) {
        uint32_t index = (minIndex + onePastMaxIndex) / 2;
        uint32_t keyAtIndex = *leafNodeKey(node, index);

        if (key == keyAtIndex) {
            cursor->cellNum = index;
            return cursor;
        }

        if (key < keyAtIndex) {
            onePastMaxIndex = index;
        } else {
            minIndex = index + 1;
        }
    }
    cursor->cellNum = minIndex;
    return cursor;
}

Table *dbOpen(const char *fileName) {
    Pager *pager = pagerOpen(fileName);

    Table *table = malloc(sizeof(Table));
    table->pager = pager;

    if (pager->numPages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void *rootNode = getPage(pager, 0);
        initializeLeafNode(rootNode);
        setNodeRoot(rootNode, true);
    }

    return table;
}

void dbClose(Table *table) {
    Pager *pager = table->pager;

    for (uint32_t i = 0; i < pager->numPages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pagerFlush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->fileDescriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
}

void printConstants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level){
    for (uint32_t i = 0; i < level; ++ i){
        printf(" ");
    }
}

void printTree(Pager* pager, uint32_t pageNum, uint32_t indentationLevel){
    void* node = getPage(pager, pageNum);
    uint32_t numKeys, child;

    switch (getNodeType(node)) {
        case (NODE_LEAF):
            numKeys = *leafNodeNumCells(node);
            indent(indentationLevel);
            printf("- leaf (size %d)\n", numKeys);
            for(uint32_t i = 0; i < numKeys; ++i){
                indent(indentationLevel + 1);
                printf("- %d\n", *leafNodeKey(node, i));
            }
            break;
        case (NODE_INTERNAL):
            numKeys = *internalNodeNumKeys(node);
            indent(indentationLevel);
            printf("- internal (size %d)\n", numKeys);
            for(uint32_t i = 0; i < numKeys; ++ i){
                child = *internalNodeChild(node, i);
                printTree(pager, child, indentationLevel + 1);

                indent(indentationLevel + 1);
                printf("- key %d\n", *internalNodeKey(node, i));
            }

            child = *internalNodeRightChild(node);
            printTree(pager, child, indentationLevel + 1);
            break;
        default:
            printf("Unrecognized node format\n");
            break;
    }
}

Cursor *tableStart(Table *table) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->pageNum = table->rootPageNum;
    cursor->cellNum = 0;

    void *rootNode = getPage(table->pager, table->rootPageNum);
    uint32_t numCells = *leafNodeNumCells(rootNode);
    cursor->endOfTable = (numCells == 0);

    return cursor;
}

/*
 * Return the position of the given key
 * If the key is not present, return the position
 * where it should be inserted
 */
Cursor* tableFind(Table* table, uint32_t key) {
    uint32_t rootPageNum = table->rootPageNum;
    void *rootNode = getPage(table->pager, rootPageNum);

    if (getNodeType(rootNode) == NODE_LEAF) {
        return leafNodeFind(table, rootPageNum, key);
    } else {
        printf("Need to implement searching an internal node\n");
        exit(EXIT_FAILURE);
    }
}

void cursorAdvance(Cursor *cursor) {
    uint32_t pageNum = cursor->pageNum;
    void *node = getPage(cursor->table->pager, pageNum);

    cursor->cellNum += 1;
    if (cursor->cellNum >= (*leafNodeNumCells(node))) {
        cursor->endOfTable = true;
    }
}

void *cursorValue(Cursor *cursor) {
    uint32_t pageNum = cursor->pageNum;
    void *page = getPage(cursor->table->pager, pageNum);
    return leafNodeValue(page, cursor->cellNum);;
}

InputBuffer* newInputBuffer() {
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

MetaCommandResult doMetaCommand(InputBuffer *inputBuffer, Table *table) {
    if (strcmp(inputBuffer->buffer, ".exit") == 0) {
        dbClose(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(inputBuffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        printTree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strcmp(inputBuffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        printConstants();
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareInsert(InputBuffer *inputBuffer, Statement *statement) {
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(inputBuffer->buffer, " ");
    char* idString = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (idString == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(idString);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
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
    }
    if (strcmp(inputBuffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult executeInsert(Statement *statement, Table *table) {
    void *node = getPage(table->pager, table->rootPageNum);
    uint32_t numCells = (*leafNodeNumCells(node));

    Row *rowToInsert = &(statement->rowToInsert);

    uint32_t keyToInsert = rowToInsert->id;
    Cursor *cursor = tableFind(table, keyToInsert);

    if (cursor->cellNum < numCells) {
        uint32_t keyAtIndex = *leafNodeKey(node, cursor->cellNum);
        if (keyAtIndex == keyToInsert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leafNodeInsert(cursor, rowToInsert->id, rowToInsert);

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement *statement, Table *table) {
    Cursor *cursor = tableStart(table);
    Row row;

    while (!(cursor->endOfTable)) {
        deserializeRow(cursorValue(cursor), &row);
        printRow(&row);
        cursorAdvance(cursor);
    }
    free(cursor);
    return EXIT_SUCCESS;
}

ExecuteResult executeStatement(Statement *statement, Table *table) {
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

    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char *fileName = argv[1];
    Table *table = dbOpen(fileName);

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
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", inputBuffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate key.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }
}


