#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <mqueue.h>
#include <errno.h>

// global variables
int dcount;
char* fname;
int tcount;
int vsize;
char* mqname;
mqd_t clientToServerQueueID;
mqd_t serverToClientQueueID;
int MAX_MESSAGES = 10;

// structs
struct mq_attr mq_attr_clt;
struct mq_attr mq_attr_srv;

struct KeyValuePair {
    int key;
    char value[64];
};

struct Reply {
    int success;
    char value[64];
};

struct Request {
    char rType[10];
    struct KeyValuePair kvp;
};

int MAX_MSG_SIZE_Q1 = sizeof(struct Request);
int MAX_MSG_SIZE_Q2 = sizeof(struct Reply);

void parseArguments(int argc, char *argv[]) {
    if (argc != 11) {
        fprintf(stderr, "Usage: %s -d <dcount> -f <fname> -t <tcount> -s <vsize> -m <mqname>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    
    for (int i = 1; i < argc; i += 2) {
        if (strcmp(argv[i], "-d") == 0) {
            dcount = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-f") == 0) {
            fname = argv[i + 1];
        } else if (strcmp(argv[i], "-t") == 0) {
            tcount = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-s") == 0) {
            vsize = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-m") == 0) {
            mqname = argv[i + 1];
        }
    }

    printf("dcount: %d\n", dcount);
    printf("fname: %s\n", fname);
    printf("tcount: %d\n", tcount);
    printf("vsize: %d\n", vsize);
    printf("mqname: %s\n", mqname);
}

void initClientToServerMessageQueue() {
    mq_attr_clt.mq_flags = 0;
    mq_attr_clt.mq_maxmsg = MAX_MESSAGES;
    mq_attr_clt.mq_msgsize = MAX_MSG_SIZE_Q1;  // Use the size of the struct directly
    mq_attr_clt.mq_curmsgs = 0;

    // Add / to the front of the name and add _1 to the end
    char mq_name[20];
    mq_name[0] = '/';
    strcpy(mq_name + 1, mqname);
    strcat(mq_name, "_1");

    // Try to open the existing message queue
    clientToServerQueueID = mq_open(mq_name, O_RDWR | O_CREAT, 0666, &mq_attr_clt);
    if (clientToServerQueueID == -1) {
        perror("Error opening client to server message queue");
        exit(EXIT_FAILURE);
    }

    printf("client to server Message queue opened successfully\n");
}

void initServerToClientMessageQueue() {
    mq_attr_srv.mq_flags = 0;
    mq_attr_srv.mq_maxmsg = MAX_MESSAGES;
    mq_attr_srv.mq_msgsize = MAX_MSG_SIZE_Q2;
    mq_attr_srv.mq_curmsgs = 0;
    
    // add / to the front of the name and add _2 to the end
    char mq_name[20];
    mq_name[0] = '/';
    strcpy(mq_name + 1, mqname);
    strcat(mq_name, "_2");

    // Try to open the existing message queue and if it does not exist, create it
    serverToClientQueueID = mq_open(mq_name, O_RDWR | O_CREAT, 0666, &mq_attr_srv);
    if (serverToClientQueueID == -1) {
        perror("Error opening message queue");
        exit(EXIT_FAILURE);
    }

    printf("server to client Message queue opened successfully\n");
}

void createDataFiles(int dcount, const char* fname) {
    for (int i = 1; i <= dcount; i++) {
        char filename[20];
        sprintf(filename, "%s%d", fname, i);
        FILE *fp = fopen(filename, "w");
        fclose(fp);
    }
}

void sendReply(struct Reply reply) {
    // Get the current number of messages in the queue
    // if (mq_getattr(serverToClientQueueID, &mq_attr_srv) == -1) {
    //     perror("Error getting message queue attributes");
    //     exit(EXIT_FAILURE);
    // }

    // check if there are less than max messages in the queue
    if (mq_attr_srv.mq_curmsgs >= mq_attr_srv.mq_maxmsg) {
        printf("server to client message queue is full");
        return;
    }

    // Increment the message count
    mq_attr_srv.mq_curmsgs++;
    printf("incremented mq_attr_srv.mq_curmsgs\n");
    printf("mq_attr_srv.mq_curmsgs: %ld\n", mq_attr_srv.mq_curmsgs);

    if (mq_send(serverToClientQueueID, (char*)&reply, MAX_MSG_SIZE_Q2, 0) == -1) {
        perror("Error sending message to server to client message queue");
        exit(EXIT_FAILURE);
    }

    printf("Reply sent successfully\n");
}

void handlePutRequest(struct Request request) {

    struct Reply reply;
    reply.success = 1;

    // Calculate the file index based on the key
    int fileIndex = (request.kvp.key % dcount) + 1;

    // Create the filename
    char filename[20];
    sprintf(filename, "%s%d", fname, fileIndex);

    // Open the file in read mode to check if the key already exists
    FILE *fpRead = fopen(filename, "r");
    if (fpRead == NULL) {
        perror("Error opening file for read");
        return;
    }

    // Temporary file for writing updated content
    char tempFilename[30];
    sprintf(tempFilename, "%s_temp", filename);
    FILE *tempFp = fopen(tempFilename, "w");
    if (tempFp == NULL) {
        perror("Error opening temporary file");
        fclose(fpRead);
        return;
    }

    int keyFound = 0;
    char line[64];
    while (fgets(line, sizeof(line), fpRead)) {
        int keyFromFile;
        char valueFromFile[64];
        if (sscanf(line, "%d %[^\n]", &keyFromFile, valueFromFile) == 2) {
            if (keyFromFile == request.kvp.key) {
                // Key found, update the value
                fprintf(tempFp, "%d %s\n", request.kvp.key, request.kvp.value);
                keyFound = 1;
            } else {
                // Copy other lines as they are
                fprintf(tempFp, "%d %s\n", keyFromFile, valueFromFile);
            }
        }
    }

    // If the key is not found, append a new entry
    if (!keyFound) {
        fprintf(tempFp, "%d %s\n", request.kvp.key, request.kvp.value);
    }

    // Close files
    fclose(fpRead);
    fclose(tempFp);

    // Remove the original file
    remove(filename);

    // Rename the temporary file to the original filename
    rename(tempFilename, filename);

    // Send the reply to the client
    printf("sending reply success: %d\n", reply.success);
    sendReply(reply);

    // Print a message indicating the write operation
    printf("Successfully wrote or updated key-value to file %s\n", filename);
}

void handleGetRequest(struct Request request) {

    struct Reply reply;
    reply.success = 0;
    // Calculate the file index based on the key
    int fileIndex = (request.kvp.key % dcount) + 1;

    // open the file at the index 
    char filename[20];
    sprintf(filename, "%s%d", fname, fileIndex);
    FILE *fp = fopen(filename, "r");

    // read the file line by line
    char line[64];
    while (fgets(line, sizeof(line), fp)) {
        // split the line into key and value
        char *key = strtok(line, " ");
        char *value = strtok(NULL, " ");

        // if the key matches the request key, send the value back to the client
        if (atoi(key) == request.kvp.key) {
            reply.success = 1;
            strcpy(reply.value, value);

            printf("request key: %s\n", key);
            printf("request value: %s\n", value);   
            // printf("reply success: %d\n", reply.success);
            // printf("reply value: %s\n", reply.value);

            // Print a message indicating the read operation
            printf("Successfully read key-value from file %s\n", filename);
            break;
        }
    }

    // Send the reply to the client
    printf("sending reply success: %d\n", reply.success);
    printf("sending reply value: %s\n", reply.value);
    sendReply(reply);

    // Close the file
    fclose(fp);
}

void handleDeleteRequest(struct Request request) {
     
    struct Reply reply;
    reply.success = 0;

    // Calculate the file index based on the key
    int fileIndex = (request.kvp.key % dcount) + 1;

    // open the file at the index 
    char filename[20];
    sprintf(filename, "%s%d", fname, fileIndex);
    FILE *fp = fopen(filename, "r");

    // create a temporary file to store updated content
    char tempFilename[50];
    sprintf(tempFilename, "%s_temp", filename);
    FILE *tempFp = fopen(tempFilename, "w");

    // read the file line by line and copy lines excluding the one to be deleted
    char line[64];
    while (fgets(line, sizeof(line), fp)) {
        char *key = strtok(line, " ");
        char *value = strtok(NULL, " ");
        if (atoi(key) != request.kvp.key) {
            // copy lines excluding the one to be deleted
            fprintf(tempFp, "%s ", line);
            fprintf(tempFp, "%s", value);
        } else {
            reply.success = 1;
            printf("Successfully deleted key-value from file %s\n", filename);
        }
    }

    // Send the reply to the client
    printf("sending reply success: %d\n", reply.success);
    sendReply(reply);

    // Close the files
    fclose(fp);
    fclose(tempFp);

    // remove the original file
    remove(filename);

    // rename the temporary file to the original filename
    rename(tempFilename, filename);
}

void receiveAndHandleRequest(mqd_t clientToServerQueueID) {
    struct Request request;

    // Get the size of the next message in the queue
    struct mq_attr msgAttr;
    if (mq_getattr(clientToServerQueueID, &msgAttr) == -1) {
        perror("Error getting message queue attributes");
        exit(EXIT_FAILURE);
    }

    // Dynamically allocate memory for the message based on its reported size
    char* buffer = (char*)malloc(msgAttr.mq_msgsize);
    if (buffer == NULL) {
        perror("Error allocating memory for message");
        exit(EXIT_FAILURE);
    }

    // Receive the request from the message queue
    if (mq_receive(clientToServerQueueID, buffer, msgAttr.mq_msgsize, 0) == -1) {
        perror("Error receiving message from client to server message queue");
        free(buffer); // Free allocated memory before exiting
        exit(EXIT_FAILURE);
    }

    // Copy the received data into the request structure
    memcpy(&request, buffer, sizeof(struct Request));

    // Free the allocated memory
    free(buffer);

    // Print the content of the received request
    printf("Received Request:\n");
    printf("Request Type: %s\n", request.rType);
    printf("Key: %d, Value: %s\n", request.kvp.key, request.kvp.value);

    // Handle the request based on its type
    if (strcmp(request.rType, "PUT") == 0) {
        handlePutRequest(request);
    }else if(strcmp(request.rType, "GET") == 0){
        handleGetRequest(request);
    }else if (strcmp(request.rType, "DEL") == 0) {
        handleDeleteRequest(request);
    }else {
        printf("Invalid request type\n");
    }

    // Decrement the message count
    mq_attr_clt.mq_curmsgs--;
}

// Function cleanup to close the message queues and unlink them
void cleanup() {

    if (mq_close(clientToServerQueueID) == -1) {
        perror("Error closing message queue");
        exit(EXIT_FAILURE);
    }

    if (mq_close(serverToClientQueueID) == -1) {
        perror("Error closing message queue");
        exit(EXIT_FAILURE);
    }

    // add / to the front of the name and add _1 to the end
    char mq_name1[20];
    mq_name1[0] = '/';
    strcpy(mq_name1 + 1, mqname);
    strcat(mq_name1, "_1");

    char mq_name2[20];
    mq_name2[0] = '/';
    strcpy(mq_name2 + 1, mqname);
    strcat(mq_name2, "_2");

    if (mq_unlink(mq_name1) == -1) {
        perror("Error unlinking message queue");
        exit(EXIT_FAILURE);
    }

    if (mq_unlink(mq_name2) == -1) {
        perror("Error unlinking message queue");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {

    parseArguments(argc, argv);

    initClientToServerMessageQueue();
    initServerToClientMessageQueue();

    createDataFiles(dcount, fname);

    while (1) {
        // Receive and print requests indefinitely
        receiveAndHandleRequest(clientToServerQueueID);
    }

    cleanup();
    
    return 0;
}