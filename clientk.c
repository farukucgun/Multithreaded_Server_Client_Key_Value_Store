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

// Global variables
int clicount;
char fname[20];
int vsize;
char mqname[20];
int dlevel;
int clientToServerQueueID;
int serverToClientQueueID;
int MAX_MESSAGES = 10;

// Mutex for protecting shared resources
pthread_mutex_t requestMutex = PTHREAD_MUTEX_INITIALIZER;
// pthread_mutex_t replyMutex = PTHREAD_MUTEX_INITIALIZER;

// Mutex and condition variable for synchronizing access to the reply buffer
pthread_mutex_t replyBufferMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t replyAvailable = PTHREAD_COND_INITIALIZER;

// Define the structure for a reply
struct Reply {
    int success; // Indicates whether the operation was successful
    char value[64]; // The retrieved value for GET requests
};

// Structs
struct mq_attr mq_attr_clt;
struct mq_attr mq_attr_srv;

struct KeyValuePair {
    int key;  
    char value[64];  
};

struct Request {
    char rType[10];
    struct KeyValuePair kvp;
};

int MAX_MSG_SIZE_Q1 = 128;
int MAX_MSG_SIZE_Q2 = 128;

// Buffer to temporarily store reply messages
struct Reply replyBuffer[10];
int replyBufferCount = 0;

// Function to parse command line arguments
void parseArguments(int argc, char *argv[]) {

    printf("parsing arguments\n");

    if (argc != 11) {
        fprintf(stderr, "Usage: %s -n <clicount> -f <fname> -s <vsize> -m <mqname> -d <dlevel>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    for (int i = 1; i < argc; i += 2) {
        if (strcmp(argv[i], "-n") == 0) {
            clicount = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-f") == 0) {
            strcpy(fname, argv[i + 1]);
        } else if (strcmp(argv[i], "-s") == 0) {
            vsize = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-m") == 0) {
            strcpy(mqname, argv[i + 1]);
        } else if (strcmp(argv[i], "-d") == 0) {
            dlevel = atoi(argv[i + 1]);
        } else {
            fprintf(stderr, "Invalid option: %s\n", argv[i]);
            exit(EXIT_FAILURE);
        }
    }
}

// Function to print a key-value pair
void printKeyValuePair(struct KeyValuePair* kvp) {
    printf("Key: %d, Value: %s\n", kvp->key, kvp->value);
}

// Function to print a request
void printRequest(struct Request* req) {
    printf("Request Type: %s\n", req->rType);
    printf("Key-Value Pair:\n");
    printKeyValuePair(&(req->kvp));
}

// Function to print all requests
void printAllRequests(struct Request* requests, int count) {
    for (int i = 0; i < count; ++i) {
        printf("Request #%d:\n", i + 1);
        printRequest(&requests[i]);
        printf("\n");
    }
}

// struct KeyValuePair initKeyValuePair() { // value should be initialized to null terminator
//     struct KeyValuePair kvp;
//     kvp.key = 0;
//     kvp.value[0] = '\0';
//     return kvp;
// }

// Function to initialize the client to server message queue
void initClientToServerMessageQueue() {
    mq_attr_clt.mq_flags = 0;
    mq_attr_clt.mq_maxmsg = MAX_MESSAGES;
    mq_attr_clt.mq_msgsize = MAX_MSG_SIZE_Q1;
    mq_attr_clt.mq_curmsgs = 0;
    
    // add / to the front of the name and add _1 to the end
    char mq_name[20];
    mq_name[0] = '/';
    strcpy(mq_name + 1, mqname);
    strcat(mq_name, "_1");

    // Try to open the existing message queue   
    // If it does not exist, create it
    clientToServerQueueID = mq_open(mq_name, O_RDWR | O_CREAT, 0666, &mq_attr_clt);
    if (clientToServerQueueID == -1) {
        perror("Error opening client to server message queue");
        exit(EXIT_FAILURE);
    }

    printf("client to server Message queue opened successfully\n");
}

// Function to initialize the server to client message queue
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

    // Try to open the existing message queue   
    // If it does not exist, create it
    serverToClientQueueID = mq_open(mq_name, O_RDWR | O_CREAT, 0666, &mq_attr_srv);
    if (serverToClientQueueID == -1) {
        perror("Error opening message queue");
        exit(EXIT_FAILURE);
    }

    printf("server to client Message queue opened successfully\n");
}

// Function to send a request to the message queue
void sendRequestToClientToServerQueue(struct Request *request) {

    // print the number of messages in the queue
    printf("\nNumber of messages in the client to server queue: %ld\n", mq_attr_clt.mq_curmsgs);
    
    // check if there are less than max messages in the queue
    if (mq_attr_clt.mq_curmsgs >= mq_attr_clt.mq_maxmsg) {
        printf("client to server message queue is full");
        return;
    }

    printf("Sending message to client to server message queue\n");
    // printf("Size of the message: %ld\n", sizeof(struct Request));
    // printf("Max message size: %ld\n", mq_attr_clt.mq_msgsize);
    // send the request to the message queue
    if (mq_send(clientToServerQueueID, (char *)request, mq_attr_clt.mq_msgsize, 0) == -1) {
        perror("Error sending message to client to server message queue");
        exit(EXIT_FAILURE);
    }

    printf("Message sent to client to server message queue succsesfully\n");

    // increment the message count
    mq_attr_clt.mq_curmsgs++;
}

// Function to receive a response from the message queue and return the received reply
struct Reply receiveReplyFromServerToClientQueue() {
    printf("\nNumber of messages in the server to client queue: %ld\n", mq_attr_srv.mq_curmsgs);

    // check if there are no messages in the queue
    if (mq_attr_srv.mq_curmsgs == 0) {
        // printf("server to client message queue is empty");
        struct Reply emptyReply;
        emptyReply.success = 0;
        emptyReply.value[0] = '\0';
        return emptyReply;
    }

    printf("\nReceiving message from server to client message queue\n");

    // receive the reply from the message queue
    struct Reply reply;
    if (mq_receive(serverToClientQueueID, (char *)&reply, sizeof(struct Reply), 0) == -1) {
        perror("Error receiving message from server to client message queue");
        exit(EXIT_FAILURE);
    }

    printf("Message received from server to client message queue succesfully");

    // decrement the message count
    mq_attr_srv.mq_curmsgs--;

    return reply;
}

// Function to add a reply to the reply buffer
void addToReplyBuffer(struct Reply *reply) {
    // Lock the mutex before adding the reply
    pthread_mutex_lock(&replyBufferMutex);

    // Add the reply to the reply buffer
    replyBuffer[replyBufferCount] = *reply;
    ++replyBufferCount;

    // Signal that a reply is available
    pthread_cond_signal(&replyAvailable);

    // Unlock the mutex
    pthread_mutex_unlock(&replyBufferMutex);
}


// Function to get a reply from the reply buffer
struct Reply getFromReplyBuffer() {
    // Lock the mutex before checking the reply buffer
    printf("Getting reply from reply buffer\n");
    pthread_mutex_lock(&replyBufferMutex);
    printf("Reply buffer mutex locked\n");
    printf("Reply buffer count: %d\n", replyBufferCount);

    // Wait until a reply is available
    while (replyBufferCount == 0) {
        pthread_cond_wait(&replyAvailable, &replyBufferMutex);
    }

    // Get the first reply from the reply buffer
    struct Reply reply = replyBuffer[0];
    --replyBufferCount;

    // Shift the replies in the buffer
    for (int i = 0; i < replyBufferCount; ++i) {
        replyBuffer[i] = replyBuffer[i + 1];
    }

    // Unlock the mutex
    pthread_mutex_unlock(&replyBufferMutex);

    return reply;
}


// Function to be executed by each client thread
void *clientThread(void *arg) {
    int thread_id = *(int *)arg;
    printf("\nClient thread %d created.\n", thread_id);

    // add interacting with the user interactively

    // read a request from the clients input file
    char filename[20];
    strcpy(filename, fname);
    char thread_id_str[5];
    sprintf(thread_id_str, "%d", thread_id);
    strcat(filename, thread_id_str);

    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        exit(EXIT_FAILURE);
    }

    char line[100];
    while (fgets(line, sizeof(line), file)) {
        char kv_rtype[10];
        int kv_key;
        char kv_value[64];
        
        int result = sscanf(line, "%s %d %s", kv_rtype, &kv_key, kv_value);
        
        struct Request request; 
        // request.kvp = initKeyValuePair();
        struct KeyValuePair kvp;
        kvp.key = 0;
        kvp.value[0] = '\0';
        request.kvp = kvp;

        //struct Request request;
        strcpy(request.rType, kv_rtype);
        request.kvp.key = kv_key;
        strcpy(request.kvp.value, kv_value);
        
        if (result == 2) { // read 2 values
            request.kvp.value[0] = '\0'; // Empty string for operations other than PUT
        } else if (result != 3) { // read invalid number of values 
            printf("Invalid line: %s", line);
            continue;
        }

        // print the processed request
        printf("\nOperation: %s, Key: %d, Value: %s\n", request.rType, request.kvp.key, request.kvp.value);
        
        // Lock the mutex before sending the request
        pthread_mutex_lock(&requestMutex);
        sendRequestToClientToServerQueue(&request);
        pthread_mutex_unlock(&requestMutex);

        // Get the reply from the reply buffer
        //struct Reply reply = getFromReplyBuffer();

        //print the reply
        //printf("\nSuccess: %d, Value: %s\n", reply.success, reply.value);
    }

    fclose(file);

    return NULL;
}

// Function to be executed by the front-end (FE) thread
void *frontEndThread(void *arg) {
    // while a reply is available in the server to client message queue, get a reply and add it to the reply buffer, 
    // then signal that a reply is available

    while (1) {
        // Get the reply from the server to client message queue
        // printf("Receiving message from server to client message queue\n");
        
        // if(mq_attr_srv.mq_curmsgs >= 1){
        //     struct Reply reply = receiveReplyFromServerToClientQueue();
        //     printf("Message received from server to client message queue successfully\n");
        //     addToReplyBuffer(&reply);
        //     printf("Success: %d, Value: %s\n", reply.success, reply.value);
        // }

        pthread_mutex_lock(&replyBufferMutex);
        while (mq_attr_srv.mq_curmsgs == 0) {
            pthread_cond_wait(&replyAvailable, &replyBufferMutex);
        }
        struct Reply reply = replyBuffer[replyBufferCount - 1];
        --replyBufferCount;
        pthread_mutex_unlock(&replyBufferMutex);

        printf("Message received from server to client message queue successfully\n");
        addToReplyBuffer(&reply);
        printf("Success: %d, Value: %s\n", reply.success, reply.value);
    }
}

// Function cleanup to close the message queues and unlink them
void cleanup() {

    // Destroy the mutexes and condition variable
    pthread_mutex_destroy(&requestMutex);
    pthread_mutex_destroy(&replyBufferMutex);
    pthread_cond_destroy(&replyAvailable);

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

    // Create the front-end thread
    pthread_t frontEndThreadID;
    if (pthread_create(&frontEndThreadID, NULL, frontEndThread, NULL) != 0) {
        perror("Error creating front-end thread");
        exit(EXIT_FAILURE);
    }

    // Create client threads
    pthread_t clientThreadIDs[clicount];
    int threadIDs[clicount];

    for (int i = 0; i < clicount; ++i) {
        threadIDs[i] = i + 1;

        if (pthread_create(&clientThreadIDs[i], NULL, clientThread, &threadIDs[i]) != 0) {
            perror("Error creating client thread");
            exit(EXIT_FAILURE);
        }
    }

    // Join client threads
    for (int i = 0; i < clicount; ++i) {
        if (pthread_join(clientThreadIDs[i], NULL) != 0) {
            perror("Error joining client thread");
            exit(EXIT_FAILURE);
        }
    }

    // Join the front-end thread
    if (pthread_join(frontEndThreadID, NULL) != 0) {
        perror("Error joining front-end thread");
        exit(EXIT_FAILURE);
    }

    cleanup();

    return 0;
}