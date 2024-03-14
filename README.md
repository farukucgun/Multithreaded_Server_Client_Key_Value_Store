# Multithreaded_Server_Client_Key_Value_Store

Store key value pairs using multithreaded server and client 

Requests are made from the client, communication is done via POSIX queues

Server will process get, put and delete requests to modify the data store

Mutex locks and condition variables are used to synchronize access to shared resources