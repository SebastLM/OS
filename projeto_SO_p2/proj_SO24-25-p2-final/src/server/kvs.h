#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>

typedef struct ClientNode {
    int pipeNoti;
    struct ClientNode *next;
} ClientNode;

typedef struct KeyNode {
    char *key;
    char *value;
    ClientNode *clients;
    struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t tablelock;
} HashTable;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key); 

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Associa o fifo notificacoes de um cliente a uma dada key
/// @param ht Hash table.
/// @param key Chave a subscrever.
/// @param pipeNoti fd do fifo de notificacoes do cliente.
/// @return 0 se subscrever com sucesso, 1 caso contrario.
int subscribe(HashTable *ht, const char *key, int pipeNoti);

/// Retira o fifo de notificacoes de um cliente de uma dada key
/// @param ht Hash table.
/// @param key chave a dar unsubscribe
/// @param pipeNoti fd do fifo de notifiacoes do cliente
/// @return 0 if the node was deleted successfully, 1 otherwise.
int unsubscribe(HashTable *ht, const char *key, int pipeNoti);

/// Retira todos os fifos de notificacoes de um cliente da Hash table
/// @param ht Hash table.
/// @param pipeNoti fd do fifo de notificacoes do cliente.
/// @return 0 se eliminou todos os fifos notificacoes da hashtable com sucesso, 1 caso contrario.
void disconnect(HashTable *ht, int pipeNoti);

/// Retira todos os fifos de notificacoes de todos os cliente da Hash table
/// @param ht Hash table.
/// @return 0 se eliminou todos os fifos notificacoes da hashtable com sucesso, 1 caso contrario.
void clean_subscriptions(HashTable *ht);

#endif  // KVS_H
