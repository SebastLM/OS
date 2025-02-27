#include "kvs.h"
#include "string.h"
#include <ctype.h>

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable* create_hash_table() {
	HashTable *ht = malloc(sizeof(HashTable));
	if (!ht) return NULL;
	for (int i = 0; i < TABLE_SIZE; i++) {
		ht->table[i] = NULL;
	}
	pthread_rwlock_init(&ht->tablelock, NULL);
	return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);

    // Search for the key node
	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // overwrite value
            free(keyNode->value);
            keyNode->value = strdup(value);

            ClientNode *current_client = keyNode->clients;
            while (current_client != NULL) {
                char message[82];
                char formatted_key[40];
                char formatted_value[40];

                strncpy(formatted_key, key, 39); 
                strncpy(formatted_value, value, 39);

                snprintf(message, sizeof(message), "(%s,%s)", formatted_key, formatted_value);

                if (write(current_client->pipeNoti, message, strlen(message)) == -1) {
                    perror("Failed to write notification to pipe");
                }
                current_client = current_client->next;
            }

            return 0;
        }
        previousNode = keyNode;
        keyNode = previousNode->next; // Move to the next node
    }
    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);

	KeyNode *keyNode = ht->table[index];
    KeyNode *previousNode;
    char *value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return the value if found
        }
        previousNode = keyNode;
        keyNode = previousNode->next; // Move to the next node
    }

    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);

    // Search for the key node
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node

            char formatted_key[40];
            char formatted_value[40];
            char message[82]; 

            strncpy(formatted_key, key, 39);  
            strncpy(formatted_value, "DELETED", 39); 

            snprintf(message, sizeof(message), "(%s,%s)", formatted_key, formatted_value);

            ClientNode *current_client = keyNode->clients;
            while (current_client != NULL) {
                if (write(current_client->pipeNoti, message, strlen(message)) == -1) {
                    perror("Failed to write notification to pipe");
                }
                current_client = current_client->next;
            }

            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);

            while (keyNode->clients != NULL) {
                ClientNode *temp = keyNode->clients;
                keyNode->clients = keyNode->clients->next;
                free(temp);
            }

            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }

    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    pthread_rwlock_destroy(&ht->tablelock);
    free(ht);
}

int subscribe(HashTable *ht, const char *key, int pipeNoti) {
    // Indice da chave
    int index = hash(key);
    
    // Obtem o no da lista
    KeyNode *keyNode = ht->table[index];

    // Percorre a lista
    while (keyNode != NULL) {
        // Compara a chave atual com a fornecida
        if (strcmp(keyNode->key, key) == 0) {
            // Se a chave for encontrada percorre a lista de clientes
            ClientNode *current_client = keyNode->clients;
            while (current_client != NULL) {
                // Caso o cliente ja esteja subscrito
                if (current_client->pipeNoti == pipeNoti) {
                    return 0;
                }
                // Proximo cliente
                current_client = current_client->next;
            }
            // Caso nao esteja inscrito
            ClientNode *new_client = malloc(sizeof(ClientNode));
            // Se falhar a alocacao de espaco
            if (new_client == NULL) {
                perror("Failed to allocate memory for new client");
                return 1;
            }
            // Armazenar os dados do cliente
            new_client->pipeNoti = pipeNoti;
            new_client->next = keyNode->clients;
            keyNode->clients = new_client;

            return 0;// Cliente subscrito
        }
        keyNode = keyNode->next; 
    }
    // Caso a chave nao seja encontrada
    printf("Key '%s' not found in the hash table\n", key);
    return 1;
}

int unsubscribe(HashTable *ht, const char *key, int pipeNoti) {
    // Indice da chave 
    int index = hash(key);

    // Obtem o no da lista
    KeyNode *keyNode = ht->table[index];

    // Percorre os nos da lista
    while (keyNode != NULL) {
        // Compara a chave atual com a fornecida
        if (strcmp(keyNode->key, key) == 0) {
            // Inicializacao de ponteiros para percorrer a lista
            ClientNode *current_client = keyNode->clients;
            ClientNode *prev_client = NULL;

            // Percorre a lista de clientes 
            while (current_client != NULL) {
                if (current_client->pipeNoti == pipeNoti) {
                    // Caso o cliente a ser removido seja o primeiro da lista
                    if (prev_client == NULL) {
                        keyNode->clients = current_client->next;
                    }
                    // Restantes posicoes
                    else { 
                        prev_client->next = current_client->next;
                    }
                    // Liberta memoria alocada para o cliente removido
                    free(current_client);
                    return 0;
                }
                // Avanca para o proximo cliente
                prev_client = current_client;
                current_client = current_client->next;
            }
            // Caso nao seja encontrado na lista
            printf("Client with pipeNoti %d not subscribed for key '%s'\n", pipeNoti, key);
            return 1;
        }
        keyNode = keyNode->next;
    }
    // Caso a chave nao seja encontrada
    printf("Key '%s' not found in the hash table\n", key);
    return 1;
}


void disconnect(HashTable *ht, int pipeNoti) {
    // Percorre todas as posicoes da hash table
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];

        // Percorre os nos da lista
        while (keyNode != NULL) {
            ClientNode *current_client = keyNode->clients;
            ClientNode *prev_client = NULL;

            // Percorre a lista de clientes 
            while (current_client != NULL) {
                // Se o pipeNoti do cliente atual corresponder ao da tabela
                if (current_client->pipeNoti == pipeNoti) {
                    // Remove o cliente atual da lista
                    if (prev_client == NULL) {
                        keyNode->clients = current_client->next;
                    } else {
                        prev_client->next = current_client->next;
                    }

                    // Liberta memoria alocada para o cliente removido
                    free(current_client);

                    // Atualiza o ponteiro para o proximo cliente
                    if (prev_client == NULL) {
                        current_client = keyNode->clients;
                    } else {
                        current_client = prev_client->next;
                    }
                    continue;
                }
                prev_client = current_client;
                current_client = current_client->next;
            }

            // Avanca para o proximo no
            keyNode = keyNode->next;
        }
    }
}

void clean_subscriptions(HashTable *ht) {

    // Percorre todas as posicoes da tabela hash
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];

        // Percorre os nos da lista
        while (keyNode) {
            ClientNode *clientNode = keyNode->clients;

            // Percorre a lista de clientes 
            while (clientNode) {
                ClientNode *temp = clientNode;
                // Avanca para o proximo cliente
                clientNode = clientNode->next;
                // Liberta memoria alocada para o cliente removido
                free(temp);
            }
            keyNode->clients = NULL;
            // Avanca para o proximo no
            keyNode = keyNode->next;
        }
    }
}