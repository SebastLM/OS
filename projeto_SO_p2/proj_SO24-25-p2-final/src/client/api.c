#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

//file descriptors do fifo de request e response e do fifo do servidor
int req_pipe = -1;
int resp_pipe = -1;
int server_pipe = -1;

//path dos fifos
char req_pipe_path[256];
char resp_pipe_path[256];
char notif_pipe_path[256];
char server_pipe_path[256];


void clean() {

  // Fecha descriptor o fifo de pedido
  if (req_pipe != -1) {
    close(req_pipe);
    req_pipe = -1;
  }

  // Fecha descriptor o fifo de resposta
  if (resp_pipe != -1) {
    close(resp_pipe);
    resp_pipe = -1;
  }
  
  // Fecha descriptor o fifo do servidor
  if (server_pipe != -1) {
    close(server_pipe);
    server_pipe = -1;
  }
}

int kvs_connect(const char* req_path, const char* resp_path, const char* notif_path, const char* server_path) {

  // Acrescentar \0 ao nome do fifo para abrir o fifo
  strncpy(req_pipe_path, req_path, sizeof(req_pipe_path) - 1);
  req_pipe_path[sizeof(req_pipe_path) - 1] = '\0';

  strncpy(resp_pipe_path, resp_path, sizeof(resp_pipe_path) - 1);
  resp_pipe_path[sizeof(resp_pipe_path) - 1] = '\0';

  strncpy(notif_pipe_path, notif_path, sizeof(notif_pipe_path) - 1);
  notif_pipe_path[sizeof(notif_pipe_path) - 1] = '\0';

  // Acrescentar o \0 ao nome dado pelo cliente do respetivo servidor para abri o pipe do server
  strncpy(server_pipe_path, server_path, sizeof(server_pipe_path) - 1);
  server_pipe_path[sizeof(server_pipe_path) - 1] = '\0';
  
  // Criacao dos fifos
  if (mkfifo(req_pipe_path, 0666) == -1) {
    perror("Failed to create request pipe");
    return -1;
  }

  if (mkfifo(resp_pipe_path, 0666) == -1) {
    perror("Failed to create response pipe");
    unlink(req_pipe_path);
    return -1;
  }

  if (mkfifo(notif_pipe_path, 0666) == -1) {
    perror("Failed to create notification pipe");
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    return -1;
  }

  // Abrir o server pipe para escrever
  server_pipe = open(server_path, O_WRONLY);
  if (server_pipe == -1) {
    perror("Failed to open server pipe for writing");
    return -1;
  }

  // Op code mais nome dos fifos para pedirem permissao ao servidor para conectar
  char pedido[122];
  pedido[0] = OP_CODE_CONNECT;
  strncpy(pedido + 1, req_pipe_path, 40);
  pedido[41] = '\0'; 

  strncpy(pedido + 41, resp_pipe_path, 40);
  pedido[81] = '\0'; 

  strncpy(pedido + 81, notif_pipe_path, 40);
  pedido[121] = '\0'; 

  // Mandar o pedido para conectar para o pipe do servidor
  if (write(server_pipe, pedido, sizeof(pedido)) == -1) {
    perror("Failed to write to request/response pipe");
    clean();
    return -1;
  }

  // Abrir o pipe que recebe as respostas
  resp_pipe = open(resp_path, O_RDONLY);
  if (resp_pipe == -1) {
    perror("Failed to open response pipe for writing");
    return -1;
  }

  // Abrir o pipe de pedidos do cliente
  req_pipe = open(req_path, O_WRONLY);
  if (req_pipe == -1) {
    perror("Failed to open response pipe for writing");
    return -1;
  }

  // Ler resposta enviada pelo servidor para o cliente ao pedido de conecxao
  char response[3];
  ssize_t bytes_read = read(resp_pipe, response, sizeof(response));

  if (bytes_read == -1) {
    perror("Failed to read from request/response pipe");
    clean();
    return -1;
  }

  // Tratamento dos diferentes op_codes
  if (strcmp(response, "10") == 0) {
    printf("Server returned 0 for operation: connect\n");
    return 0;
  } else if (strcmp(response, "11") == 0) {
    printf("Server returned 1 for operation: connect\n");
    clean();
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    return 1;
  }

  return 0;
}


int kvs_disconnect(void) {
  
  // Escrever o pedido para disconectar para o request pipe
  char msgDisc[2] = "2";
  if (write(req_pipe, msgDisc, sizeof(msgDisc)) == -1) {
    perror("Error writing to file");
    clean(); 
    return 1;
  }

  // Leitura do op_code enviado pelo servidor para o response pipe 
  char buffer[3];
  ssize_t bytes_read = read(resp_pipe, buffer, sizeof(buffer));
  if (bytes_read < 0) {
    perror("Failed to read from response pipe");
    clean();
    return 1;
  }
  // Tratamento dos diferentes op_codes
  if (strcmp(buffer, "20") == 0) {
    printf("Server returned 0 for operation: disconnect\n");
    clean();
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    return 0;
  } else if (strcmp(buffer, "21") == 0) { 
    printf("Server returned 1 for operation: disconnect\n");
  }
  clean();
  return 1;
}



int kvs_subscribe(const char* key) {
  // Pedido de subscribe do cliente a key
  char pedido[42]= "3";
  strncpy(pedido + 1, key, 41);
  pedido[41] = '\0';

  // Escrever op_code + key para o pipe
  if (write(req_pipe, pedido, sizeof(pedido)) == -1) {
    perror("Failed to write to request/response pipe");
    clean();
    return 1;
  }

  // Ler do response pipe a resposta do servidor
  char buffer[3];
  ssize_t bytes_read = read(resp_pipe, buffer, sizeof(buffer));
  if (bytes_read < 0) {
    perror("Failed to read from response pipe");
    clean();
    return 1;
  }
  // Tratamento dos diferentes op_codes
  if (strcmp(buffer, "30") == 0) {
    printf("Server returned 0 for operation: subscribe\n");
    return 0;
  } else if (strcmp(buffer, "31") == 0) {
    printf("Server returned 1 for operation: subscribe\n");
  }
  return 1;
}

int kvs_unsubscribe(const char* key) {
  // Pedido de unsubscribe do cliente a key
  char pedido[42] = "4" ;
  strncpy(pedido + 1, key, 41);
  pedido[41] = '\0';
  
  // Escrever op_code + key para o pipe
  if (write(req_pipe, pedido, sizeof(pedido)) == -1) {
    perror("Failed to write to request/response pipe");
    clean();       
    return 1;
  }

  // Ler do response pipe a resposta do servidor
  char buffer[3];
  ssize_t bytes_read = read(resp_pipe, buffer, sizeof(buffer));
  if (bytes_read < 0) {
    perror("Failed to read from response pipe");
    clean();
    return 1;
  }
  
  // Tratamento dos diferentes op_codes
  if (strcmp(buffer, "40") == 0) {
    printf("Server returned 0 for operation: unsubscribe\n");
    return 0;
  } else if (strcmp(buffer, "41") == 0) {
    printf("Server returned 1 for operation: unsubscribe\n");
  }
  return 1;
}


