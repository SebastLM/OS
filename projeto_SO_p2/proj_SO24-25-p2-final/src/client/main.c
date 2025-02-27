#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "parser.h"

#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int notif_pipe = -1;

// Funcao para a leitura do notification pipe
void* read_notifications(void*) {
    char buffer[256]; 

    while (1) {
      // Ler em loop infinito o notification pipe
      ssize_t bytes_read = read(notif_pipe, buffer, sizeof(buffer));
      if (bytes_read <= 0) {
        break;
      }
      // Dar print a mensagem do servidor no terminal do cliente
      if (bytes_read > 0) {
        buffer[bytes_read] = '\0';
        printf("%s\n", buffer);
      }
    }
  exit(0);
  return NULL;
}

int main(int argc, char* argv[]) { 
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  // Caminho para os pipes do cliente
  char req_pipe_path[256] = "/tmp/req";
  char resp_pipe_path[256] = "/tmp/resp";
  char notif_pipe_path[256] = "/tmp/notif";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  // Acrescentar o id do cliente ao nome do fifo
  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // Acrescentar o nome do server a conectar
  char server_pipe_path[256];
  snprintf(server_pipe_path, sizeof(server_pipe_path), "/tmp/%s", argv[2]);

  // Funcao para conectar ao server
  if (kvs_connect(req_pipe_path, resp_pipe_path, notif_pipe_path, server_pipe_path) != 0) {
    // Caso nao consiga conectar ao server apaga o pipe de noti
    // Os outros pipes sao fechados no proprio kvs_connect
    unlink(notif_pipe_path);
    return 0;
  }
  // Abrir pipe de notificacoes do cliente
  notif_pipe = open(notif_pipe_path, O_RDONLY);
  if (notif_pipe == -1) {
    perror("Failed to open notification pipe");
    return 1;
  }

  // Criacao da thread para poder ler o pipe de notificacoes em paralelo com a execucao do cliente
  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, read_notifications, NULL) != 0) {
    perror("Failed to create notification thread");
    return 1;
  }
  
  while (1) {
    // Ciclo de leitura dos comandos do cliente e as respetivas execucoes
    switch (get_next(STDIN_FILENO)) {

      case CMD_DISCONNECT:
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        close(notif_pipe);
        unlink(notif_pipe_path);
        printf("Disconnected from server\n");
        break;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
        if (kvs_subscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0])) {
          fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
  //quando o pipe de noti deixar de ler(levar unlink) o programa acaba
  pthread_join(notif_thread, NULL);
  return 0;
}
