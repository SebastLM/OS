#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <stdio.h>
#include <semaphore.h>
#include <signal.h>
#include <errno.h>


#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"


#define BUFFER_SIZE 8

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
  char *pedi_path;
  int pipePedi;
  char *resp_path;
  int pipeResp;
  char *noti_path;
  int pipeNoti;
} ClientInfo;


typedef struct {
    ClientInfo* buffer[BUFFER_SIZE]; 
    int in;  
    int out; 
    pthread_mutex_t mutex; 
    sem_t clients;  
    sem_t livre; 
} Buffer;

//inicializa o in, out e o mutex do buffer
Buffer request_buffer = {
    .in = 0,
    .out = 0,
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

void initialize_buffer() {
  sem_init(&request_buffer.clients, 0, 0); // Inicializa o semaforo de clientes como 0
  sem_init(&request_buffer.livre, 0, BUFFER_SIZE); // Inicializa o semaforo livre com o tamanho do buffer
}

void produce(ClientInfo* client) {
  sem_wait(&request_buffer.livre); // Aguarda ate que haja espaco disponivel no buffer
  pthread_mutex_lock(&request_buffer.mutex); // Bloqueia o mutex para acessar o buffer

  // Insere o cliente no indice de entrada do buffer
  request_buffer.buffer[request_buffer.in] = client;
  request_buffer.in = (request_buffer.in + 1) % BUFFER_SIZE;

  pthread_mutex_unlock(&request_buffer.mutex); // Liberta o mutex 
  sem_post(&request_buffer.clients); // Incrementa o semaforo de clients
}

ClientInfo* consume() {
  sem_wait(&request_buffer.clients); // Aguarda ate que haja um client disponivel no buffer
  pthread_mutex_lock(&request_buffer.mutex); // Bloqueia o mutex para acessar o buffer

  // Remove o cliente do indice de saida do buffer
  ClientInfo* client = request_buffer.buffer[request_buffer.out];
  request_buffer.out = (request_buffer.out + 1) % BUFFER_SIZE;

  pthread_mutex_unlock(&request_buffer.mutex); // Liberta o mutex
  sem_post(&request_buffer.livre); // Incrementa o semaforo de livre

  return client; // Retorna o cliente consumido do buffer 
}

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }


  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int remove_clients() {

  // Tenta bloquear o mutex associado ao buffer
  if (pthread_mutex_lock(&request_buffer.mutex) != 0) {
    fprintf(stderr, "Erro ao trancar o mutex do buffer\n");
    return -1;
  }

  // Percorre o buffer
  for (int i = 0; i < BUFFER_SIZE; i++) {
    ClientInfo* client = request_buffer.buffer[i]; // Obtem o cliente na posicao atual
    if (client != NULL) { // Verifica se ha um cliente nessa posicao
      // Fecha o descritor do fifo de pedidos
      if (client->pipePedi != -1) {
        close(client->pipePedi);
        client->pipePedi = -1;
      }
      // Fecha o descritor do fifo de resposta
      if (client->pipeResp != -1) {
        close(client->pipeResp);
        client->pipeResp = -1;
      }
      // Fecha o descritor do fifo de notificacoes
      if (client->pipeNoti != -1) {
        close(client->pipeNoti);
        client->pipeNoti = -1 ;
      }

      // Remove o fifo de pedidos
      if (client->pedi_path != NULL) {
        if (unlink(client->pedi_path) == -1) {
          perror("Erro ao remover pedi_path");
        }
        client->pedi_path = NULL;
      }
      // Remove o fifo de respostas
      if (client->resp_path != NULL) {
        if (unlink(client->resp_path) == -1) {
          perror("Erro ao remover resp_path");
        }
        client->resp_path = NULL;
      }
      // Remove o fifo de notificacoes
      if (client->noti_path != NULL) {
        if (unlink(client->noti_path) == -1) {
          perror("Erro ao remover noti_path");
        }
        client->noti_path = NULL;
      }
      // Liberta a memoria alocada para o cliente
      free(client);
    }
  }

  // Tenta destrancar o mutex associado ao buffer
  if (pthread_mutex_unlock(&request_buffer.mutex) != 0) {
    fprintf(stderr, "Erro ao destrancar o mutex do buffer\n");
    return -1;
  }

  return 0; // Retorna sucesso
}

void handle_sigusr1() {
  // Chama a funcao que limpa as subscritions da Hash table
  if (kvs_clean_subscriptions() != 0) {
    printf("error cleaning subscriptions\n");
  }
  // Chama a funcao que remove todos os clientes do buffer e fecha os fd dos fifos
  if (remove_clients() != 0) {
    printf("error removing clients\n");
  }
}


void pedi_reader(void* arg) {

  // Obtem o cliente passada como argumento
  ClientInfo* client = (ClientInfo*)arg;
  if (!client) {
    fprintf(stderr, "Erro: cliente inválido.\n");
    return;
  }

  // Escreve no fifo resposta a mens
  const char* resp1 = "10";
  if (write(client->pipeResp, resp1, strlen(resp1) + 1) == -1) {
    perror("Failed to write to FIFO");
    close(client->pipeResp);
    close(client->pipePedi);
    free(client);
    return;
  }
  printf("Enviado para FIFO: %s\n", resp1);
      
   // Abre o fifo de notificacao para escrita
  client->pipeNoti = open(client->noti_path, O_WRONLY);
  if (client->pipeNoti == -1) {
    perror("Failed to open response pipe for writing");
    return;
  }

  // Loop para leitura dos pedidos do cliente
  while (1) {
    char buffer[42]; // Buffer para armazenar a pedido
    ssize_t bytes_read = read(client->pipePedi, buffer, sizeof(buffer));

    // Verifica se ha dados lidos
    if (bytes_read > 0) {
      // Extrai o codigo de operacao do buffer
      int op_code = atoi(&buffer[0]);

      switch (op_code) {
        case 2: // Desconectar cliente
          const char* resp2 = "20";
          if (kvs_disconnect(client->pipeNoti) != 0) {
            resp2 = "21";
          }
          // Envia a resposta
          if (write(client->pipeResp, resp2, strlen(resp2) + 1) == -1) {
            perror("Failed to write to FIFO");
            
            close(client->pipeNoti);
            close(client->pipeResp); 
            close(client->pipePedi);
            free(client);
            return;
          }
          break;

        case 3: // Subscrever uma chave
          if (bytes_read >= 42) { 
            char key[41];
            strncpy(key, buffer + 1, 40);
            key[40] = '\0';

            const char* resp3 = "30";

            // Chama a funcao que subscreve a key
            if (kvs_subscribe(key, client->pipeNoti) != 0) { 
              resp3 = "31";
            }

            // Envia a resposta
            if (write(client->pipeResp, resp3, strlen(resp3) + 1) == -1) {
              perror("Failed to write to FIFO");
              close(client->pipeNoti);
              close(client->pipeResp);
              close(client->pipePedi);
              free(client);
              return;
            }
          }
          break;

        case 4: // Cancelar inscricao de uma chave
          if (bytes_read >= 42) {
            char key[41];
            strncpy(key, buffer + 1, 41);
            key[40] = '\0';

            const char* resp4 = "40";

            // Chama a fucao que da unsubscribe a 
            if (kvs_unsubscribe(key, client->pipeNoti) != 0) {
              resp4 = "41";
            }

            // Envia a resposta
            if (write(client->pipeResp, resp4, strlen(resp4)+ 1) == -1) {
              perror("failed to writte to FIFO");
              close(client->pipeNoti);
              close(client->pipeResp);
              close(client->pipePedi);
              free(client);
              return;
            }
          }
          break;

        default:
          break;
    }
    } else if (bytes_read == 0) {
      printf("EOF: Client closed pipePedi\n");
      break;
    } else {
      perror("Failed to read from pipePedi");
      break;
    }
  }

  close(client->pipeNoti);
  close(client->pipeResp);
  close(client->pipePedi);
  free(client);
  return;
}

void* manager_thread(void* arg) {
  (void)arg;

  // Inicializa um conjunto de sinais
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  // Bloqueia o sinal SIGUSR1 nesta thread
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  while (1) {
    // Aguarda ate consomir um cliente do buffer
    
    ClientInfo* client = consume();
    // Chama a funcao que processa os pedidos dos clientes
    pedi_reader((void*)client);
  }

  // Desbloqueia o sinal SIGUSR1 antes de sair da thread
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);
  return NULL;
}

void* fifo_reader(void* arg) {
  const char* fifo_name = (const char*)arg;

  // Configura o tratamento do sinal SIGUSR1 para chamar a funcao handle_sigusr1
  signal(SIGUSR1, handle_sigusr1);

  // Abre o fifo do servidor para leitura
  int server_fifo = open(fifo_name, O_RDONLY);
  if (server_fifo == -1) {
    perror("Failed to open FIFO for reading");
    return NULL;
  }
  // Buffer para leitura de dados do fifo
  char buffer[122];

  // Loop para ler os pedidos de conecao do fifo
  while (1) {
    ssize_t bytes_read = read(server_fifo, buffer, sizeof(buffer));
    
    if (bytes_read == sizeof(buffer)) {
      ClientInfo* client = (ClientInfo*)malloc(sizeof(ClientInfo));
      if (client == NULL) {
        perror("Failed to allocate memory for ClientInfo");
        continue;
      }

      // Extrai e copia os fifos enviados pelo cliente
      char Pedi[40];
      strncpy(Pedi, buffer + 1, 40);
      Pedi[40] = '\0'; 
      char Resp[40];
      strncpy(Resp, buffer + 41, 40);
      Resp[40] = '\0';
      char Noti[40];
      strncpy(Noti, buffer + 81, 40);
      Noti[40] = '\0';

      // Atribui os caminhos extraidos ao cliente
      client->pedi_path = Pedi;
      client->resp_path = Resp;
      client->noti_path = Noti;

      // Abre o fifo de resposta para escrita
      client->pipeResp = open(Resp, O_WRONLY);
      if (client->pipeResp == -1) {
        perror("Failed to open response pipe for writing");
        free(client);
        continue;
      }
   
      // Abre o fifo de pedido para leitura
      client->pipePedi = open(Pedi, O_RDONLY);
      if (client->pipePedi == -1) {
        perror("Failed to open response pipe for writing");
        free(client);
        continue;
      }
      // Adiciona o cliente ao buffer
      produce(client);
    }
  }
  close(server_fifo);
}


int main(int argc, char** argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }

  jobs_directory = argv[1];

  // Numero de threads gestoras
  size_t s = 8;

  // Inicializacao da thread anfitria
  pthread_t thread_anfitria;
  pthread_t* threads = malloc(s * sizeof(pthread_t));
  // Verifica se a alocacao de memoria foi bem sucedida
    if (threads == NULL) {
      fprintf(stderr, "Failed to allocate memory for threads\n");
      return 1;
    }

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

	if (max_backups <= 0) {
		write_str(STDERR_FILENO, "Invalid number of backups\n");
		return 0;
	}

	if (max_threads <= 0) {
		write_str(STDERR_FILENO, "Invalid number of threads\n");
		return 0;
	}

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // Inicializa o buffer produtor consumidor
  initialize_buffer();

  // Buffer com o caminho do fifo do servidor
  char server_pipe_path[256];
  snprintf(server_pipe_path, sizeof(server_pipe_path), "/tmp/%s", argv[4]);

  // Criacao do fifo do servidor
  if (mkfifo(server_pipe_path, 0666) == -1) {
    printf("%s\n",server_pipe_path);
    perror("Failed to create FIFO");
    return 1;
  }

  // Thread que efetua a leitura do server pipe
  if (pthread_create(&thread_anfitria, NULL, fifo_reader, (void*)server_pipe_path) != 0) {
    perror("Failed to create FIFO reader thread");
    unlink(server_pipe_path); 
    return 1;
  }
  
  // Cria s threads gestoras
  // s defenido como 8
  for (size_t i = 0; i < s; i++) {
    if (pthread_create(&threads[i], NULL, manager_thread, NULL) != 0) {
      perror("Failed to create FIFO reader thread");
      unlink(server_pipe_path); 
      return 1;
    }
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  // Espera que todas as threads gestoras acabem
  for (size_t i = 0; i < s; i++) {
    pthread_join(threads[i], NULL);
  }

  // Aguarda a thread anfitria acabar
  pthread_join(thread_anfitria, NULL);
  
  // Quando acabarem de correr as threads apaga o fifo do servidor
  if (unlink(server_pipe_path) == -1) {
    perror("Failed to remove FIFO");
  } else {
    printf("FIFO '%s' removed successfully.\n", server_pipe_path);
  }

  kvs_terminate();

  return 0;
}

