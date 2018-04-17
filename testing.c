
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include "threadQuery.h"


pthread_mutex_t mutexlock;
WorkQueue_t * queue_pool;
char * dir = NULL;
void queryWiki(char * input, char * output);

// Target function
void * work (void * voidtdata)
{
  //initialize structures
  char * file_name = NULL;
  Par_Data_t * data = (Par_Data_t*)voidtdata;

  while (queue_pool->idx_txtAvailable < queue_pool->len)
  {
    pthread_mutex_lock(&mutexlock);
    if (queue_pool->idx_txtAvailable < queue_pool->len )
    {
      file_name = queue_pool->Queue[queue_pool->idx_txtAvailable];
      strtok(file_name, "\n");
      data->flag = 1;
      queue_pool->idx_txtAvailable +=1;
    }
    pthread_mutex_unlock(&mutexlock);
    if(data->flag == 1)
    {
      char * OutFile = (char*)malloc(40);
      char * inputFile = (char*)malloc(35);
      //Create output path and output file in tmp locally
      snprintf(OutFile, 40, "/tmp/MyMonth_out/%s/%s", dir, file_name); //touch /tmp/MyMonth_out/2015-02/part-r-00000
      //creating appropiate input path into tmp
      snprintf(inputFile, 35, "/tmp/MyMonth/%s/%s", dir, file_name); ///tmp/MyMonth/2015-02/part-r-00000

      queryWiki(inputFile, OutFile);
    }
    data->flag = 0;
  }
}

// Parallel Helper --> initializer can callee
void GetCategoriesParallel (int p, char * dir)
{
  // Declaration and initialization of variables
  int i, j, n, rc, node;
  char locatioOfDirNames[40]; //
  snprintf(locatioOfDirNames, 40, "/tmp/MyMonth/%s/NameOfFiles.txt", dir); ///tmp/MyMonth/2015-05/NameOfFiles.txt
  char line[13];
  FILE * DirContent;
  DirContent = fopen(locatioOfDirNames, "r");
  if (DirContent == NULL) {
    printf("ERROR: No directory content file.\n");
    exit(-1);
  }
  int numFileInDir = 0;
  i = 0;
  while (fgets(line, sizeof(line), DirContent)) {
    queue_pool->Queue[i] = (char *)malloc(sizeof(line));
    for(j = 0; j < sizeof(line); j++) {
      queue_pool->Queue[i][j] = line[j];
    }
    strtok(queue_pool->Queue[i], "\n");
    numFileInDir += 1;
    i += 1;
  }
  fclose(DirContent);
  queue_pool->len = numFileInDir;
  queue_pool->idx_txtAvailable = 0;

  // Initialize heap arrays for PTHREADS
  pthread_t * Threads;
  Par_Data_t * thread_data;
  Threads = (pthread_t *)malloc(p * sizeof(pthread_t));
  thread_data = (Par_Data_t *)malloc(p * sizeof(Par_Data_t));
  // Initialize data for threads
  for (i = 0; i < p; i++) {
    thread_data[i].ID = i;
    thread_data[i].flag = 0;
  }
  pthread_mutex_init(&mutexlock, NULL);
  for (i = 0; i < p; i++) {
    rc = pthread_create(&Threads[i], NULL, work, (void *)&thread_data[i]);
    if (rc) {
      printf("ERROR; return code from pthread_create() is %d\n", rc);
      exit(-1);
    }
  }

  for (int t = 0; t < p; t++) {
    pthread_join(Threads[t],NULL);
  }
  free(thread_data);
  return;
}

int main(int argc, char ** argv)
{
  int n = 60;
  int nthreads = 4;
  dir = "2015-02";
  queue_pool = (WorkQueue_t *)malloc( sizeof(WorkQueue_t) );
  queue_pool->Queue = (char * *)malloc((n-1) * sizeof(char *));
  queue_pool->idx_txtAvailable = -1;
  queue_pool->len = 0;
  GetCategoriesParallel(nthreads, dir);
  for (int i = 0; i < queue_pool->len; i++) {
    free(queue_pool->Queue[i]);
  }
  free(queue_pool);
  pthread_exit(NULL);
  return 0;
}
