#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "string.h"
#include "sys/stat.h"
#include "pthread.h"
//#include "cmapreduce.h"
#include "sys/time.h"
#include "mapreduce.h"
#include "keyvalue.h"
#include "unistd.h"

using namespace MAPREDUCE_NS;

#define round(x) ((x)>=0?(long)((x)+0.5):(long)((x)-0.5))


void fileread(int, KeyValue *, void *);
void * hb_function(void *);
void * pingRecv(void *);
void newMap(int itask, char *str, int size, KeyValue *kv, void *ptr);
void newMap2(int itask, char *str, int size, KeyValue *kv, void *ptr);
void binMap(int, KeyValue *, void *);
void sum(char *, int, char *, int, int *, KeyValue *, void *);
void sum2(char *, int, char *, int, int *, KeyValue *, void *);
int ncompare(char *, int, char *, int);
void output(uint64_t, char *, int, char *, int , KeyValue *, void *);
void histoutput(uint64_t, char *, int, char *, int , KeyValue *, void *);
void getHistogram(MapReduce *, char *);


typedef struct
{
  int n, limit, flag;
} Count;

/* ---------------------------------------------------------------------- */

int myhash(char * key, int keysize)
{
  return strtol(key, NULL, 10) % 4;
}

int main(int narg, char **args)
{
  int me, nprocs;
  int nwords, nunique;
  double tstart, tstop;
  Count count;
  pthread_t thread1;
  MPI_Init(&narg, &args);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

  if (narg <= 1)
  {
    if (me == 0)
    {
      printf("Syntax: cwordfreq file1 file2 ...\n");
    }

    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  if (me != 0)
  {
    pthread_create(&thread1, NULL, hb_function, &me);
  }
else
{

    pthread_create(&thread1, NULL, pingRecv, &me);
sleep(10);
}

  MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
  //MapReduce *mra = new MapReduce(MPI_COMM_WORLD);
  //MapReduce *mrb = new MapReduce(MPI_COMM_WORLD);
  MapReduce *mra = new MapReduce();
  MapReduce *mrb = new MapReduce();
  mra->open();
  mrb->open();
  MPI_Barrier(MPI_COMM_WORLD);
  int test = 1;
  void *arg[4] = {args[1], args[2], mra->kv, mrb->kv};
  //printf("%s %s\n", arg[0], arg[1]);
printf("starting map\n");
  nwords = mr->map(nprocs, &fileread, arg);
  MPI_Barrier(MPI_COMM_WORLD);
printf("map done\n");
  mra->close();
  mrb->close();
printf("%d mr closed\n",me);
  mr->collate(NULL);
  MPI_Barrier(MPI_COMM_WORLD);
printf("collate done\n");
  nunique = mr->reduce(&sum2, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
printf("reduce done\n");
  mr->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->sort_keys(&ncompare);
  MPI_Barrier(MPI_COMM_WORLD);
  FILE * pFile;
  pFile = fopen("result.out", "w");
  mr->map(mr, &output, pFile);
  MPI_Barrier(MPI_COMM_WORLD);
  fclose(pFile);
//sum done
  //getHistogram(mra, "hist.a");
  //getHistogram(mr, "hist.a");
  //getHistogram(mrb, "hist.b");
  /* FILE * pFilea;
printf("sum done\n");
   pFilea = fopen("test", "w");
   mra->gather(1);
   mra->map(mra, &histoutput, pFilea);
   fclose(pFilea);*/
  mra->collate(NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mra->reduce(&sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mra->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  mra->sort_keys(&ncompare);
  MPI_Barrier(MPI_COMM_WORLD);
  mra->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  FILE * pFile3;
  pFile3 = fopen("hist.a", "w");
  mra->map(mra, &histoutput, pFile3);
  fclose(pFile3);
  mrb->collate(NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mrb->reduce(&sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mrb->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  mrb->sort_keys(&ncompare);
  MPI_Barrier(MPI_COMM_WORLD);
  FILE * pFile4;
  pFile4 = fopen("hist.b", "w");
  mrb->map(mrb, &histoutput, pFile4);
  fclose(pFile4);
  mr->collate(NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->reduce(&sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->sort_keys(&ncompare);
  MPI_Barrier(MPI_COMM_WORLD);
  FILE * pFile2;
  pFile2 = fopen("hist.c", "w");
  mr->map(mr, &histoutput, pFile2);
  fclose(pFile2);
  delete mr;
  MPI_Barrier(MPI_COMM_WORLD);
  pthread_cancel(thread1);

  MPI_Finalize();
//exit(0);
}

void getHistogram(MapReduce *mr, char * file)
{
  FILE * pFile2;
  pFile2 = fopen("test", "w");
  mr->map(mr, &histoutput, pFile2);
  fclose(pFile2);
  mr->collate(NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->reduce(&sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->gather(1);
  MPI_Barrier(MPI_COMM_WORLD);
  mr->sort_keys(&ncompare);
  MPI_Barrier(MPI_COMM_WORLD);
  FILE * pFile;
  pFile = fopen(file, "w");
  mr->map(mr, &histoutput, pFile);
  fclose(pFile);
}

void * hb_function(void *rank)
{
  timeval t1, t2;
  gettimeofday(&t1, NULL);
  int heartbeatVal = *((int *)rank);
  MPI_Request request;

  while(1)
  {
    gettimeofday(&t2, NULL);

    if((t2.tv_sec - t1.tv_sec) > 5)
    {
      //send heartbeat
      MPI_Isend(&heartbeatVal,1,MPI_INT,0,1337,MPI_COMM_WORLD,&request);
      gettimeofday(&t1, NULL);
    }

    sleep(1);
  }
}

void * pingRecv(void * ptr)
{
  int rank = -1;
  timeval time[10], tnow;
  MPI_Status status;
  //need to initialize time
	
    gettimeofday(&time[0], NULL);
    gettimeofday(&time[1], NULL);
    gettimeofday(&time[2], NULL);
    gettimeofday(&time[3], NULL);
  while(rank == -1)
  {
    //Recv
    int recvRank = 8;
    MPI_Request req;
    //MPI_Recv(&recvRank, 1, MPI_INT, MPI_ANY_SOURCE, 1337, MPI_COMM_WORLD, &status);
    printf("%d\n", recvRank);
    gettimeofday(&tnow, NULL);
if(recvRank > 10) continue;
time[recvRank-1]=tnow;
    if(tnow.tv_sec - time[0].tv_sec > 20)
    {
      rank = 1;
    }
    if(tnow.tv_sec - time[1].tv_sec > 20)
    {
      rank = 2;
    }
    if(tnow.tv_sec - time[2].tv_sec > 20)
    {
      rank = 3;
    }
    if(tnow.tv_sec - time[3].tv_sec > 20)
    {
      rank = 4;
    }

    sleep(1);
  }
    printf("%d exiting\n", rank);
//exit(-1);
/*  MPI_Group orig_group, new_group;
  MPI_Comm new_comm;
  MPI_Comm_group(MPI_COMM_WORLD, &orig_group);
  MPI_Group_excl(orig_group, 4, &rank, &new_group);
  MPI_Comm_create(MPI_COMM_WORLD, new_group, &new_comm);
*/  //use new_comm as communicator for new mr
}

void fileread(int itask, KeyValue *kv, void *ptr)
{
  //char hostname[1024];
  //hostname[1023] = '\0';
  //gethostname(hostname, 1023);
  //printf("Hostname: %s %i\n", hostname, itask);
  if(itask == 0 )
  {
    void **arg = (void **) ptr;
    char *file1 = (char *) arg[0];
    char *file2 = (char *) arg[1];
    char *files[2] = {file1, file2};
    char *whitespace = " \t\n\f\r\0";
    float f;
    int i;
    char key[10];
    int j;
    int index = 0;
    struct stat stbuf;
    int flag = stat(files[0], &stbuf);

    if (flag < 0)
    {
      printf("ERROR: Could not query file size\n");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int filesize = stbuf.st_size;
    FILE *fp = fopen(files[0], "r");
//wtf is this    char text[filesize + 1];
    char *text = (char *) malloc(filesize+1);
    int nchar = fread(text, 1, filesize, fp);
    text[nchar] = '\0';
    fclose(fp);
    char *word = strtok(text, whitespace);

    while (word)
    {
      f = strtof(word, NULL);
      i = (int) ((f + 10) / .5);
      kv->add((char *)&index, sizeof(int), (char *) &f, sizeof(float));
      KeyValue *kv2 = (KeyValue *) arg[2];
      //    printf("%p\n",kv2);
      kv2->add((char *)&i, sizeof(int), NULL, 0);
      word = strtok(NULL, whitespace);
      index++;
    }
printf("file1 done\n");
    free(text);
    int index2 = 0;
    struct stat stbuf2;
    int flag2 = stat(files[1], &stbuf2);

    if (flag2 < 0)
    {
      printf("ERROR: Could not query file size\n");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int filesize2 = stbuf2.st_size;
    FILE *fp2 = fopen(files[1], "r");
    //  char text2[filesize2 + 1];
    char * text2 = (char *)malloc(filesize2+1);
    int nchar2 = fread(text2, 1, filesize2, fp2);
    text2[nchar2] = '\0';
    fclose(fp2);
    char *word2 = strtok(text2, whitespace);

    while (word2)
    {
      f = strtof(word2, NULL);
      i = (int) ((f + 10) / .5);
      kv->add((char *)&index2, sizeof(int), (char *) &f, sizeof(float));
      ((KeyValue *)arg[3])->add((char *)&i, sizeof(int), (char *)&i, sizeof(int));
      word2 = strtok(NULL, whitespace);
      index2++;
    }

    free(text2);
    //Needs this but untested

    if (index != index2)
    {
      printf("ERROR: Different file size?\n");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
printf("file2 done\n");
  }
}


void newMap2(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *whitespace = " \t\n\f\r\0";
  char *word = strtok(str, whitespace);
  float f;
  int i;
  char key[10];
  int index = 0;

  while (word)
  {
    f = strtof(word, NULL);
    i = (int) ((f + 10) / .5);
    kv->add((char *)&index, sizeof(int), (char *) &f, sizeof(float));
    word = strtok(NULL, whitespace);
    index++;
  }
}

void newMap(int itask, char *str, int size, KeyValue *kv, void *ptr)
{
  char *whitespace = " \t\n\f\r\0";
  char *word = strtok(str, whitespace);
  float f;
  int i;
  char key[10];

  while (word)
  {
    f = strtof(word, NULL);
    i = (int) ((f + 10) / .5);
    kv->add((char *)&i, sizeof(int), NULL, 0);
    word = strtok(NULL, whitespace);
  }
}

void binMap(int itask, KeyValue *kv, void *ptr)
{
  char **files = (char **) ptr;
  struct stat stbuf;
  int flag = stat(files[itask], &stbuf);

  if (flag < 0)
  {
    printf("ERROR: Could not query file size\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int filesize = stbuf.st_size;
  FILE *fp = fopen(files[itask], "r");
  char text[filesize + 1];
  int nchar = fread(text, 1, filesize, fp);
  text[nchar] = '\0';
  fclose(fp);
  char *whitespace = " \t\n\f\r\0";
  char *word = strtok(text, whitespace);
  float f;
  int i;
  char key[10];

  while (word)
  {
    f = strtof(word, NULL);
    f = (int)((f + 10) / .5);
    i = (int) f;
    memset(key, 0, 10);
    sprintf(key, "%i", i);
    kv->add(key, sizeof(i), NULL, 0);
    //printf("%d\n",i );
    word = strtok(NULL, whitespace);
  }
}

void sum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  kv->add(key, keybytes, (char *) &nvalues, sizeof(int));
  //printf("%i %i %i \n", *(int *) key, nvalues);
}

void sum2(char *key, int keybytes, char *multivalue,
          int nvalues, int *valuebytes, KeyValue *kv, void *ptr)
{
  int i;
  //kv->add(key, keybytes, (char *) &nvalues, sizeof(int));
  float sum = 0;

  for(i = 0; i < nvalues; i++)
  {
    sum += *(float *) & (multivalue[i **valuebytes]);
    /*
        if(*(int *) key == 1)
        {
          printf("%f\n", *(float *) & (multivalue[i **valuebytes]));
        }
    */
  }

  //printf("%i %i %i %f\n", *(int *) key, nvalues, sum);
  kv->add(key, keybytes, (char *)&sum, sizeof(float));
}
/* ----------------------------------------------------------------------
   compare two counts
   order values by count, largest first
------------------------------------------------------------------------- */

int ncompare(char *p1, int len1, char *p2, int len2)
{
  int i1 = *(int *) p1;
  int i2 = *(int *) p2;

  if (i1 > i2)
  {
    return 1;
  }
  else if (i1 < i2)
  {
    return -1;
  }
  else
  {
    return 0;
  }
}

void m_prepareoutput(uint64_t itask, char *key, int keybytes, char *value,
                     int valuebytes, KeyValue *kv, void *ptr)
{
  int n = *(int *) value;
  kv->add(key, keybytes, (char *) &n, sizeof(int));
}

void output(uint64_t itask, char *key, int keybytes, char *value,
            int valuebytes, KeyValue *kv, void *ptr)
{
  int i = (int) ((*(float *)value + 20) / .5);
  fprintf((FILE *)ptr, "%.2f ", * (float *) value);
  kv->add((char *)&i, sizeof(int), NULL, 0);
  //printf("key:%i  value:%f\n", *(int *) key, *(float *)value);
}

void histoutput(uint64_t itask, char *key, int keybytes, char *value,
                int valuebytes, KeyValue *kv, void *ptr)
{
  fprintf((FILE *)ptr, "%i, %i\n", * (int *) key, * (int *) value);
// printf("key:%i  value:%f\n", *(int *) key, *(float *)value);
}
