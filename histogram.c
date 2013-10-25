#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "string.h"
#include "sys/stat.h"
#include "cmapreduce.h"
#define round(x) ((x)>=0?(long)((x)+0.5):(long)((x)-0.5))

void fileread(int, void *, void *);
void newMap(int itask, char *str, int size, void *kv, void *ptr);
void newMap2(int itask, char *str, int size, void *kv, void *ptr);
void binMap(int, void *, void *);
void sum(char *, int, char *, int, int *, void *, void *);
int ncompare(char *, int, char *, int);
void m_prepareoutput(uint64_t, char *, int, char *, int, void *, void *);
void output(uint64_t, char *, int, char *, int , void *, void *);

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

  void *mr = (void *) MR_create(MPI_COMM_WORLD);
  //MR_set_verbosity(mr, 2);
  MPI_Barrier(MPI_COMM_WORLD);
  int test = 1;
  //nwords = MR_map(mr, narg - 1, &binMap, &args[1]);
  char *arg[2] = {args[1],args[2]};
  nwords = MR_map_file_char(mr, 2, 1, arg, 0, 0, ' ', 100, &newMap, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  MR_collate(mr, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  nunique = MR_reduce(mr, &sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
//  MR_gather(mr, 1);
  MR_map_mr(mr, mr, m_prepareoutput, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  MR_gather(mr, 1);
  MPI_Barrier(MPI_COMM_WORLD);
//  MR_collate(mr, NULL);
//  MPI_Barrier(MPI_COMM_WORLD);
//  nunique = MR_reduce(mr, &sum, NULL);
  printf("%d words\n%d unique\n", nwords, nunique);
  //MR_map_mr(mr, mr, &output, NULL);
  //MPI_Barrier(MPI_COMM_WORLD);
  //MR_sort_values(mr,&ncompare);
  MR_map_mr(mr, mr , &output, NULL);
  //MR_gather(mr,1);
  //MR_sort_values(mr,&ncompare);
  MR_destroy(mr);
  MPI_Finalize();
}

void newMap2(int itask, char *str, int size, void *kv, void *ptr)
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
    MR_kv_add(kv, &i, sizeof(int), NULL, 0);
    word = strtok(NULL, whitespace);
  }
}
void newMap(int itask, char *str, int size, void *kv, void *ptr)
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
    MR_kv_add(kv, &i, sizeof(int), NULL, 0);
    word = strtok(NULL, whitespace);
  }
}

void binMap(int itask, void *kv, void *ptr)
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
    MR_kv_add(kv, key, sizeof(i), NULL, 0);
    //printf("%d\n",i );
    word = strtok(NULL, whitespace);
  }
}



// TODO: reduce function needs to add things up
//reduce function
void sum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, void *kv, void *ptr)
{
  MR_kv_add(kv, key, keybytes, (char *) &nvalues, sizeof(int));
  printf("%s %d\n", key, nvalues);
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
    return -1;
  }
  else if (i1 < i2)
  {
    return 1;
  }
  else
  {
    return 0;
  }
}

void m_prepareoutput(uint64_t itask, char *key, int keybytes, char *value,
                     int valuebytes, void *kv, void *ptr)
{
  int n = *(int *) value;
  MR_kv_add(kv, key, keybytes, (char *) &n, sizeof(int));
}

void output(uint64_t itask, char *key, int keybytes, char *value,
            int valuebytes, void *kv, void *ptr)
{
  int n = *(int *) value;
  printf("key:%i  value:%i\n", *(int *) key, n);
}
