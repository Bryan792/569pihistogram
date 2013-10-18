#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "stdint.h"
#include "string.h"
#include "sys/stat.h"
#include "cmapreduce.h"
#define round(x) ((x)>=0?(long)((x)+0.5):(long)((x)-0.5))

void fileread(int, void *, void *);
void binMap(int, void *, void *);
void sum(char *, int, char *, int, int *, void *, void *);
int ncompare(char *, int, char *, int);
void output(int, char *, int, char *, int, void *, void *);

typedef struct
{
  int n, limit, flag;
} Count;

/* ---------------------------------------------------------------------- */

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
  nwords = MR_map(mr, narg - 1, &binMap, &args[1]);
  MR_collate(mr, NULL);
  nunique = MR_reduce(mr, &sum, NULL);
  MPI_Barrier(MPI_COMM_WORLD);
  printf("%d words\n%d unique\n", nwords, nunique);
  count.n = 0;
  count.limit = 10;
  count.flag = 0;
  MR_map_mr(mr, mr, &output, &count);
  MPI_Barrier(MPI_COMM_WORLD);
  /*MR_sort_values(mr,&ncompare);

  count.n = 0;
  count.limit = 10;
  count.flag = 0;
  MR_map(mr,&output,&count);

  MR_gather(mr,1);
  MR_sort_values(mr,&ncompare);

  count.n = 0;
  count.limit = 10;
  count.flag = 1;
  MR_map(mr,&output,&count);
  */ MR_destroy(mr);
  MPI_Finalize();
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

  while (word)
  {
    f = strtof(word, NULL);
    f = (int)((f + 10) / .5);
    i = (int) f;
    MR_kv_add(kv, &i, sizeof(i), NULL, 0);
    //printf("%d\n",i );
    word = strtok(NULL, whitespace);
  }
}

void sum(char *key, int keybytes, char *multivalue,
         int nvalues, int *valuebytes, void *kv, void *ptr)
{
  MR_kv_add(kv, key, keybytes, (char *) &nvalues, sizeof(int));
  printf("%c %d\n",key[0],nvalues);
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

void output(int itask, char *key, int keybytes, char *value,
            int valuebytes, void *kv, void *ptr)
{
  int n = *(int *) value;
  printf("%s \n", key);
  /*Count *count = (Count *) ptr;
  count->n++;

  if (count->n > count->limit)
  {
    return;
  }

  int n = *(int *) value;

  if (count->flag)
  {
    printf("%d %s\n", n, key);
  }
  else
  {
    MR_kv_add(kv, key, keybytes, (char *) &n, sizeof(int));
  }*/
}
