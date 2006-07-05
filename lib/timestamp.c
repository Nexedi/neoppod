#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "neo_struct.h"



/* int */
/* inc_tid (u_int16_t tid[ID_LEN], int pos) */
/* { */
/* /\*   printf ("inc_tid : pos %d, %d\n", pos, tid[pos]); *\/ */
/*   tid[pos]++; */
/* /\*   printf ("inc %d\n", tid[pos]); *\/ */

/*   if (tid[pos] > 255) */
/*     { */
/*       tid[pos] = 0; */
/*       if (pos >= 0) */
/*         inc_tid (tid, pos - 1); */
/*       else */
/*         { */
/*           printf ("no more tid left to generate\n"); */
/*           return 0; */
/*         } */
/*     } */
/*   return 1; */
/* } */


int
generate_tid (u_int16_t ntid[ID_LEN], u_int16_t ltid[ID_LEN])
{
  int i;
  time_t epoch_time;
  struct tm *gm_time;
  u_int32_t v;
  double sconv = 0;
  unsigned char new_tid[ID_LEN], last_tid[ID_LEN];

  /* generate time */
  time (&epoch_time);
  gm_time = (struct tm *) malloc (sizeof (struct tm));
  gm_time = gmtime (&epoch_time);

  /* create an integer array */
  v =
    ((((gm_time->tm_year) * 12 + gm_time->tm_mon) * 31 +
      gm_time->tm_mday - 1) * 24 + gm_time->tm_hour) * 60 + gm_time->tm_min;
  ntid[0] = v / 16777216;
  ntid[1] = (v % 16777216) / 65536;
  ntid[2] = (v % 65536) / 256;
  ntid[3] = v % 256;
  gm_time->tm_sec /= sconv;
  v = (u_int32_t) epoch_time % 60;
  ntid[4] = v / 16777216;
  ntid[5] = (v % 16777216) / 65536;
  ntid[6] = (v % 65536) / 256;
  ntid[7] = v % 256;

 compare:
  /* create the string array */
  for (i = 0; i < ID_LEN; i++)
    {
      new_tid[i] = (unsigned char)ntid[i];
      last_tid[i] = (unsigned char)ltid[i];
    }

  if (memcmp (new_tid, last_tid, ID_LEN) > 0)
    return 1;

  for (i = ID_LEN - 1; i > 3; i--)
    {
      if (ntid[i] == 255)
        ntid[i] = 0;
      else
        {         
          ntid[i]++;
          goto compare;
        }
    }

  printf ("oups must finish compare function...\n");
  return 0;
}
