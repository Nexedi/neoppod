#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>

#include "neo_struct.h"

int
generate_tid (unsigned char ntid[ID_LEN], unsigned char ltid[ID_LEN])
{
  struct timeval tv;
  struct tm tm;
  u_int32_t v;
  double sec;

  /* generate time */
  if (gettimeofday (&tv, 0) < 0)
    return 0;

  if (! gmtime_r (&tv.tv_sec, &tm))
    return 0;

  /* create an integer array */
  v =
    ((((tm.tm_year) * 12 + tm.tm_mon) * 31 +
      tm.tm_mday - 1) * 24 + tm.tm_hour) * 60 + tm.tm_min;
  v = htonl (v);
  memcpy (ntid, &v, 4);
  sec = (tv.tv_sec % 60) + ((double) tv.tv_usec / 1000000);
  sec /= ((double) 60) / ((double) (1 << 16)) / ((double) (1 << 16));
  v = (u_int32_t) sec;
  v = htonl (v);
  memcpy (ntid + 4, &v, 4);

  while (memcmp (ntid, ltid, ID_LEN) <= 0)
    {
      int i;

      for (i = ID_LEN - 1; i > 3; i--)
        {
          if (ntid[i] == 255)
            ntid[i] = 0;
          else
            {         
              ntid[i]++;
              continue;
            }
        }

      return 0;
    }

  return 1;
}
