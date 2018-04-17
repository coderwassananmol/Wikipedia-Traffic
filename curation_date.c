/* Description: This file is used for downloading missing dates. (NO MPI INVOLVED HERE)
  * gcc CURATE_DAY.c -o CURATE_DAY -std=c99
  * ./CURATE_DAY
*/

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

int cmd;

void HDFS()
{
  printf("Moving from Hadoop to HDFS......\n");
  cmd = system("hdfs dfs -put /tmp/WikiData /user/valdiv_n1/Wiki_Data/");  //move data to HDFS

  printf("Removing Data from Hadoop......\n");
  cmd = system("rm -r /tmp/WikiData");
}

void workernode(int mon, int day, int st_hr, int end_hr)
{
  int cmd, start, end, i, j;
  char date[9];
  char year[4];
  char month[2];
  time_t t = time(NULL);
  struct tm * ptm = gmtime(&t);

  char url_day[82];
  char gz_url[100];
  char make_dir[28];
  char wget[122];

  cmd = system("mkdir /tmp/WikiData");  //Create WikiData directory

  ptm->tm_year = 0163;
  ptm->tm_mon = mon-1;
  ptm->tm_mday = day;
  mktime(ptm);
  strftime(date, 9, "%Y%m%d", ptm);
  memcpy(&year, &date[0], 4);
  year[4] = '\0';
  memcpy(&month, &date[4], 2);
  month[2] = '\0';

  snprintf(url_day, 82, "https://dumps.wikimedia.org/other/pagecounts-raw/%s/%s-%s/pagecounts-%s", year, year, month, date);
  snprintf(make_dir, 28, "mkdir /tmp/WikiData/%s-%s", year, month);

  cmd = system(make_dir);

  for (j = st_hr; j <= end_hr; j++) {
    if (j < 10) {
      snprintf(gz_url, 100, "%s-0%d0000.gz", url_day, j);
    }
    else {
      snprintf(gz_url, 100, "%s-%d0000.gz", url_day, j);
    }

    snprintf(wget, 122, "wget %s -P /tmp/WikiData/%s-%s", gz_url, year, month);

    cmd = system(wget);
  }
}

int main(int argc, char** argv)
{
  printf("Start!\n");
  workernode(5, 1, 0, 23);
  HDFS();

}
