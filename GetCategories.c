/*
 Explanation:
          This is the part of Job 1 in our entire final project.
          This file contains the script of the data movement from HDFS to hadoop2 to 219 machines,
          plus having each slave node to call the extern function MultiThreadQuery from thread-wiki.c

 Compile instrucitons:
  * mpicc -c GetCategories.c -o GetCategories.o
  * gcc -c thread_wiki.c -o thread_wiki.o -std=c11 -lpthread
  * gcc -c query_wiki.c -o query_wiki.o -std=c11 -lcurl
  * mpicc -o QUERY GetCategories.o thread_wiki.o query_wiki.o -lpthread -lcurl
  * mpiexec -f hosts -n 12 ./QUERY
*/

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <mpi.h>

#define DATA_MSG 0
#define NEWDATA_MSG 1

int nnodes;
int me; 
int recv_buff[3];
int cmd;

void MultiThreadQuery(char * directory, int ThreadNum, int rank);

void init(int argc, char **argv) {
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nnodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &me);
}

void Manger_DistributeWork() {
  MPI_Status status;
  const char * a[nnodes-1];
  const char * dirs[nnodes-1];

  a[0]="219a";
  a[1]="219b";
  a[2]="219c";
  a[3]="219d";
  a[4]="219e";
  a[5]="219f";
  a[6]="219g";
  a[7]="219h";
  a[8]="219i";
  a[9]="219j";
  a[10]="219k";

  dirs[0]="2015-01";
  dirs[1]="2015-02";
  dirs[2]="2015-03";
  dirs[3]="2015-04";
  dirs[4]="2015-06";
  dirs[5]="2015-07";
  dirs[6]="2015-08";
  dirs[7]="2015-09";
  dirs[8]="2015-10";
  dirs[9]="2015-11";
  dirs[10]="2015-12";
  int i;
  char getThisMonth[63];
  char makeDir[30];
  char makeOutputDir[35];
  char makeOutputMon[42];
  char MasterToslaveTmp[48];
  char rmSlaveTmpData[29];

  printf("Hello, I am hadoop2. I am making a directory in Tmp.....\n");
  cmd = system("mkdir /tmp/ThisMonth");

  for (i = 1; i < nnodes-1; i++)
  {
    printf("Hadoop2: Getting %s from HDFS......\n", dirs[i]);
    snprintf(getThisMonth, 63, "hdfs dfs -get /user/valdiv_n1/cleanWiki/%s /tmp/ThisMonth", dirs[i]);
    cmd = system(getThisMonth); 
    printf("%s: Creating Dirs in tmp......\n", a[i]);
    snprintf(makeDir, 30, "ssh %s 'mkdir /tmp/MyMonth'" , a[i]);  
    snprintf(makeOutputDir, 35, "ssh %s 'mkdir /tmp/MyMonth_out'" , a[i]); 
    snprintf(makeOutputMon, 42, "ssh %s 'mkdir /tmp/MyMonth_out/%s'" , a[i], dirs[i]);
    cmd = system(makeDir);    
    cmd = system(makeOutputDir);    
    cmd = system(makeOutputMon);    
    printf("Hadoop2: Sending data to %s......\n", a[i]);
    snprintf(MasterToslaveTmp, 48, "scp -r /tmp/ThisMonth/%s %s:/tmp/MyMonth/",dirs[i], a[i]);
    cmd = system(MasterToslaveTmp);

    printf("Hadoop2: Copy finished! Now remove tmp data......\n");
    snprintf(rmSlaveTmpData, 29, "rm -r /tmp/ThisMonth/%s", dirs[i]);
    cmd = system(rmSlaveTmpData);  //move data to hadoop2
    //get Month for slave i ito hadoop2's tmp
  }
  cmd = system("rm -r /tmp/ThisMonth"); //delete this project directory from hadoop2
}

void MasterPutHDFS() {
  MPI_Status status;
  char slaveToMaster[55];
  char rmSlaveTmpData[35];
  char cmdToHDFS[100];
  char rmhadoop[100];

  const char * a[nnodes-1];
  const char * dirs[nnodes-1];

  a[0]="219a";
  a[1]="219b";
  a[2]="219c";
  a[3]="219d";
  a[4]="219e";
  a[5]="219f";
  a[6]="219g";
  a[7]="219h";
  a[8]="219i";
  a[9]="219j";
  a[10]="219k";

  dirs[0]="2015-01";
  dirs[1]="2015-02";
  dirs[2]="2015-03";
  dirs[3]="2015-04";
  dirs[4]="2015-06";
  dirs[5]="2015-07";
  dirs[6]="2015-08";
  dirs[7]="2015-09";
  dirs[8]="2015-10";
  dirs[9]="2015-11";
  dirs[10]="2015-12";

  int i;
  cmd = system("mkdir /tmp/QueriedData"); //make a directory in hadoop2, where all operations happen here
  //for loop from 1-16 missing 17
  for (i = 0; i < nnodes-1; i++) {
    snprintf(slaveToMaster, 55, "scp -r %s:/tmp/MyMonth_out/%s /tmp/QueriedData", a[i], dirs[i]);
    snprintf(rmSlaveTmpData, 35, "ssh %s 'rm -r /tmp/MyMonth_out/'", a[i]); '

    printf("HELLOO this is %s.... Moving data to Hadoop2\n", a[i]);
    cmd = system(slaveToMaster);    //copy data to Hadoop2

    printf("Removing from Slave......\n");
    cmd = system(rmSlaveTmpData);   //erase data from slave

    printf("Moving from Hadoop to HDFS......\n");
    snprintf(cmdToHDFS, 100, "hdfs dfs -put /tmp/QueriedData/%s ./Wiki_Data/", dirs[i]);
    cmd = system(cmdToHDFS);  //move data to HDFS

    printf("Removing Data from Hadoop......\n");
    snprintf(rmhadoop, 100, "rm -r /tmp/QueriedData/%s", dirs[i]);
    cmd = system(rmhadoop);  //move data to HDFS

  }
  cmd = system("rm -r /tmp/QueriedData"); //delete this project directory from hadoop2
}

void workernode()
{
  MPI_Status status;

  char * dirs[11];
  dirs[0]="2015-01";
  dirs[1]="2015-02";
  dirs[2]="2015-03";
  dirs[3]="2015-04";
  dirs[4]="2015-06";
  dirs[5]="2015-07";
  dirs[6]="2015-08";
  dirs[7]="2015-09";
  dirs[8]="2015-10";
  dirs[9]="2015-11";
  dirs[10]="2015-12";
  printf("Wokred node %d: Quering Wikipedia......\n", me);
  MultiThreadQuery(dirs[me-1], 8, me);
}

int main(int argc, char** argv)
{
  int i;
  init(argc, argv);

  if (me != 0) {
    workernode();
  }
  MPI_Barrier(MPI_COMM_WORLD);
  printf("upto here BROOOOOOOOOOOOOOOO! %d\n", me);
  /* Get files into Hadoop and put them into HDFS*/
  if (me == 0) {
    MasterPutHDFS();
  }

  MPI_Finalize();
}
