#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>
#include <string.h>
#include <assert.h>

#ifndef THREADQUERY_H
#define THREADQUERY_H

double doubletime()
{
	struct timeval t;
	double retval;

	// Get seconds and microseconds since the Epoch in
	// the provided struct timeval.
	gettimeofday(&t, NULL);
	retval = t.tv_sec;
	retval += t.tv_usec / 1000000.0;
	return retval;
}
//-----------------------------------------------------------------------------
// Parallel data structure
//-----------------------------------------------------------------------------
typedef struct {
    int ID;   
    int flag; 
} Par_Data_t;
//-----------------------------------------------------------------------------
// Work Pool data structure
//-----------------------------------------------------------------------------
typedef struct {
    char * * Queue;
    int idx_txtAvailable;
    int len;
} WorkQueue_t;

#endif
