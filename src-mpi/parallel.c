/// \file
/// Wrappers for MPI functions.  This should be the only compilation 
/// unit in the code that directly calls MPI functions.  To build a pure
/// serial version of the code with no MPI, do not define DO_MPI.  If
/// DO_MPI is not defined then all MPI functionality is replaced with
/// equivalent single task behavior.

#include "parallel.h"

#ifdef DO_MPI
#include <shmem.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>

static int myRank = 0;
static int nRanks = 1;
//#ifdef DO_MPI
//static MPI_Request* requestList;
//#endif
static int* rUsed;
static int reqCount = 0;

#ifdef DO_MPI
#ifdef SINGLE
#define REAL_MPI_TYPE MPI_FLOAT
#else
#define REAL_MPI_TYPE MPI_DOUBLE
#endif

#endif

int getNRanks()
{
   return nRanks;
}

int getMyRank()   
{
   return myRank;
}

/// \details
/// For now this is just a check for rank 0 but in principle it could be
/// more complex.  It is also possible to suppress practically all
/// output by causing this function to return 0 for all ranks.
int printRank()
{
   if (myRank == 0) return 1;
   return 0;
}

void timestampBarrier(const char* msg)
{
   barrierParallel();
   if (! printRank())
      return;
   time_t t= time(NULL);
   char* timeString = ctime(&t);
   timeString[24] = '\0'; // clobber newline
   fprintf(screenOut, "%s: %s\n", timeString, msg);
   fflush(screenOut);
}

void initParallel(int* argc, char*** argv)
{
#ifdef DO_MPI
   shmem_init();
   myRank = shmem_my_pe();
   nRanks = shmem_n_pes();
//    MPI_Init(argc, argv);
//   MPI_Comm_rank(MPI_COMM_WORLD, &myRank);
//   MPI_Comm_size(MPI_COMM_WORLD, &nRanks);

//   requestList = (MPI_Request*) malloc(nRanks*sizeof(MPI_Request));
//   rUsed = (int*) malloc(nRanks*sizeof(int));
//   for (int i = 0; i < nRanks; i++) { rUsed[i] = 0; }
#endif
}

void destroyParallel()
{
#ifdef DO_MPI
//   free(requestList);

   shmem_finalize(); //MPI_Finalize();
#endif
}

#ifdef DO_MPI
int saveRequest(MPI_Request req)
{
  for (int i = 0; i < nRanks; i++)
  {
    if (rUsed[i] == 0)
    {
//      requestList[i] = req;
//      rUsed[i] = 1;
      return i;
    } 
  }
}
#endif

void barrierParallel()
{
#ifdef DO_MPI
   shmem_barrier_all(); //MPI_Barrier(MPI_COMM_WORLD);
#endif
}

/// \param [in]  sendBuf Data to send.
/// \param [in]  sendLen Number of bytes to send.
/// \param [in]  dest    Rank in MPI_COMM_WORLD where data will be sent.
/// \param [out] recvBuf Received data.
/// \param [in]  recvLen Maximum number of bytes to receive.
/// \param [in]  source  Rank in MPI_COMM_WORLD from which to receive.
/// \return Number of bytes received.
int sendReceiveParallel(void* sendBuf, int sendLen, int dest,
                        void* recvBuf, int recvLen, int source)
{
#ifdef DO_MPI
   int bytesReceived;
  // MPI_Status status;
  // MPI_Sendrecv(sendBuf, sendLen, MPI_BYTE, dest,   0,
  //              recvBuf, recvLen, MPI_BYTE, source, 0,
  //              MPI_COMM_WORLD, &status);
  // MPI_Get_count(&status, MPI_BYTE, &bytesReceived);

   shmem_putmem(recvBuf, sendBuf, sendLen, dest);
   if (sendLen ==recvLen){
       return sendLen;
   }else{
       return sendLen;
   }
   //return bytesReceived;
#else
   assert(source == dest);
   memcpy(recvBuf, sendBuf, sendLen);

   return sendLen;
#endif
}

/// \details
/// Send to another processor.
/// \param [in]  sendBuf Data to send.
/// \param [in]  sendLen Number of bytes to send.
/// \param [in]  dest    Rank in MPI_COMM_WORLD where data will be sent.
int isendParallel(void* sendBuf, int sendLen, int dest)
{
#ifdef DO_MPI
  MPI_Request request;
  MPI_Isend(sendBuf, sendLen, MPI_BYTE,
        dest, 0, MPI_COMM_WORLD, &request);

  //int rind = saveRequest(request);  

  //return rind;
  return sendLen;
#else
  return 0; 
#endif
}

int nbput_Parallel(void *sendBuf, void *RecvBuf, int SendLen, int dest){
    shmem_putmem_nbi(RecvBuf, sendBuf, SendLen, dest);
    return SendLen;
}

int put_Parallel(void *sendBuf, void *RecvBuf, int SendLen, int dest){
    shmem_putmem(RecvBuf, sendBuf, SendLen, dest);
    return SendLen;
}
/// \details
/// Send to another processor.
/// \param [in]  sendBuf Data to send.
/// \param [in]  sendLen Number of bytes to send.
/// \param [in]  dest    Rank in MPI_COMM_WORLD where data will be sent.
int sendParallel(void* sendBuf, int sendLen, int dest)
{
#ifdef DO_MPI
  MPI_Send(sendBuf, sendLen, MPI_BYTE,
    dest, 0, MPI_COMM_WORLD);

  return sendLen;
#else
  return sendLen;
#endif
}

/// \details
/// Receive from any processor.
/// \param [out] recvBuf Received data.
/// \param [in]  recvLen Maximum number of bytes to receive.
/*int recvAnyParallel(void* recvBuf, int recvLen)
{
#ifdef DO_MPI
  int bytesReceived;
  MPI_Status status;
  MPI_Recv(recvBuf, recvLen, MPI_BYTE,
        MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_BYTE, &bytesReceived);

  //if (printRank()) printf("from %d\n", status.MPI_SOURCE);

  return bytesReceived;
#else
  return recvLen;
#endif
}*/

/// \details
/// Receive from any processor.
/// \param [out] recvBuf Received data.
/// \param [in]  recvLen Maximum number of bytes to receive.
int irecvAnyParallel(void* recvBuf, int recvLen)
{
#ifdef DO_MPI
  MPI_Request request;
  MPI_Irecv(recvBuf, recvLen, MPI_BYTE,
    MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &request);

  int rind = saveRequest(request);

  return rind;
#else
  return 0;
#endif
}

int wait(){
    shmem_quiet();
}

int waitIrecv(int rind)
{
#ifdef DO_MPI
//  MPI_Status status;
  int bytesReceived;

//  MPI_Wait(&requestList[rind], &status);
//  MPI_Get_count(&status, MPI_BYTE, &bytesReceived); 

  rUsed[rind] = 0;

  return bytesReceived;
#endif
}

int testIrecv(int rind)
{
#ifdef DO_MPI
//  MPI_Status status;
  int bytesReceived;
  int flag;

//  MPI_Test(&requestList[rind], &flag, &status);
  if (flag > 0)
  {
//    MPI_Get_count(&status, MPI_BYTE, &bytesReceived);
//    rUsed[rind] = 0;
    return bytesReceived;
  }

  return -1;
#endif
}

int waitIsend(int rind)
{
#ifdef DO_MPI
//  MPI_Status status;

 // MPI_Wait(&requestList[rind], &status);
  
  rUsed[rind] = 0;

  return 1;
#endif
}

int testIsend(int rind)
{
#ifdef DO_MPI
  //MPI_Status status;
  int flag;

 // MPI_Test(&requestList[rind], &flag, &status);
  if (flag > 0)
  {
 //   rUsed[rind] = 0;

    return 1;
  }

  return -1;
#endif
}

/// \details
/// Receive from any processor.
/// \param [out] recvBuf Received data.
/// \param [in]  recvLen Maximum number of bytes to receive.
/// \param [in]  src MPI rank message source
int recvParallel(void* recvBuf, int recvLen, int source)
{
#ifdef DO_MPI
  int bytesReceived;
  MPI_Status status;
  MPI_Recv(recvBuf, recvLen, MPI_BYTE,
    source, 0, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_BYTE, &bytesReceived);

  //if (printRank()) printf("from %d\n", status.MPI_SOURCE);

  return bytesReceived;
#else
  return recvLen;
#endif
}

int irecvParallel(void* recvBuf, int recvLen, int source)
{
#ifdef DO_MPI
  int bytesReceived;
  MPI_Request request;
  MPI_Irecv(recvBuf, recvLen, MPI_BYTE,
    source, 0, MPI_COMM_WORLD, &request);

  int rind = saveRequest(request);
  return rind;
#else
  return 0;
#endif
}

void addIntParallel(int* sendBuf, int* recvBuf, int count)
{
#ifdef DO_MPI
    shmem_int_sum_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count);
   //MPI_Allreduce(sendBuf, recvBuf, count, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void addRealParallel(real_t* sendBuf, real_t* recvBuf, int count)
{
#ifdef DO_MPI
   shmem_double_sum_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count);
//   MPI_Allreduce(sendBuf, recvBuf, count, REAL_MPI_TYPE, MPI_SUM, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void addDoubleParallel(double* sendBuf, double* recvBuf, int count)
{
#ifdef DO_MPI
    shmem_double_sum_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count);
   //MPI_Allreduce(sendBuf, recvBuf, count, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void maxIntParallel(int* sendBuf, int* recvBuf, int count)
{
#ifdef DO_MPI
    shmem_int_max_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count);
//   MPI_Allreduce(sendBuf, recvBuf, count, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void maxRealParallel(real_t* sendBuf, real_t* recvBuf, int count)
{
#ifdef DO_MPI
   shmem_double_max_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count); 
//   MPI_Allreduce(sendBuf, recvBuf, count, REAL_MPI_TYPE, MPI_MAX, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void minRealParallel(real_t* sendBuf, real_t* recvBuf, int count)
{
#ifdef DO_MPI
   shmem_double_min_reduce(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count); 
//   MPI_Allreduce(sendBuf, recvBuf, count, REAL_MPI_TYPE, MPI_MIN, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
      recvBuf[ii] = sendBuf[ii];
#endif
}

void minRankDoubleParallel(RankReduceData* sendBuf, RankReduceData* recvBuf, int count)
{
#ifdef DO_MPI
 //  shmemx_double_minloc(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count); /* TODO:  */
 //  UTILIZE MINMAXLOC IN FURTHER INTEGRATION FOR SHMEM REDUCTION OPERATIONS AS
 //  EXPERIMENTAL
//   MPI_Allreduce(sendBuf, recvBuf, count, MPI_DOUBLE_INT, MPI_MINLOC, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
   {
      recvBuf[ii].val = sendBuf[ii].val;
      recvBuf[ii].rank = sendBuf[ii].rank;
   }
#endif
}

void maxRankDoubleParallel(RankReduceData* sendBuf, RankReduceData* recvBuf, int count)
{
#ifdef DO_MPI
   //shmemx_double_maxloc(SHMEM_TEAM_WORLD, recvBuf, sendBuf, count); 
//   MPI_Allreduce(sendBuf, recvBuf, count, MPI_DOUBLE_INT, MPI_MAXLOC, MPI_COMM_WORLD);
#else
   for (int ii=0; ii<count; ++ii)
   {
      recvBuf[ii].val = sendBuf[ii].val;
      recvBuf[ii].rank = sendBuf[ii].rank;
   }
#endif
}

real_t rsLocal[2], rsGlobal[2];
double dsLocal[2], dsGlobal[2];
int    isLocal[2], isGlobal[2];

void minRealReduce(real_t* value)
{
 //  real_t sLocal[1], sGlobal[1];

   rsLocal[0] = *value;

   minRealParallel(rsLocal, rsGlobal, 1);

   *value = rsGlobal[0];
}

void maxRealReduce(real_t* value)
{
//   real_t sLocal[1], sGlobal[1];

   rsLocal[0] = *value;

   maxRealParallel(rsLocal, rsGlobal, 1);

   *value = sGlobal[0];
}

void maxIntReduce2(int* value0, int* value1)
{
//   int isLocal[2], isGlobal[2];

   isLocal[0] = *value0;
   isLocal[1] = *value1;

   maxIntParallel(isLocal, isGlobal, 2);

   *value0 = isGlobal[0];
   *value1 = isGlobal[1];
}

void addIntReduce2(int* value0, int* value1)
{
  // int sLocal[2], sGlobal[2];

   isLocal[0] = *value0;
   isLocal[1] = *value1;

   addIntParallel(isLocal, isGlobal, 2);

   *value0 = isGlobal[0];
   *value1 = isGlobal[1];
}

void addRealReduce2(real_t* value0, real_t* value1)
{
 //  real_t *sLocal = shmem_malloc(2 * sizeof(real_t)), 
 //         *sGlobal = shmem_malloc(2 * sizeof(real_t));

   rsLocal[0] = *value0;
   rsLocal[1] = *value1;

   addRealParallel(rsLocal, rsGlobal, 2);

   *value0 = rsGlobal[0];
   *value1 = rsGlobal[1];
//   shmem_free(sLocal);
//   shmem_free(sLocal);
}

/// \param [in] count Length of buf in bytes.
void bcastParallel(void* buf, int count, int root)
{
#ifdef DO_MPI
   shmem_broadcastmem(SHMEM_TEAM_WORLD, buf, buf, count, root); 
//   MPI_Bcast(buf, count, MPI_BYTE, root, MPI_COMM_WORLD);
#endif
}

int builtWithMpi(void)
{
#ifdef DO_MPI
   return 1;
#else
   return 0;
#endif
}


