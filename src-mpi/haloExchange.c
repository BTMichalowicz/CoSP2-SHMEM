/// \file
/// Communicate halo data such as "ghost" rows with neighboring tasks.

#include "haloExchange.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <shmem.h>
#include "sparseMatrix.h"
#include "decomposition.h"
#include "parallel.h"
#include "performance.h"
#include "constants.h"
#include "mytype.h"


#define dbgben 1

#if dbgben
#define DEBUG_BEN(fmt, args...)                         \
    do {                                                \
        int rank = getMyRank();                         \
        fflush(stdout); fflush(stderr);                 \
        fprintf(stdout, "[rank_%d][%s][%s:%d] "fmt,     \
                rank, __FILE__, __func__, __LINE__,     \
                ##args);                                \
        fflush(stdout); fflush(stderr);                 \
    } while(0);
#else
#define DEBUG_BEN(...)
#endif

/// A structure to package data for a single row to pack into a
/// send/recv buffer.
typedef struct NonZeroMsgSt
{
   int irow;
   int icol;
   real_t val;
}
NonZeroMsg;

/// \details
HaloExchange* initHaloExchange(struct DomainSt* domain)
{
   HaloExchange* hh = (HaloExchange*)malloc(sizeof(HaloExchange));
   
   hh->maxHalo = domain->totalProcs;

   hh->haloCount = 0;
   hh->haloProc = (int*)malloc(hh->maxHalo*sizeof(int));

   hh->bufferSize = domain->localRowExtent * domain->totalCols * (
                    2 * sizeof(int) + sizeof(real_t)); // row, col, value

   if (printRank() && debug == 1)
     printf("bufferSize = %d\n", hh->bufferSize);

   hh->sendBuf = (char*)shmem_malloc((hh->bufferSize*2)*sizeof(char));
   hh->recvBuf = (char**)malloc(getNRanks()*sizeof(char*));
   for (int i = 0; i < getNRanks(); i++)
   {
     hh->recvBuf[i] = (char*)shmem_malloc((hh->bufferSize+8)*sizeof(char));
   }

   return hh;
}

/// \details
void destroyHaloExchange(struct HaloExchangeSt* haloExchange)
{
  free(haloExchange->haloProc);
  shmem_free(haloExchange->sendBuf);

  for (int i = 0; i < getNRanks(); i++)
  {
    shmem_free(haloExchange->recvBuf[i]);
  }
  free(haloExchange->recvBuf);

  free(haloExchange);
}

/// Setup for data exchange - post non-blocking reads
void exchangeSetup(struct HaloExchangeSt* haloExchange, struct SparseMatrixSt* spmatrix, struct DomainSt* domain)
{
  // Update halo processors for matrix
  updateData(haloExchange, spmatrix, domain);

  // Post receives from halo processors and
  // Send local row to halo processors
//  if (haloExchange->haloCount > 0)
//  {
    // Post non-blocking receives
//    haloExchange->rlist = (int*)malloc(haloExchange->haloCount*sizeof(int));
//    for (int i = 0; i < haloExchange->haloCount; i++)
//    {
//      haloExchange->rlist[i] = irecvAnyParallel(haloExchange->recvBuf[i], haloExchange->bufferSize);
//    }
//  }
}    

/// This is the function that does the heavy lifting for the
/// communication of halo data.
void exchangeData(struct HaloExchangeSt* haloExchange, struct SparseMatrixSt* spmatrix, struct DomainSt* domain)
{
  if (haloExchange->haloCount > 0)
  {
    // Send local rows to each halo processor
    int nSendLen = loadBuffer(haloExchange->sendBuf, spmatrix, domain);
    for (int i = 0; i < haloExchange->haloCount; i++)
    {
        DEBUG_BEN("halo %d block put of buffer %p to recvBuf %p of len %d to proc %d\n",
                i, haloExchange->sendBuf, haloExchange->recvBuf[i], nSendLen, haloExchange->haloProc[i]);
      int nSend = put_Parallel(haloExchange->sendBuf, haloExchange->recvBuf[i], nSendLen, haloExchange->haloProc[i]);
      collectCounter(sendCounter, nSendLen);
    }

    // Receive remote rows from each halo processor
    for (int i = 0; i < haloExchange->haloCount; i++)
    {
      int nRecv = nSendLen; //waitIrecv(haloExchange->rlist[i]);
      
     DEBUG_BEN("halo %d unloading of buffer %p to spMatrix %p of len %d to domain %p\n",
                i, haloExchange->recvBuf[i], spmatrix, nSendLen, domain);
      unloadBuffer(haloExchange->recvBuf[i], nRecv, spmatrix, domain); 
      collectCounter(recvCounter, nRecv);
    }
  }

}

/// \details
/// Determine processors in halo
void updateData(struct HaloExchangeSt* haloExchange, struct SparseMatrixSt* spmatrix, struct DomainSt* domain)
{
  haloExchange->haloCount = 0;

  for (int i = domain->localRowMin; i < domain->localRowMax; i++)
  {
    for (int j = 0; j < spmatrix->iia[i]; j++)
    {
      int rnum = spmatrix->jja[i][j];
      if (rnum < domain->localRowMin ||
          rnum >= domain->localRowMax)
      {
        int rowProc = processorNum(domain, rnum);
        addHaloProc(haloExchange, rowProc);
      }
    }
  }
}

/// \details
/// Gather sparse matrix data to processor 0
void gatherData(struct HaloExchangeSt* haloExchange, struct SparseMatrixSt* spmatrix, struct DomainSt* domain)
{
  int myRank = getMyRank();

  // If rank 0, read all blocks that have not been received as halos
  int *nRecv = shmem_malloc(sizeof(int));
  int *nSend = shmem_malloc(sizeof(int));
  if (myRank != 0){
      if (!(isHaloProc(haloExchange, 0))){
          int nSendLen = loadBuffer(haloExchange->sendBuf, spmatrix, domain);
          *nSend = nSendLen;
          if (myRank == 1)
              put_Parallel(nRecv,nSend, sizeof(int), 0);
          put_Parallel(haloExchange->sendBuf, haloExchange->recvBuf[myRank], nSendLen, 0);
          collectCounter(sendCounter, nSendLen);
      }
  }

  barrierParallel();

  if (myRank == 0){
      int ir = 0;
      int i = 1;
      int nranks = getNRanks();
      for (; i<nranks; i++){
          if (!isHaloProc(haloExchange, i)){
              ir++;
          }
      }
      for (i = 0 ; i< ir; i++){
          unloadBuffer(haloExchange->recvBuf[i], *nRecv, spmatrix, domain);
          collectCounter(recvCounter, *nRecv);
      }
  }
  shmem_free(nRecv);
  shmem_free(nSend);

      
/*      

  if (myRank == 0)
  {
    //free(haloExchange->rlist);
//    haloExchange->rlist = (int*)malloc((getNRanks() - haloExchange->haloCount)*sizeof(int));
    int ir = 0;
    for (int i = 1; i < getNRanks(); i++)
    {
      if (!isHaloProc(haloExchange, i))
      {
        //haloExchange
        //haloExchange->rlist[ir] = irecvAnyParallel(haloExchange->recvBuf[ir], haloExchange->bufferSize);
        nb_get_Parallel(haloExchange->recvBuf[ir++], haloExchange->SendBuf[i], haloExchange->bufferSize);
        ir++;
      }
    }
    
    for (int i = 0; i < ir; i++)
    {
      //int nRecv = waitIrecv(haloExchange->rlist[i]);
      unloadBuffer(haloExchange->recvBuf[i], nRecv, spmatrix, domain);
      collectCounter(recvCounter, nRecv);
    }  
  }

  // Else send block if wasn't sent as halo
  else 
  {
    if (!isHaloProc(haloExchange, 0))
    {
      int nSendLen = loadBuffer(haloExchange->sendBuf, spmatrix, domain);
      int nSend = sendParallel(haloExchange->sendBuf, nSendLen, 0);
      collectCounter(sendCounter, nSendLen);
    }
  }
  */
}

/// \details
/// Gather sparse matrix data to processor 0
void allGatherData(struct HaloExchangeSt* haloExchange, struct SparseMatrixSt* spmatrix, struct DomainSt* domain)
{
    int myRank = getMyRank();

    // Post reads for all blocks that have not been received as halos
    //free(haloExchange->rlist);
    //  haloExchange->rlist = (int*)malloc((getNRanks() - haloExchange->haloCount)*sizeof(int));

    int ir = 0;
    int nSend = 0; //shmem_malloc(sizeof(int));
    int nRecv = 0; //shmem_malloc(sizeof(int));
    nSend = loadBuffer(haloExchange->sendBuf, spmatrix, domain);
    nRecv = nSend;
    int i = 0,
        nRanks = getNRanks();

    for (i = 0; i<nRanks; i++){
        if (i != myRank && !isHaloProc(haloExchange, i)){
            ir++;
            put_Parallel(haloExchange->sendBuf, haloExchange->recvBuf[i], nSend, i);
            collectCounter(sendCounter, nSend);
        }
    }

   for (i = 0; i<ir; i++){
       unloadBuffer(haloExchange->recvBuf[i], nRecv, spmatrix, domain);
       collectCounter(recvCounter, nRecv);
   }
/*
//int ir = 0;
  for (int i = 0; i < getNRanks(); i++)
  {
    if (i != myRank && !isHaloProc(haloExchange, i))
    {
//      haloExchange->rlist[ir] = irecvAnyParallel(haloExchange->recvBuf[ir], haloExchange->bufferSize);
      ir++;
    }
  }

  // Send to other ranks that aren't halos
  int nSendLen = loadBuffer(haloExchange->sendBuf, spmatrix, domain);
  for (int i = 0; i < getNRanks(); i++)
  {
    if ( i != myRank && !isHaloProc(haloExchange, i))
    {
      int nSend = sendParallel(haloExchange->sendBuf, nSendLen, i);
      collectCounter(sendCounter, nSendLen);
    }
  }

  for (int i = 0; i < ir; i++)
  {
    int nRecv = waitIrecv(haloExchange->rlist[i]);
    unloadBuffer(haloExchange->recvBuf[i], nRecv, spmatrix, domain);
    collectCounter(recvCounter, nRecv);
  }
  */
}

/// \details
/// Check if proc is a halo proc
int isHaloProc(struct HaloExchangeSt* haloExchange, int rproc)
{
  for (int i = 0; i < haloExchange->haloCount; i++)
  {
    if (haloExchange->haloProc[i] == rproc) return 1;
  }
  return 0;
}

/// \details
/// Add halo processor number to list if not currently present
void addHaloProc(struct HaloExchangeSt* haloExchange, int rproc)
{
  if (haloExchange->haloCount == 0)
  {
    haloExchange->haloProc[0] = rproc;
    haloExchange->haloCount = 1;

  }
  else
  {
    if (isHaloProc(haloExchange, rproc) == 1) return;

    haloExchange->haloProc[haloExchange->haloCount] = rproc;
    haloExchange->haloCount++;
  }
}


/// \details
/// The loadBuffer function for a halo exchange of row data.
int loadBuffer(char* buf, struct SparseMatrixSt* xmatrix, struct DomainSt* domain)
{
  NonZeroMsg* rbuf = (NonZeroMsg*) buf;

  int nBuf = 0;
  for (int i = domain->localRowMin; i < domain->localRowMax; i++)
  {
    for (int j = 0; j < xmatrix->iia[i]; j++)
    {
      rbuf[nBuf].irow = i;
      rbuf[nBuf].icol = xmatrix->jja[i][j];
      rbuf[nBuf].val = xmatrix->val[i][j];

      nBuf++;
    }
  }

  return nBuf*sizeof(NonZeroMsg);  
}

/// \details
/// The unloadBuffer function for a halo exchange of row data.
void unloadBuffer(char* buf, int bufSize, struct SparseMatrixSt* xmatrix, struct DomainSt* domain)
{
  NonZeroMsg* rbuf = (NonZeroMsg*) buf;

  int nBuf = bufSize / sizeof(NonZeroMsg);
  assert(bufSize % sizeof(NonZeroMsg) == 0);

  // Assume all non-zeros for a row areastored contiguously
  int rcurrent = -1;
  int j = 0;
  for (int i = 0; i < nBuf; i++)
  {
    int irow = rbuf[i].irow;

    if (irow != rcurrent)
    {
      xmatrix->iia[irow] = 0;
      rcurrent = irow;
      j = 0;
    }
    xmatrix->iia[irow]++;
    xmatrix->jja[irow][j] = rbuf[i].icol;
    xmatrix->val[irow][j] = rbuf[i].val;
    j++;

  }

}

