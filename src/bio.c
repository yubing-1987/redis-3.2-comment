/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread wait for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * 这个模块主要实现了后台执行io操作的功能，后台文件同步，后台关闭文件等
 */

#include "server.h"
#include "bio.h"

//后台操作的线程
static pthread_t bio_threads[BIO_NUM_OPS];
//后台操作的线程使用的锁
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];
//后台线程关联的条件变量
static pthread_cond_t bio_condvar[BIO_NUM_OPS];
//后台操作的任务列表
static list *bio_jobs[BIO_NUM_OPS];
/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting. */
//标记线程中需要执行的任务个数
static unsigned long long bio_pending[BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all. */
//后台任务结构体
struct bio_job {
    //任务的创建时间
    time_t time; /* Time at which the job was created. */
    /* Job specific arguments pointers. If we need to pass more than three
     * arguments we can just pass a pointer to a structure or alike. */
    //任务使用到的参数，最多三个
    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. */
//redis线程的堆栈大小
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread. */
/*
 *初始化后台io操作线程
 */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects */
    //初始化后台线程使用到的数据，如：锁，关联变量，任务列表。。。
    for (j = 0; j < BIO_NUM_OPS; j++) {
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_condvar[j],NULL);
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system */
    //设置线程堆栈大小
    pthread_attr_init(&attr);
    pthread_attr_getstacksize(&attr,&stacksize);
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of. */
    //创建后台线程
    for (j = 0; j < BIO_NUM_OPS; j++) {
        void *arg = (void*)(unsigned long) j;
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            //创建失败，需要退出程序
            serverLog(LL_WARNING,"Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

/*
 * 创建一个后台运行的程序
 */
void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3) {
    //分配内存
    struct bio_job *job = zmalloc(sizeof(*job));
    //初始化数据
    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;
    //需要考虑线程同步
    pthread_mutex_lock(&bio_mutex[type]);
    //添加到表尾
    listAddNodeTail(bio_jobs[type],job);
    //增加个数
    bio_pending[type]++;
    //发送信号，唤醒线程
    pthread_cond_signal(&bio_condvar[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}

/*
 * 线程执行的回调函数
 */
void *bioProcessBackgroundJobs(void *arg) {
    struct bio_job *job;
    //线程处理的任务类型
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Check that the type is within the right interval. */
    if (type >= BIO_NUM_OPS) {
        //类型错误
        serverLog(LL_WARNING,
            "Warning: bio thread started with wrong type %lu",type);
        return NULL;
    }

    /* Make the thread killable at any time, so that bioKillThreads()
     * can work reliably. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal. */
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));
    //线程循环，一直循环
    while(1) {
        listNode *ln;

        /* The loop always starts with the lock hold. */
        //如果当前没有需要处理的事件，就一直挂起等待下一次唤醒
        if (listLength(bio_jobs[type]) == 0) {
            pthread_cond_wait(&bio_condvar[type],&bio_mutex[type]);
            continue;
        }
        /* Pop the job from the queue. */
        //获取第一个需要处理的事件
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.*/
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. */
        if (type == BIO_CLOSE_FILE) {
            //如果是关闭文件的事件，就去关闭文件
            close((long)job->arg1);
        } else if (type == BIO_AOF_FSYNC) {
            //如果是文件同步的事件就去同步文件
            aof_fsync((long)job->arg1);
        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        释放资源
        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait(). */
        pthread_mutex_lock(&bio_mutex[type]);
        listDelNode(bio_jobs[type],ln);
        bio_pending[type]--;
    }
}

/* Return the number of pending jobs of the specified type. */
/*
 * 获取任务个数
 */
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;
    //考虑线程安全
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
/*
 * 关闭线程，因为线程回调函数里是一个死循环，所以需要外部强制关闭
 */
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++) {
        //关闭
        if (pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d can be joined: %s",
                        j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d terminated",j);
            }
        }
    }
}
