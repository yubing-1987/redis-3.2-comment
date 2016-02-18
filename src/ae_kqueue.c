/* Kqueue(2)-based ae.c module
 *
 * Copyright (C) 2009 Harish Mallipeddi - harish.mallipeddi@gmail.com
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
 * 基于kqueue的事件驱动封装模块
 */

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

//事件驱动信息
typedef struct aeApiState {
    int kqfd;
    struct kevent *events;
} aeApiState;

/*
 * 创建一个事件驱动
 * eventLoop    事件池指针
 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    //分配驱动信息的内存空间
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    //分配事件缓冲区
    state->events = zmalloc(sizeof(struct kevent)*eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    //建立一个事件队列
    state->kqfd = kqueue();
    if (state->kqfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    eventLoop->apidata = state;
    return 0;
}

/*
 * 更新事件容量
 * eventLoop    事件池指针
 * setsize      新的缓冲区大小
 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    aeApiState *state = eventLoop->apidata;
    //重新分配缓冲区的内存
    state->events = zrealloc(state->events, sizeof(struct kevent)*setsize);
    return 0;
}

/*
 * 关闭队列，释放内存
 * eventLoop    事件池指针
 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;
    //关闭队列
    close(state->kqfd);
    //释放内存
    zfree(state->events);
    zfree(state);
}

/*
 * 添加事件
 * eventLoop    事件池指针
 * fd           事件索引
 * mask         监听的类型掩码
 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;
    //建立对读操作的监听
    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    //建立写操作的监听
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    return 0;
}

/*
 * 删除事件
 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    struct kevent ke;
    //删除读操作的监听
    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
    //删除写操作的监听
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
}

/*
 * 事件操作的监听
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;

    if (tvp != NULL) {
        //计算超时时间
        struct timespec timeout;
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        //等待
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        &timeout);
    } else {
        //等待
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize,
                        NULL);
    }

    if (retval > 0) {
        int j;
        //循环遍历需要处理的事件
        numevents = retval;
        for(j = 0; j < numevents; j++) {
            int mask = 0;
            struct kevent *e = state->events+j;
            //计算事件类型掩码
            if (e->filter == EVFILT_READ) mask |= AE_READABLE;
            if (e->filter == EVFILT_WRITE) mask |= AE_WRITABLE;
            //标记需要处理的事件信息
            eventLoop->fired[j].fd = e->ident;
            eventLoop->fired[j].mask = mask;
        }
    }
    return numevents;
}

/*
 * 获取事件驱动名称
 */
static char *aeApiName(void) {
    return "kqueue";
}
