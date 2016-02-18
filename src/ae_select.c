/* Select()-based ae.c module.
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
 * 通过select的方式来进行事件驱动
 */

#include <string.h>

//事件驱动信息
typedef struct aeApiState {
    fd_set rfds, wfds;
    /* We need to have a copy of the fd sets as it's not safe to reuse
     * FD sets after select(). */
    fd_set _rfds, _wfds;
} aeApiState;

/*
 * 创建一个事件驱动
 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    aeApiState *state = zmalloc(sizeof(aeApiState));

    if (!state) return -1;
    //创建读监听
    FD_ZERO(&state->rfds);
    //创建写监听
    FD_ZERO(&state->wfds);
    eventLoop->apidata = state;
    return 0;
}

/*
 * 重新设置事件容量
 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    /* Just ensure we have enough room in the fd_set type. */
    //不能超过上限
    if (setsize >= FD_SETSIZE) return -1;
    return 0;
}

/*
 * 删除事件监听
 */
static void aeApiFree(aeEventLoop *eventLoop) {
    //直接释放内存就可以了
    zfree(eventLoop->apidata);
}

/*
 * 添加事件监听
 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    //添加读监听
    if (mask & AE_READABLE) FD_SET(fd,&state->rfds);
    //添加写监听
    if (mask & AE_WRITABLE) FD_SET(fd,&state->wfds);
    return 0;
}

/*
 * 删除事件监听
 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    //删除读监听
    if (mask & AE_READABLE) FD_CLR(fd,&state->rfds);
    //删除写监听
    if (mask & AE_WRITABLE) FD_CLR(fd,&state->wfds);
}

/*
 * 等待监听的事件唤醒
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    int retval, j, numevents = 0;

    memcpy(&state->_rfds,&state->rfds,sizeof(fd_set));
    memcpy(&state->_wfds,&state->wfds,sizeof(fd_set));
    //等待
    retval = select(eventLoop->maxfd+1,
                &state->_rfds,&state->_wfds,NULL,tvp);
    if (retval > 0) {
        //遍历需要处理的事件
        for (j = 0; j <= eventLoop->maxfd; j++) {
            int mask = 0;
            aeFileEvent *fe = &eventLoop->events[j];
            //计算监听到的事件类型
            if (fe->mask == AE_NONE) continue;
            if (fe->mask & AE_READABLE && FD_ISSET(j,&state->_rfds))
                mask |= AE_READABLE;
            if (fe->mask & AE_WRITABLE && FD_ISSET(j,&state->_wfds))
                mask |= AE_WRITABLE;
            //标记事件信息
            eventLoop->fired[numevents].fd = j;
            eventLoop->fired[numevents].mask = mask;
            numevents++;
        }
    }
    return numevents;
}

/*
 * 获取事件驱动的名称
 */
static char *aeApiName(void) {
    return "select";
}
