/* ae.c module for illumos event ports.
 *
 * Copyright (c) 2012, Joyent, Inc. All rights reserved.
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
 * 封装的使用poll进行事件驱动，对外实现了一些统一的接口
 */

#include <assert.h>
#include <errno.h>
#include <port.h>
#include <poll.h>

#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>

//标记为是否是调试模式
static int evport_debug = 0;

/*
 * This file implements the ae API using event ports, present on Solaris-based
 * systems since Solaris 10.  Using the event port interface, we associate file
 * descriptors with the port.  Each association also includes the set of poll(2)
 * events that the consumer is interested in (e.g., POLLIN and POLLOUT).
 *
 * There's one tricky piece to this implementation: when we return events via
 * aeApiPoll, the corresponding file descriptors become dissociated from the
 * port.  This is necessary because poll events are level-triggered, so if the
 * fd didn't become dissociated, it would immediately fire another event since
 * the underlying state hasn't changed yet.  We must re-associate the file
 * descriptor, but only after we know that our caller has actually read from it.
 * The ae API does not tell us exactly when that happens, but we do know that
 * it must happen by the time aeApiPoll is called again.  Our solution is to
 * keep track of the last fds returned by aeApiPoll and re-associate them next
 * time aeApiPoll is invoked.
 *
 * To summarize, in this module, each fd association is EITHER (a) represented
 * only via the in-kernel association OR (b) represented by pending_fds and
 * pending_masks.  (b) is only true for the last fds we returned from aeApiPoll,
 * and only until we enter aeApiPoll again (at which point we restore the
 * in-kernel association).
 */
#define MAX_EVENT_BATCHSZ 512

/*
 * poll方式的事件驱动信息
 */
typedef struct aeApiState {
    //端口句柄
    int     portfd;                             /* event port */
    //需要处理的事情个数
    int     npending;                           /* # of pending fds */
    //处理中的事件索引
    int     pending_fds[MAX_EVENT_BATCHSZ];     /* pending fds */
    //处理中的事件类型
    int     pending_masks[MAX_EVENT_BATCHSZ];   /* pending fds' masks */
} aeApiState;

/*
 * 创建事件驱动
 * eventLoop    关联的事件池指针
 */
static int aeApiCreate(aeEventLoop *eventLoop) {
    int i;
    //申请内存空间
    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;

    //创建端口
    state->portfd = port_create();
    if (state->portfd == -1) {
        //创建不成功，释放资源
        zfree(state);
        return -1;
    }

    state->npending = 0;
    //全部标记为未启用
    for (i = 0; i < MAX_EVENT_BATCHSZ; i++) {
        state->pending_fds[i] = -1;
        state->pending_masks[i] = AE_NONE;
    }

    eventLoop->apidata = state;
    return 0;
}

/*
 * 调整事件缓冲区容量，这里什么都没干
 */
static int aeApiResize(aeEventLoop *eventLoop, int setsize) {
    /* Nothing to resize here. */
    return 0;
}

/*
 * 关闭事件驱动，释放内存
 * eventLoop    事件池指针
 */
static void aeApiFree(aeEventLoop *eventLoop) {
    aeApiState *state = eventLoop->apidata;
    //关闭事件驱动端口
    close(state->portfd);
    //释放内存
    zfree(state);
}

/*
 * 查找指定事件句柄的索引
 * state    事件驱动的状态指针
 * fd       事件句柄
 */
static int aeApiLookupPending(aeApiState *state, int fd) {
    int i;
    //遍历查找
    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == fd)
            return (i);
    }
    //没找到
    return (-1);
}

/*
 * Helper function to invoke port_associate for the given fd and mask.
 */
/*
 * 将portfd和fd进行关联，响应mask标记的事件
 * where    描述信息指针
 * portfd   事件驱动端口描述符
 * fd       流描述符，被监听对象
 * mask     监听内容
 */
static int aeApiAssociate(const char *where, int portfd, int fd, int mask) {
    int events = 0;
    int rv, err;
    //计算监听内容
    if (mask & AE_READABLE)
        events |= POLLIN;
    if (mask & AE_WRITABLE)
        events |= POLLOUT;

    if (evport_debug)
        fprintf(stderr, "%s: port_associate(%d, 0x%x) = ", where, fd, events);
    //进行关联
    rv = port_associate(portfd, PORT_SOURCE_FD, fd, events,
        (void *)(uintptr_t)mask);
    err = errno;

    if (evport_debug)
        fprintf(stderr, "%d (%s)\n", rv, rv == 0 ? "no error" : strerror(err));

    if (rv == -1) {
        //关联失败，输出信息
        fprintf(stderr, "%s: port_associate: %s\n", where, strerror(err));

        if (err == EAGAIN)
            fprintf(stderr, "aeApiAssociate: event port limit exceeded.");
    }

    return rv;
}

/*
 * 添加事件
 * eventLoop    事件池指针
 * fd           流描述符
 * mask         监听类型
 */
static int aeApiAddEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;

    if (evport_debug)
        fprintf(stderr, "aeApiAddEvent: fd %d mask 0x%x\n", fd, mask);

    /*
     * Since port_associate's "events" argument replaces any existing events, we
     * must be sure to include whatever events are already associated when
     * we call port_associate() again.
     */
    //计算新的类型
    fullmask = mask | eventLoop->events[fd].mask;
    //查找是否已经正在被要求处理
    pfd = aeApiLookupPending(state, fd);

    if (pfd != -1) {
        /*
         * This fd was recently returned from aeApiPoll.  It should be safe to
         * assume that the consumer has processed that poll event, but we play
         * it safer by simply updating pending_mask.  The fd will be
         * re-associated as usual when aeApiPoll is called again.
         */
        if (evport_debug)
            fprintf(stderr, "aeApiAddEvent: adding to pending fd %d\n", fd);
        //如果已经在监听就直接设置新的掩码，这里不需要立刻关联，等到aeApiPoll调用的时候再关联
        state->pending_masks[pfd] |= fullmask;
        return 0;
    }
    //调用关联程序
    return (aeApiAssociate("aeApiAddEvent", state->portfd, fd, fullmask));
}

/*
 * 删除一个事件
 * eventLoop    事件池指针
 * fd           流描述符
 * mask         待删除的监听掩码
 */
static void aeApiDelEvent(aeEventLoop *eventLoop, int fd, int mask) {
    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;

    if (evport_debug)
        fprintf(stderr, "del fd %d mask 0x%x\n", fd, mask);
    //判断是否正在被要求处理
    pfd = aeApiLookupPending(state, fd);

    if (pfd != -1) {
        if (evport_debug)
            fprintf(stderr, "deleting event from pending fd %d\n", fd);

        /*
         * This fd was just returned from aeApiPoll, so it's not currently
         * associated with the port.  All we need to do is update
         * pending_mask appropriately.
         */
        //删除被监听的类型
        state->pending_masks[pfd] &= ~mask;
        //如果删除后已经是未启用状态，需要再设置事件句柄为无效
        if (state->pending_masks[pfd] == AE_NONE)
            state->pending_fds[pfd] = -1;
        //处理函数中会进行剩余的删除操作
        return;
    }

    /*
     * The fd is currently associated with the port.  Like with the add case
     * above, we must look at the full mask for the file descriptor before
     * updating that association.  We don't have a good way of knowing what the
     * events are without looking into the eventLoop state directly.  We rely on
     * the fact that our caller has already updated the mask in the eventLoop.
     */
    //删除当前挂起的事件
    fullmask = eventLoop->events[fd].mask;
    if (fullmask == AE_NONE) {
        /*
         * We're removing *all* events, so use port_dissociate to remove the
         * association completely.  Failure here indicates a bug.
         */
        if (evport_debug)
            fprintf(stderr, "aeApiDelEvent: port_dissociate(%d)\n", fd);
        //删除关联
        if (port_dissociate(state->portfd, PORT_SOURCE_FD, fd) != 0) {
            perror("aeApiDelEvent: port_dissociate");
            abort(); /* will not return */
        }
    } else if (aeApiAssociate("aeApiDelEvent", state->portfd, fd,
        fullmask) != 0) {//修改关联
        /*
         * ENOMEM is a potentially transient condition, but the kernel won't
         * generally return it unless things are really bad.  EAGAIN indicates
         * we've reached an resource limit, for which it doesn't make sense to
         * retry (counter-intuitively).  All other errors indicate a bug.  In any
         * of these cases, the best we can do is to abort.
         */
        abort(); /* will not return */
    }
}

/*
 * 事件处理函数
 * eventLoop    事件池指针
 * tvp          超时时间
 */
static int aeApiPoll(aeEventLoop *eventLoop, struct timeval *tvp) {
    aeApiState *state = eventLoop->apidata;
    struct timespec timeout, *tsp;
    int mask, i;
    uint_t nevents;
    port_event_t event[MAX_EVENT_BATCHSZ];

    /*
     * If we've returned fd events before, we must re-associate them with the
     * port now, before calling port_get().  See the block comment at the top of
     * this file for an explanation of why.
     */
    //重新关联驱动和事件
    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == -1)
            /* This fd has since been deleted. */
            continue;

        if (aeApiAssociate("aeApiPoll", state->portfd,
            state->pending_fds[i], state->pending_masks[i]) != 0) {
            /* See aeApiDelEvent for why this case is fatal. */
            abort();
        }
        //因为下面马上等待了，所以全部设置为未启用
        state->pending_masks[i] = AE_NONE;
        state->pending_fds[i] = -1;
    }

    state->npending = 0;
    //计算超时事件
    if (tvp != NULL) {
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        tsp = &timeout;
    } else {
        tsp = NULL;
    }

    /*
     * port_getn can return with errno == ETIME having returned some events (!).
     * So if we get ETIME, we check nevents, too.
     */
    nevents = 1;
    //监听，得到需要处理的事件，或等到超时
    if (port_getn(state->portfd, event, MAX_EVENT_BATCHSZ, &nevents,
        tsp) == -1 && (errno != ETIME || nevents == 0)) {
        if (errno == ETIME || errno == EINTR)
            return 0;

        /* Any other error indicates a bug. */
        perror("aeApiPoll: port_get");
        abort();
    }
    //需要处理的事件个数
    state->npending = nevents;
    //循环遍历，构建待处理的事件列表
    for (i = 0; i < nevents; i++) {
            mask = 0;
            if (event[i].portev_events & POLLIN)
                mask |= AE_READABLE;
            if (event[i].portev_events & POLLOUT)
                mask |= AE_WRITABLE;
            //需要处理的事件信息记录在这里
            eventLoop->fired[i].fd = event[i].portev_object;
            eventLoop->fired[i].mask = mask;

            if (evport_debug)
                fprintf(stderr, "aeApiPoll: fd %d mask 0x%x\n",
                    (int)event[i].portev_object, mask);
            //内部使用的事件信息记录在这里
            state->pending_fds[i] = event[i].portev_object;
            state->pending_masks[i] = (uintptr_t)event[i].portev_user;
    }

    return nevents;
}

/*
 * 获取事件驱动名称
 */
static char *aeApiName(void) {
    return "evport";
}
