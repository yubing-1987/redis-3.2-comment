/* Bit operations.
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
  * redis位操作的命令实现模块
  */

#include "server.h"

/* -----------------------------------------------------------------------------
 * Helpers and low level bit functions.
 * -------------------------------------------------------------------------- */

/* This helper function used by GETBIT / SETBIT parses the bit offset argument
 * making sure an error is returned if it is negative or if it overflows
 * Redis 512 MB limit for the string value. */
/*
 * 从object中读取位的偏移量，0~512MB之间
 */
static int getBitOffsetFromArgument(client *c, robj *o, size_t *offset) {
    long long loffset;
    char *err = "bit offset is not an integer or out of range";
    //获取偏移量的值，如果失败，就会发送错误信息到客户端，并返回
    if (getLongLongFromObjectOrReply(c,o,&loffset,err) != C_OK)
        return C_ERR;

    /* Limit offset to 512MB in bytes */
    //判断偏移量是否合法
    if ((loffset < 0) || ((unsigned long long)loffset >> 3) >= (512*1024*1024))
    {
        addReplyError(c,err);
        return C_ERR;
    }

    *offset = (size_t)loffset;
    return C_OK;
}

/* Count number of bits set in the binary array pointed by 's' and long
 * 'count' bytes. The implementation of this function is required to
 * work with a input string length up to 512 MB. */
/*
 * 计算内存区域中bit位是[1]的个数，内存大小不能草果512MB
 */
size_t redisPopcount(void *s, long count) {
    size_t bits = 0;
    unsigned char *p = s;
    uint32_t *p4;
    //字节中对应的bit为[1]的个数
    static const unsigned char bitsinbyte[256] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

    /* Count initial bytes not aligned to 32 bit. */
    //不是32bit对齐的需要先对齐
    while((unsigned long)p & 3 && count) {
        bits += bitsinbyte[*p++];
        count--;
    }

    /* Count bits 28 bytes at a time */
    //28个字节一组的进行计算
    p4 = (uint32_t*)p;
    while(count>=28) {
        uint32_t aux1, aux2, aux3, aux4, aux5, aux6, aux7;

        aux1 = *p4++;
        aux2 = *p4++;
        aux3 = *p4++;
        aux4 = *p4++;
        aux5 = *p4++;
        aux6 = *p4++;
        aux7 = *p4++;
        count -= 28;

        aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
        aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
        aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
        aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
        aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
        aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
        aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
        aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
        aux5 = aux5 - ((aux5 >> 1) & 0x55555555);
        aux5 = (aux5 & 0x33333333) + ((aux5 >> 2) & 0x33333333);
        aux6 = aux6 - ((aux6 >> 1) & 0x55555555);
        aux6 = (aux6 & 0x33333333) + ((aux6 >> 2) & 0x33333333);
        aux7 = aux7 - ((aux7 >> 1) & 0x55555555);
        aux7 = (aux7 & 0x33333333) + ((aux7 >> 2) & 0x33333333);
        bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) +
                    ((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) +
                    ((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) +
                    ((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) +
                    ((aux5 + (aux5 >> 4)) & 0x0F0F0F0F) +
                    ((aux6 + (aux6 >> 4)) & 0x0F0F0F0F) +
                    ((aux7 + (aux7 >> 4)) & 0x0F0F0F0F))* 0x01010101) >> 24;
    }
    /* Count the remaining bytes. */
    //计算剩余的
    p = (unsigned char*)p4;
    while(count--) bits += bitsinbyte[*p++];
    return bits;
}

/* Return the position of the first bit set to one (if 'bit' is 1) or
 * zero (if 'bit' is 0) in the bitmap starting at 's' and long 'count' bytes.
 *
 * The function is guaranteed to return a value >= 0 if 'bit' is 0 since if
 * no zero bit is found, it returns count*8 assuming the string is zero
 * padded on the right. However if 'bit' is 1 it is possible that there is
 * not a single set bit in the bitmap. In this special case -1 is returned. */
/*
 * 查找第一个bit和传入的参数[bit]一致的位置
 * 没有找到匹配的时候，[bit]==[0]
 */
long redisBitpos(void *s, unsigned long count, int bit) {
    unsigned long *l;
    unsigned char *c;
    unsigned long skipval, word = 0, one;
    long pos = 0; /* Position of bit, to return to the caller. */
    unsigned long j;

    /* Process whole words first, seeking for first word that is not
     * all ones or all zeros respectively if we are lookig for zeros
     * or ones. This is much faster with large strings having contiguous
     * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
     *
     * Note that if we start from an address that is not aligned
     * to sizeof(unsigned long) we consume it byte by byte until it is
     * aligned. */

    /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
    //当[bit]==[0]的时候，如果一个byte=-1，那就表面这个byte里面没有哪一个bit为[0]，就需要跳过
    //同理，[bit]==[1]的时候，byte==0也是需要跳过的
    skipval = bit ? 0 : UCHAR_MAX;
    c = (unsigned char*) s;
    //这里主要是为了进行内存地址对齐的
    while((unsigned long)c & (sizeof(*l)-1) && count) {
        if (*c != skipval) break;//走到了break表示找到相应的值
        //没有符合的bit，跳到下一个byte
        c++;
        count--;
        pos += 8;
    }

    /* Skip bits with full word step. */
    //这个和上面是一样的道理，由于上一步已经进行了内存对齐，
    //所以，可以直接取一个long的值进行校验，这样会快很多
    skipval = bit ? 0 : ULONG_MAX;
    l = (unsigned long*) c;
    while (count >= sizeof(*l)) {
        if (*l != skipval) break;
        l++;
        count -= sizeof(*l);
        pos += sizeof(*l)*8;
    }

    /* Load bytes into "word" considering the first byte as the most significant
     * (we basically consider it as written in big endian, since we consider the
     * string as a set of bits from left to right, with the first bit at position
     * zero.
     *
     * Note that the loading is designed to work even when the bytes left
     * (count) are less than a full word. We pad it with zero on the right. */
    //这里处理剩余的byte，主要是把剩余的byte组合到一个long里面
    c = (unsigned char*)l;
    for (j = 0; j < sizeof(*l); j++) {
        //右移8bit
        word <<= 8;
        if (count) {
            //把byte的值复制到word里面
            word |= *c;
            c++;
            count--;
        }
    }

    /* Special case:
     * If bits in the string are all zero and we are looking for one,
     * return -1 to signal that there is not a single "1" in the whole
     * string. This can't happen when we are looking for "0" as we assume
     * that the right of the string is zero padded. */
    //如果想找到bit为[1]的，结果到这里[word]=0，那就是表示没有找到，返回[-1]
    if (bit == 1 && word == 0) return -1;

    /* Last word left, scan bit by bit. The first thing we need is to
     * have a single "1" set in the most significant position in an
     * unsigned long. We don't know the size of the long so we use a
     * simple trick. */
    //全部bit设置为[1]
    one = ULONG_MAX; /* All bits set to 1.*/
    //右移一位，最高位设置为[0]，其它全部是[1]
    one >>= 1;       /* All bits set to 1 but the MSB. */
    //取反，最高位为[1]，其它全是[0]
    one = ~one;      /* All bits set to 0 but the MSB. */

    while(one) {
        //从高到低探测bit是不是为1
        //（one & word）!= 0    如果指定位为[0] 则为[false]->[0]
        //如果指定位为[1] 则为[true]->[1]
        //如果bit == [1] 对应的是 （one & word）!= 0为真
        //如果bit == [0] 对应的是 （one & word）!= 0为假
        if (((one & word) != 0) == bit) return pos;
        pos++;
        one >>= 1;
    }

    /* If we reached this point, there is a bug in the algorithm, since
     * the case of no match is handled as a special case before. */
    serverPanic("End of redisBitpos() reached.");
    //走到这里一般是不应该的
    return 0; /* Just to avoid warnings. */
}

/* -----------------------------------------------------------------------------
 * Bits related string commands: GETBIT, SETBIT, BITCOUNT, BITOP.
 * -------------------------------------------------------------------------- */

#define BITOP_AND   0
#define BITOP_OR    1
#define BITOP_XOR   2
#define BITOP_NOT   3

/* SETBIT key offset bitvalue */
/*
 * 设置指定偏移的bit值
 */
void setbitCommand(client *c) {
    robj *o;
    char *err = "bit is not an integer or out of range";
    size_t bitoffset;
    int byte, bit;
    int byteval, bitval;
    long on;
    //得到偏移
    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != C_OK)
        return;
    //得到需要设置的bit值
    if (getLongFromObjectOrReply(c,c->argv[3],&on,err) != C_OK)
        return;

    /* Bits can only be set or cleared... */
    //bit的值只能够是[0]和[1]
    if (on & ~1) {
        addReplyError(c,err);
        return;
    }
    //计算byte的偏移
    byte = bitoffset >> 3;
    //得到redis数据
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        //如果原来不存在，就创建一个，并初始设置为空字符串，长度为刚好容下位偏移
        o = createObject(OBJ_STRING,sdsnewlen(NULL, byte+1));
        //添加到数据库
        dbAdd(c->db,c->argv[1],o);
    } else {
        //只有是字符串类型才可以进行位计算
        if (checkType(c,o,OBJ_STRING)) return;
        //拷贝一个不会被其它地方使用的redis数据
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        //如果字符串的长度小于[byte+1]，就进行扩容，用[0]进行填充
        o->ptr = sdsgrowzero(o->ptr,byte+1);
    }

    /* Get current values */
    //得到当前的btye值
    byteval = ((uint8_t*)o->ptr)[byte];
    //得到当前bit值
    bit = 7 - (bitoffset & 0x7);
    //计算指定位置的bit值
    bitval = byteval & (1 << bit);

    /* Update byte with new bit value and return original value */
    //指定位置的值设置为[0]
    byteval &= ~(1 << bit);
    //计算出指定位置新的值
    byteval |= ((on & 0x1) << bit);
    //更新
    ((uint8_t*)o->ptr)[byte] = byteval;
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"setbit",c->argv[1],c->db->id);
    server.dirty++;
    //返回原来指定位置的bit值
    addReply(c, bitval ? shared.cone : shared.czero);
}

/* GETBIT key offset */
/*
 * GETBIT命令，用来得到redis数据指定位置的bit值
 */
void getbitCommand(client *c) {
    robj *o;
    char llbuf[32];
    size_t bitoffset;
    size_t byte, bit;
    size_t bitval = 0;
    //得到位偏移
    if (getBitOffsetFromArgument(c,c->argv[2],&bitoffset) != C_OK)
        return;
    //查找redis数据
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    //计算byte的偏移
    byte = bitoffset >> 3;
    //计算在最后一个byte中的bit偏移
    bit = 7 - (bitoffset & 0x7);
    if (sdsEncodedObject(o)) {
        //普通的字符串
        if (byte < sdslen(o->ptr))
            //如果没有超出最大长度，直接就可以计算bit的值
            bitval = ((uint8_t*)o->ptr)[byte] & (1 << bit);
    } else {
        //数字表示的字符串，需要先转换成真的字符串
        if (byte < (size_t)ll2string(llbuf,sizeof(llbuf),(long)o->ptr))
            //计算bit值
            bitval = llbuf[byte] & (1 << bit);
    }
    //返回得到的bit值，如果越界就返回[0]
    addReply(c, bitval ? shared.cone : shared.czero);
}

/* BITOP op_name target_key src_key1 src_key2 src_key3 ... src_keyN */
/*
 * BITOP操作，位与，位或，取反，异或等四种操作
 */
void bitopCommand(client *c) {
    char *opname = c->argv[1]->ptr;
    robj *o, *targetkey = c->argv[2];
    unsigned long op, j, numkeys;
    robj **objects;      /* Array of source objects. */
    unsigned char **src; /* Array of source strings pointers. */
    unsigned long *len, maxlen = 0; /* Array of length of src strings,
                                       and max len. */
    unsigned long minlen = 0;    /* Min len among the input keys. */
    unsigned char *res = NULL; /* Resulting string. */

    /* Parse the operation name. */
    //校验操作名称
    //[and]---与操作，[or]---或操作，[xor]---异或操作，[not]---非操作
    //操作名称不区分大小写
    if ((opname[0] == 'a' || opname[0] == 'A') && !strcasecmp(opname,"and"))
        op = BITOP_AND;
    else if((opname[0] == 'o' || opname[0] == 'O') && !strcasecmp(opname,"or"))
        op = BITOP_OR;
    else if((opname[0] == 'x' || opname[0] == 'X') && !strcasecmp(opname,"xor"))
        op = BITOP_XOR;
    else if((opname[0] == 'n' || opname[0] == 'N') && !strcasecmp(opname,"not"))
        op = BITOP_NOT;
    else {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Sanity check: NOT accepts only a single key argument. */
    //对于非操作，只能够支持一个被操作对象
    if (op == BITOP_NOT && c->argc != 4) {
        addReplyError(c,"BITOP NOT must be called with a single source key.");
        return;
    }

    /* Lookup keys, and store pointers to the string objects into an array. */
    //被操作对象的个数
    numkeys = c->argc - 3;
    //存放源数据字符串指针
    src = zmalloc(sizeof(unsigned char*) * numkeys);
    //存放源数据字符串长度
    len = zmalloc(sizeof(long) * numkeys);
    //存放存放源数据
    objects = zmalloc(sizeof(robj*) * numkeys);
    for (j = 0; j < numkeys; j++) {
        //获取redis数据
        o = lookupKeyRead(c->db,c->argv[j+3]);
        /* Handle non-existing keys as empty strings. */
        if (o == NULL) {
            //redis数据没有找到，就直接跳过
            objects[j] = NULL;
            src[j] = NULL;
            len[j] = 0;
            minlen = 0;
            continue;
        }
        /* Return an error if one of the keys is not a string. */
        if (checkType(c,o,OBJ_STRING)) {
            //如果其中任意一个不是字符串类型的，就返回错误
            unsigned long i;
            for (i = 0; i < j; i++) {
                if (objects[i])
                    decrRefCount(objects[i]);
            }
            //释放资源
            zfree(src);
            zfree(len);
            zfree(objects);
            return;
        }
        //获取一个解码过的redis数据
        objects[j] = getDecodedObject(o);
        //源数据字符串指针
        src[j] = objects[j]->ptr;
        //源数据字符串长度
        len[j] = sdslen(objects[j]->ptr);
        //计算出全部字符串中最长的长度
        if (len[j] > maxlen) maxlen = len[j];
        //计算出全部字符串中最短的长度
        if (j == 0 || len[j] < minlen) minlen = len[j];
    }

    /* Compute the bit operation, if at least one string is not empty. */
    if (maxlen) {//[maxlen]不等于[0]表面至少有一个字符串长度不为空
        res = (unsigned char*) sdsnewlen(NULL,maxlen);
        unsigned char output, byte;
        unsigned long i;

        /* Fast path: as far as we have data for all the input bitmaps we
         * can take a fast path that performs much better than the
         * vanilla algorithm. */
        j = 0;
        if (minlen >= sizeof(unsigned long)*4 && numkeys <= 16) {
            //只对全部字符串长度都大于4个[unsigned long]的进行处理
            //如果有一个字符串已经处理完了，这里也不需要进来
            //这里一次会处理4个[unsigned long]的数据
            unsigned long *lp[16];
            unsigned long *lres = (unsigned long*) res;

            /* Note: sds pointer is always aligned to 8 byte boundary. */
            memcpy(lp,src,sizeof(unsigned long*)*numkeys);
            //第一个字符串直接复制过去就可以了
            //长度是全部字符串的最小长度
            memcpy(res,src[0],minlen);

            /* Different branches per different operations for speed (sorry). */
            if (op == BITOP_AND) {
                //与操作
                while(minlen >= sizeof(unsigned long)*4) {
                    //遍历每一个字符，进行与操作
                    for (i = 1; i < numkeys; i++) {
                        lres[0] &= lp[i][0];
                        lres[1] &= lp[i][1];
                        lres[2] &= lp[i][2];
                        lres[3] &= lp[i][3];
                        lp[i]+=4;
                    }
                    //累加偏移
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_OR) {
                //或操作
                while(minlen >= sizeof(unsigned long)*4) {
                    //循环进行遍历，对每一个进行或操作
                    for (i = 1; i < numkeys; i++) {
                        lres[0] |= lp[i][0];
                        lres[1] |= lp[i][1];
                        lres[2] |= lp[i][2];
                        lres[3] |= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_XOR) {
                //异或操作
                while(minlen >= sizeof(unsigned long)*4) {
                    //循环遍历
                    for (i = 1; i < numkeys; i++) {
                        lres[0] ^= lp[i][0];
                        lres[1] ^= lp[i][1];
                        lres[2] ^= lp[i][2];
                        lres[3] ^= lp[i][3];
                        lp[i]+=4;
                    }
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            } else if (op == BITOP_NOT) {
                //非操作，自己取反就可以了
                while(minlen >= sizeof(unsigned long)*4) {
                    lres[0] = ~lres[0];
                    lres[1] = ~lres[1];
                    lres[2] = ~lres[2];
                    lres[3] = ~lres[3];
                    lres+=4;
                    j += sizeof(unsigned long)*4;
                    minlen -= sizeof(unsigned long)*4;
                }
            }
        }

        /* j is set to the next byte to process by the previous loop. */
        //剩余的字符串再进行处理
        for (; j < maxlen; j++) {
            output = (len[0] <= j) ? 0 : src[0][j];
            //对于非操作，剩余的也是直接取反
            if (op == BITOP_NOT) output = ~output;
            for (i = 1; i < numkeys; i++) {
                //如果超出字符串长度，就取值[0]，否则就去字符串的字符
                byte = (len[i] <= j) ? 0 : src[i][j];
                switch(op) {
                //与操作
                case BITOP_AND: output &= byte; break;
                //或操作
                case BITOP_OR:  output |= byte; break;
                //异或操作
                case BITOP_XOR: output ^= byte; break;
                }
            }
            res[j] = output;
        }
    }
    //降低redis数据的引用
    for (j = 0; j < numkeys; j++) {
        if (objects[j])
            decrRefCount(objects[j]);
    }
    //释放资源
    zfree(src);
    zfree(len);
    zfree(objects);

    /* Store the computed value into the target key */
    //存储计算得到的结果
    if (maxlen) {
        //计算得到了结果，就需要生成一个redis数据，并存储
        o = createObject(OBJ_STRING,res);
        //存储起来
        setKey(c->db,targetkey,o);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",targetkey,c->db->id);
        decrRefCount(o);
    } else if (dbDelete(c->db,targetkey)) {
        //[maxlen]==[0]标示输入的全部redis数据都是空或不存在的
        //所以需要删除[targetkey]标识的数据
        signalModifiedKey(c->db,targetkey);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",targetkey,c->db->id);
    }
    server.dirty++;
    //返回计算后得到的数据字符串长度
    addReplyLongLong(c,maxlen); /* Return the output string length in bytes. */
}

/* BITCOUNT key [start end] */
/*
 * BITCOUNT命令，计算key对应的值的[1]的位数
 */
void bitcountCommand(client *c) {
    robj *o;
    long start, end, strlen;
    unsigned char *p;
    char llbuf[32];

    /* Lookup, check for type, and return 0 for non existing keys. */
    //获取redis数据
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    /* Set the 'p' pointer to the string, that can be just a stack allocated
     * array if our string was integer encoded. */
    //获取redis数据实际的字符串
    if (o->encoding == OBJ_ENCODING_INT) {
        p = (unsigned char*) llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        p = (unsigned char*) o->ptr;
        strlen = sdslen(o->ptr);
    }

    /* Parse start/end range if any. */
    //计算起止位置
    if (c->argc == 4) {
        //计算开始位置
        if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
            return;
        //计算结束位置
        if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
            return;
        /* Convert negative indexes */
        //规范化起始位置
        if (start < 0) start = strlen+start;
        if (end < 0) end = strlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= strlen) end = strlen-1;
    } else if (c->argc == 2) {
        //没有设置起始位置，就是需要计算整个字符串
        /* The whole string. */
        start = 0;
        end = strlen-1;
    } else {
        //参数个数不合法
        /* Syntax error. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * zero can be returned is: start > end. */
    if (start > end) {
        //起止位置设置错误，直接返回0
        addReply(c,shared.czero);
    } else {
        long bytes = end-start+1;
        //计算位中为[1]的个数，并返回
        addReplyLongLong(c,redisPopcount(p+start,bytes));
    }
}

/* BITPOS key bit [start [end]] */
/*
 * BITPOST命令的实现，查找第一个出现[bit]的位置
 */
void bitposCommand(client *c) {
    robj *o;
    long bit, start, end, strlen;
    unsigned char *p;
    char llbuf[32];
    int end_given = 0;

    /* Parse the bit argument to understand what we are looking for, set
     * or clear bits. */
    //获取需要查找的标识位，只能是[0]或[1]
    if (getLongFromObjectOrReply(c,c->argv[2],&bit,NULL) != C_OK)
        return;
    if (bit != 0 && bit != 1) {
        addReplyError(c, "The bit argument must be 1 or 0.");
        return;
    }

    /* If the key does not exist, from our point of view it is an infinite
     * array of 0 bits. If the user is looking for the fist clear bit return 0,
     * If the user is looking for the first set bit, return -1. */
    //查找key对应的redis数据
    if ((o = lookupKeyRead(c->db,c->argv[1])) == NULL) {
        addReplyLongLong(c, bit ? -1 : 0);
        return;
    }
    //不是字符串的数据就直接返回，只有字符串的数据才有位操作
    if (checkType(c,o,OBJ_STRING)) return;

    /* Set the 'p' pointer to the string, that can be just a stack allocated
     * array if our string was integer encoded. */
    //字符串有两种内部结构，如果是字符串数字，就使用INT进行存储，其它使用字符串存储
    if (o->encoding == OBJ_ENCODING_INT) {
        //int类型需要先转成真正的字符串
        p = (unsigned char*) llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        p = (unsigned char*) o->ptr;
        strlen = sdslen(o->ptr);
    }

    /* Parse start/end range if any. */
    //计算查找的起止位置
    if (c->argc == 4 || c->argc == 5) {
        //命令中设置了开始的位置
        if (getLongFromObjectOrReply(c,c->argv[3],&start,NULL) != C_OK)
            return;
        if (c->argc == 5) {
            //命令中设置了结束的位置
            if (getLongFromObjectOrReply(c,c->argv[4],&end,NULL) != C_OK)
                return;
            end_given = 1;
        } else {
            //否则结束位置设置为字符串末尾
            end = strlen-1;
        }
        /* Convert negative indexes */
        //规范化起始位置
        if (start < 0) start = strlen+start;
        if (end < 0) end = strlen+end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= strlen) end = strlen-1;
    } else if (c->argc == 3) {
        //如果没有设置起止位置，就是表示从字符串头查找到字符串尾
        /* The whole string. */
        start = 0;
        end = strlen-1;
    } else {
        //其它情况表示参数错误
        /* Syntax error. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* For empty ranges (start > end) we return -1 as an empty range does
     * not contain a 0 nor a 1. */
    if (start > end) {
        //开始大于结尾，直接返回错误
        addReplyLongLong(c, -1);
    } else {
        long bytes = end-start+1;
        //查找第一个满足要求的位置
        long pos = redisBitpos(p+start,bytes,bit);

        /* If we are looking for clear bits, and the user specified an exact
         * range with start-end, we can't consider the right of the range as
         * zero padded (as we do when no explicit end is given).
         *
         * So if redisBitpos() returns the first bit outside the range,
         * we return -1 to the caller, to mean, in the specified range there
         * is not a single "0" bit. */
        if (end_given && bit == 0 && pos == bytes*8) {
            addReplyLongLong(c,-1);
            return;
        }
        if (pos != -1) pos += start*8; /* Adjust for the bytes we skipped. */
        addReplyLongLong(c,pos);
    }
}
