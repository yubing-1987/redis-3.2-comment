/*
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez@gmail.com>.
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
 * 这个模块是redis对于位置数据操作的模块，
 * 位置数据本质使用的是redis中有序集来实现的
 */

#include "geo.h"
#include "geohash_helper.h"

/* Things exported from t_zset.c only for geo.c, since it is the only other
 * part of Redis that requires close zset introspection. */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
int zslValueLteMax(double value, zrangespec *spec);

/* ====================================================================
 * This file implements the following commands:
 *
 *   - geoadd - add coordinates for value to geoset
 *   - georadius - search radius by coordinates in geoset
 *   - georadiusbymember - search radius based on geoset member position
 * ==================================================================== */

/* ====================================================================
 * geoArray implementation
 * ==================================================================== */

/* Create a new array of geoPoints. */
/*
 * 创建一个新的位置数据，数据是空的
 */
geoArray *geoArrayCreate(void) {
    //分配内存
    geoArray *ga = zmalloc(sizeof(*ga));
    /* It gets allocated on first geoArrayAppend() call. */
    //初始化数据
    ga->array = NULL;
    ga->buckets = 0;
    ga->used = 0;
    return ga;
}

/* Add a new entry and return its pointer so that the caller can populate
 * it with data. */
/*
 * 从地理位置数组中获取一个未被使用的地理位置指针，如果缓冲区都被使用了，就会扩容
 */
geoPoint *geoArrayAppend(geoArray *ga) {
    if (ga->used == ga->buckets) {
        //缓冲区已经被用光了，需要扩容
        //第一次大小为8，以后扩容都是翻倍的
        ga->buckets = (ga->buckets == 0) ? 8 : ga->buckets*2;
        //重新分配内存空间
        ga->array = zrealloc(ga->array,sizeof(geoPoint)*ga->buckets);
    }
    //获取指针
    geoPoint *gp = ga->array+ga->used;
    //标记被使用数加一
    ga->used++;
    return gp;
}

/* Destroy a geoArray created with geoArrayCreate(). */
/*
 * 销毁地理位置数组，释放内存
 */
void geoArrayFree(geoArray *ga) {
    size_t i;
    //释放每一个地理位置名称字符串
    for (i = 0; i < ga->used; i++) sdsfree(ga->array[i].member);
    //释放缓冲区
    zfree(ga->array);
    //释放数组内存
    zfree(ga);
}

/* ====================================================================
 * Helpers
 * ==================================================================== */
/*
 * 根据bits得到xy的值
 * bits实际就是redis有序集的分数，xy就是地理位置的经纬度
 * 所以这个函数就是通过有序集的分数得到地理位置的经纬度
 */
int decodeGeohash(double bits, double *xy) {
    GeoHashBits hash = { .bits = (uint64_t)bits, .step = GEO_STEP_MAX };
    return geohashDecodeToLongLatWGS84(hash, xy);
}

/* Input Argument Helper */
/* Take a pointer to the latitude arg then use the next arg for longitude.
 * On parse error C_ERR is returned, otherwise C_OK. */
/*
 * 从参数中得到经纬度的值，并对值进行基本的校验
 * 如果有任何的不合法，就会直接向客户端发送错误信息，并返回错误标识
 */
int extractLongLatOrReply(client *c, robj **argv, double *xy) {
    int i;
    for (i = 0; i < 2; i++) {
        //读取经纬度的值，如果读取错误就返回错误信息
        if (getDoubleFromObjectOrReply(c, argv[i], xy + i, NULL) !=
            C_OK) {
            return C_ERR;
        }
    }
    //对经纬度进行基本的校验
    //维度范围[180]~[-180]
    //经度范围[85.05112878]~[-85.05112878]
    if (xy[0] < GEO_LONG_MIN || xy[0] > GEO_LONG_MAX ||
        xy[1] < GEO_LAT_MIN  || xy[1] > GEO_LAT_MAX) {
        addReplySds(c, sdscatprintf(sdsempty(),
            "-ERR invalid longitude,latitude pair %f,%f\r\n",xy[0],xy[1]));
        return C_ERR;
    }
    return C_OK;
}

/* Input Argument Helper */
/* Decode lat/long from a zset member's score.
 * Returns C_OK on successful decoding, otherwise C_ERR is returned. */
/*
 * 从redis数据中读取经纬度
 */
int longLatFromMember(robj *zobj, robj *member, double *xy) {
    double score = 0;
    //从redis有序表中读取数据的分数
    if (zsetScore(zobj, member, &score) == C_ERR) return C_ERR;
    //从分数中解码出经纬度
    if (!decodeGeohash(score, xy)) return C_ERR;
    return C_OK;
}

/* Check that the unit argument matches one of the known units, and returns
 * the conversion factor to meters (you need to divide meters by the conversion
 * factor to convert to the right unit).
 *
 * If the unit is not valid, an error is reported to the client, and a value
 * less than zero is returned. */
/*
 * 根据不同的单位名称获取换算比例，区分大小写
 * 支持的单位名称：
 *      m        米
 *      km       前面
 *      ft       英尺
 *      mi       英里
 */
double extractUnitOrReply(client *c, robj *unit) {
    //获取名称
    char *u = unit->ptr;

    if (!strcmp(u, "m")) {
        //米
        return 1;
    } else if (!strcmp(u, "km")) {
        //千米
        return 1000;
    } else if (!strcmp(u, "ft")) {
        //英尺
        return 0.3048;
    } else if (!strcmp(u, "mi")) {
        //英里
        return 1609.34;
    } else {
        //不识别的单位，直接告诉前台错误
        addReplyError(c,
            "unsupported unit provided. please use m, km, ft, mi");
        return -1;
    }
}

/* Input Argument Helper.
 * Extract the dinstance from the specified two arguments starting at 'argv'
 * that shouldbe in the form: <number> <unit> and return the dinstance in the
 * specified unit on success. *conversino is populated with the coefficient
 * to use in order to convert meters to the unit.
 *
 * On error a value less than zero is returned. */
/*
 * 根据单位还原到以米为单位的距离
 * 如果操作失败，就会返回[-1]，否则返回还原后的距离
 */
double extractDistanceOrReply(client *c, robj **argv,
                                     double *conversion) {
    double distance;
    //从参数中获取距离
    if (getDoubleFromObjectOrReply(c, argv[0], &distance,
                                   "need numeric radius") != C_OK) {
        return -1;
    }
    //从参数中获取单位的比例
    double to_meters = extractUnitOrReply(c,argv[1]);
    //单位错误，返回错误
    if (to_meters < 0) return -1;
    //把单位的放大比例返回给调用者
    if (conversion) *conversion = to_meters;
    //乘上单位对应的放大比例，然后返回
    return distance * to_meters;
}

/* The defailt addReplyDouble has too much accuracy.  We use this
 * for returning location distances. "5.2145 meters away" is nicer
 * than "5.2144992818115 meters away." We provide 4 digits after the dot
 * so that the returned value is decently accurate even when the unit is
 * the kilometer. */
/*
 * 返回一个距离数据到客户端
 * 之所以需要这个函数，是因为如果直接返回一个double的数据，精度会很高，完全没有必要
 * 所以通过这个函数控制了返回的double字符串的精度，只保留了4位小数
 */
void addReplyDoubleDistance(client *c, double d) {
    char dbuf[128];
    //double转字符串，只保留4位小数
    int dlen = snprintf(dbuf, sizeof(dbuf), "%.4f", d);
    //返回给客户端
    addReplyBulkCBuffer(c, dbuf, dlen);
}

/* Helper function for geoGetPointsInRange(): given a sorted set score
 * representing a point, and another point (the center of our search) and
 * a radius, appends this entry as a geoPoint into the specified geoArray
 * only if the point is within the search area.
 *
 * returns C_OK if the point is included, or REIDS_ERR if it is outside. */
/*
 * 判断一个地理位置的是不是在指定范围内
 * 如果是就加入地理位置数组，标记处距离
 */
int geoAppendIfWithinRadius(geoArray *ga, double lon, double lat, double radius, double score, sds member) {
    double distance, xy[2];
    //从有序集分数中解析出经纬度
    if (!decodeGeohash(score,xy)) return C_ERR; /* Can't decode. */
    /* Note that geohashGetDistanceIfInRadiusWGS84() takes arguments in
     * reverse order: longitude first, latitude later. */
    //判断位置是不是在指定范围内，并计算距离
    if (!geohashGetDistanceIfInRadiusWGS84(lon,lat, xy[0], xy[1],
                                           radius, &distance))
    {
        //不在范围内，直接返回
        return C_ERR;
    }

    /* Append the new element. */
    //如果在范围内，就需要添加到数组中去
    geoPoint *gp = geoArrayAppend(ga);
    //记录经度
    gp->longitude = xy[0];
    //记录维度
    gp->latitude = xy[1];
    //记录距离
    gp->dist = distance;
    //记录名称
    gp->member = member;
    //记录分值
    gp->score = score;
    return C_OK;
}

/* Query a Redis sorted set to extract all the elements between 'min' and
 * 'max', appending them into the array of geoPoint structures 'gparray'.
 * The command returns the number of elements added to the array.
 *
 * Elements which are farest than 'radius' from the specified 'x' and 'y'
 * coordinates are not included.
 *
 * The ability of this function to append to an existing set of points is
 * important for good performances because querying by radius is performed
 * using multiple queries to the sorted set, that we later need to sort
 * via qsort. Similarly we need to be able to reject points outside the search
 * radius area ASAP in order to allocate and process more points than needed. */
/*
 * 在一个地理位置集中查找出处于某个范围的位置集合
 */
int geoGetPointsInRange(robj *zobj, double min, double max, double lon, double lat, double radius, geoArray *ga) {
    /* minex 0 = include min in range; maxex 1 = exclude max in range */
    /* That's: min <= val < max */
    //范围
    zrangespec range = { .min = min, .max = max, .minex = 0, .maxex = 1 };
    //地理位置数组中原来的个数
    size_t origincount = ga->used;
    sds member;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        //ziplist的类型
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr = NULL;
        unsigned int vlen = 0;
        long long vlong = 0;
        double score = 0;
        //在列表中查找出第一个在范围内的元素
        if ((eptr = zzlFirstInRange(zl, &range)) == NULL) {
            /* Nothing exists starting at our min.  No results. */
            return 0;
        }
        //获取指向下一个的迭代器
        sptr = ziplistNext(zl, eptr);
        while (eptr) {
            //获取有序集分数
            score = zzlGetScore(sptr);

            /* If we fell out of range, break. */
            //判断分数是不是在范围内
            if (!zslValueLteMax(score, &range))
                break;

            /* We know the element exists. ziplistGet should always succeed */
            //获取列表的值
            ziplistGet(eptr, &vstr, &vlen, &vlong);
            //得到地理位置名称
            member = (vstr == NULL) ? sdsfromlonglong(vlong) :
                                      sdsnewlen(vstr,vlen);
            //判断地理位置是不是在范围内，是的话加入数组
            if (geoAppendIfWithinRadius(ga,lon,lat,radius,score,member)
                == C_ERR) sdsfree(member);
            //指向下一个
            zzlNext(zl, &eptr, &sptr);
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        //使用skiplist的方式
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        //获取第一个在范围内的元素
        if ((ln = zslFirstInRange(zsl, &range)) == NULL) {
            /* Nothing exists starting at our min.  No results. */
            return 0;
        }

        while (ln) {
            //元素内容
            robj *o = ln->obj;
            /* Abort when the node is no longer in range. */
            //判断分数是不是在范围内
            if (!zslValueLteMax(ln->score, &range))
                break;
            //获取地理位置名称
            member = (o->encoding == OBJ_ENCODING_INT) ?
                        sdsfromlonglong((long)o->ptr) :
                        sdsdup(o->ptr);
            //判断地理位置是不是在范围内，是的话加入数组
            if (geoAppendIfWithinRadius(ga,lon,lat,radius,ln->score,member)
                == C_ERR) sdsfree(member);
            //指向下一个位置
            ln = ln->level[0].forward;
        }
    }
    //返回找到的地理位置个数
    return ga->used - origincount;
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
/*
 * 根据hash计算52位的最大和最小值
 * hash包括两个量：bits是值，step是步长
 * min：bits左移（52-step*2）位的值
 * max：bits+1左移（52-step*2）位的值
 * 这个函数有一个副作用，hash的bits会自增1
 */
void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
    /* We want to compute the sorted set scores that will include all the
     * elements inside the specified Geohash 'hash', which has as many
     * bits as specified by hash.step * 2.
     *
     * So if step is, for example, 3, and the hash value in binary
     * is 101010, since our score is 52 bits we want every element which
     * is in binary: 101010?????????????????????????????????????????????
     * Where ? can be 0 or 1.
     *
     * To get the min score we just use the initial hash value left
     * shifted enough to get the 52 bit value. Later we increment the
     * 6 bit prefis (see the hash.bits++ statement), and get the new
     * prefix: 101011, which we align again to 52 bits to get the maximum
     * value (which is excluded from the search). So we get everything
     * between the two following scores (represented in binary):
     *
     * 1010100000000000000000000000000000000000000000000000 (included)
     * and
     * 1010110000000000000000000000000000000000000000000000 (excluded).
     */
    //计算下限
    *min = geohashAlign52Bits(hash);
    //自增1
    hash.bits++;
    //计算上限
    *max = geohashAlign52Bits(hash);
}

/* Obtain all members between the min/max of this geohash bounding box.
 * Populate a geoArray of GeoPoints by calling geoGetPointsInRange().
 * Return the number of points added to the array. */
/*
 * 计算离指定位置在一定范围内的地理位置
 */
int membersOfGeoHashBox(robj *zobj, GeoHashBits hash, geoArray *ga, double lon, double lat, double radius) {
    GeoHashFix52Bits min, max;
    //计算hash的上下限
    scoresOfGeoHashBox(hash,&min,&max);
    //计算满足要求的地理位置，存储在ga中
    return geoGetPointsInRange(zobj, min, max, lon, lat, radius, ga);
}

/* Search all eight neighbors + self geohash box */
/*
 * 查找八个方位+自身的里指定位置距离在[radius]之内的地理位置
 */
int membersOfAllNeighbors(robj *zobj, GeoHashRadius n, double lon, double lat, double radius, geoArray *ga) {
    GeoHashBits neighbors[9];
    unsigned int i, count = 0, last_processed = 0;
    //八个方位+自身的位置hash值
    neighbors[0] = n.hash;
    neighbors[1] = n.neighbors.north;
    neighbors[2] = n.neighbors.south;
    neighbors[3] = n.neighbors.east;
    neighbors[4] = n.neighbors.west;
    neighbors[5] = n.neighbors.north_east;
    neighbors[6] = n.neighbors.north_west;
    neighbors[7] = n.neighbors.south_east;
    neighbors[8] = n.neighbors.south_west;

    /* For each neighbor (*and* our own hashbox), get all the matching
     * members and add them to the potential result list. */
    //循环遍历全部的方位，计算全部满足要求的位置
    for (i = 0; i < sizeof(neighbors) / sizeof(*neighbors); i++) {
        if (HASHISZERO(neighbors[i]))
            continue;

        /* When a huge Radius (in the 5000 km range or more) is used,
         * adjacent neighbors can be the same, leading to duplicated
         * elements. Skip every range which is the same as the one
         * processed previously. */
        /*
         * 如果八个方位中有和自己位置一样的，直接跳过
         */
        if (last_processed &&
            neighbors[i].bits == neighbors[last_processed].bits &&
            neighbors[i].step == neighbors[last_processed].step)
            continue;
        //计算满足要求的地理位置
        count += membersOfGeoHashBox(zobj, neighbors[i], ga, lon, lat, radius);
        last_processed = i;
    }
    //返回满足要求的个数
    return count;
}

/* Sort comparators for qsort() */
/*
 * 排序用的比较函数，比较的是地理位置的距离
 * 由近到远排序
 */
static int sort_gp_asc(const void *a, const void *b) {
    const struct geoPoint *gpa = a, *gpb = b;
    /* We can't do adist - bdist because they are doubles and
     * the comparator returns an int. */
    //比较两个点的位置距离
    if (gpa->dist > gpb->dist)
        return 1;
    else if (gpa->dist == gpb->dist)
        return 0;
    else
        return -1;
}

/*
 * 排序比较函数，根据距离排序
 * 由远到近排序
 */
static int sort_gp_desc(const void *a, const void *b) {
    return -sort_gp_asc(a, b);
}

/* ====================================================================
 * Commands
 * ==================================================================== */

/* GEOADD key long lat name [long2 lat2 name2 ... longN latN nameN] */
/*
 * 添加地理位置
 */
void geoaddCommand(client *c) {
    /* Check arguments number for sanity. */
    //参数个数必须是2，5，8...这种方式
    if ((c->argc - 2) % 3 != 0) {
        /* Need an odd number of arguments if we got this far... */
        addReplyError(c, "syntax error. Try GEOADD key [x1] [y1] [name1] "
                         "[x2] [y2] [name2] ... ");
        return;
    }
    //计算元素个数
    int elements = (c->argc - 2) / 3;
    //计算有序集参数个数
    int argc = 2+elements*2; /* ZADD key score ele ... */
    //分配有序集参数内存
    robj **argv = zcalloc(argc*sizeof(robj*));
    //添加有序集命令
    argv[0] = createRawStringObject("zadd",4);
    //有序集的名称
    argv[1] = c->argv[1]; /* key */
    incrRefCount(argv[1]);

    /* Create the argument vector to call ZADD in order to add all
     * the score,value pairs to the requested zset, where score is actually
     * an encoded version of lat,long. */
    int i;
    //遍历计算每一个地理位置
    for (i = 0; i < elements; i++) {
        double xy[2];
        //计算经纬度
        if (extractLongLatOrReply(c, (c->argv+2)+(i*3),xy) == C_ERR) {
            for (i = 0; i < argc; i++)
                if (argv[i]) decrRefCount(argv[i]);
            zfree(argv);
            return;
        }

        /* Turn the coordinates into the score of the element. */
        GeoHashBits hash;
        //根据经纬度计算hash
        geohashEncodeWGS84(xy[0], xy[1], GEO_STEP_MAX, &hash);
        //计算52位的hash值
        GeoHashFix52Bits bits = geohashAlign52Bits(hash);
        //得到分值
        robj *score = createObject(OBJ_STRING, sdsfromlonglong(bits));
        //名称
        robj *val = c->argv[2 + i * 3 + 2];
        argv[2+i*2] = score;
        argv[3+i*2] = val;
        incrRefCount(val);
    }

    /* Finally call ZADD that will do the work for us. */
    //执行有序集添加的命令
    replaceClientCommandVector(c,argc,argv);
    zaddCommand(c);
}

#define SORT_NONE 0
#define SORT_ASC 1
#define SORT_DESC 2

#define RADIUS_COORDS 1
#define RADIUS_MEMBER 2

/* GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                               [COUNT count]
 * GEORADIUSBYMEMBER key member radius unit ... options ... */
/*
 * 计算位置集中到里指定位置距离在[radius]之内的位置
 */
void georadiusGeneric(client *c, int type) {
    robj *key = c->argv[1];

    /* Look up the requested zset */
    robj *zobj = NULL;
    //获取redis数据
    if ((zobj = lookupKeyReadOrReply(c, key, shared.emptymultibulk)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) {
        return;
    }

    /* Find long/lat to use for radius search based on inquiry type */
    int base_args;
    double xy[2] = { 0 };
    if (type == RADIUS_COORDS) {
        //给出了具体的比较位置的经纬度
        base_args = 6;
        //计算作为比较的经纬度
        if (extractLongLatOrReply(c, c->argv + 2, xy) == C_ERR)
            return;
    } else if (type == RADIUS_MEMBER) {
        //跟位置集中的某个成员做比较
        base_args = 5;
        robj *member = c->argv[2];
        //从成员中计算作为比较的经纬度
        if (longLatFromMember(zobj, member, xy) == C_ERR) {
            addReplyError(c, "could not decode requested zset member");
            return;
        }
    } else {
        //不支持的类型，返回错误信息给客户端
        addReplyError(c, "unknown georadius search type");
        return;
    }

    /* Extract radius and units from arguments */
    double radius_meters = 0, conversion = 1;
    //计算出以米为单位的距离，并且计算单位的转换比例
    if ((radius_meters = extractDistanceOrReply(c, c->argv + base_args - 2,
                                                &conversion)) < 0) {
        return;
    }

    /* Discover and populate all optional parameters. */
    int withdist = 0, withhash = 0, withcoords = 0;
    int sort = SORT_NONE;
    long long count = 0;
    //获取option参数
    if (c->argc > base_args) {
        int remaining = c->argc - base_args;
        for (int i = 0; i < remaining; i++) {
            char *arg = c->argv[base_args + i]->ptr;
            if (!strcasecmp(arg, "withdist")) {
                //带距离
                withdist = 1;
            } else if (!strcasecmp(arg, "withhash")) {
                //带hash值
                withhash = 1;
            } else if (!strcasecmp(arg, "withcoord")) {
                //带坐标
                withcoords = 1;
            } else if (!strcasecmp(arg, "asc")) {
                //由近到远
                sort = SORT_ASC;
            } else if (!strcasecmp(arg, "desc")) {
                //由远到近
                sort = SORT_DESC;
            } else if (!strcasecmp(arg, "count") && (i+1) < remaining) {
                //如果带了[count]参数，还需要获取count的值
                //获取[count]的值
                if (getLongLongFromObjectOrReply(c, c->argv[base_args+i+1],
                    &count, NULL) != C_OK) return;
                if (count <= 0) {
                    //如果[count]的值必须大于[0]
                    addReplyError(c,"COUNT must be > 0");
                    return;
                }
                i++;
            } else {
                //不支持的参数，必须返回错误信息
                addReply(c, shared.syntaxerr);
                return;
            }
        }
    }

    /* COUNT without ordering does not make much sense, force ASC
     * ordering if COUNT was specified but no sorting was requested. */
    //如果没有指定排序方式，并且[count]大于0，就按照由近到远来排序
    if (count != 0 && sort == SORT_NONE) sort = SORT_ASC;

    /* Get all neighbor geohash boxes for our radius search */
    //根据经纬度，范围计算需要搜索的地理位置区间
    GeoHashRadius georadius =
        geohashGetAreasByRadiusWGS84(xy[0], xy[1], radius_meters);

    /* Search the zset for all matching points */
    //创建一个存储的列表
    geoArray *ga = geoArrayCreate();
    //查找全部在区间内的位置
    membersOfAllNeighbors(zobj, georadius, xy[0], xy[1], radius_meters, ga);

    /* If no matching results, the user gets an empty reply. */
    //如果没有满足要求的位置，就直接返回空
    if (ga->used == 0) {
        addReply(c, shared.emptymultibulk);
        geoArrayFree(ga);
        return;
    }

    long result_length = ga->used;
    long option_length = 0;

    /* Our options are self-contained nested multibulk replies, so we
     * only need to track how many of those nested replies we return. */
     //计算每一个地理位置需要返回的数据个数
    if (withdist)
        option_length++;

    if (withcoords)
        option_length++;

    if (withhash)
        option_length++;

    /* The multibulk len we send is exactly result_length. The result is either
     * all strings of just zset members  *or* a nested multi-bulk reply
     * containing the zset member string _and_ all the additional options the
     * user enabled for this request. */
    //返回满足要求的地理位置的个数
    //如果指定了[count]，个数就是[count]指定的值
    //如果满足要求的总个数少于[count]，就返回总个数
    addReplyMultiBulkLen(c, (count == 0 || result_length < count) ?
                            result_length : count);

    /* Process [optional] requested sorting */
    //根据排序方式进行排序
    if (sort == SORT_ASC) {
        qsort(ga->array, result_length, sizeof(geoPoint), sort_gp_asc);
    } else if (sort == SORT_DESC) {
        qsort(ga->array, result_length, sizeof(geoPoint), sort_gp_desc);
    }

    /* Finally send results back to the caller */
    int i;
    //循环遍历全部满足要求的位置
    for (i = 0; i < result_length; i++) {
        geoPoint *gp = ga->array+i;
        //根据单位的比例进行转换
        gp->dist /= conversion; /* Fix according to unit. */

        /* If we have options in option_length, return each sub-result
         * as a nested multi-bulk.  Add 1 to account for result value itself. */
        //返回个数
        if (option_length)
            addReplyMultiBulkLen(c, option_length + 1);
        //返回名称
        addReplyBulkSds(c,gp->member);
        gp->member = NULL;
        //返回距离
        if (withdist)
            addReplyDoubleDistance(c, gp->dist);
        //返回分数
        if (withhash)
            addReplyLongLong(c, gp->score);
        //返回经纬度坐标
        if (withcoords) {
            addReplyMultiBulkLen(c, 2);
            addReplyDouble(c, gp->longitude);
            addReplyDouble(c, gp->latitude);
        }

        /* Stop if COUNT was specified and we already provided the
         * specified number of elements. */
        //如果返回的个数已经达到上限，就退出
        if (count != 0 && count == i+1) break;
    }
    //释放地理位置集内存
    geoArrayFree(ga);
}

/* GEORADIUS wrapper function. */
/*
 * 根据指定坐标，查找满足要求的位置
 */
void georadiusCommand(client *c) {
    georadiusGeneric(c, RADIUS_COORDS);
}

/* GEORADIUSBYMEMBER wrapper function. */
/*
 * 根据指定成员，查找满足要求的位置
 */
void georadiusByMemberCommand(client *c) {
    georadiusGeneric(c, RADIUS_MEMBER);
}

/* GEOHASH key ele1 ele2 ... eleN
 *
 * Returns an array with an 11 characters geohash representation of the
 * position of the specified elements. */
/*
 * 计算地理位置的hash值，得到的hash值是一个11个字符，由数字和小写字母组成
 * 由代码上看，字母中缺少[a]，不知道为什么
 */
void geohashCommand(client *c) {
    //组成hash值的可选字符
    char *geoalphabet= "0123456789bcdefghjkmnpqrstuvwxyz";
    int j;

    /* Look up the requested zset */
    robj *zobj = NULL;
    //获取地理位置的redis数据
    if ((zobj = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk))
        == NULL || checkType(c, zobj, OBJ_ZSET)) return;

    /* Geohash elements one after the other, using a null bulk reply for
     * missing elements. */
    //返回元素个数
    addReplyMultiBulkLen(c,c->argc-2);
    //遍历每一个元素，计算并转换hash值，并返回给客户端
    for (j = 2; j < c->argc; j++) {
        double score;
        if (zsetScore(zobj, c->argv[j], &score) == C_ERR) {
            //获取地理位置的分值失败，返回空
            addReply(c,shared.nullbulk);
        } else {
            /* The internal format we use for geocoding is a bit different
             * than the standard, since we use as initial latitude range
             * -85,85, while the normal geohashing algorithm uses -90,90.
             * So we have to decode our position and re-encode using the
             * standard ranges in order to output a valid geohash string. */

            /* Decode... */
            double xy[2];
            //通过分值计算经纬度
            if (!decodeGeohash(score,xy)) {
                //计算失败，返回空
                addReply(c,shared.nullbulk);
                continue;
            }

            /* Re-encode */
            GeoHashRange r[2];
            GeoHashBits hash;
            r[0].min = -180;
            r[0].max = 180;
            r[1].min = -90;
            r[1].max = 90;
            //计算hash值
            geohashEncode(&r[0],&r[1],xy[0],xy[1],26,&hash);

            char buf[12];
            int i;
            //遍历计算得到的hash值中的每一位，转换成32进制
            for (i = 0; i < 11; i++) {
                //计算32进制每一位的值
                int idx = (hash.bits >> (52-((i+1)*5))) & 0x1f;
                //转换成32进制的字符
                buf[i] = geoalphabet[idx];
            }
            buf[11] = '\0';
            //返回给客户端
            addReplyBulkCBuffer(c,buf,11);
        }
    }
}

/* GEOPOS key ele1 ele2 ... eleN
 *
 * Returns an array of two-items arrays representing the x,y position of each
 * element specified in the arguments. For missing elements NULL is returned. */
/*
 * 获取一系列元素的经纬度
 */
void geoposCommand(client *c) {
    int j;

    /* Look up the requested zset */
    robj *zobj = NULL;
    //获取redis有序集
    if ((zobj = lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk))
        == NULL || checkType(c, zobj, OBJ_ZSET)) return;

    /* Report elements one after the other, using a null bulk reply for
     * missing elements. */
    //返回总的位置个数
    addReplyMultiBulkLen(c,c->argc-2);
    for (j = 2; j < c->argc; j++) {
        double score;
        //获取分数
        if (zsetScore(zobj, c->argv[j], &score) == C_ERR) {
            //没有找到，所以返回[NULL]
            addReply(c,shared.nullmultibulk);
        } else {
            /* Decode... */
            double xy[2];
            //把分数转换成经纬度
            if (!decodeGeohash(score,xy)) {
                //转换不成功，所以返回[NULL]
                addReply(c,shared.nullmultibulk);
                continue;
            }
            //转换成功，返回具体数据
            addReplyMultiBulkLen(c,2);
            addReplyDouble(c,xy[0]);
            addReplyDouble(c,xy[1]);
        }
    }
}

/* GEODIST key ele1 ele2 [unit]
 *
 * Return the distance, in meters by default, otherwise accordig to "unit",
 * between points ele1 and ele2. If one or more elements are missing NULL
 * is returned. */
/*
 * 计算两个地理位置之间的距离，默认单位为[米]
 * 如果计算不成功会返回[NULL]
 */
void geodistCommand(client *c) {
    double to_meter = 1;

    /* Check if there is the unit to extract, otherwise assume meters. */
    //如果设置了单位，就去计算该单位转为成米的比例。
    if (c->argc == 5) {
        to_meter = extractUnitOrReply(c,c->argv[4]);
        //单位错误
        if (to_meter < 0) return;
    } else if (c->argc > 5) {
        //参数过多
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Look up the requested zset */
    robj *zobj = NULL;
    //查找[key]对应的redis数据，必须是有序集的类型
    if ((zobj = lookupKeyReadOrReply(c, c->argv[1], shared.emptybulk))
        == NULL || checkType(c, zobj, OBJ_ZSET)) return;

    /* Get the scores. We need both otherwise NULL is returned. */
    double score1, score2, xyxy[4];
    //获取元素的分数，可以通过这个分数来获取经纬度
    if (zsetScore(zobj, c->argv[2], &score1) == C_ERR ||
        zsetScore(zobj, c->argv[3], &score2) == C_ERR)
    {
        addReply(c,shared.nullbulk);
        return;
    }

    /* Decode & compute the distance. */
    //通过分数计算经纬度
    if (!decodeGeohash(score1,xyxy) || !decodeGeohash(score2,xyxy+2))
        addReply(c,shared.nullbulk);
    else
        //计算得到的经纬度直接的距离，并进行单位的换算
        addReplyDouble(c,
            geohashGetDistance(xyxy[0],xyxy[1],xyxy[2],xyxy[3]) / to_meter);
}
