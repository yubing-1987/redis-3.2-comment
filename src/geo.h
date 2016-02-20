#ifndef __GEO_H__
#define __GEO_H__

#include "server.h"

/* Structures used inside geo.c in order to represent points and array of
 * points on the earth. */
/*
 * 地理位置数据
 */
typedef struct geoPoint {
    //经度
    double longitude;
    //维度
    double latitude;
    //距离
    double dist;
    //分数
    double score;
    //名称
    char *member;
} geoPoint;

/*
 * 地理位置数组
 */
typedef struct geoArray {
    struct geoPoint *array;
    size_t buckets;
    size_t used;
} geoArray;

#endif
