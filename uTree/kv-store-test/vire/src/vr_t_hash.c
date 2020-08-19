#include <math.h>
#include "funcForC.h"
#include <vr_core.h>

#define USE_UTREE

void* bt = NULL;

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* Encode given objects in-place when the hash uses a dict. */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        if (o1) *o1 = tryObjectEncoding(*o1);
        if (o2) *o2 = tryObjectEncoding(*o2);
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;
    robj *field_new;

    ASSERT(o->encoding == OBJ_ENCODING_ZIPLIST);

    field_new = getDecodedObject(field);

    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        fptr = ziplistFind(fptr, field_new->ptr, sdslen(field_new->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            vptr = ziplistNext(zl, fptr);
            ASSERT(vptr != NULL);
        }
    }

    if (field_new != field) freeObject(field_new);

    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        ASSERT(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    ASSERT(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return -1;
    *value = dictGetVal(de);
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
robj *hashTypeGetObject(robj *o, robj *field) {
    robj *value = NULL;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            if (vstr) {
                value = createStringObject((char*)vstr, vlen);
            } else {
                value = createStringObjectFromLongLong(vll);
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
            value = aux;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return value;
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
size_t hashTypeGetValueLength(robj *o, robj *field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0)
            len = stringObjectLen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
int hashTypeExists(robj *o, robj *field) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add an element, discard the old if the key already exists.
 * Return 0 on insert and 1 on update.
 * This function will take care of dump the fields and value 
 * objects when insert into the hash table.
 * Filed and value objects just borrow to hashTypeSet(). */
int hashTypeSet(robj *o, robj *field, robj *value) {
    int update = 0;
    robj *field_new, *value_new;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        field_new = getDecodedObject(field);
        value_new = getDecodedObject(value);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field_new->ptr, sdslen(field_new->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                vptr = ziplistNext(zl, fptr);
                ASSERT(vptr != NULL);
                update = 1;

                /* Delete value */
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                zl = ziplistInsert(zl, vptr, value_new->ptr, sdslen(value_new->ptr));
            }
        }

        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            zl = ziplistPush(zl, field_new->ptr, sdslen(field_new->ptr), ZIPLIST_TAIL);
            zl = ziplistPush(zl, value_new->ptr, sdslen(value_new->ptr), ZIPLIST_TAIL);
        }
        o->ptr = zl;
        if (field_new != field) freeObject(field_new);
        if (value_new != value) freeObject(value_new);

        /* Check if the ziplist needs to be converted to a hash table */
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        field_new = dupStringObjectUnconstant(field);
        value_new = dupStringObjectUnconstant(value);
        if (dictReplace(o->ptr, field_new, value_new)) { /* Insert */
            /* Do nothing */
        } else { /* Update */
            update = 1;
            freeObject(field_new);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, robj *field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;
        robj *field_new;

        field_new = getDecodedObject(field);

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field_new->ptr, sdslen(field_new->ptr), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }

        if (field_new != field) freeObject(field_new);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == VR_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return length;
}

hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = dalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }

    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT) {
        dictReleaseIterator(hi->di);
    }

    dfree(hi);
}

/* Move to the next entry in the hash. Return VR_OK when the next entry
 * could be found and VR_ERROR when the iterator reaches the end. */
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            ASSERT(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            ASSERT(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return VR_ERROR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = ziplistNext(zl, fptr);
        ASSERT(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return VR_ERROR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return VR_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    ASSERT(hi->encoding == OBJ_ENCODING_ZIPLIST);

    if (what & OBJ_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        ASSERT(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        ASSERT(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    ASSERT(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        *dst = dictGetKey(hi->de);
    } else {
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). It is up
 * to the caller to decrRefCount() the object if no reference is retained. */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what) {
    robj *dst;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            dst = createStringObject((char*)vstr, vlen);
        } else {
            dst = createStringObjectFromLongLong(vll);
        }
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hashTypeCurrentFromHashTable(hi, what, &dst);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return dst;
}

robj *hashTypeLookupWriteOrCreate(client *c, robj *key, int *expired) {
    robj *o = lookupKeyWrite(c->db,key,expired);
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    } else {
        if (o->type != OBJ_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

void hashTypeConvertZiplist(robj *o, int enc) {
    ASSERT(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *d;
        int ret;

        hi = hashTypeInitIterator(o);
        d = dictCreate(&hashDictType, NULL);

        while (hashTypeNext(hi) != VR_ERROR) {
            robj *field, *value;

            field = hashTypeCurrentObject(hi, OBJ_HASH_KEY);
            field = tryObjectEncoding(field);
            value = hashTypeCurrentObject(hi, OBJ_HASH_VALUE);
            value = tryObjectEncoding(value);
            ret = dictAdd(d, field, value);
            if (ret != DICT_OK) {
                //serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                //    o->ptr,ziplistBlobLen(o->ptr));
                ASSERT(ret == DICT_OK);
            }
        }

        hashTypeReleaseIterator(hi);
        dfree(o->ptr);

        o->encoding = OBJ_ENCODING_HT;
        o->ptr = d;

    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

int64_t str2int64_t(const char *str){
    int64_t ans = 0;
    for (int i=0; str[i] != '\0'; i++){
        ans = 49891 * ans + str[i];
    }
    return ans;
}

void hsetCommand(client *c) {
    int update;
    robj *o;
    int expired = 0;
    log_debug(LOG_DEBUG,"enter hsetCommand");
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],&expired)) == NULL) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }
    hashTypeTryConversion(o,c->argv,2,3);
    hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);
    update = hashTypeSet(o,c->argv[2],c->argv[3]);
    addReply(c, update ? shared.czero : shared.cone);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    c->vel->dirty++;
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void hsetnxCommand(client *c) {
    robj *o;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],&expired)) == NULL) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }
    hashTypeTryConversion(o,c->argv,2,3);

    if (hashTypeExists(o, c->argv[2])) {
        addReply(c, shared.czero);
    } else {
        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);
        hashTypeSet(o,c->argv[2],c->argv[3]);
        addReply(c, shared.cone);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

int count = 0;

void hmsetCommand(client *c) {
    int i;
    robj *o;
    int expired = 0;
    //log_debug(LOG_DEBUG,"enter hmsetCommand");
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],&expired)) == NULL) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    hashTypeTryConversion(o,c->argv,2,c->argc-1);
#ifdef USE_UTREE
    if (bt == NULL){
        bt = cptree_init();
        log_debug(LOG_DEBUG, "alloc bt succeed! value of bt is %p", bt);
        log_debug(LOG_DEBUG, "bt->list_head is %p", cptree_getListHead(bt));
    }
    
    for (i = 2; i < c->argc; i += 2){
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        int64_t key = 999999999999991 * str2int64_t(c->argv[1]->ptr) + str2int64_t(c->argv[i]->ptr);
        //sds val = sdsdup(c->argv[i+1]->ptr);
        log_debug(LOG_DEBUG,"bt : %p, key : %d, val : %p", bt, key, (char*)key);
        cptree_insert(bt, key, (char*)key);
    }
    /*
    count++;
    if (count % 10000000 == 0){
        print_btree(bt);
    }*/
#else
    log_debug(LOG_DEBUG,"fieldcount : %d", (c->argc - 2) / 2);
    for (i = 2; i < c->argc; i += 2) {
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }
#endif
    addReply(c, shared.ok);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    c->vel->dirty++;

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;
    int expired = 0;

    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != VR_OK) return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],&expired)) == NULL) goto end;
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        if (getLongLongFromObjectOrReply(c,current,&value,
            "hash value is not an integer") != VR_OK) {
            if (o->encoding == OBJ_ENCODING_ZIPLIST) freeObject(current);
            goto end;
        }
        if (o->encoding == OBJ_ENCODING_ZIPLIST) freeObject(current);
    } else {
        value = 0;
    }

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        goto end;
    }
    value += incr;
    new = createStringObjectFromLongLong(value);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    hashTypeSet(o,c->argv[2],new);
    freeObject(new);
    addReplyLongLong(c,value);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    c->vel->dirty++;

end:
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
}

void hincrbyfloatCommand(client *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;
    int expired = 0;

    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != VR_OK) return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1],&expired)) == NULL) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
        return;
    }
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        if (getLongDoubleFromObjectOrReply(c,current,&value,
            "hash value is not a valid float") != VR_OK) {
            if (o->encoding == OBJ_ENCODING_ZIPLIST) freeObject(current);
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
            return;
        }
        if (o->encoding == OBJ_ENCODING_ZIPLIST) freeObject(current);
    } else {
        value = 0;
    }

    value += incr;
    new = createStringObjectFromLongDouble(value,1);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    hashTypeSet(o,c->argv[2],new);
    addReplyBulk(c,new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    c->vel->dirty++;

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("HSET",4);
    rewriteClientCommandArgument(c,0,aux);
    rewriteClientCommandArgument(c,3,new);
}

static void addHashFieldToReply(client *c, robj *o, robj *field) {
    int ret;

    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == OBJ_ENCODING_HT) {
        robj *value;

        ret = hashTypeGetFromHashTable(o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
}

void hgetCommand(client *c) {
    robj *o;
    log_debug(LOG_DEBUG,"enter hgetCommand");
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addHashFieldToReply(c, o, c->argv[2]);
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void hmgetCommand(client *c) {
    robj *o;
    int i;
    //log_debug(LOG_DEBUG,"enter hmgetCommand");
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
#ifdef USE_UTREE
    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]);

        int64_t key = 999999999999991 * str2int64_t(c->argv[1]->ptr) + str2int64_t(c->argv[i]->ptr);
        //log_debug(LOG_DEBUG,"bt : %p, key : %d", bt, key);
        cptree_search(bt, key);
        //addReplyBulkCBuffer(c, value, sdslen(value));
    }
#else
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL && o->type != OBJ_HASH) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        addReply(c, shared.wrongtypeerr);
        return;
    }

    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]);
    }

    if (o == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    }
#endif
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero,&expired)) == NULL ||
        checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j])) {
            deleted++;
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
    
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void hlenCommand(client *c) {
    robj *o;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addReplyLongLong(c,hashTypeLength(o));
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void hstrlenCommand(client *c) {
    robj *o;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]));

    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    } else if (hi->encoding == OBJ_ENCODING_HT) {
        robj *value;

        hashTypeCurrentFromHashTable(hi, what, &value);
        addReplyBulk(c, value);

    } else {
        serverPanic("Unknown hash encoding");
    }
}

void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;
    
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    if (flags & OBJ_HASH_KEY) multiplier++;
    if (flags & OBJ_HASH_VALUE) multiplier++;

    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != VR_ERROR) {
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);
    ASSERT(count == length);
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

void hgetallCommand(client *c) {
    log_debug(LOG_DEBUG,"enter hgetallCommand");
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

void hexistsCommand(client *c) {
    robj *o;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_HASH)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addReply(c, hashTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void hscanCommand(client *c) {
    scanGenericCommand(c,SCAN_TYPE_HASH);
}