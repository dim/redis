#include "redis.h"
#include "endianconv.h"
#include <sys/stat.h>
#include <fcntl.h>

#define REDIS_MMSET_NO_FLAGS 0
#define REDIS_MMSET_NX (1<<0)     /* Set if key not exists. */
#define REDIS_MMSET_XX (1<<1)     /* Set if key exists. */
#define REDIS_MDB_ROLLBACK (MDB_LAST_ERRCODE+1)
#define REDIS_MDB_CHUNKSIZE 1024

#if (BYTE_ORDER == LITTLE_ENDIAN)
#define htonll(v) intrev64(v)
#define ntohll(v) intrev64(v)
#else
#define htonll(v) (v)
#define ntohll(v) (v)
#endif

long long rdbLoadMillisecondTime(rio *rdb);

/*================================ Serialization =============================== */

static sds mdbDumpObject(robj *o, int64_t expireat) {
    unsigned char buf[16];
    sds res;
    size_t buflen;
    uint32_t objlen = sdslen(o->ptr);

    /* We only support raw strings for now */
    redisAssert(o != NULL);
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    redisAssertWithInfo(NULL,o,o->encoding == REDIS_ENCODING_RAW);

    /* Save a nib holding type + encoding */
    buf[0] = (o->type << 4) + o->encoding;
    buflen = 1;

    /* Save expiration time */
    if (expireat > -1) {
        int64_t nhx = (htonll(expireat)>>16)|0x80;
        memcpy(&buf[buflen],&nhx,6);
        buflen+=6;
    } else {
        buf[buflen] = 0;
        buflen+=1;
    }

    res = sdsnewlen(buf,buflen);
    return sdscatlen(res,o->ptr,objlen);
}

static void mdbLoadObject(MDB_val *mv, rmobj *mo, int shallow) {
    unsigned char *p = (unsigned char*) mv->mv_data;

    /* Read nib holding type + encoding */
    mo->type = (*p >> 4) & 0x0F;
    mo->encoding = *p & 0x0F;
    mo->len = mv->mv_size;
    p+=1; mo->len-=1;

    /* We only support raw strings for now */
    redisAssertWithInfo(NULL,NULL,mo->type == REDIS_STRING);
    redisAssertWithInfo(NULL,NULL,mo->encoding == REDIS_ENCODING_RAW);

    /* Read expiration time */
    if ((*p&0x80)>>7) {
        int64_t nhx = 0;
        memcpy(&nhx,p,6);
        p+=6; mo->len-=6;
        mo->expireat = ntohll((nhx&0xffffffffffffff7f)<<16);
    } else {
        p+=1; mo->len-=1;
        mo->expireat = -1;
    }

    if (shallow) {
        mo->o = NULL;
    } else {
        mo->o = createObject(REDIS_STRING,sdsnewlen(p,mo->len));
    }
}

/*================================= Helpers ================================= */

static void mdbAddReplyError(redisClient *c, int rc) {
    redisLog(REDIS_WARNING, "MDB: %s (on '%s')",
        mdb_strerror(rc), (char*)c->argv[0]->ptr);
    addReplyError(c,mdb_strerror(rc));
}

static void mdbReadOnlyTransaction(redisClient *c, int (*fun)(redisClient*)) {
    int rc = MDB_SUCCESS;
    if ((rc = mdb_txn_renew(mdbc.txn)) != MDB_SUCCESS)
        goto mdberr;
    if ((rc = mdb_cursor_renew(mdbc.txn, mdbc.cur)) != MDB_SUCCESS)
        goto mdberr;
    if ((rc = fun(c)) != MDB_SUCCESS)
        goto mdberr;
    goto cleanup;
mdberr:
    if (rc != REDIS_MDB_ROLLBACK) mdbAddReplyError(c,rc);
cleanup:
    mdb_txn_reset(mdbc.txn);
}

static void mdbReadWriteTransaction(redisClient *c, int (*fun)(redisClient*, rmtxn*)) {
    rmtxn txn = {0, NULL};
    int rc = MDB_SUCCESS;

    if ((rc = mdb_txn_begin(mdbc.env, NULL, 0, &txn.t)) != MDB_SUCCESS)
        goto mdberr;
    if ((rc = fun(c, &txn)) != MDB_SUCCESS)
        goto mdberr;
    return;
mdberr:
    if (!txn.committed) mdb_txn_abort(txn.t);
    if (rc != REDIS_MDB_ROLLBACK) mdbAddReplyError(c,rc);
}

static int mdbRedisExists(MDB_txn *txn, sds key) {
    MDB_val mk = {sdslen(key),key}, mv;
    return mdb_get(txn, mdbc.maindb, &mk, &mv) == MDB_SUCCESS;
}

static void mdbPropagateExpire(MDB_val *mk) {
    if (mdbc.slave_expiration) return;

    robj *argv[2];
    argv[0] = mdbc.mmdel;
    argv[1] = createObject(REDIS_STRING,sdsnewlen(mk->mv_data,mk->mv_size));
    propagate(mdbc.mmdelCommand,0,argv,2,REDIS_PROPAGATE_REPL|REDIS_PROPAGATE_AOF);
    decrRefCount(argv[1]);
}

static int mdbRedisFind(sds key, rmobj *mo, int shallow) {
    int rc = MDB_SUCCESS;
    MDB_val mk = {sdslen(key),key}, mv;

    switch (rc = mdb_cursor_get(mdbc.cur, &mk, &mv, MDB_SET)) {
    case MDB_SUCCESS:
        mdbLoadObject(&mv, mo, shallow);
        return MDB_SUCCESS;
    default:
        return rc;
    }
}

static int mdbRedisFindOrReply(redisClient *c, sds key, rmobj *mo, int shallow, robj *reply) {
    int rc = mdbRedisFind(key,mo,shallow);
    if (rc == MDB_NOTFOUND) {
        addReply(c,reply);
        return REDIS_MDB_ROLLBACK;
    }
    return rc;
}

static int mdbRedisGet(MDB_txn *txn, sds key, rmobj *mo, int shallow) {
    int rc = MDB_SUCCESS;
    MDB_val mk = {sdslen(key),key}, mv;

    switch (rc = mdb_get(txn, mdbc.maindb, &mk, &mv)) {
    case MDB_SUCCESS:
        mdbLoadObject(&mv, mo, shallow);
        return MDB_SUCCESS;
    default:
        return rc;
    }
}

static int mdbRedisPut(MDB_txn *txn, sds key, robj *val, int64_t expireat) {
    int rc;
    sds raw = mdbDumpObject(val,expireat);
    MDB_val mk = {sdslen(key), key};
    MDB_val mv = {sdslen(raw), raw};

    rc = mdb_put(txn,mdbc.maindb,&mk,&mv,0);
    sdsfree(raw);
    return rc;
}

static int mdbRedisCommit(rmtxn *txn) {
    txn->committed = 1;
    return mdb_txn_commit(txn->t);
}

static char *mdbStrType(int type) {
    switch(type) {
    case REDIS_STRING: return "string";
    default: return "unknown";
    }
}

/*================================= RDB Additions ================================= */

static int mdbRdbSaveBody(void *ceo, size_t size, size_t total, char *ptr) {
    rio *rdb = (rio*) ceo;
    ssize_t chunksize = REDIS_MDB_CHUNKSIZE;
    REDIS_NOTUSED(total);

    while (size > 0) {
        if (size < chunksize) chunksize = size;
        if (rioWrite(rdb,ptr,chunksize) == 0) return errno;
        ptr += chunksize;
        size -= chunksize;
    }

    return 0;
}

static int mdbRdbSaveHeader(void *ceo, size_t size, size_t total, char *ptr) {
    rio *rdb = (rio*) ceo;
    size_t prefix = total;
    memrev64ifbe(&prefix);
    if (rioWrite(rdb,&prefix,8) == 0) return errno;

    return mdbRdbSaveBody(ceo, size, total, ptr);
}

int mdbRdbSave(rio *rdb, long long now) {
    int rc = MDB_SUCCESS;

    /* Skip if MDB is disabled or server is a slave */
    if (!mdbc.enabled || server.masterhost != NULL)
        return REDIS_OK;

    /* Write opcode & dbid */
    if (rdbSaveType(rdb,REDIS_RDB_OPCODE_SELECTDB) == -1) goto saverr;
    if (rdbSaveLen(rdb,mdbc.dbid) == -1) goto saverr;

    /* Copy env to RDB */
    rc = mdb_env_copycb(mdbc.env, rdb, mdbRdbSaveHeader, mdbRdbSaveBody);
    if (rc != MDB_SUCCESS) {
        redisLog(REDIS_WARNING,"MDB: RDB copying failed: %s",mdb_strerror(rc));
        goto saverr;
    }

    redisLog(REDIS_VERBOSE,"MDB: RDB saving complete");
    return REDIS_OK;
saverr:
    redisLog(REDIS_WARNING,"MDB: RDB saving failed");
    return REDIS_ERR;
}

int mdbRdbLoad(rio *rdb, long loops) {

    /* Skip loading if server is a master, or MDB is disabled
     * still run through the file for correct checksum */
    int perform = mdbc.enabled && server.masterhost != NULL;
    uint64_t size, chunksize;
    char buffer[REDIS_MDB_CHUNKSIZE], datafile[8] = "data.mdb";
    int fd = -1;

    /* Read data length */
    if (rioRead(rdb,&size,8) == 0) goto rerr;
    memrev64ifbe(&size);

    /* Close env, open data file */
    if (perform) {
        mdbEnvClose();
        if ((fd = open(datafile,O_CREAT|O_WRONLY,0644)) == -1) {
            redisLog(REDIS_WARNING,"MDB: Opening data file failed: %s",strerror(errno));
            goto rerr;
        }
    }

    /* Write temp file */
    while (size > 0) {
        /* Serve the clients from time to time */
        if (!(loops++ % 10)) {
            loadingProgress(rioTell(rdb));
            aeProcessEvents(server.el, AE_FILE_EVENTS|AE_DONT_WAIT);
        }

        chunksize = REDIS_MDB_CHUNKSIZE;
        if (chunksize > size) chunksize = size;
        if (rioRead(rdb,buffer,chunksize) == 0) goto rerr;
        if (perform && write(fd,buffer,chunksize) == 0) goto rerr;
        size -= chunksize;
    }

    /* EOF should follow in the very end */
    if (rdbLoadType(rdb) != REDIS_RDB_OPCODE_EOF) goto rerr;

    /* Re-open env */
    if (perform) {
        close(fd);
        if (mdbEnvOpen() != MDB_SUCCESS) goto rerr;
    }

    redisLog(REDIS_VERBOSE,"MDB: RDB load complete");
    return REDIS_OK;
rerr:
    close(fd);
    redisLog(REDIS_WARNING,"MDB: RDB loading failed");
    return REDIS_ERR;
}

/*================================= Callbacks ================================ */

/* Open the environment, configure shared objects */
int mdbEnvOpen(void) {
    if (!mdbc.enabled) return MDB_SUCCESS;

    MDB_txn *txn = NULL;
    int rc = mdb_env_create(&mdbc.env);
    if (!rc) rc = mdb_env_set_mapsize(mdbc.env, mdbc.mapsize);
    if (!rc) rc = mdb_env_set_maxreaders(mdbc.env, server.maxclients);
    if (!rc) rc = mdb_env_set_maxdbs(mdbc.env, 1);
    if (!rc) rc = mdb_env_open(mdbc.env, ".", MDB_NOSYNC, 0644);
    if (!rc) rc = mdb_txn_begin(mdbc.env, NULL, 0, &txn);
    if (!rc) rc = mdb_dbi_open(txn, NULL, MDB_CREATE, &mdbc.maindb);
    if (!rc) {
        rc = mdb_txn_commit(txn);
        txn = NULL;
    }
    if (txn) mdb_txn_abort(txn);

    if (!rc) rc = mdb_txn_begin(mdbc.env, NULL, MDB_RDONLY, &mdbc.txn);
    if (!rc && mdbc.cur == NULL)
        rc = mdb_cursor_open(mdbc.txn, mdbc.maindb, &mdbc.cur);
    if (!rc && mdbc.xmc == NULL)
        rc = mdb_cursor_open(mdbc.txn, mdbc.maindb, &mdbc.xmc);
    if (mdbc.txn) mdb_txn_reset(mdbc.txn);

    return rc;
}

/* Close & free the environment */
void mdbEnvClose(void) {
    if (!mdbc.enabled) return;

    mdb_txn_abort(mdbc.txn);
    mdbc.txn = NULL;
    if (mdbc.maindb > 0) mdb_dbi_close(mdbc.env, mdbc.maindb);
    mdbc.maindb = 0;
    if (mdbc.env) mdb_env_close(mdbc.env);
    mdbc.env = NULL;
}

/* Active expiration iteration for MDB keys */
static int mdbActiveExpireRun(void) {
    int rc = MDB_SUCCESS, expired = 0;
    long long now = mstime();
    long num = MDB_EXPIRE_CYCLE_LOOKUPS_PER_LOOP;
    int lookup = MDB_SET_RANGE;
    MDB_val mk = {sdslen(mdbc.xlk), mdbc.xlk}, mv;
    MDB_txn *txn = NULL;

    if ((rc = mdb_txn_renew(mdbc.txn)) != MDB_SUCCESS)
        goto mdberr;
    if ((rc = mdb_cursor_renew(mdbc.txn, mdbc.xmc)) != MDB_SUCCESS)
        goto mdberr;

    while (num--) {
        rmobj mo;

        /* Start with a 'set-range' lookup, then use 'next' */
        rc = mdb_cursor_get(mdbc.xmc,&mk,&mv,lookup);
        if (rc == MDB_NOTFOUND) {     /* no (more) keys left, rewind */
            sdsfree(mdbc.xlk);
            mdbc.xlk = sdsnewlen("\0", 1);
            goto cleanup;
        } else if (rc != MDB_SUCCESS) /* something went wrong */
            goto mdberr;

        lookup = MDB_NEXT;
        mdbLoadObject(&mv,&mo,1);

        if (mo.expireat > -1 && now > mo.expireat) {
            if (txn == NULL && (rc = mdb_txn_begin(mdbc.env,NULL,0,&txn)) != MDB_SUCCESS)
                goto mdberr;
            if ((rc = mdb_del(txn,mdbc.maindb,&mk,NULL)) != MDB_SUCCESS)
                goto mdberr;

            mdbPropagateExpire(&mk);
            expired++;
        }
    }

    /* move the cursor and store next key */
    if (num < 0) {
        mdb_cursor_get(mdbc.xmc,&mk,&mv,MDB_NEXT);
        sdsfree(mdbc.xlk);
        mdbc.xlk = sdsnewlen(mk.mv_data, mk.mv_size);
    }

    goto cleanup;
mdberr:
    redisLog(REDIS_WARNING, "MDB: %s", mdb_strerror(rc));
cleanup:
    if (txn) mdb_txn_commit(txn);
    mdb_txn_reset(mdbc.txn);
    return expired;
}

/* Active expiration iteration for MDB keys */
void mdbActiveExpireCycle(void) {
    /* Some global state */
    static long long last_cycle = 0; /* When last cycle ran. */
    static long long last_update = 0; /* When interval was last updated. */
    static unsigned long long interval = MDB_EXPIRE_CYCLE_DEFAULT_INTERVAL;
    static unsigned int exceedance = 1; /* Rate at which runtime exceeds time limit */

    unsigned int expired = 0;
    long long start = ustime(), timelimit = 0, runtime = 0;

    /* Don't start a cycle unless due */
    if (start < last_cycle + interval) return;
    last_cycle = start;

    /* Ten cycles after last update */
    if (start > last_update + interval*10) {
        /* Increase/decrease interval based on last performances */
        if (exceedance > 14) interval = interval * 1.2;
        else if (exceedance < 4) interval = interval * 0.9;

        if (interval > MDB_EXPIRE_CYCLE_MAXIMUM_INTERVAL)
            interval = MDB_EXPIRE_CYCLE_MAXIMUM_INTERVAL;
        if (interval < MDB_EXPIRE_CYCLE_MINIMUM_INTERVAL)
            interval = MDB_EXPIRE_CYCLE_MINIMUM_INTERVAL;

        exceedance = 1;
        last_update = start;
    }

    /* Determine time limit */
    timelimit = 1000000*ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC/server.hz/100;
    if (timelimit <= 0) timelimit = 1;

    do {
        expired = mdbActiveExpireRun();
        runtime = (ustime()-start);
    } while (expired > MDB_EXPIRE_CYCLE_LOOKUPS_PER_LOOP/4 && runtime < timelimit);

    /* Increase exceedance, if runtime exceeded time limit */
    if (runtime/timelimit > exceedance)
        exceedance = runtime/timelimit;
}

/* Init config */
void mdbInitConfig(void) {
    mdbc.enabled = 0;
    mdbc.slave_expiration = 0;
    mdbc.env = NULL;
    mdbc.txn = NULL;
    mdbc.cur = NULL;
    mdbc.xmc = NULL;
    mdbc.xlk = sdsnewlen("\0", 1);
    mdbc.maindb = 0;
    mdbc.dbid = 16381;
    mdbc.mapsize = (32LL*1024*1024)-1; /* 32M */
    mdbc.mmdelCommand = lookupCommandByCString("mmdel");
    mdbc.mmdel = createStringObject("MMDEL",5);
}

/* Init mdb */
void mdbInit(void) {
    /* If disabled, delete all commands and return */
    if (!mdbc.enabled) {
        dictIterator *di = dictGetSafeIterator(server.commands);
        dictEntry *de;
        int retval;

        while((de = dictNext(di)) != NULL) {
            sds cmd = dictGetKey(de);
            if (memcmp(cmd,"mm",2) == 0) {
                retval = dictDelete(server.commands, cmd);
                redisAssert(retval == DICT_OK);
            }
        }
        dictReleaseIterator(di);
        return;
    }

    int rc = mdbEnvOpen();
    if (rc != 0) {
        redisLog(REDIS_WARNING, "MDB: %s", mdb_strerror(rc));
        exit(1);
    }
}

/* MDB background tasks */
void mdbCron(void) {
    /* Skip if MDB is disabled */
    if (!mdbc.enabled) return;

    /* Perform active expiration if enabled */;
    if (server.active_expire_enabled && (server.masterhost == NULL || mdbc.slave_expiration))
        mdbActiveExpireCycle();
}

/*================================= Commands ================================= */

int mdbDbsize(redisClient *c) {
    MDB_stat stat;
    int rc = mdb_stat(mdbc.txn, mdbc.maindb, &stat);
    if (!rc)
        addReplyLongLong(c,stat.ms_entries);
    return rc;
}

int mdbFlushdb(redisClient *c, rmtxn *txn) {
    int rc;
    MDB_stat stat;

    if ((rc = mdb_stat(txn->t, mdbc.maindb, &stat)) != MDB_SUCCESS)
        return rc;
    if ((rc = mdb_drop(txn->t, mdbc.maindb, 0)) != MDB_SUCCESS)
        return rc;
    if ((rc = mdbRedisCommit(txn)) != MDB_SUCCESS)
        return rc;

    server.dirty += stat.ms_entries;
    addReply(c,shared.ok);
    return MDB_SUCCESS;
}

int mdbInfo(redisClient *c) {
    MDB_stat stat;
    MDB_envinfo info;
    int rc;

    if ((rc = mdb_stat(mdbc.txn, mdbc.maindb, &stat)) != MDB_SUCCESS)
        return rc;
    if ((rc = mdb_env_info(mdbc.env, &info)) != MDB_SUCCESS)
        return rc;

    addReplyStatusFormat(c,
        "MDBInfo mapsize:%zu readers:%u/%u main:%zu",
        info.me_mapsize, info.me_numreaders, info.me_maxreaders, stat.ms_entries);
    return MDB_SUCCESS;
}

int mdbDebugObject(redisClient *c) {
    rmobj mo;
    int rc = mdbRedisFindOrReply(c,c->argv[2]->ptr,&mo,1,shared.nokeyerr);
    if (!rc)
        addReplyStatusFormat(c,
            "Value type:%s encoding:%s "
            "length:%" PRIu32 " expiration:%" PRId64,
            mdbStrType(mo.type), strEncoding(mo.encoding),
            mo.len, mo.expireat);
    return rc;
}

int mdbKeys(redisClient *c) {
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern),
        rc = MDB_SUCCESS,
        allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    while (1) {
        MDB_val mk;
        rc = mdb_cursor_get(mdbc.cur,&mk,NULL,MDB_NEXT);
        if (rc != MDB_SUCCESS) break;

        if (allkeys || stringmatchlen(pattern,plen,(char *)mk.mv_data,mk.mv_size,0)) {
            numkeys++;
            addReplyBulkCBuffer(c, mk.mv_data, mk.mv_size);
        }
    }
    setDeferredMultiBulkLength(c,replylen,numkeys);
    return MDB_SUCCESS;
}

int mdbGet(redisClient *c) {
    rmobj mo;
    int rc = mdbRedisFindOrReply(c,c->argv[1]->ptr,&mo,0,shared.nullbulk);
    if (!rc) {
        addReplyBulk(c,mo.o);
        decrRefCount(mo.o);
    }
    return rc;
}

int mdbSet(redisClient *c, rmtxn *txn) {
    int j;
    robj *expire = NULL;
    long long expiretime = -1;
    int unit = UNIT_SECONDS;
    int flags = REDIS_MMSET_NO_FLAGS;

    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_MMSET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0') {
            flags |= REDIS_MMSET_XX;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && next) {
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            addReply(c,shared.syntaxerr);
            return REDIS_MDB_ROLLBACK;
        }
    }

    if (expire) {
        if (getLongLongFromObjectOrReply(c,expire,&expiretime,NULL) != REDIS_OK ||
            expiretime <= 0) {
            addReplyError(c,"invalid expire time");
            return REDIS_MDB_ROLLBACK;
        }
        if (unit == UNIT_SECONDS) expiretime *= 1000;
        expiretime += mstime();
    }

    if ((flags & REDIS_MMSET_NX && mdbRedisExists(txn->t, c->argv[1]->ptr)) ||
        (flags & REDIS_MMSET_XX && !mdbRedisExists(txn->t, c->argv[1]->ptr)))
    {
        addReply(c, shared.nullbulk);
        return REDIS_MDB_ROLLBACK;
    }

    int rc = mdbRedisPut(txn->t, c->argv[1]->ptr, c->argv[2], expiretime);
    if (!rc) rc = mdbRedisCommit(txn);
    if (rc) return rc;

    addReply(c,shared.ok);
    server.dirty++;
    return MDB_SUCCESS;
}

int mdbDel(redisClient *c, rmtxn *txn) {
    int deleted = 0, j, rc;

    for (j = 1; j < c->argc; j++) {
        sds key = c->argv[j]->ptr;
        MDB_val mk = {sdslen(key), key};

        switch(rc = mdb_del(txn->t, mdbc.maindb, &mk, NULL)) {
        case 0: deleted++; break;
        case MDB_NOTFOUND: break;
        default: return rc;
        }
    }
    if ((rc = mdbRedisCommit(txn)) != 0)
        return rc;

    server.dirty += deleted;
    addReplyLongLong(c,deleted);
    return MDB_SUCCESS;
}

int mdbExists(redisClient *c) {
    if (mdbRedisExists(mdbc.txn,c->argv[1]->ptr)) {
        addReply(c, shared.cone);
    } else {
        addReply(c, shared.czero);
    }
    return MDB_SUCCESS;
}

int mdbType(redisClient *c) {
    rmobj mo;
    char *type;
    int rc = mdbRedisFind(c->argv[1]->ptr,&mo,1);

    switch (rc) {
    case MDB_SUCCESS: type = mdbStrType(mo.type); break;
    case MDB_NOTFOUND: type = "none"; break;
    default: return rc;
    }

    addReplyStatus(c,type);
    return MDB_SUCCESS;
}

int mdbStrlen(redisClient *c) {
    rmobj mo;
    int rc = mdbRedisFindOrReply(c,c->argv[1]->ptr,&mo,1,shared.czero);
    if (!rc) addReplyLongLong(c,mo.len);
    return rc;
}

int mdbIncrby(redisClient *c, rmtxn *txn) {
    long long incr, oldval, newval;
    int rc;
    rmobj old;
    robj *new;

    if (getLongLongFromObjectOrReply(c,c->argv[2],&incr, NULL) != REDIS_OK)
        return REDIS_MDB_ROLLBACK;

    switch (rc = mdbRedisGet(txn->t,c->argv[1]->ptr,&old,0)) {
    case MDB_SUCCESS:
        if (getLongLongFromObjectOrReply(c,old.o,&oldval,NULL) != REDIS_OK) {
            decrRefCount(old.o);
            return REDIS_MDB_ROLLBACK;
        }
        decrRefCount(old.o);
        break;
    case MDB_NOTFOUND:
        oldval = 0;
        old.expireat = -1;
        break;
    default:
        return rc;
    }

    if ((incr < 0 && oldval < 0 && incr < (LLONG_MIN-oldval)) ||
        (incr > 0 && oldval > 0 && incr > (LLONG_MAX-oldval))) {
        addReplyError(c,"increment or decrement would overflow");
        return REDIS_MDB_ROLLBACK;
    }

    newval = oldval + incr;
    new  = createObject(REDIS_STRING,sdsfromlonglong(newval));
    rc = mdbRedisPut(txn->t,c->argv[1]->ptr,new,old.expireat);
    decrRefCount(new);

    if (!rc) rc = mdbRedisCommit(txn);
    if (rc) return rc;

    addReplyLongLong(c,newval);
    server.dirty++;
    return MDB_SUCCESS;
}

int mdbTtlGeneric(redisClient *c, int output_ms) {
    int64_t ttl = -1;
    rmobj mo;

    int rc = mdbRedisFindOrReply(c,c->argv[1]->ptr,&mo,1,shared.cnegone);
    if (!rc) {
        if (mo.expireat != -1) {
            ttl = mo.expireat-mstime();
            if (ttl < 0) ttl = -1;
        }
        if (ttl == -1) {
            addReplyLongLong(c,-1);
        } else {
            addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
        }
    }
    return rc;
}
int mdbTtl(redisClient *c) {
    return mdbTtlGeneric(c, 0);
}
int mdbPttl(redisClient *c) {
    return mdbTtlGeneric(c, 1);
}

int mdbExpireGeneric(redisClient *c, rmtxn *txn, long long basetime, int unit) {
    long long when; /* unix time in milliseconds when the key will expire. */
    rmobj mo;
    int rc;

    if (getLongLongFromObjectOrReply(c,c->argv[2],&when,NULL) != REDIS_OK)
        return REDIS_MDB_ROLLBACK;

    rc = mdbRedisGet(txn->t,c->argv[1]->ptr,&mo,0);
    if (rc == MDB_NOTFOUND) {
        addReply(c,shared.czero);
        return REDIS_MDB_ROLLBACK;
    } else if (rc != MDB_SUCCESS)
        return rc;

    if (unit == UNIT_SECONDS) when *= 1000;
    mo.expireat = when + basetime;

    rc = mdbRedisPut(txn->t,c->argv[1]->ptr,mo.o,mo.expireat);
    decrRefCount(mo.o);

    if (!rc) rc = mdbRedisCommit(txn);
    if (!rc) {
        addReply(c,shared.cone);
        server.dirty++;
    }
    return rc;
}
int mdbExpire(redisClient *c, rmtxn *txn) {
    return mdbExpireGeneric(c,txn,mstime(),UNIT_SECONDS);
}
int mdbExpireat(redisClient *c, rmtxn *txn) {
    return mdbExpireGeneric(c,txn,0,UNIT_SECONDS);
}
int mdbPexpire(redisClient *c, rmtxn *txn) {
    return mdbExpireGeneric(c,txn,mstime(),UNIT_MILLISECONDS);
}
int mdbPexpireat(redisClient *c, rmtxn *txn) {
    return mdbExpireGeneric(c,txn,0,UNIT_MILLISECONDS);
}

int mdbAppend(redisClient *c, rmtxn *txn) {
    rmobj mo;
    int rc = MDB_SUCCESS;
    size_t totlen = 0;

    switch (rc = mdbRedisGet(txn->t,c->argv[1]->ptr,&mo,0)) {
    case MDB_SUCCESS:
        mo.o->ptr = sdscatlen(mo.o->ptr,c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
        rc = mdbRedisPut(txn->t,c->argv[1]->ptr,mo.o,mo.expireat);
        totlen = stringObjectLen(mo.o);
        decrRefCount(mo.o);
        break;
    case MDB_NOTFOUND:
        rc = mdbRedisPut(txn->t,c->argv[1]->ptr,c->argv[2],-1);
        totlen = stringObjectLen(c->argv[2]);
        break;
    default: return rc;
    }

    if (!rc) rc = mdbRedisCommit(txn);
    if (!rc) {
        server.dirty++;
        addReplyLongLong(c,totlen);
    }
    return rc;
}

int mdbGetrange(redisClient *c) {
    long start, end;
    int rc;
    rmobj mo;
    size_t slen;

    if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK)
        return REDIS_MDB_ROLLBACK;
    if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK)
        return REDIS_MDB_ROLLBACK;

    rc = mdbRedisFindOrReply(c,c->argv[1]->ptr,&mo,0,shared.emptybulk);
    if (rc != MDB_SUCCESS) return rc;

    slen = sdslen(mo.o->ptr);

    /* Convert negative indexes */
    if (start < 0) start = slen+start;
    if (end < 0) end = slen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned)end >= slen) end = slen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end) {
        addReply(c,shared.emptybulk);
    } else {
        addReplyBulkCBuffer(c,(char*)mo.o->ptr+start,end-start+1);
    }

    decrRefCount(mo.o);
    return MDB_SUCCESS;
}

/*============================= Command Wrappers ============================= */

void mdbFlushdbCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbFlushdb);
}
void mdbDbsizeCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbDbsize);
}
void mdbInfoCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbInfo);
}
void mdbDebugCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"object") && c->argc == 3) {
        mdbReadOnlyTransaction(c,mdbDebugObject);
    } else {
        addReplyErrorFormat(c, "Unknown MMDEBUG subcommand or wrong number of arguments for '%s'",
            (char*)c->argv[1]->ptr);
    }
}
void mdbKeysCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbKeys);
}
void mdbGetCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbGet);
}
void mdbSetCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbSet);
}
void mdbDelCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbDel);
}
void mdbExistsCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbExists);
}
void mdbTypeCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbType);
}
void mdbStrlenCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbStrlen);
}
void mdbIncrbyCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbIncrby);
}
void mdbTtlCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbTtl);
}
void mdbPttlCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbPttl);
}
void mdbExpireCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbExpire);
}
void mdbExpireatCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbExpireat);
}
void mdbPexpireCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbPexpire);
}
void mdbPexpireatCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbPexpireat);
}
void mdbAppendCommand(redisClient *c) {
    mdbReadWriteTransaction(c,mdbAppend);
}
void mdbGetrangeCommand(redisClient *c) {
    mdbReadOnlyTransaction(c,mdbGetrange);
}
