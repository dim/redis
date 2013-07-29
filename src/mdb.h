#include <lmdb.h>
#include "rio.h"

struct mdbConfigStruct {
    int enabled;      /* Boolean indicator */
    size_t mapsize;   /* Mapsize, configurable */
    int dbid;         /* Redis internal DBID */
    MDB_env *env;     /* Runtime MDB environment */
    MDB_dbi maindb;   /* Main DB ID */
    MDB_txn *txn;     /* Reusable RO transaction */
    MDB_cursor *cur;  /* Reusable RO cursor */
    MDB_cursor *xmc;  /* Active expiration cursor */
    MDB_val xmk;      /* Active expiration, last key */
    struct redisCommand *mmdelCommand;
    robj *mmdel;
};

typedef struct redisMdbObject {
    unsigned int type;
    unsigned int encoding;
    int64_t expireat;
    uint32_t len;
    robj *o;
} rmobj;

/* Global shared config */
struct mdbConfigStruct mdbc;

/* Prototypes */
int mdbRdbLoad(rio *rdb, long loops);
int mdbRdbSave(rio *rdb, long long now);
int mdbEnvOpen(void);
void mdbEnvClose(void);
void mdbInitConfig(void);
void mdbInit(void);
void mdbCron(void);
void mdbFlushdbCommand(redisClient *c);
void mdbDbsizeCommand(redisClient *c);
void mdbInfoCommand(redisClient *c);
void mdbDebugCommand(redisClient *c);
void mdbKeysCommand(redisClient *c);
void mdbGetCommand(redisClient *c);
void mdbSetCommand(redisClient *c);
void mdbDelCommand(redisClient *c);
void mdbExistsCommand(redisClient *c);
void mdbTypeCommand(redisClient *c);
void mdbStrlenCommand(redisClient *c);
void mdbIncrbyCommand(redisClient *c);
void mdbTtlCommand(redisClient *c);
void mdbPttlCommand(redisClient *c);
void mdbExpireCommand(redisClient *c);
void mdbExpireatCommand(redisClient *c);
void mdbPexpireCommand(redisClient *c);
void mdbPexpireatCommand(redisClient *c);
void mdbAppendCommand(redisClient *c);
void mdbGetrangeCommand(redisClient *c);