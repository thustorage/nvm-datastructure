#ifndef __SET_H__
#define __SET_H__

/*************************************
 * INTERNAL DEFINITIONS 应该是这个 */

/* Internal key values with special meanings. */
#define INVALID_FIELD   (0)    /* Uninitialised field value.     */
#define SENTINEL_KEYMIN (1UL)  /* Key value of first dummy node. */
#define SENTINEL_KEYMAX (~0UL) /* Key value of last dummy node.  */
#define BLIST_SPLIT_KEY (2UL)

/* Used internally be set access functions, so that callers can use
 * key values 0 and 1, without knowing these have special meanings. */
#define CALLER_TO_INTERNAL_KEY(_k) ((_k) + 3)

/* SUPPORT FOR WEAK ORDERING OF MEMORY ACCESSES */
#ifdef WEAK_MEM_ORDER

/* Read field @_f into variable @_x. */
#define READ_FIELD(_x,_f)                                       \
do {                                                            \
    (_x) = (_f);                                                \
    if ( (_x) == INVALID_FIELD ) { RMB(); (_x) = (_f); }        \
    assert((_x) != INVALID_FIELD);                              \
} while ( 0 )

#else

/* Read field @_f into variable @_x. */
#define READ_FIELD(_x,_f) ((_x) = (_f))

#endif

/*************************************
 * PUBLIC DEFINITIONS */

/* Key range accepted by set functions.
 * We lose three values (conveniently at top end of key space).
 *  - Known invalid value to which all fields are initialised.
 *  - Sentinel key values for up to two dummy nodes. */
#define KEY_MIN  ( 0U)
#define KEY_MAX  ((~0U) - 3)

#endif /* __SET_H__ */
