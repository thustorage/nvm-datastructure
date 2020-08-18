#ifndef	_UTILS_H_
#define	_UTILS_H_

#include <stdbool.h>
#include <inttypes.h>
#include <limits.h>
#include <assert.h>

#ifndef __predict_true
#define	__predict_true(x)	__builtin_expect((x) != 0, 1)
#define	__predict_false(x)	__builtin_expect((x) != 0, 0)
#endif

#ifndef __constructor
#define	__constructor		__attribute__((constructor))
#endif

#ifndef __packed
#define	__packed		__attribute__((__packed__))
#endif

#ifndef __aligned
#define	__aligned(x)		__attribute__((__aligned__(x)))
#endif

#ifndef __unused
#define	__unused		__attribute__((__unused__))
#endif

#ifndef __arraycount
#define	__arraycount(__x)	(sizeof(__x) / sizeof(__x[0]))
#endif

#ifndef __UNCONST
#define	__UNCONST(a)		((void*)(const void*)a)
#endif

/*
 * Minimum, maximum and rounding macros.
 */

#ifndef MIN
#define	MIN(x, y)	((x) < (y) ? (x) : (y))
#endif

#ifndef MAX
#define	MAX(x, y)	((x) > (y) ? (x) : (y))
#endif

/*
 * Byte-order.
 */

#ifndef htobe64
#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define	htobe64(x)	OSSwapHostToBigInt64(x)
#endif
#ifdef __linux__
#define _BSD_SOURCE
#include <endian.h>
#endif
#endif

/*
 * A regular assert (debug/diagnostic only).
 */
#if !defined(ASSERT)
#define	ASSERT		assert
#else
#define	ASSERT(x)
#endif
#if defined(NOSMP)
#define	NOSMP_ASSERT	assert
#else
#define	NOSMP_ASSERT(x)
#endif

/*
 * Compile-time assertion: if C11 static_assert() is not available,
 * then emulate it.
 */
#ifndef static_assert
#ifndef CTASSERT
#define	CTASSERT(x)		__CTASSERT99(x, __INCLUDE_LEVEL__, __LINE__)
#define	__CTASSERT99(x, a, b)	__CTASSERT0(x, __CONCAT(__ctassert,a), \
					       __CONCAT(_,b))
#define	__CTASSERT0(x, y, z)	__CTASSERT1(x, y, z)
#define	__CTASSERT1(x, y, z)	typedef char y ## z[(x) ? 1 : -1] __unused
#endif
#define	static_assert(exp, msg)	CTASSERT(exp)
#endif

/*
 * Atomic operations and memory barriers.  If C11 API is not available,
 * then wrap the GCC builtin routines.
 */

#ifndef atomic_compare_exchange_weak
#define	atomic_compare_exchange_weak(ptr, expected, desired) \
    __sync_bool_compare_and_swap(ptr, expected, desired)
#endif
#ifndef atomic_exchange
static inline void *
atomic_exchange(volatile void *ptr, void *nptr)
{
	volatile void * volatile old;

	do {
		old = *(volatile void * volatile *)ptr;
	} while (!atomic_compare_exchange_weak(
	    (volatile void * volatile *)ptr, old, nptr));

	return (void *)(uintptr_t)old; // workaround for gcc warnings
}
#endif
#ifndef atomic_fetch_add
#define	atomic_fetch_add(x,a)	__sync_fetch_and_add(x, a)
#endif

#ifndef atomic_thread_fence
/*
 * memory_order_acquire	- membar_consumer/smp_rmb
 * memory_order_release	- membar_producer/smp_wmb
 */
#define	memory_order_acquire	__atomic_thread_fence(__ATOMIC_ACQUIRE)
#define	memory_order_release	__atomic_thread_fence(__ATOMIC_RELEASE)
#define	atomic_thread_fence(m)	m
#endif

/*
 * Exponential back-off for the spinning paths.
 */
#define	SPINLOCK_BACKOFF_MIN	4
#define	SPINLOCK_BACKOFF_MAX	128
#if defined(__x86_64__) || defined(__i386__)
#define SPINLOCK_BACKOFF_HOOK	__asm volatile("pause" ::: "memory")
#else
#define SPINLOCK_BACKOFF_HOOK
#endif
#define	SPINLOCK_BACKOFF(count)					\
do {								\
	int __i;						\
	for (__i = (count); __i != 0; __i--) {			\
		SPINLOCK_BACKOFF_HOOK;				\
	}							\
	if ((count) < SPINLOCK_BACKOFF_MAX)			\
		(count) += (count);				\
} while (/* CONSTCOND */ 0);

/*
 * Cache line size - a reasonable upper bound.
 */
#define	CACHE_LINE_SIZE		64

#endif
