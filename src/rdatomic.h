#pragma once

#include "tinycthread.h"

typedef struct {
	int32_t val;
#ifndef HAVE_ATOMICS_32
	mtx_t lock;
#endif
} rd_atomic32_t;

typedef struct {
	int64_t val;
#ifndef HAVE_ATOMICS_64
	mtx_t lock;
#endif
} rd_atomic64_t;


static RD_INLINE RD_UNUSED void rd_atomic32_init (rd_atomic32_t *ra, int32_t v) {
	ra->val = v;
#if !defined(_MSC_VER) && !defined(HAVE_ATOMICS_32)
	mtx_init(&ra->lock, mtx_plain);
#endif
}


static RD_INLINE int32_t RD_UNUSED rd_atomic32_add (rd_atomic32_t *ra, int32_t v) {
#ifdef _MSC_VER
	return InterlockedAdd(&ra->val, v);
#elif !defined(HAVE_ATOMICS_32)
	int32_t r;
	mtx_lock(&ra->lock);
	ra->val += v;
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(add, fetch, &ra->val, v);
#endif
}

static RD_INLINE int32_t RD_UNUSED rd_atomic32_sub(rd_atomic32_t *ra, int32_t v) {
#ifdef _MSC_VER
	return InterlockedAdd(&ra->val, -v);
#elif !defined(HAVE_ATOMICS_32)
	int32_t r;
	mtx_lock(&ra->lock);
	ra->val -= v;
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(sub, fetch, &ra->val, v);
#endif
}

static RD_INLINE int32_t RD_UNUSED rd_atomic32_get(rd_atomic32_t *ra) {
#ifdef _MSC_VER
	return ra->val;
#elif !defined(HAVE_ATOMICS_32)
	int32_t r;
	mtx_lock(&ra->lock);
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(fetch, add, &ra->val, 0);
#endif
}

static RD_INLINE int32_t RD_UNUSED rd_atomic32_set(rd_atomic32_t *ra, int32_t v) {
#ifdef _MSC_VER
	return InterlockedExchange(&ra->val, v);
#elif !defined(HAVE_ATOMICS_32)
	int32_t r;
	mtx_lock(&ra->lock);
	r = ra->val = v;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ra->val = v; // FIXME
#endif
}



static RD_INLINE RD_UNUSED void rd_atomic64_init (rd_atomic64_t *ra, int64_t v) {
	ra->val = v;
#if !defined(_MSC_VER) && !defined(HAVE_ATOMICS_64)
	mtx_init(&ra->lock, mtx_plain);
#endif
}

static RD_INLINE int64_t RD_UNUSED rd_atomic64_add (rd_atomic64_t *ra, int64_t v) {
#ifdef _MSC_VER
	return InterlockedAdd64(&ra->val, v);
#elif !defined(HAVE_ATOMICS_64)
	int64_t r;
	mtx_lock(&ra->lock);
	ra->val += v;
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(add, fetch, &ra->val, v);
#endif
}

static RD_INLINE int64_t RD_UNUSED rd_atomic64_sub(rd_atomic64_t *ra, int64_t v) {
#ifdef _MSC_VER
	return InterlockedAdd64(&ra->val, -v);
#elif !defined(HAVE_ATOMICS_64)
	int64_t r;
	mtx_lock(&ra->lock);
	ra->val -= v;
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(sub, fetch, &ra->val, v);
#endif
}

static RD_INLINE int64_t RD_UNUSED rd_atomic64_get(rd_atomic64_t *ra) {
#ifdef _MSC_VER
	return ra->val;
#elif !defined(HAVE_ATOMICS_64)
	int64_t r;
	mtx_lock(&ra->lock);
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ATOMIC_OP(fetch, add, &ra->val, 0);
#endif
}


static RD_INLINE int64_t RD_UNUSED rd_atomic64_set(rd_atomic64_t *ra, int64_t v) {
#ifdef _MSC_VER
	return InterlockedExchange64(&ra->val, v);
#elif !defined(HAVE_ATOMICS_64)
	int64_t r;
	mtx_lock(&ra->lock);
	ra->val = v;
	r = ra->val;
	mtx_unlock(&ra->lock);
	return r;
#else
	return ra->val = v; // FIXME
#endif
}
