#pragma once



#include "rd.h"

typedef struct rd_interval_s {
        rd_ts_t    ri_ts_last; /* last interval timestamp */
        rd_ts_t    ri_fixed;   /* fixed interval if provided interval is 0 */
        int        ri_backoff; /* back off the next interval by this much */
} rd_interval_t;


static __inline RD_UNUSED void rd_interval_init (rd_interval_t *ri) {
        memset(ri, 0, sizeof(*ri));
}



/**
 * Returns the number of microseconds the interval has been over-shot.
 * If the return value is >0 (i.e., time for next intervalled something) then
 * the time interval is updated for the next inteval.
 *
 * A current time can be provided in 'now', if set to 0 the time will be
 * gathered automatically.
 *
 * If 'interval_us' is set to 0 the fixed interval will be used, see
 * 'rd_interval_fixed()'.
 */
static __inline RD_UNUSED rd_ts_t rd_interval (rd_interval_t *ri,
                                               rd_ts_t interval_us,
                                               rd_ts_t now) {
        rd_ts_t diff;

        if (!now)
                now = rd_clock();
        if (!interval_us)
                interval_us = ri->ri_fixed;

        diff = now - (ri->ri_ts_last + interval_us + ri->ri_backoff);
        if (unlikely(diff > 0)) {
                ri->ri_ts_last = now;
                ri->ri_backoff = 0;
        }

        return diff;
}


/**
 * Reset the interval to zero, i.e., the next call to rd_interval()
 * will be immediate.
 */
static __inline RD_UNUSED void rd_interval_reset (rd_interval_t *ri) {
        ri->ri_ts_last = 0;
        ri->ri_backoff = 0;
}

/**
 * Back off the next interval by `backoff_us` microseconds.
 */
static __inline RD_UNUSED void rd_interval_backoff (rd_interval_t *ri,
                                                    int backoff_us) {
        ri->ri_backoff = backoff_us;
}

/**
 * Expedite (speed up) the next interval by `expedite_us` microseconds.
 * If `expedite_us` is 0 the interval will be set to trigger
 * immedately on the next rd_interval() call.
 */
static __inline RD_UNUSED void rd_interval_expedite (rd_interval_t *ri,
						     int expedite_us) {
	if (!expedite_us)
		ri->ri_ts_last = 0;
	else
		ri->ri_backoff = -expedite_us;
}

/**
 * Specifies a fixed interval to use if rd_interval() is called with
 * `interval_us` set to 0.
 */
static __inline RD_UNUSED void rd_interval_fixed (rd_interval_t *ri,
                                                  rd_ts_t fixed_us) {
        ri->ri_fixed = fixed_us;
}

/**
 * Disables the interval (until rd_interval_init()/reset() is called).
 * A disabled interval will never return a positive value from
 * rd_interval().
 */
static __inline RD_UNUSED void rd_interval_disable (rd_interval_t *ri) {
        /* Set last beat to a large value a long time in the future. */
        ri->ri_ts_last = 6000000000000000000LL; /* in about 190000 years */
}

/**
 * Returns true if the interval is disabled.
 */
static __inline RD_UNUSED int rd_interval_disabled (const rd_interval_t *ri) {
        return ri->ri_ts_last == 6000000000000000000LL;
}
