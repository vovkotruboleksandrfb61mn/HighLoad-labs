// liburing, for counter-server's io_uring engine. Only built when the package
// is evaluated with HLS_IO_URING=1 (scripts/env.sh sets it when liburing is
// available), since the library is Linux-only and not installed everywhere.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE    // for sched_setaffinity and cpu_set_t
#endif
#include <sched.h>
#include <liburing.h>

// IOSQE_IO_LINK is a macro over an enum constant, which Swift does not import.
static inline unsigned int hls_iosqe_io_link(void) {
    return IOSQE_IO_LINK;
}
