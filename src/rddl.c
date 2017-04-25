/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2017 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
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

#include "rd.h"
#include "rddl.h"

#if WITH_LIBDL
#include <dlfcn.h>

#elif defined( _MSC_VER)

#else
#error "Dynamic library loading not supported on this platform"
#endif



/**
 * @brief Latest thread-local dl error, normalized to suit our logging.
 * @returns a newly allocated string that must be freed
 */
static char *rd_dl_error (void) {
#if WITH_LIBDL
        char *errstr;
        char *s;
        errstr = dlerror();
        if (!errstr)
                return rd_strdup("No error returned from dlerror()");

        errstr = rd_strdup(errstr);
        /* Change newlines to separators. */
        while ((s = strchr(errstr, '\n')))
                *s = '.';

        return errstr;

#elif defined(_MSC_VER)
        char buf[1024];
        rd_strerror_w32(GetLastError(), buf, sizeof(buf));
        return rd_strdup(buf);
#endif
}

/**
 * @brief Attempt to load library \p path.
 * @returns the library handle (platform dependent, thus opaque) on success,
 *          else NULL.
 */
void *rd_dl_open (const char *path, char *errstr, size_t errstr_size) {
        void *handle;
        const char *loadfunc;
#if WITH_LIBDL
        loadfunc = "dlopen()";
        handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
#elif defined(_MSC_VER)
        loadfunc = "LoadLibrary()";
        handle = (void *)LoadLibraryA(path);
#endif
        if (!handle) {
                char *dlerrstr = rd_dl_error();
                rd_snprintf(errstr, errstr_size, "%s failed: %s",
                            loadfunc, dlerrstr);
                rd_free(dlerrstr);
        }
        return handle;
}


/**
 * @brief Close handle previously returned by rd_dl_open()
 * @remark errors are ignored (what can we do anyway?)
 */
void rd_dl_close (void *handle) {
#if WITH_LIBDL
        dlclose(handle);
#elif defined(_MSC_VER)
        FreeLibrary((HMODULE)handle);
#endif
}

/**
 * @brief look up address of \p symbol in library handle \p handle
 * @returns the function pointer on success or NULL on error.
 */
void *
rd_dl_sym (void *handle, const char *symbol, char *errstr, size_t errstr_size) {
        void *func;
#if WITH_LIBDL
        func = dlsym(handle, symbol);
#elif defined(_MSC_VER)
        func = GetProcAddress((HMODULE)handle, symbol);
#endif
        if (!func) {
                char *dlerrstr = rd_dl_error();
                rd_snprintf(errstr, errstr_size,
                            "Failed to load symbol \"%s\": %s",
                            symbol, dlerrstr);
                rd_free(dlerrstr);
        }
        return func;
}

