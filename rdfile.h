/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012, Magnus Edenhill
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

#pragma once

#include <unistd.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


/**
 * Behaves pretty much like basename(3) but does not alter the
 * input string in any way.
 */
const char *rd_basename (const char *path);

/**
 * Returns the current directory in a static buffer.
 */
const char *rd_pwd (void);


/**
 * Returns the size of the file, or -1 on failure.
 */
ssize_t rd_file_size (const char *path);

/**
 * Returns the size of the file already opened, or -1 on failure.
 */
ssize_t rd_file_size_fd (int fd);


/**
 * Performs stat(2) on 'path' and returns 'struct stat.st_mode' on success
 * or 0 on failure.
 *
 * Example usage:
 *  if (S_ISDIR(rd_file_mode(mypath)))
 *     ..
 */
mode_t rd_file_mode (const char *path);


/**
 * Opens the specified file and reads the entire content into a malloced
 * buffer which is null-terminated. The actual length of the buffer, without
 * the conveniant null-terminator, is returned in '*lenp'.
 * The buffer is returned, or NULL on failure.
 */
char *rd_file_read (const char *path, int *lenp);


/**
 * Writes 'buf' of 'len' bytes to 'path'.
 * Hint: Use O_APPEND or O_TRUNC in 'flags'.
 * Returns 0 on success or -1 on error.
 * See open(2) for more info.
 */
int rd_file_write (const char *path, const char *buf, int len,
		   int flags, mode_t mode);
