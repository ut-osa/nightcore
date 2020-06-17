#pragma once

#include <linux/limits.h>  // for PIPE_BUF

// Put this in the declarations for a class to be uncopyable.
#define DISALLOW_COPY(TypeName) \
    TypeName(const TypeName&) = delete

// Put this in the declarations for a class to be unassignable.
#define DISALLOW_ASSIGN(TypeName) \
    TypeName& operator=(const TypeName&) = delete

// Put this in the declarations for a class to be uncopyable and unassignable.
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    DISALLOW_COPY(TypeName);               \
    DISALLOW_ASSIGN(TypeName)

// A macro to disallow all the implicit constructors, namely the
// default constructor, copy constructor and operator= functions.
// This is especially useful for classes containing only static methods.
#define DISALLOW_IMPLICIT_CONSTRUCTORS(TypeName) \
    TypeName() = delete;                         \
    DISALLOW_COPY_AND_ASSIGN(TypeName)

#define __FAAS_PREDICT_FALSE(x) __builtin_expect(x, 0)
#define __FAAS_PREDICT_TRUE(x)  __builtin_expect(false || (x), true)

// We're always on x86_64
#define __FAAS_CACHE_LINE_SIZE 64
#define __FAAS_PAGE_SIZE       4096

#ifndef __FAAS_FILE_CREAT_MODE
#define __FAAS_FILE_CREAT_MODE 0664
#endif

#ifndef __FAAS_DIR_CREAT_MODE
#define __FAAS_DIR_CREAT_MODE 0775
#endif

#ifndef __FAAS_MESSAGE_SIZE
#define __FAAS_MESSAGE_SIZE 1024
#endif
static_assert(__FAAS_MESSAGE_SIZE <= PIPE_BUF,
              "__FAAS_MESSAGE_SIZE cannot exceed PIPE_BUF");
static_assert(__FAAS_MESSAGE_SIZE >= __FAAS_CACHE_LINE_SIZE * 2,
              "__FAAS_MESSAGE_SIZE is too small");
