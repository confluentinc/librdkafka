#ifndef __OS400_ASSERT_H_
#define __OS400_ASSERT_H_

// Of course, ILE C library has an assert function. But unfortunately, it has no Ascii equivalence for it. 
// We have to implement handmade assert function to be able to use it with QAdrt ascii runtime
#ifdef assert
#undef assert
#endif
#define assert(__expr) ((__expr) ? ((void)0) : \
        (fprintf(stderr,"Assertion failed: %s file %s, line %d, function %s\n", \
                 #__expr,__FILE__,__LINE__, __func__), \
        abort()))                                                                
#endif