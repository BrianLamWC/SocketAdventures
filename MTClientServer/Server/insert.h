#include "utils.h"

#ifndef INSERT_H
#define INSERT_H

void factory();


void insertAlgorithm();

// specialize std::hash for DataItem struct
namespace std
{
    template <>
    struct hash<DataItem>
    {
        size_t operator()(DataItem const &d) const noexcept
        {
            
            size_t h = 0;
            hash_combine(h, d.val);
            hash_combine(h, d.primaryCopyID);
            return h;

        }
    };
}

#endif
