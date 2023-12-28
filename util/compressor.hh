//Compressor, author: ZeitHaum.
#pragma once

#ifndef __UTIL_COMPRESSOR__
#define __UTIL_COMPRESSOR__
#include <cstdint>
class Compressor{
public:
    uint8_t* buffer;
    uint64_t buffer_size;
    Compressor();
    virtual ~Compressor();
    inline void ajustBuffer(uint64_t buffer_len);
    virtual void compress(uint8_t* src, uint64_t src_len, uint64_t& dest_len) = 0;
    virtual void decompress(uint8_t* src, uint64_t src_len, uint64_t& dest_len) = 0;
};

class LZ4Compressor: public Compressor{
    public:
        inline int getMaxDecompressLen();
        inline int getMaxCompressLen(uint64_t comp_len);
        void compress(uint8_t* src, uint64_t src_len, uint64_t& dest_len);
        void decompress(uint8_t* src, uint64_t src_len, uint64_t& dest_len);
};
#endif