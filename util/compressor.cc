//Compressor, author: ZeitHaum.
#include "util/compressor.hh"
extern "C"{
    #include "lib/lz4/lib/lz4.c"
}
#include "string.h"

Compressor::Compressor()
{
    //default size 512KB
    buffer_size = 512 * 1024;
    buffer = new uint8_t[buffer_size];
}

Compressor::Compressor(uint64_t buffer_sz)
:buffer_size(buffer_sz)
{
    buffer = new uint8_t[buffer_size];
}

Compressor::~Compressor(){
    delete[] buffer;
}

inline void Compressor::ajustBuffer(uint64_t buffer_len){
    if(buffer_size >= buffer_len) return;
    else{
        delete[] buffer;
        buffer_size = buffer_len;
        buffer = new uint8_t[buffer_size];
    }
}

LZ4Compressor::LZ4Compressor()
:Compressor()
{}

LZ4Compressor::LZ4Compressor(uint64_t buffer_size)
:Compressor(buffer_size)
{}

inline int LZ4Compressor::getMaxDecompressLen(){
    return (int) buffer_size; // maxComtain a buffer Size
}

inline int LZ4Compressor::getMaxCompressLen(uint64_t comp_len){
    return (int) LZ4_compressBound((int)comp_len);
}

void LZ4Compressor::compress(uint8_t* src, uint64_t src_len, uint64_t& dest_len){
    int max_compresslen = getMaxCompressLen(src_len);
    this->ajustBuffer(max_compresslen);
    dest_len = LZ4_compress_default((char*)src, (char*)this->buffer, (int)src_len, max_compresslen);
}

void LZ4Compressor::decompress(uint8_t* src, uint64_t src_len, uint64_t& dest_len){
    int max_decompresslen = getMaxDecompressLen();
    this->ajustBuffer(max_decompresslen);
    dest_len = LZ4_decompress_safe((char*)src, (char*) this->buffer, (int)src_len, max_decompresslen);
    assert(dest_len <= max_decompresslen);
}