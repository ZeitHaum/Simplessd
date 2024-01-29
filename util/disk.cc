/*
 * Copyright (C) 2017 CAMELab
 *
 * This file is part of SimpleSSD.
 *
 * SimpleSSD is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SimpleSSD is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SimpleSSD.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "util/disk.hh"

#include <cstring>

#ifdef _MSC_VER
#include <Windows.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "sim/trace.hh"
#include <string>

namespace SimpleSSD {

Disk::Disk() : diskSize(0), sectorSize(0){}

Disk::~Disk() {
  close();
}

uint64_t Disk::open(std::string path, uint64_t desiredSize, uint32_t lbaSize) {
  filename = path;
  sectorSize = lbaSize;

  // Validate size
#ifdef _MSC_VER
  LARGE_INTEGER size;
  HANDLE hFile = CreateFileA(path.c_str(), GENERIC_READ | GENERIC_WRITE, NULL,
                             NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);

  if (GetLastError() == ERROR_ALREADY_EXISTS) {
    if (GetFileSizeEx(hFile, &size)) {
      diskSize = size.QuadPart;
    }
    else {
      // panic("Get file size failed!");
    }
  }
  else {
    size.QuadPart = desiredSize;

    if (SetFilePointerEx(hFile, size, nullptr, FILE_BEGIN)) {
      if (SetEndOfFile(hFile)) {
        diskSize = desiredSize;
      }
      else {
        // panic("SetEndOfFile failed");
      }
    }
    else {
      // panic("SetFilePointerEx failed");
    }
  }

  CloseHandle(hFile);
#else
  struct stat s;

  if (stat(filename.c_str(), &s) == 0) {
    // File exists
    if (S_ISREG(s.st_mode)) {
      diskSize = s.st_size;
    }
    else {
      // panic("nvme_disk: Specified file %s is not regular file.\n",
      //       filename.c_str());
    }
  }
  else {
    // Create zero-sized file
    disk.open(filename, std::ios::out | std::ios::binary);
    disk.close();

    // Set file size
    if (truncate(filename.c_str(), desiredSize) == 0) {
      diskSize = desiredSize;
    }
    else {
      // panic("nvme_disk: Failed to set disk size %" PRIu64 " errno=%d\n",
      //       diskSize, errno);
    }
  }
#endif

  // Open file
  disk.open(filename, std::ios::in | std::ios::out | std::ios::binary);

  if (!disk.is_open()) {
    // panic("failed to open file");
  }

  return diskSize;
}

void Disk::close() {
  if (disk.is_open()) {
    disk.close();
  }
}

uint16_t Disk::read(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t ret = 0;

  if (disk.is_open()) {
    uint64_t avail;

    slba *= sectorSize;
    avail = nlblk * sectorSize;

    if (slba + avail > diskSize) {
      if (slba >= diskSize) {
        avail = 0;
      }
      else {
        avail = diskSize - slba;
      }
    }

    if (avail > 0) {
      disk.seekg(slba, std::ios::beg);
      if (!disk.good()) {
        // panic("nvme_disk: Fail to seek to %" PRIu64 "\n", slba);
      }
      disk.read((char *)buffer, avail);
    }

    memset(buffer + avail, 0, nlblk * sectorSize - avail);

    // DPRINTF(NVMeDisk, "DISK    | READ  | BYTE %016" PRIX64 " + %X\n",
    //         slba, nlblk * sectorSize);

    ret = nlblk;
  }
  return ret;
}

uint16_t Disk::write(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t ret = 0;

  if (disk.is_open()) {
    slba *= sectorSize;

    disk.seekp(slba, std::ios::beg);
    if (!disk.good()) {
      // panic("nvme_disk: Fail to seek to %" PRIu64 "\n", slba);
    }

    uint64_t offset = disk.tellp();
    disk.write((char *)buffer, sectorSize * nlblk);
    offset = (uint64_t)disk.tellp() - offset;

    ret = offset / sectorSize;
  }
  return ret;
}

uint16_t Disk::erase(uint64_t, uint16_t nlblk) {
  return nlblk;
}

CoWDisk::CoWDisk() {}

CoWDisk::~CoWDisk() {
  close();
}

void CoWDisk::close() {
  table.clear();

  Disk::close();
}

uint16_t CoWDisk::read(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t read = 0;

  for (uint64_t i = 0; i < nlblk; i++) {
    auto block = table.find(slba + i);

    if (block != table.end()) {
      memcpy(buffer + i * sectorSize, block->second.data(), sectorSize);
      read++;
    }
    else {
      read += Disk::read(slba + i, 1, buffer + i * sectorSize);
    }
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("COWDISK TEST READ" + std::to_string(read)).c_str());
  #endif
  return read;
}

uint16_t CoWDisk::write(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t write = 0;

  for (uint64_t i = 0; i < nlblk; i++) {
    auto block = table.find(slba + i);

    if (block != table.end()) {
      memcpy(block->second.data(), buffer + i * sectorSize, sectorSize);
    }
    else {
      std::vector<uint8_t> data;

      data.resize(sectorSize);
      memcpy(data.data(), buffer + i * sectorSize, sectorSize);

      table.insert({slba + i, data});
    }

    write++;
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("COWDISK TEST WRITE" + std::to_string(write)).c_str());
  #endif
  return write;
}

uint64_t MemDisk::open(std::string, uint64_t size, uint32_t lbaSize) {
  diskSize = size;
  sectorSize = lbaSize;

  return size;
}

void MemDisk::close() {
  table.clear();
}

MemDisk::MemDisk() {}

MemDisk::~MemDisk() {
  close();
}

uint16_t MemDisk::read(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t read = 0;

  for (uint64_t i = 0; i < nlblk; i++) {
    auto block = table.find(slba + i);

    if (block != table.end()) {
      memcpy(buffer + i * sectorSize, block->second.data(), sectorSize);
    }
    else {
      memset(buffer + i * sectorSize, 0, sectorSize);
    }

    read++;
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("MEMDISK TEST READ" + std::to_string(read)).c_str());
  #endif
  return read;
}

uint16_t MemDisk::write(uint64_t slba, uint16_t nlblk, uint8_t *buffer) {
  uint16_t write = 0;

  for (uint64_t i = 0; i < nlblk; i++) {
    auto block = table.find(slba + i);

    if (block != table.end()) {
      memcpy(block->second.data(), buffer + i * sectorSize, sectorSize);
    }
    else {
      std::vector<uint8_t> data;

      data.resize(sectorSize);
      memcpy(data.data(), buffer + i * sectorSize, sectorSize);

      table.insert({slba + i, data});
    }

    write++;
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("MEMDISK TEST WRITE" + std::to_string(write)).c_str());
  #endif
  return write;
}
uint16_t MemDisk::erase(uint64_t slba, uint16_t nlblk) {
  uint16_t erase = 0;

  for (uint64_t i = 0; i < nlblk; i++) {
    auto block = table.find(slba + i);

    if (block != table.end()) {
      table.erase(block);
    }

    erase++;
  }

  return erase;
}

uint64_t Disk::readOrdinary(uint64_t offset, uint64_t length, uint8_t* buffer){
  uint16_t ret = 0;

  if (disk.is_open()) {
    uint64_t avail = length;

    if (offset + avail > diskSize) {
      if (offset >= diskSize) {
        avail = 0;
      }
      else {
        avail = diskSize - offset;
      }
    }

    if (avail > 0) {
      disk.seekg(offset, std::ios::beg);
      if (!disk.good()) {
        // panic("nvme_disk: Fail to seek to %" PRIu64 "\n", slba);
      }
      disk.read((char *)buffer, avail);
    }

    memset(buffer + avail, 0, length - avail);

    // DPRINTF(NVMeDisk, "DISK    | READ  | BYTE %016" PRIX64 " + %X\n",
    //         slba, nlblk * sectorSize);

    ret = length;
  }
  return ret;
}

uint64_t Disk::writeOrdinary(uint64_t offset, uint64_t length, uint8_t*  buffer){
  uint16_t ret = 0;

  if (disk.is_open()) {

    disk.seekp(offset, std::ios::beg);
    if (!disk.good()) {
      // panic("nvme_disk: Fail to seek to %" PRIu64 "\n", slba);
    }

    ret = disk.tellp();
    disk.write((char *)buffer, length);
    ret = (uint64_t)disk.tellp() - offset;
  }
  return ret;
}

CompressedDisk::CompressedDisk(){
  compressor = nullptr;
}

CompressedDisk::~CompressedDisk(){
  if(compressor){
    delete compressor;
  }
}


void CompressedDisk::init(uint32_t cdsize){
  compress_unit_size = cdsize;
  compressor = new LZ4Compressor(compress_unit_size);
  this->compress_unit_totalcnt = diskSize / compress_unit_size  + 1;
  compressed_table.reserve(this->compress_unit_totalcnt);
  debugprint(LOG_COMMON, "CompressedDiskInit: compress_unit_size = %" PRIu32 ", compress_unit_totalcnt = %" PRIu32, compress_unit_size, compress_unit_totalcnt);
}

uint16_t CompressedDisk::read(uint64_t slba, uint16_t nlblk, uint8_t *buffer){
  //default Need DeCompress
  // debugprint(LOG_COMMON, "CompressedDiskRead: slba | nlblk ");
  // debugprint(LOG_COMMON, "CompressedDiskRead: %" PRIu64 " | %"  PRIu16 , slba, nlblk);
  uint16_t ret = 0;
  //Read 
  if(disk.is_open()){  
    uint64_t rd_offset = slba * sectorSize;
    uint64_t rd_length = nlblk * sectorSize;
    uint8_t* rd_buffer_ptr = buffer;
    uint8_t* rd_buffer = new uint8_t[compress_unit_size];
    if(rd_offset % compress_unit_size !=0){
      uint64_t tmp_idx = rd_offset / compress_unit_size;
      uint64_t tmp_ret = Disk::readOrdinary(tmp_idx * compress_unit_size, compress_unit_size, rd_buffer);
      decompressRead(tmp_idx, rd_buffer);
      uint64_t inner_offset = (rd_offset - tmp_idx * compress_unit_size);
      ret += (tmp_ret - inner_offset) / sectorSize;
      memcpy(rd_buffer_ptr, rd_buffer + inner_offset, compress_unit_size - inner_offset);
      if(tmp_ret != compress_unit_size){
        return ret;
      }
      else{
        rd_offset = (tmp_idx + 1) * compress_unit_size;
        rd_length -= (tmp_ret - inner_offset);
        rd_buffer_ptr +=  (tmp_ret - inner_offset);
      }
    }
    // aligned
    if(rd_offset % compress_unit_size !=0){
      panic("Aligned Logic Failed.");
    }
    while(rd_length !=0){
      uint64_t tmp_idx = rd_offset / compress_unit_size;
      uint64_t tmp_ret = Disk::readOrdinary(tmp_idx * compress_unit_size, compress_unit_size, rd_buffer);
      tmp_ret = std::min(tmp_ret, rd_length);
      decompressRead(tmp_idx, rd_buffer);
      ret += tmp_ret / sectorSize;
      memcpy(rd_buffer_ptr, rd_buffer, tmp_ret);
      rd_offset += tmp_ret;
      rd_length -= tmp_ret;
      rd_buffer_ptr += tmp_ret;
    }
    delete[] rd_buffer;
  }
  // debugprint(LOG_COMMON, "CompressedDiskRead: Actual Read Blocks: %" PRIu16, ret);
  return ret;
}

void CompressedDisk::decompressRead(uint64_t idx, uint8_t* buffer){
  if(!isCompressed(idx)){
    // do Nothing.
  }
  else{
    uint64_t src_len = getCompressedLength(idx);
    uint64_t dest_len = 0;
    compressor->decompress(buffer, src_len, dest_len);
    memcpy(buffer, compressor->buffer, dest_len);
    // debugprint(LOG_COMMON, "DecompressRead In CompressedDisk: idx = %" PRIu64 "src_len = %" PRIu64 "dest_len = %" PRIu64, idx, src_len, dest_len);
  }
}

uint16_t CompressedDisk::write(uint64_t slba, uint16_t nlblk, uint8_t *buffer){
  //default No Need Compress
  // debugprint(LOG_COMMON, "CompressedDiskWrite: slba | nlblk ");
  // debugprint(LOG_COMMON, "CompressedDiskWrite: %" PRIu64 " | %" PRIu64, slba, nlblk);
  uint64_t ret =  Disk::write(slba, nlblk, buffer);
  // I/O write Amplification
  uint64_t now_idx = (slba * sectorSize) / compress_unit_size;
  while(now_idx * compress_unit_size < (slba + nlblk) * sectorSize){
    if(isCompressed(now_idx)){
      compressed_table.erase(now_idx);
      // debugprint(LOG_COMMON, "CompressedDiskWrite: OverRide Erase Compressed Data In %" PRIu64, now_idx);
    }
    ++now_idx;
  }
  // debugprint(LOG_COMMON, "CompressedDiskRead: Actual Write Blocks: %" PRIu16, ret);
  return ret;
}

bool CompressedDisk::compressWrite(uint64_t idx, uint8_t* buffer){
//ret 表示是否压缩成功
  if(isCompressed(idx)){
    panic("Error: Already Compressed");
  }
  else{
    uint64_t src_len = compress_unit_size;
    uint64_t dest_len = 0;
    compressor->compress(buffer, src_len, dest_len);
    if(dest_len >= src_len) {
      debugprint(LOG_COMMON, "Compress Failed,  idx = %" PRIu64 " ,src_len = %" PRIu64 " ,dest_len = %" PRIu64, idx, src_len, dest_len);
      return false;
    }
    compressed_table[idx] = dest_len;
    writeOrdinary(idx * compress_unit_size, compress_unit_size, compressor->buffer);
    // debugprint(LOG_COMMON, "CompressWrite In CompressedDisk: idx = %" PRIu64 " ,src_len = %" PRIu64 " ,dest_len = %" PRIu64, idx, src_len, dest_len);
  }
  return true;
}

uint16_t CompressedDisk::erase(uint64_t, uint16_t nlblk) {
  return nlblk;
}


uint64_t CompressedDisk::getCompressedLength(uint64_t idx){
  uint64_t ret = 0;
  if(!isCompressed(idx)){
    ret = compress_unit_size;
  }
  else{
    ret = compressed_table[idx];
    if(ret > compress_unit_size){
      panic("Error, compressed size more than one compress unit size");
    }
  }
  return ret;
}

void CompressedDisk::setCompressedLength(uint64_t idx, uint64_t comp_len){
  if(idx >= compress_unit_totalcnt){
    panic("Error: idx more than compress unit totalcnt, which is %" PRIu64, idx);
  }
  compressed_table[idx] = comp_len;
}

bool CompressedDisk::isCompressed(uint64_t idx){
  if(idx >= compress_unit_totalcnt){
    panic("Error: idx more than compress unit totalcnt, which is %" PRIu64, idx);
  }
  auto it = compressed_table.find(idx);
  return it != compressed_table.end() && it->second < compress_unit_size;
}

}  // namespace SimpleSSD
