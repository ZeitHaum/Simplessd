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

// #define DEBUG
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
      // uint64_t compress_len = 0;
      // uint64_t decompress_len = 0;
      // char* compress_len_buf = new char[8];
      // disk.read((char*)compress_len_buf, 8);
      // compress_len = *((uint64_t*) compress_len_buf);
      // disk.seekp(slba + 8, std::ios::beg);
      // // disk.read((char *)buffer, avail);
      // disk.read((char *)(buffer), compress_len);
      // compressor->decompress(buffer, compress_len, decompress_len);
      // memcpy(buffer, compressor->buffer, decompress_len);
      // std::string info = "DeCompressed Occured: ";
      // info += std::to_string(slba);
      // info += " , Src Length" + std::to_string(compress_len);
      // info += " , DeCompressed Length" + std::to_string(decompress_len);
      // debugprint(LOG_COMMON, info.c_str());
      disk.read((char *)buffer, avail);
    }

    memset(buffer + avail, 0, nlblk * sectorSize - avail);

    // DPRINTF(NVMeDisk, "DISK    | READ  | BYTE %016" PRIX64 " + %X\n",
    //         slba, nlblk * sectorSize);

    ret = nlblk;
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("DISK TEST READ" + std::to_string(ret)).c_str());
  #endif
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
    // uint64_t compress_len = 0;
    // char* compress_len_buf = new char[8];
    // *((uint64_t *)(compress_len_buf)) = compress_len;
    // compressor->compress(buffer, sectorSize * nlblk, compress_len);
    // disk.write((char *)compress_len_buf, 8);
    // disk.seekp(slba + 8, std::ios::beg);
    // disk.write((char *)(compressor->buffer), compress_len);
    // std::string info = "Compressed Occured: ";
    // info += std::to_string(slba);
    // info += " , Src Length" + std::to_string(sectorSize * nlblk);
    // info += " , Compressed Length" + std::to_string(compress_len);
    // debugprint(LOG_COMMON, info.c_str());
    // offset = (uint64_t)disk.tellp() - offset;
    disk.write((char *)buffer, sectorSize * nlblk);
    offset = (uint64_t)disk.tellp() - offset;

    ret = offset / sectorSize;
  }
  #ifdef DEBUG
  debugprint(LOG_COMMON, ("DISK TEST WRITE" + std::to_string(ret)).c_str());
  #endif
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

CompressedDisk::CompressedDisk(){
  compressor = new LZ4Compressor();
}

CompressedDisk::~CompressedDisk(){
  delete compressor;
}

uint64_t CompressedDisk::open(std::string path, uint64_t desiredSize, uint32_t lbaSize){
  uint64_t disk_size = Disk::open(path, desiredSize, lbaSize);
  return disk_size;
}

void CompressedDisk::init(uint32_t cdsize){
  compress_unit_size = cdsize;
  this->compress_unit_totalcnt = diskSize / compress_unit_size  + 1;
  compressed_table.reserve(this->compress_unit_totalcnt);
  const std::string info_head = "CompressedDiskInit:";
  std::string info = info_head + " compress_unit_size: " + std::to_string(this->compress_unit_size) + ", compress_unit_totalcnt: " + std::to_string(this->compress_unit_totalcnt);
  debugprint(LOG_COMMON, info.c_str());
}

uint16_t CompressedDisk::read(uint64_t slba, uint16_t nlblk, uint8_t *buffer){
  //default Need DeCompress
  debugprint(LOG_COMMON, "CompressedDiskRead: slba | nlblk ");
  const std::string info_head = "CompressedDiskRead:";
  std::string info = info_head;
  info += std::to_string(slba) + " | " + std::to_string(nlblk);
  debugprint(LOG_COMMON, info.c_str());
  info = info_head;
  uint16_t ret = Disk::read(slba, nlblk, buffer);
  info += "Actual Read Blocks:" + std::to_string(ret);
  debugprint(LOG_COMMON, info.c_str());
  // Decompress
  uint64_t offset = 0;
  for(uint16_t i = 0; i<ret; ++i){
    readInternal((slba + i) * sectorSize, sectorSize, buffer + offset);
    offset += this->sectorSize;
  }
  return ret;
}

uint16_t CompressedDisk::write(uint64_t slba, uint16_t nlblk, uint8_t *buffer){
  //default Need Compress
  debugprint(LOG_COMMON, "CompressedDiskWrite: slba | nlblk ");
  const std::string info_head = "CompressedDiskWrite:";
  std::string info = info_head;
  info+= std::to_string(slba) + " | " + std::to_string(nlblk);
  debugprint(LOG_COMMON, info.c_str());
  //compress
  std::vector<uint64_t> compressed_lens(nlblk, 0);
  uint64_t offset = 0;
  for(uint16_t i = 0; i<nlblk; ++i){
    // compressor->compress(buffer+offset, this->sectorSize, compressed_lens[i]);
    // memcpy(buffer+offset, compressor->buffer, compressed_lens[i]);
    writeInternal(sectorSize, buffer+offset, compressed_lens[i]);
    offset += this->sectorSize;
  }
  //actual write
  info = info_head;
  uint16_t ret = Disk::write(slba, nlblk, buffer); 
  info += "Actual Write Blocks: " + std::to_string(ret);
  debugprint(LOG_COMMON, info.c_str());
  for(uint16_t i = 0; i<ret; ++i){
    for(uint64_t j = 0; j<sectorSize / compress_unit_size; ++j){
      uint64_t offset = (slba + i) * sectorSize + j * compress_unit_size;
      setCompressedLength(offset, compressed_lens[i]);
      //debug print;
      info = info_head;
      info += "Compress idx: " + std::to_string(offset / compress_unit_size) + ", src_len: " + std::to_string(this->compress_unit_size) + ", dest_len: " + std::to_string(compressed_lens[i]);
      debugprint(LOG_COMMON, info.c_str()); 
    }
  }
  return ret;
}

uint16_t CompressedDisk::erase(uint64_t slba, uint16_t nlblk) {
  for(uint16_t i = 0; i<nlblk; ++i){
    eraseInternal((slba + i) * sectorSize, sectorSize);
  }
  return nlblk;
}

void CompressedDisk::eraseInternal(uint64_t offset, uint64_t length){
  if(offset % compress_unit_size !=0 || length % compress_unit_size !=0){
    panic("Error: unmatched access I/O Size");
  }
  uint64_t idx = offset / compress_unit_size;
  uint64_t cnt = length / compress_unit_size;
  for(uint64_t i = 0; i<cnt ; ++i){
    if(compressed_table.find(idx + i) != compressed_table.end()){
      compressed_table.erase(idx + i);
    }
  }
}
void CompressedDisk::readInternal(uint64_t offset, uint64_t length, uint8_t * buffer){
  if(offset % compress_unit_size !=0 || length % compress_unit_size !=0){
    panic("Error: unmatched access I/O Size");
  }
  const std::string info_head = "CompressedDiskRead:";
  uint64_t idx = offset / compress_unit_size;
  uint64_t cnt = length / compress_unit_size;
  uint64_t buf_offset = 0;
  for(uint64_t i = 0; i<cnt; ++i){
    if(compressed_table.find(idx + i)==compressed_table.end() || compressed_table.at(idx + i) == compress_unit_size){
      continue; 
    }
    uint64_t src_len = getCompressedLength(buf_offset + offset, compress_unit_size), dest_len = 0;
    compressor->decompress(buffer + buf_offset, src_len, dest_len);
    if(dest_len > this->compress_unit_size){
      panic("DecompressedSize OverFlow, greater than a compress unit size.");
    }
    memcpy(buffer + buf_offset, compressor->buffer,  dest_len);
    buf_offset += this->compress_unit_size;
    std::string info = info_head;
    info += "Decompress idx: " + std::to_string(idx + i) + ", src_len: " + std::to_string(src_len) + ", dest_len: " + std::to_string(dest_len);
    debugprint(LOG_COMMON, info.c_str());  
  }
}
void CompressedDisk::writeInternal(uint64_t length, uint8_t* buffer, uint64_t& comp_len){
  if(length % compress_unit_size !=0){
    panic("Error: unmatched access I/O Size");
  }
  const std::string info_head = "CompressedDiskWrite:";
  uint64_t buff_offset = 0;
  uint64_t cnt = length / compress_unit_size;
  for(uint64_t i = 0; i<cnt; ++i){
    compressor->compress(buffer+buff_offset, this->compress_unit_size, comp_len);
    if(comp_len > compress_unit_size){
      panic("CompressedSize OverFlow, greater than a compress unit size.");
    }
    memcpy(buffer+buff_offset, compressor->buffer, comp_len);
    buff_offset += compress_unit_size;
  }
}

void CompressedDisk::writeInternal(uint64_t offset, uint64_t length, uint8_t* buffer, uint64_t& comp_len){
  if(offset % compress_unit_size !=0 || length % compress_unit_size !=0){
    panic("Error: unmatched access I/O Size");
  }
  comp_len = 0;
  uint64_t cnt = length / compress_unit_size;
  uint64_t sub_comp_len = 0;
  uint64_t idx = offset / compress_unit_size;
  for(uint64_t i = 0; i<cnt; ++i){
    sub_comp_len = 0;
    writeInternal(offset, buffer + (compress_unit_size * i), sub_comp_len);
    if(sub_comp_len > compress_unit_size){
      panic("CompressedSize OverFlow, greater than a compress unit size.");
    }
    comp_len += sub_comp_len;
    compressed_table[idx + i] = sub_comp_len;
    std::string info = "CompressedDiskWriteInternal: ";
    info += "Compress idx: " + std::to_string(idx + i) + ", src_len: " + std::to_string(this->compress_unit_size) + ", dest_len: " + std::to_string(sub_comp_len);
    debugprint(LOG_COMMON, info.c_str()); 
  }
}

uint64_t CompressedDisk::getCompressedLength(uint64_t offset, uint64_t length){
  uint64_t ret = 0;
  if(!isInCompressedTable(offset, length)){
    std::string warn_s = "Uncompressed In Disk, offset = " + std::to_string(offset) + ", length = " + std::to_string(length);
    warn(warn_s.c_str());
    ret = length;
  }
  else{
    uint64_t idx = offset / compress_unit_size;
    uint64_t cnt = length / compress_unit_size;
    for(uint64_t i = 0; i<cnt; ++i){
      if(compressed_table.find(idx + i) != compressed_table.end()){
        ret += compressed_table.at(idx + i);
      }
      else{
        ret += compress_unit_size;
      }
    }
  }
  return ret;
}

void CompressedDisk::setCompressedLength(uint64_t offset, uint64_t com_len){
  if(offset % compress_unit_size !=0 ){
    panic("Error: unmatched access I/O Size");
  }
  uint64_t idx = offset / compress_unit_size;
  compressed_table[idx] = com_len;
}

bool CompressedDisk::isInCompressedTable(uint64_t offset, uint64_t length){
  if(offset % compress_unit_size !=0 || length % compress_unit_size !=0){
    panic("Error: unmatched access I/O Size");
  }
  uint64_t idx = offset / compress_unit_size;
  uint64_t cnt = length / compress_unit_size;
  for(uint64_t i = 0; i<cnt; ++i){
    if(compressed_table.find(idx + i) != compressed_table.end()){
      return true;
    }
  }
  return false;
}

}  // namespace SimpleSSD
