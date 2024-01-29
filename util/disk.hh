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

#pragma once

#ifndef __UTIL_DISK__
#define __UTIL_DISK__

#include <cinttypes>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <util/compressor.hh>

namespace SimpleSSD {

class Disk {
 protected:
  std::string filename;
  uint64_t diskSize;
  uint32_t sectorSize;
  std::fstream disk;

 public:
  Disk();
  Disk(const Disk &) = delete;
  virtual ~Disk();

  virtual uint64_t open(std::string, uint64_t, uint32_t);
  virtual void close();

  virtual uint16_t read(uint64_t, uint16_t, uint8_t *);
  virtual uint64_t readOrdinary(uint64_t, uint64_t, uint8_t *);
  virtual uint16_t write(uint64_t, uint16_t, uint8_t *);
  virtual uint64_t writeOrdinary(uint64_t, uint64_t, uint8_t *);
  virtual uint16_t erase(uint64_t, uint16_t);
};

class CoWDisk : public Disk {
 private:
  std::unordered_map<uint64_t, std::vector<uint8_t>> table;

 public:
  CoWDisk();
  CoWDisk(const CoWDisk &) = delete;
  ~CoWDisk();

  void close() override;

  uint16_t read(uint64_t, uint16_t, uint8_t *) override;
  uint16_t write(uint64_t, uint16_t, uint8_t *) override;
};

class MemDisk : public Disk {
 private:
  std::unordered_map<uint64_t, std::vector<uint8_t>> table;

 public:
  MemDisk();
  MemDisk(const MemDisk &) = delete;
  ~MemDisk();

  uint64_t open(std::string, uint64_t, uint32_t) override;
  void close() override;

  uint16_t read(uint64_t, uint16_t, uint8_t *) override;
  uint16_t write(uint64_t, uint16_t, uint8_t *) override;
  uint16_t erase(uint64_t, uint16_t) override;
};

enum class CompressType{
  NONE, LZ4, LZMA
};
  
class CompressedDisk: public Disk{
  private:
    Compressor* compressor;
    uint32_t compress_unit_size;
    uint32_t compress_unit_totalcnt; 
    std::unordered_map<uint64_t, uint64_t> compressed_table;
  public:
    CompressedDisk();
    void init(uint32_t, CompressType);
    uint64_t getCompressedLength(uint64_t idx);
    void setCompressedLength(uint64_t idx, uint64_t comp_len);
    //只要有任意一段在table中就视为压缩过
    bool isCompressed(uint64_t idx);
    virtual ~CompressedDisk();

    virtual uint16_t read(uint64_t, uint16_t, uint8_t *);
    virtual uint16_t write(uint64_t, uint16_t, uint8_t *);
    virtual uint16_t erase(uint64_t, uint16_t);
    bool compressWrite(uint64_t idx, uint8_t* );
    void decompressRead(uint64_t idx, uint8_t* );
};
}  // namespace SimpleSSD

#endif
  