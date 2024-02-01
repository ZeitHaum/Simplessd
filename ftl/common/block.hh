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

#ifndef __FTL_COMMON_BLOCK__
#define __FTL_COMMON_BLOCK__

#include <cinttypes>
#include <vector>

#include "util/bitset.hh"

namespace SimpleSSD {

namespace FTL {

struct LpnInfo{
  uint64_t lpn;
  uint32_t idx;
}; 

struct BlockStat{
  uint64_t totalDataLength;
  uint64_t validDataLength;
  uint64_t validIoUnitCount;
  uint64_t compressUnitCount;
  uint64_t totalUnitCount;
  
  BlockStat() = default;

  BlockStat(const BlockStat& other) = default;

  BlockStat(BlockStat&& other) noexcept
      : totalDataLength(other.totalDataLength),
        validDataLength(other.validDataLength),
        validIoUnitCount(other.validIoUnitCount),
        compressUnitCount(other.compressUnitCount),
        totalUnitCount(other.totalUnitCount) {
    // No pointer need to handle
  }

  BlockStat& operator=(const BlockStat& other) = default;

  BlockStat& operator=(BlockStat&& other) noexcept = default;

  void copy(const BlockStat& other){
    totalDataLength = other.totalDataLength;
    validDataLength = other.validDataLength;
    validIoUnitCount = other.validIoUnitCount;
    compressUnitCount = other.compressUnitCount;
    totalUnitCount = other.totalUnitCount;
  }
  void reset(){
    totalDataLength = 0;
    validDataLength = 0;
    validIoUnitCount = 0;
    compressUnitCount = 0;
    totalUnitCount = 0;
  }
  BlockStat operator+(const BlockStat& other) const {
    BlockStat result;
    result.totalDataLength = totalDataLength + other.totalDataLength;
    result.validDataLength = validDataLength + other.validDataLength;
    result.validIoUnitCount = validIoUnitCount + other.validIoUnitCount;
    result.compressUnitCount = compressUnitCount + other.compressUnitCount;
    result.totalUnitCount = totalUnitCount + other.totalUnitCount;
    return result;
  }
  BlockStat& operator+=(const BlockStat& other) {
    totalDataLength += other.totalDataLength;
    validDataLength += other.validDataLength;
    validIoUnitCount += other.validIoUnitCount;
    compressUnitCount += other.compressUnitCount;
    totalUnitCount += other.totalUnitCount;
    return *this;
  }

};

class Block {
 private:
  static uint32_t iounitSize;
  static uint16_t maxCompressedPageCount;
  uint32_t idx;
  uint32_t pageCount;
  uint32_t ioUnitInPage;
  uint32_t *pNextWritePageIndex;

  //Unused
  Bitset *pValidBits;
  LpnInfo *pLPNs;
  // Following variables are used when ioUnitInPage == 1
  Bitset *pErasedBits;
  std::vector<Bitset> validBits;
  LpnInfo **ppLPNs; 

  // Following variables are used when ioUnitInPage > 1
  std::vector<Bitset> erasedBits;
  // ppLPNs[i][j]表示第i个物理页第j个压缩块的LPN.
  std::vector<std::vector<Bitset>> cvalidBits;
  // pppLPNs[i][j][k]表示第i个物理页第j个ioUnit第k个压缩块的LPN.
  LpnInfo ***pppLPNs;

  uint64_t lastAccessed;
  uint32_t eraseCount;

  BlockStat blockstat;

 public:
  Block(uint32_t, uint32_t, uint32_t);
  Block(const Block &);      // Copy constructor
  Block(Block &&) noexcept;  // Move constructor
  ~Block();

  Block &operator=(const Block &);  // Copy assignment
  Block &operator=(Block &&);       // Move assignment

  uint32_t getBlockIndex() const;
  uint64_t getLastAccessedTime();
  uint32_t getEraseCount();
  uint32_t getValidPageCount();
  uint32_t getValidPageCountRaw();
  uint32_t getDirtyPageCount();
  uint32_t getNextWritePageIndex();
  uint32_t getNextWritePageIndex(uint32_t);
  bool getPageInfo(uint32_t, std::vector<std::vector<LpnInfo>> &, std::vector<Bitset> &);
  void getLPNs(uint32_t, std::vector<LpnInfo>&, Bitset& bits, uint32_t);
  LpnInfo getLPN(uint32_t, uint16_t, uint16_t);
  bool read(uint32_t, uint32_t, uint64_t);
  // bool write(uint32_t, uint64_t, uint32_t, uint64_t);
  bool write(uint32_t, std::vector<LpnInfo>&, std::vector<uint32_t>&,  Bitset& , uint32_t, uint64_t);
  void updateStatWrite(std::vector<uint32_t>&lens, Bitset& validmask);
  static void setStaticAttr(uint32_t , uint16_t);
  void erase();
  bool isvalid(uint32_t, uint16_t);
  bool isvalid(uint32_t, uint16_t, uint16_t);
  // void invalidate(uint32_t, uint16_t);
  void invalidate(uint32_t, uint16_t, uint16_t, uint32_t);
  void updateStatInvalidate(uint32_t len);
  const BlockStat& getBlockStat(); //getter
  void checkstat();
};

}  // namespace FTL

}  // namespace SimpleSSD

#endif
