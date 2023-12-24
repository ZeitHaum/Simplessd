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

struct WriteInfo{
  bool valid;
  uint32_t pageIndex;
  std::vector<uint64_t> lpns;
  std::vector<uint64_t> invalidate_idxs;
  std::vector<uint64_t> invalidate_blocks;
  std::vector<uint64_t> invalidate_pages;
  uint32_t idx;
  uint32_t validcount;
  uint64_t beginAt;
  Bitset validmask;
  uint8_t maxCompressedPageCount;
  WriteInfo(uint8_t mcpn):
    valid(false), 
    validcount(0),
    maxCompressedPageCount(mcpn)
  {
    lpns = std::vector<uint64_t>(8, 0);
    validmask = Bitset(maxCompressedPageCount);
  }
};

class Block {
 private:
  uint32_t idx;
  uint32_t pageCount;
  uint32_t ioUnitInPage;
  uint32_t *pNextWritePageIndex;

  //Unused
  Bitset *pValidBits;
  uint64_t *pLPNs;
  // Following variables are used when ioUnitInPage == 1
  Bitset *pErasedBits;
  std::vector<Bitset> validBits;
  uint64_t **ppLPNs; 

  // Following variables are used when ioUnitInPage > 1
  std::vector<Bitset> erasedBits;
  // ppLPNs[i][j]表示第i个物理页第j个压缩块的LPN.
  std::vector<std::vector<Bitset>> cvalidBits;
  // pppLPNs[i][j][k]表示第i个物理页第j个ioUnit第k个压缩块的LPN.
  uint64_t ***pppLPNs;

  uint8_t maxCompressedPageCount;

  uint64_t lastAccessed;
  uint32_t eraseCount;

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
  bool getPageInfo(uint32_t, std::vector<uint64_t> &, Bitset &);
  void getLPNs(uint32_t, std::vector<uint64_t>&, Bitset& bits, uint32_t);
  bool read(uint32_t, uint32_t, uint64_t);
  // bool write(uint32_t, uint64_t, uint32_t, uint64_t);
  bool write(uint32_t, std::vector<uint64_t>&, Bitset& , uint32_t, uint64_t);
  bool write(WriteInfo&);
  void erase();
  void invalidate(uint32_t, uint32_t);
  void invalidate(uint32_t, uint32_t, uint8_t);
};

}  // namespace FTL

}  // namespace SimpleSSD

#endif
