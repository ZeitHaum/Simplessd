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

#ifndef __FTL_PAGE_MAPPING__
#define __FTL_PAGE_MAPPING__

#include <cinttypes>
#include <unordered_map>
#include <vector>

#include "ftl/abstract_ftl.hh"
#include "ftl/common/block.hh"
#include "ftl/ftl.hh"
#include "pal/pal.hh"
#include "util/disk.hh"


namespace SimpleSSD {

namespace FTL {

class PageMapping : public AbstractFTL {
 private:
  struct PhysicalAddress{
    uint32_t blockIndex;
    uint32_t pageIndex; //may be superpage
    uint16_t iounitIndex; // used in superpage
    uint16_t compressunitIndex; // used in compressed
    PhysicalAddress():
      blockIndex(0), pageIndex(0), iounitIndex(0), compressunitIndex(0)
    {}
    PhysicalAddress(uint32_t bid, uint32_t pid, uint16_t iid, uint16_t cid):
      blockIndex(bid), pageIndex(pid), iounitIndex(iid), compressunitIndex(cid)
    {}
    std::string toString(){
      std::string ret = "{";
      ret += "blockIndex : " + std::to_string(blockIndex) + ", ";
      ret += "pageIndex : " + std::to_string(pageIndex) + ", ";
      ret += "iounitIndex :" + std::to_string(iounitIndex) + ", ";
      ret += "compressunitIndex :" + std::to_string(compressunitIndex) + "}"; 
      return ret;
    }
    void copy(const PhysicalAddress& p){
      blockIndex = p.blockIndex;
      pageIndex = p.pageIndex;
      iounitIndex = p.iounitIndex;
      compressunitIndex = p.compressunitIndex;
    }
  };
  //declare private struct.
  struct WriteInfo{
    bool valid;
    PhysicalAddress toCopyAddr;
    std::vector<LpnInfo> lpns;
    std::vector<uint32_t> lens;
    std::vector<PhysicalAddress> invalidate_addrs;
    uint32_t validcount;
    uint64_t beginAt;
    Bitset validmask;
    uint8_t maxCompressedPageCount;
    WriteInfo(uint8_t mcpn):
      valid(false), validcount(0),
      maxCompressedPageCount(mcpn)
    {
      lpns = std::vector<LpnInfo>(maxCompressedPageCount, {0, 0});
      lens = std::vector<uint32_t>(maxCompressedPageCount, 0);
      validmask = Bitset(maxCompressedPageCount);
    }
  };
  struct MapEntry{
    PhysicalAddress paddr;
    uint64_t is_compressed : 1;
    uint64_t offset : 31;
    uint64_t length : 31;
    // unused 1 bit
    MapEntry():
      paddr(PhysicalAddress()), is_compressed(0), offset(0), length(0)
    {}
    //writeInternal specal adjust
    MapEntry(uint32_t bid, uint32_t pid, uint16_t iid, uint16_t cid, bool is_c, uint32_t off, uint32_t len):
      paddr(PhysicalAddress(bid, pid, iid, cid)), is_compressed(is_c), offset(off), length(len)
    {}
    //Change To Uncompressed.
    void reset(){
      is_compressed = 0;
      offset = 0;
      length = 0;
    }
  };
  PAL::PAL *pPAL;

  ConfigReader &conf;
  std::unordered_map<uint64_t, std::vector<MapEntry>>
      table; // FTL 层映射表
  std::unordered_map<uint32_t, Block> blocks;
  std::list<Block> freeBlocks;
  uint32_t nFreeBlocks;  // For some libraries which std::list::size() is O(n)
  std::vector<uint32_t> lastFreeBlock;
  Bitset lastFreeBlockIOMap;
  uint32_t lastFreeBlockIndex;

  bool bReclaimMore;
  bool bRandomTweak;
  uint32_t bitsetSize;

  struct {
    uint64_t gcCount;
    uint64_t reclaimedBlocks;
    uint64_t validSuperPageCopies;
    uint64_t validPageCopies;
    uint64_t erasedTotalBlocks;
    // new added to observe compress and gc
    uint64_t decompressCount; // decompressed cout (trigged in pdisk read)
    uint64_t totalReadIoUnitCount; //total read I/O unit
    uint64_t overwriteCompressUnitCount; // overwrite compressed unit count (trigged in newly write)
    uint64_t totalWriteIoUnitCount; // total write I/O unit
  } stat;

  CompressedDiskInfo* cd_info;
  uint8_t* compressedBuffer;

  float freeBlockRatio();
  uint32_t convertBlockIdx(uint32_t);
  uint32_t getFreeBlock(uint32_t);
  uint32_t getLastFreeBlock(Bitset &);
  void calculateVictimWeight(std::vector<std::pair<uint32_t, float>> &,
                             const EVICT_POLICY, uint64_t);
  void selectVictimBlock(std::vector<uint32_t> &, uint64_t &);
  void doGarbageCollection(std::vector<uint32_t> &, uint64_t &);  

  float calculateWearLeveling();
  void calculateTotalPages(uint64_t &, uint64_t &);

  void readInternal(Request &, uint64_t &);
  void writeInternal(Request &, uint64_t &, bool = true);
  void writeSubmit(WriteInfo&, PAL::Request&, std::vector<PAL::Request>&);
  void trimInternal(Request &, uint64_t &);
  void eraseInternal(PAL::Request &, uint64_t &);
  BlockStat calculateBlockStat();

 public:
  PageMapping(ConfigReader &, Parameter &, PAL::PAL *, DRAM::AbstractDRAM *);
  ~PageMapping();

  bool initialize() override;

  void read(Request &, uint64_t &) override;
  void write(Request &, uint64_t &) override;
  void trim(Request &, uint64_t &) override;

  void format(LPNRange &, uint64_t &) override;

  Status *getStatus(uint64_t, uint64_t) override;

  void getStatList(std::vector<Stats> &, std::string) override;
  void getStatValues(std::vector<double> &) override;
  void resetStatValues() override;
};

}  // namespace FTL

}  // namespace SimpleSSD

#endif
