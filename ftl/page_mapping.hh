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
#include "time.h"


namespace SimpleSSD {

namespace FTL {

class PageMapping : public AbstractFTL {
 private:
  struct PhysicalAddress{
    uint32_t blockIndex;
    uint32_t pageIndex; //may be 
    //default ioUnitIndex is the same between lpn and ppn.
    uint16_t compressunitIndex; // used in compressed
    PhysicalAddress();
    PhysicalAddress(uint32_t bid, uint32_t pid, uint16_t cid);
    std::string toString();
    void copy(const PhysicalAddress& p);
  };
  //declare private struct.
  struct CompressInfo{
    uint64_t lpn;
    uint32_t new_len;// new write len;
    CompressInfo();
    CompressInfo(uint64_t, uint32_t);
  };
  struct CompressRequest{
    std::vector<std::vector<CompressInfo>> compress_infos;
    std::vector<uint32_t> lens;// calculate the total lens of compress data.
    std::vector<uint16_t> counts;// calculate the counts of every ioUnit
    Parameter* param;
    CompressRequest(Parameter* p);
    void clear();
    void addCompressInfo(CompressInfo compress_info,  uint32_t idx);
    void merge(CompressRequest& compress_req);
    bool empty();
    void update(uint32_t idx, uint32_t comp_ind);
  };
  struct MapEntry{
    PhysicalAddress paddr;
    uint64_t is_compressed : 1;
    uint64_t is_actual : 1;// Check for whether has actual data, used for calculate compress ratio, default is true;
    uint64_t is_buffered : 1;// Check for whether data is in buffer. Only valid in online compress scheme.
    uint64_t offset : 30;
    uint64_t length : 30;
    //unused 1 bit
    MapEntry():
      paddr(PhysicalAddress()), is_compressed(0), is_actual(1), is_buffered(0), offset(0), length(0)
    {}
    //writeInternal specal adjust
    MapEntry(uint32_t bid, uint32_t pid, uint16_t cid, bool is_c, uint32_t off, uint32_t len):
      paddr(PhysicalAddress(bid, pid, cid)), is_compressed(is_c), is_actual(1), is_buffered(0), offset(off), length(len)
    {}
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
  CompressRequest nowCompressReq;
  uint32_t lastFreeBlockIndex;

  bool bReclaimMore;
  bool bRandomTweak;
  uint32_t bitsetSize;

  struct {
    uint64_t gcCount;
    uint64_t reclaimedBlocks;
    uint64_t validSuperPageCopies;
    uint64_t validPageCopies;
    // new added to observe compress and gc
    uint64_t decompressCount; // decompressed cout (trigged in pdisk read)
    uint64_t totalReadIoUnitCount; //total read I/O unit
    uint64_t overwriteCompressUnitCount; // overwrite compressed unit count (trigged in newly write)
    uint64_t totalWriteIoUnitCount; // total write I/O unit
    uint64_t failedCompressCout; // trigged compress but failed count(may be compressed length too large).
    // new added to observe latency model
  } stat;

  CompressedDiskInfo cd_info;
  uint8_t* compressedBuffer;

  float freeBlockRatio();
  uint32_t convertBlockIdx(uint32_t);
  uint32_t getFreeBlock(uint32_t);
  bool isAvailable(const CompressRequest& compress_req);
  uint32_t getLastFreeBlock(Bitset&);
  void calculateVictimWeight(std::vector<std::pair<uint32_t, float>> &,
                             const EVICT_POLICY, uint64_t);
  void selectVictimBlock(std::vector<uint32_t> &, uint64_t &);
  void getCompressedLengthFromDisk(uint64_t, uint32_t,const MapEntry&, CompressInfo&, uint64_t&);
  void doGarbageCollection(std::vector<uint32_t> &, uint64_t &);  

  float calculateWearLeveling();
  void calculateTotalPages(uint64_t &, uint64_t &);

  void readInternal(Request &, uint64_t &);
  void writeInternal(Request &, uint64_t &, bool = true);
  void compressSubmit(PAL::Request&, std::vector<PAL::Request>&, Bitset&,  uint64_t&);
  void trimInternal(Request &, uint64_t &);
  void eraseInternal(PAL::Request &, uint64_t &);
  BlockStat calculateBlockStat();  
  CompressedDisk::CompressDiskStats* getCompDiskStat();

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
