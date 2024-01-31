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

#include "ftl/page_mapping.hh"

#include <algorithm>
#include <limits>
#include <random>

#include "util/algorithm.hh"
#include "util/bitset.hh"


namespace SimpleSSD {

namespace FTL {

PageMapping::PageMapping(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
    : AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      lastFreeBlock(param.pageCountToMaxPerf),
      lastFreeBlockIOMap(param.ioUnitInPage),
      bReclaimMore(false),
      cd_info(nullptr)
       {
  Block::setStaticAttr(param.ioUnitSize, param.maxCompressUnitInPage);
  blocks.reserve(param.totalPhysicalBlocks);
  table.reserve(param.totalLogicalBlocks * param.pagesInBlock);
  compressedBuffer = new uint8_t[param.ioUnitSize];

  for (uint32_t i = 0; i < param.totalPhysicalBlocks; i++) {
    freeBlocks.emplace_back(Block(i, param.pagesInBlock, param.ioUnitInPage));
  }

  nFreeBlocks = param.totalPhysicalBlocks;

  status.totalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;

  // Allocate free blocks
  for (uint32_t i = 0; i < param.pageCountToMaxPerf; i++) {
    lastFreeBlock.at(i) = getFreeBlock(i);
  }

  lastFreeBlockIndex = 0;

  memset(&stat, 0, sizeof(stat));

  bRandomTweak = conf.readBoolean(CONFIG_FTL, FTL_USE_RANDOM_IO_TWEAK);
  bitsetSize = bRandomTweak ? param.ioUnitInPage : 1;
}

PageMapping::~PageMapping() {
  delete[] compressedBuffer;
}

bool PageMapping::initialize() {
  uint64_t nPagesToWarmup;
  uint64_t nPagesToInvalidate;
  uint64_t nTotalLogicalPages;
  uint64_t maxPagesBeforeGC;
  uint64_t tick;
  uint64_t valid;
  uint64_t invalid;
  FILLING_MODE mode;

  Request req(param.ioUnitInPage);

  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization started");

  nTotalLogicalPages = param.totalLogicalBlocks * param.pagesInBlock;
  nPagesToWarmup =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_FILL_RATIO);
  nPagesToInvalidate =
      nTotalLogicalPages * conf.readFloat(CONFIG_FTL, FTL_INVALID_PAGE_RATIO);
  mode = (FILLING_MODE)conf.readUint(CONFIG_FTL, FTL_FILLING_MODE);
  maxPagesBeforeGC =
      param.pagesInBlock *
      (param.totalPhysicalBlocks *
           (1 - conf.readFloat(CONFIG_FTL, FTL_GC_THRESHOLD_RATIO)) -
       param.pageCountToMaxPerf);  // # free blocks to maintain

  if (nPagesToWarmup + nPagesToInvalidate > maxPagesBeforeGC) {
    warn("ftl: Too high filling ratio. Adjusting invalidPageRatio.");
    nPagesToInvalidate = maxPagesBeforeGC - nPagesToWarmup;
  }

  debugprint(LOG_FTL_PAGE_MAPPING, "Total logical pages: %" PRIu64,
             nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total logical pages to fill: %" PRIu64 " (%.2f %%)",
             nPagesToWarmup, nPagesToWarmup * 100.f / nTotalLogicalPages);
  debugprint(LOG_FTL_PAGE_MAPPING,
             "Total invalidated pages to create: %" PRIu64 " (%.2f %%)",
             nPagesToInvalidate,
             nPagesToInvalidate * 100.f / nTotalLogicalPages);

  req.ioFlag.set();

  // Step 1. Filling
  if (mode == FILLING_MODE_0 || mode == FILLING_MODE_1) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToWarmup; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Step 2. Invalidating
  if (mode == FILLING_MODE_0) {
    // Sequential
    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = i;
      writeInternal(req, tick, false);
    }
  }
  else if (mode == FILLING_MODE_1) {
    // Random
    // We can successfully restrict range of LPN to create exact number of
    // invalid pages because we wrote in sequential mannor in step 1.
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nPagesToWarmup - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }
  else {
    // Random
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, nTotalLogicalPages - 1);

    for (uint64_t i = 0; i < nPagesToInvalidate; i++) {
      tick = 0;
      req.lpn = dist(gen);
      writeInternal(req, tick, false);
    }
  }

  // Report
  calculateTotalPages(valid, invalid);
  debugprint(LOG_FTL_PAGE_MAPPING, "Filling finished. Page status:");
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total valid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             valid, valid * 100.f / nTotalLogicalPages, nPagesToWarmup,
             (int64_t)(valid - nPagesToWarmup));
  debugprint(LOG_FTL_PAGE_MAPPING,
             "  Total invalid physical pages: %" PRIu64
             " (%.2f %%, target: %" PRIu64 ", error: %" PRId64 ")",
             invalid, invalid * 100.f / nTotalLogicalPages, nPagesToInvalidate,
             (int64_t)(invalid - nPagesToInvalidate));
  debugprint(LOG_FTL_PAGE_MAPPING, "Initialization finished");

  return true;
}

void PageMapping::read(Request &req, uint64_t &tick) {
  uint64_t begin = tick;
  this->cd_info = &req.cd_info;

  if (req.ioFlag.count() > 0) {
    readInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "READ  | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ);
}

void PageMapping::write(Request &req, uint64_t &tick) {
  uint64_t begin = tick;
  this->cd_info = &req.cd_info;

  if (req.ioFlag.count() > 0) {
    writeInternal(req, tick);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "WRITE | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
               ")",
               req.lpn, begin, tick, tick - begin);
  }
  else {
    warn("FTL got empty request");
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE);
}

void PageMapping::trim(Request &req, uint64_t &tick) {
  uint64_t begin = tick;
  this->cd_info = &req.cd_info;

  trimInternal(req, tick);

  debugprint(LOG_FTL_PAGE_MAPPING,
             "TRIM  | LPN %" PRIu64 " | %" PRIu64 " - %" PRIu64 " (%" PRIu64
             ")",
             req.lpn, begin, tick, tick - begin);

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::TRIM);
}

void PageMapping::format(LPNRange &range, uint64_t &tick) {
  PAL::Request req(param.ioUnitInPage);
  std::vector<uint32_t> list;

  req.ioFlag.set();

  for (auto iter = table.begin(); iter != table.end();) {
    if (iter->first >= range.slpn && iter->first < range.slpn + range.nlp) {
      auto &mappingList = iter->second;

      // Do trim
      for (uint32_t idx = 0; idx < bitsetSize; idx++) {
        MapEntry &mapping = mappingList.at(idx);
        auto block = blocks.find(mapping.paddr.blockIndex);

        if (block == blocks.end()) {
          panic("Block is not in use");
        }

        if(mapping.is_compressed){
          block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, mapping.length);
        }
        else{
          block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, param.ioUnitSize);
        }

        // Collect block indices
        list.push_back(mapping.paddr.blockIndex);
      }

      iter = table.erase(iter);
    }
    else {
      iter++;
    }
  }

  // Get blocks to erase
  std::sort(list.begin(), list.end());
  auto last = std::unique(list.begin(), list.end());
  list.erase(last, list.end());

  // Do GC only in specified blocks
  doGarbageCollection(list, tick);

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::FORMAT);
}

Status *PageMapping::getStatus(uint64_t lpnBegin, uint64_t lpnEnd) {
  status.freePhysicalBlocks = nFreeBlocks;

  if (lpnBegin == 0 && lpnEnd >= status.totalLogicalPages) {
    status.mappedLogicalPages = table.size();
  }
  else {
    status.mappedLogicalPages = 0;

    for (uint64_t lpn = lpnBegin; lpn < lpnEnd; lpn++) {
      if (table.count(lpn) > 0) {
        status.mappedLogicalPages++;
      }
    }
  }

  return &status;
}

float PageMapping::freeBlockRatio() {
  return (float)nFreeBlocks / param.totalPhysicalBlocks;
}

uint32_t PageMapping::convertBlockIdx(uint32_t blockIdx) {
  return blockIdx % param.pageCountToMaxPerf;
}

uint32_t PageMapping::getFreeBlock(uint32_t idx) {
  uint32_t blockIndex = 0;

  if (idx >= param.pageCountToMaxPerf) {
    panic("Index out of range");
  }

  if (nFreeBlocks > 0) {
    // Search block which is blockIdx % param.pageCountToMaxPerf == idx
    auto iter = freeBlocks.begin();

    for (; iter != freeBlocks.end(); iter++) {
      blockIndex = iter->getBlockIndex();

      if (blockIndex % param.pageCountToMaxPerf == idx) {
        break;
      }
    }

    // Sanity check
    if (iter == freeBlocks.end()) {
      // Just use first one
      iter = freeBlocks.begin();
      blockIndex = iter->getBlockIndex();
    }

    // Insert found block to block list
    if (blocks.find(blockIndex) != blocks.end()) {
      panic("Corrupted");
    }

    blocks.emplace(blockIndex, std::move(*iter));

    // Remove found block from free block list
    freeBlocks.erase(iter);
    nFreeBlocks--;
  }
  else {
    panic("No free block left");
  }

  return blockIndex;
}

uint32_t PageMapping::getLastFreeBlock(Bitset &iomap) {
  if (!bRandomTweak || (lastFreeBlockIOMap & iomap).any()) {
    // Update lastFreeBlockIndex
    lastFreeBlockIndex++;

    if (lastFreeBlockIndex == param.pageCountToMaxPerf) {
      lastFreeBlockIndex = 0;
    }

    lastFreeBlockIOMap = iomap;
  }
  else {
    lastFreeBlockIOMap |= iomap;
  }

  auto freeBlock = blocks.find(lastFreeBlock.at(lastFreeBlockIndex));

  // Sanity check
  if (freeBlock == blocks.end()) {
    panic("Corrupted");
  }

  // If current free block is full, get next block
  if (freeBlock->second.getNextWritePageIndex() == param.pagesInBlock) {
    lastFreeBlock.at(lastFreeBlockIndex) = getFreeBlock(lastFreeBlockIndex);

    bReclaimMore = true;
  }

  return lastFreeBlock.at(lastFreeBlockIndex);
}

// calculate weight of each block regarding victim selection policy
void PageMapping::calculateVictimWeight(
    std::vector<std::pair<uint32_t, float>> &weight, const EVICT_POLICY policy,
    uint64_t tick) {
  float temp;

  weight.reserve(blocks.size());

  switch (policy) {
    case POLICY_GREEDY:
    case POLICY_RANDOM:
    case POLICY_DCHOICE:
      for (auto &iter : blocks) {
        if (iter.second.getNextWritePageIndex() != param.pagesInBlock) {
          continue;
        }

        weight.push_back({iter.first, iter.second.getValidPageCountRaw()});
      }

      break;
    case POLICY_COST_BENEFIT:
      for (auto &iter : blocks) {
        if (iter.second.getNextWritePageIndex() != param.pagesInBlock) {
          continue;
        }

        temp = (float)(iter.second.getValidPageCountRaw()) / param.pagesInBlock;

        weight.push_back(
            {iter.first,
             temp / ((1 - temp) * (tick - iter.second.getLastAccessedTime()))});
      }

      break;
    default:
      panic("Invalid evict policy");
  }
}

void PageMapping::selectVictimBlock(std::vector<uint32_t> &list,
                                    uint64_t &tick) {
  static const GC_MODE mode = (GC_MODE)conf.readInt(CONFIG_FTL, FTL_GC_MODE);
  static const EVICT_POLICY policy =
      (EVICT_POLICY)conf.readInt(CONFIG_FTL, FTL_GC_EVICT_POLICY);
  static uint32_t dChoiceParam =
      conf.readUint(CONFIG_FTL, FTL_GC_D_CHOICE_PARAM);
  uint64_t nBlocks = conf.readUint(CONFIG_FTL, FTL_GC_RECLAIM_BLOCK);
  std::vector<std::pair<uint32_t, float>> weight;

  list.clear();

  // Calculate number of blocks to reclaim
  if (mode == GC_MODE_0) {
    // DO NOTHING
  }
  else if (mode == GC_MODE_1) {
    static const float t = conf.readFloat(CONFIG_FTL, FTL_GC_RECLAIM_THRESHOLD);

    nBlocks = param.totalPhysicalBlocks * t - nFreeBlocks;
  }
  else {
    panic("Invalid GC mode");
  }

  // reclaim one more if last free block fully used
  if (bReclaimMore) {
    nBlocks += param.pageCountToMaxPerf;

    bReclaimMore = false;
  }

  // Calculate weights of all blocks
  calculateVictimWeight(weight, policy, tick);

  if (policy == POLICY_RANDOM || policy == POLICY_DCHOICE) {
    uint64_t randomRange =
        policy == POLICY_RANDOM ? nBlocks : dChoiceParam * nBlocks;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, weight.size() - 1);
    std::vector<std::pair<uint32_t, float>> selected;

    while (selected.size() < randomRange) {
      uint64_t idx = dist(gen);

      if (weight.at(idx).first < std::numeric_limits<uint32_t>::max()) {
        selected.push_back(weight.at(idx));
        weight.at(idx).first = std::numeric_limits<uint32_t>::max();
      }
    }

    weight = std::move(selected);
  }

  // Sort weights
  std::sort(
      weight.begin(), weight.end(),
      [](std::pair<uint32_t, float> a, std::pair<uint32_t, float> b) -> bool {
        return a.second < b.second;
      });

  // Select victims from the blocks with the lowest weight
  nBlocks = MIN(nBlocks, weight.size());

  for (uint64_t i = 0; i < nBlocks; i++) {
    list.push_back(weight.at(i).first);
  }

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::SELECT_VICTIM_BLOCK);
}

void PageMapping::doGarbageCollection(std::vector<uint32_t> &blocksToReclaim,
                                      uint64_t &tick) {
  PAL::Request req(param.ioUnitInPage);
  std::vector<PAL::Request> readRequests;
  std::vector<PAL::Request> writeRequests;
  std::vector<PAL::Request> eraseRequests;
  std::vector<std::vector<LpnInfo>> lpns(param.ioUnitInPage, vector<LpnInfo>(param.maxCompressUnitInPage));
  std::vector<Bitset> bits(param.ioUnitInPage, Bitset(param.maxCompressUnitInPage));
  std::vector<LpnInfo> t_lpn(param.maxCompressUnitInPage, {0, 0}); // used for temp get lpnInfo.
  Bitset t_bst(param.maxCompressUnitInPage);// used for temp get lpnvalidBits.
  Bitset iomap(param.ioUnitInPage);
  uint64_t beginAt;
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;

  if (blocksToReclaim.size() == 0) {
    return;
  }

  WriteInfo w_info = WriteInfo(param.maxCompressUnitInPage);

  // For all blocks to reclaim, collecting request structure only
  for (auto &iter : blocksToReclaim) {
    auto block = blocks.find(iter);

    if (block == blocks.end()) {
      panic("Invalid block");
    }

    // Copy valid pages to free block
    for (uint32_t pageIndex = 0; pageIndex < param.pagesInBlock; pageIndex++) {
      // Valid?
      if (block->second.getPageInfo(pageIndex, lpns, bits)) {
        if (!bRandomTweak) {
          // bit.set();
          warn("Not Support No bRandomTweak.");
        }

        //Generate iomap
        for(uint32_t i = 0; i< param.ioUnitInPage; ++i){
          iomap.set(i, bits[i].any());
        }

        // Retrive free block
        auto freeBlock = blocks.find(getLastFreeBlock(iomap));

        // Issue Read
        req.blockIndex = block->first;
        req.pageIndex = pageIndex;
        req.ioFlag = iomap;

        readRequests.push_back(req);

        // Update mapping table
        uint32_t nowPageCnt = 0;
        uint32_t nowPageIdx = 0;
        uint32_t nowOffset = 0;

        bool is_filled = true;
        for (uint16_t idx = 0; idx < bitsetSize; idx++) {
          if (iomap.test(idx)) {
            for(uint16_t cIdx = 0; cIdx < param.maxCompressUnitInPage; cIdx++){
              if(bits[idx].test(cIdx)){
                LpnInfo& lpn_info = lpns.at(idx).at(cIdx);
                auto mappingList = table.find(lpn_info.lpn);
                if (mappingList == table.end()) {
                  panic("Invalid mapping table entry");
                }
                pDRAM->read(&(*mappingList), (sizeof(MapEntry)) * param.ioUnitInPage, tick);

                MapEntry &mapping = mappingList->second.at(lpn_info.idx);
                uint64_t idxSize = param.ioUnitSize;
                uint64_t CompressedLength = idxSize;
                //Get Compressed Length
                if(!conf.readBoolean(CONFIG_NVME, HIL::NVMe::NVME_ENABLE_COMPRESS)){
                  //Disable Compress
                  CompressedLength = idxSize;
                }
                else if(mapping.is_compressed == 0x0){
                  //Not Compressed, Need Compress.
                  uint64_t disk_offset = (lpn_info.lpn * param.ioUnitInPage + lpn_info.idx) * idxSize - cd_info->offset;
                  uint64_t disk_length = idxSize;
                  CompressedDisk* pcDisk = ((CompressedDisk*)(cd_info->pDisk));
                  if(cd_info->pDisk){
                    CompressedLength = pcDisk->getCompressedLength(disk_offset/idxSize);
                  }
                  if(CompressedLength == idxSize){
                    //Need Compress
                    pcDisk -> readOrdinary(disk_offset, disk_length, compressedBuffer);
                    bool is_comp = pcDisk -> compressWrite(disk_offset / idxSize, compressedBuffer);
                    if(is_comp){
                      // debugprint(LOG_FTL_PAGE_MAPPING, "Compressed Trigged In GC! pageIndex =%" PRIu64 ", idx = %" PRIu64, pageIndex, idx);
                    }
                    else{
                      ++stat.failedCompressCout;
                      debugprint(LOG_FTL_PAGE_MAPPING, "Compressed Trigged Failed! pageIndex =%" PRIu64 ", idx = %" PRIu64 , pageIndex, idx);
                    }
                  }
                  CompressedLength = pcDisk->getCompressedLength(disk_offset/idxSize);
                  assert(CompressedLength <= idxSize && "panic: compresslength too large");
                }
                else {
                  CompressedLength = mapping.length;
                }
                // |= 处理w_info初始值赋值的情况
                is_filled |= (nowOffset + CompressedLength > idxSize) || (w_info.validcount >= param.maxCompressUnitInPage);
                // Handle if is_filled
                if(is_filled){
                  if(w_info.valid) {
                    writeSubmit(w_info, req, writeRequests);
                  }
                  nowPageIdx = freeBlock->second.getNextWritePageIndex(idx);
                  nowPageCnt = 0;
                  nowOffset = 0;
                  is_filled = false;
                  w_info.valid = true;
                  w_info.toCopyAddr.blockIndex = freeBlock->second.getBlockIndex();
                  w_info.toCopyAddr.pageIndex = nowPageIdx;
                  w_info.toCopyAddr.iounitIndex = idx;
                  //check toCopyAddr valid
                  if(w_info.toCopyAddr.blockIndex >= param.totalPhysicalBlocks || w_info.toCopyAddr.pageIndex >= param.pagesInBlock || w_info.toCopyAddr.iounitIndex >= param.ioUnitInPage) {
                    panic("TocopyAddress overflow!");
                  }
                  w_info.validmask.reset();
                  w_info.beginAt = beginAt;
                }
                mapping.paddr.copy(w_info.toCopyAddr);
                mapping.paddr.compressunitIndex = nowPageCnt++;
                nowOffset += CompressedLength;
                //w_info.lens should update carefully
                if(!mapping.is_compressed){
                  w_info.old_lens[w_info.validcount] = param.ioUnitSize;
                }
                else{
                  w_info.old_lens[w_info.validcount] = mapping.length;
                }
                mapping.is_compressed = (CompressedLength < idxSize);
                assert(CompressedLength <= idxSize);
                mapping.offset = nowOffset;
                mapping.length = CompressedLength;
                block->second.getLPNs(pageIndex, t_lpn, t_bst,idx);
                w_info.validmask.set(w_info.validcount);
                w_info.new_lens.at(w_info.validcount) = mapping.length;
                w_info.lpns.at(w_info.validcount) = t_lpn[0];
                w_info.invalidate_addrs.push_back({(uint32_t)(iter), pageIndex, idx, cIdx});
                w_info.validcount++;
              }  
            }
            stat.validPageCopies++;//Invalid
          }
        }
        // 禁止跨页
        if(w_info.valid){
          writeSubmit(w_info, req, writeRequests);
        }

        stat.validSuperPageCopies++;//Invalid
      }
    }

    // Erase block
    req.blockIndex = block->first;
    req.pageIndex = 0;
    req.ioFlag.set();

    eraseRequests.push_back(req);
  }

  // Do actual I/O here
  // This handles PAL2 limitation (SIGSEGV, infinite loop, or so-on)
  for (auto &iter : readRequests) {
    beginAt = tick;

    pPAL->read(iter, beginAt);

    readFinishedAt = MAX(readFinishedAt, beginAt);
  }

  for (auto &iter : writeRequests) {
    beginAt = readFinishedAt;

    pPAL->write(iter, beginAt);

    writeFinishedAt = MAX(writeFinishedAt, beginAt);
  }

  for (auto &iter : eraseRequests) {
    beginAt = readFinishedAt;

    eraseInternal(iter, beginAt);

    eraseFinishedAt = MAX(eraseFinishedAt, beginAt);
  }

  tick = MAX(writeFinishedAt, eraseFinishedAt);
  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::DO_GARBAGE_COLLECTION);
}

void PageMapping::readInternal(Request &req, uint64_t &tick) {
  PAL::Request palRequest(req);
  uint64_t beginAt;
  uint64_t finishedAt = tick;

  auto mappingList = table.find(req.lpn);

  if (mappingList != table.end()) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), sizeof(MapEntry) * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), sizeof(MapEntry), tick);
    }

    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      if (req.ioFlag.test(idx) || !bRandomTweak) {
        MapEntry &mapping = mappingList->second.at(idx);

        if (mapping.paddr.blockIndex < param.totalPhysicalBlocks &&
            mapping.paddr.pageIndex < param.pagesInBlock) {
          palRequest.blockIndex = mapping.paddr.blockIndex;
          palRequest.pageIndex = mapping.paddr.pageIndex;
          if (bRandomTweak) {
            palRequest.ioFlag.reset();
            if(mapping.is_compressed == 0x1){
              palRequest.ioFlag.set(mapping.paddr.iounitIndex);
            }
            else palRequest.ioFlag.set(idx);
          }
          else {
            palRequest.ioFlag.set();
          }

          auto block = blocks.find(palRequest.blockIndex);

          if (block == blocks.end()) {
            panic("Block is not in use");
          }

          beginAt = tick;

          block->second.read(palRequest.pageIndex, idx, beginAt);
          pPAL->read(palRequest, beginAt);

          //DECOMPRESSE
          ++stat.totalReadIoUnitCount;
          if(mapping.is_compressed == 0x1){
            // Actual decompress trigged in HIL layer.
            ++stat.decompressCount;
            debugprint(LOG_FTL_PAGE_MAPPING,("Compressed info: IsCompressed = " + std::to_string(mapping.is_compressed) + ", C_IND = "+ std::to_string(mapping.paddr.compressunitIndex) + ", OFFSET = "+ std::to_string(mapping.offset) + ", LENGTH = " + std::to_string(mapping.length)).c_str());
          }

          finishedAt = MAX(finishedAt, beginAt);
        }
      }
    }

    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::READ_INTERNAL);
  }
}

void PageMapping::writeInternal(Request &req, uint64_t &tick, bool sendToPAL) {
  PAL::Request palRequest(req);
  std::unordered_map<uint32_t, Block>::iterator block;
  auto mappingList = table.find(req.lpn);
  uint64_t beginAt;
  uint64_t finishedAt = tick;
  bool readBeforeWrite = false;

  if (mappingList != table.end()) {
    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      if (req.ioFlag.test(idx) || !bRandomTweak) {
        MapEntry &mapping = mappingList->second.at(idx);

        if (mapping.paddr.blockIndex < param.totalPhysicalBlocks &&
            mapping.paddr.pageIndex < param.pagesInBlock) {
          block = blocks.find(mapping.paddr.blockIndex);

          if(block == blocks.end()){
            panic("Error: pageMapping Invalid Block!");
          }

          // Invalidate current page
          // 根据压缩行为分别判断
          ++stat.totalWriteIoUnitCount;
          if(mapping.is_compressed){
            ++stat.overwriteCompressUnitCount;
            block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, mapping.length);
          }
          else{
            block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, param.ioUnitInPage);
          }
        }
      }
    }
  }
  else {
    //Check Range
    if(param.totalPhysicalBlocks > std::numeric_limits<uint32_t>::max() || param.totalPhysicalBlocks > std::numeric_limits<uint32_t>::max()){
      panic("Total number of blocks or pages is too large, may be overflow.");
    }
    // Create empty mapping
    auto ret = table.emplace(
        req.lpn,
        std::vector<MapEntry>(
            bitsetSize, {(uint32_t)param.totalPhysicalBlocks, (uint32_t)param.pagesInBlock, 0, 0, false, 0, param.ioUnitInPage}));
    if (!ret.second) {
      panic("Failed to insert new mapping");
    }

    mappingList = ret.first;
  }

  // Write data to free block
  block = blocks.find(getLastFreeBlock(req.ioFlag));

  if (block == blocks.end()) {
    panic("No such block");
  }

  if (sendToPAL) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), sizeof(MapEntry) * req.ioFlag.count(), tick);
      pDRAM->write(&(*mappingList), sizeof(MapEntry) * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), sizeof(MapEntry), tick);
      pDRAM->write(&(*mappingList), sizeof(MapEntry), tick);
    }
  }

  if (!bRandomTweak && !req.ioFlag.all()) {
    // We have to read old data
    readBeforeWrite = true;
  }

  //First Write Don't Compress.
  Bitset validmask = Bitset(param.maxCompressUnitInPage);
  std::vector<LpnInfo> lpns(param.maxCompressUnitInPage, {0, 0});
  std::vector<uint32_t> lens(param.maxCompressUnitInPage, 0);
  validmask.set(0);
  for (uint32_t idx = 0; idx < bitsetSize; idx++) {
    if (req.ioFlag.test(idx) || !bRandomTweak) {
      uint32_t pageIndex = block->second.getNextWritePageIndex(idx);
      MapEntry &mapping = mappingList->second.at(idx);

      beginAt = tick;
      
      //first write, write 0.
      lpns[0] = {req.lpn, idx};
      lens[0] = param.ioUnitSize;
      block->second.write(pageIndex, lpns, lens, validmask, idx, beginAt);

      // Read old data if needed (Only executed when bRandomTweak = false)
      // Maybe some other init procedures want to perform 'partial-write'
      // So check sendToPAL variable
      if (readBeforeWrite && sendToPAL) {
        palRequest.blockIndex = mapping.paddr.blockIndex;
        palRequest.pageIndex = mapping.paddr.pageIndex;

        // We don't need to read old data
        palRequest.ioFlag = req.ioFlag;
        palRequest.ioFlag.flip();

        pPAL->read(palRequest, beginAt);
      }

      // update mapping to table
      mapping.paddr.blockIndex = block->first;
      mapping.paddr.pageIndex = pageIndex;
      mapping.paddr.iounitIndex = idx;
      mapping.paddr.compressunitIndex = 0;
      mapping.is_compressed = 0;
      mapping.length = param.ioUnitSize;
      mapping.offset = 0;

      if (sendToPAL) {
        palRequest.blockIndex = block->first;
        palRequest.pageIndex = pageIndex;

        if (bRandomTweak) {
          palRequest.ioFlag.reset();
          palRequest.ioFlag.set(idx);
        }
        else {
          palRequest.ioFlag.set();
        }

        pPAL->write(palRequest, beginAt);
      }

      finishedAt = MAX(finishedAt, beginAt);
    }
  }

  // Exclude CPU operation when initializing
  if (sendToPAL) {
    tick = finishedAt;
    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::WRITE_INTERNAL);
  }

  // GC if needed
  // I assumed that init procedure never invokes GC
  static float gcThreshold = conf.readFloat(CONFIG_FTL, FTL_GC_THRESHOLD_RATIO);

  if (freeBlockRatio() < gcThreshold) {
    if (!sendToPAL) {
      panic("ftl: GC triggered while in initialization");
    }

    std::vector<uint32_t> list;
      uint64_t beginAt = tick;

    selectVictimBlock(list, beginAt);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "GC   | On-demand | %u blocks will be reclaimed", list.size());

    doGarbageCollection(list, beginAt);

    debugprint(LOG_FTL_PAGE_MAPPING,
               "GC   | Done | %" PRIu64 " - %" PRIu64 " (%" PRIu64 ")", tick,
               beginAt, beginAt - tick);

    stat.gcCount++;
    stat.reclaimedBlocks += list.size();
  }
}

void PageMapping::writeSubmit(WriteInfo& w_info, PAL::Request& req, std::vector<PAL::Request>& writeRequests){
  // Block
  assert(w_info.valid);
  std::unordered_map<uint32_t, Block>::iterator freeBlock = blocks.find(w_info.toCopyAddr.blockIndex);
  for(uint32_t i = 0; i<w_info.invalidate_addrs.size(); ++i){
    auto indv_block = blocks.find(w_info.invalidate_addrs[i].blockIndex);
    indv_block->second.invalidate(w_info.invalidate_addrs[i].pageIndex, w_info.invalidate_addrs[i].iounitIndex, w_info.invalidate_addrs[i].compressunitIndex, w_info.old_lens[i]);
  } 
  freeBlock->second.write(w_info.toCopyAddr.pageIndex, w_info.lpns, w_info.new_lens, w_info.validmask,  w_info.toCopyAddr.iounitIndex, w_info.beginAt);
  req.blockIndex = freeBlock->first;
  req.pageIndex = w_info.toCopyAddr.pageIndex;

  if (bRandomTweak) {
    req.ioFlag.reset();
    req.ioFlag.set(w_info.toCopyAddr.iounitIndex);
  }
  else {
    req.ioFlag.set();
  }

  writeRequests.push_back(req);
  //Clear W_info
  w_info.valid = false;
  w_info.validcount = 0;
  w_info.invalidate_addrs.clear();
}

void PageMapping::trimInternal(Request &req, uint64_t &tick) {
  auto mappingList = table.find(req.lpn);

  if (mappingList != table.end()) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), sizeof(MapEntry) * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), sizeof(MapEntry), tick);
    }

    // Do trim
    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      MapEntry &mapping = mappingList->second.at(idx);
      auto block = blocks.find(mapping.paddr.blockIndex);

      if (block == blocks.end()) {
        panic("Block is not in use");
      }

      if(mapping.is_compressed){
        block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, mapping.length);
      }
      else{
        block->second.invalidate(mapping.paddr.pageIndex, mapping.paddr.iounitIndex, mapping.paddr.compressunitIndex, mapping.length);
      }
    }

    // Remove mapping
    table.erase(mappingList);

    tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::TRIM_INTERNAL);
  }
}

void PageMapping::eraseInternal(PAL::Request &req, uint64_t &tick) {
  static uint64_t threshold =
      conf.readUint(CONFIG_FTL, FTL_BAD_BLOCK_THRESHOLD);
  auto block = blocks.find(req.blockIndex);

  // Sanity checks
  if (block == blocks.end()) {
    panic("No such block");
  }

  if (block->second.getValidPageCount() != 0) {
    panic("There are valid pages in victim block");
  }

  // Erase block
  block->second.erase();

  pPAL->erase(req, tick);
  ++stat.erasedTotalBlocks;

  // Check erase count
  uint32_t erasedCount = block->second.getEraseCount();

  if (erasedCount < threshold) {
    // Reverse search
    auto iter = freeBlocks.end();

    while (true) {
      iter--;

      if (iter->getEraseCount() <= erasedCount) {
        // emplace: insert before pos
        iter++;

        break;
      }

      if (iter == freeBlocks.begin()) {
        break;
      }
    }

    // Insert block to free block list
    freeBlocks.emplace(iter, std::move(block->second));
    nFreeBlocks++;
  }

  // Remove block from block list
  blocks.erase(block);

  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::ERASE_INTERNAL);
}

float PageMapping::calculateWearLeveling() {
  uint64_t totalEraseCnt = 0;
  uint64_t sumOfSquaredEraseCnt = 0;
  uint64_t numOfBlocks = param.totalLogicalBlocks;
  uint64_t eraseCnt;

  for (auto &iter : blocks) {
    eraseCnt = iter.second.getEraseCount();
    totalEraseCnt += eraseCnt;
    sumOfSquaredEraseCnt += eraseCnt * eraseCnt;
  }

  // freeBlocks is sorted
  // Calculate from backward, stop when eraseCnt is zero
  for (auto riter = freeBlocks.rbegin(); riter != freeBlocks.rend(); riter++) {
    eraseCnt = riter->getEraseCount();

    if (eraseCnt == 0) {
      break;
    }

    totalEraseCnt += eraseCnt;
    sumOfSquaredEraseCnt += eraseCnt * eraseCnt;
  }

  if (sumOfSquaredEraseCnt == 0) {
    return -1;  // no meaning of wear-leveling
  }

  return (float)totalEraseCnt * totalEraseCnt /
         (numOfBlocks * sumOfSquaredEraseCnt);
}

void PageMapping::calculateTotalPages(uint64_t &valid, uint64_t &invalid) {
  valid = 0;
  invalid = 0;

  for (auto &iter : blocks) {
    valid += iter.second.getValidPageCount();
    invalid += iter.second.getDirtyPageCount();
  }
}

void PageMapping::getStatList(std::vector<Stats> &list, std::string prefix) {
  Stats temp;

  temp.name = prefix + "page_mapping.gc.count";
  temp.desc = "Total GC count";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.reclaimed_blocks";
  temp.desc = "Total reclaimed blocks in GC";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.superpage_copies";
  temp.desc = "Total copied valid superpages during GC";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.gc.page_copies";
  temp.desc = "Total copied valid pages during GC";
  list.push_back(temp);

  // For the exact definition, see following paper:
  // Li, Yongkun, Patrick PC Lee, and John Lui.
  // "Stochastic modeling of large-scale solid-state storage systems: analysis,
  // design tradeoffs and optimization." ACM SIGMETRICS (2013)
  temp.name = prefix + "page_mapping.wear_leveling";
  temp.desc = "Wear-leveling factor";
  list.push_back(temp);

  //Add by ZeitHaum
  temp.name = prefix + "page_mapping.gc.erasedTotalBlocks";
  temp.desc = "Total erased blocks during GC";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.totalDataLength";
  temp.desc = "Total valid data length (before compress)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.validDataLength";
  temp.desc = "Total valid data length (after compress)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.validIoUnitCount";
  temp.desc = "Total valid iounit count (in physical).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.compressUnitCount";
  temp.desc = "Total compressunit count (don't include uncompressed unit)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.totalUnitCount";
  temp.desc = "Total unit count (include uncompressed unit and compressed unit)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.decompressCount";
  temp.desc = "Total decompress count (Trigged in read)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.totalReadIoUnitCount";
  temp.desc = "Total Read unit count";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.overwriteCompressUnitCount";
  temp.desc = "Total overwrite compressunit Count(Trigged in write)";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.totalWriteIoUnitCount";
  temp.desc = "Total Write unit Count";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compress.failedCompressCout";
  temp.desc = "Trigged compress but failed count(may be compressed length too large).";
  list.push_back(temp);
}

void PageMapping::getStatValues(std::vector<double> &values) {
  values.push_back(stat.gcCount);
  values.push_back(stat.reclaimedBlocks);
  values.push_back(stat.validSuperPageCopies);
  values.push_back(stat.validPageCopies);
  values.push_back(calculateWearLeveling());
  values.push_back(stat.erasedTotalBlocks);
  BlockStat blockstat = calculateBlockStat();
  values.push_back(blockstat.totalDataLength);
  values.push_back(blockstat.validDataLength);
  values.push_back(blockstat.validIoUnitCount);
  values.push_back(blockstat.compressUnitCount);
  values.push_back(blockstat.totalUnitCount);
  values.push_back(stat.decompressCount);
  values.push_back(stat.totalReadIoUnitCount);
  values.push_back(stat.overwriteCompressUnitCount);
  values.push_back(stat.totalWriteIoUnitCount);
  values.push_back(stat.failedCompressCout);
}

void PageMapping::resetStatValues() {
  memset(&stat, 0, sizeof(stat));
}

BlockStat PageMapping::calculateBlockStat(){
  BlockStat ret;
  ret.reset();
  //iterate blocks
  for(auto& iter:blocks){
    //iterate pages
    auto& block = iter.second;
    ret+= block.getBlockStat();
  }
  return ret;
}

}  // namespace FTL

}  // namespace SimpleSSD
