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
#include "lib/mytest/testclass.cc"

#define DEBUG_TEST

namespace SimpleSSD {

namespace FTL {

PageMapping::PageMapping(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
    : AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      lastFreeBlock(param.pageCountToMaxPerf),
      lastFreeBlockIOMap(param.ioUnitInPage),
      bReclaimMore(false) {
  blocks.reserve(param.totalPhysicalBlocks);
  table.reserve(param.totalLogicalBlocks * param.pagesInBlock);

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

PageMapping::~PageMapping() {}

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
  mytest::TestClass tc = mytest::TestClass();
  tc.setIntToMax(stat.testThirdParty);
  debugprint(LOG_FTL_PAGE_MAPPING, std::to_string(stat.testThirdParty).c_str());

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

  if (req.ioFlag.count() > 0) {
    #ifdef DEBUG_TEST
      /*
      Test 1 Begin
      GarbageCollection 0 Block.
      And Read.
      *//*
      Test 1 Begin
      GarbageCollection 0 Block.
      And Read.
      */
      Request test_req = Request(param.ioUnitInPage);
      test_req.lpn = 0;
      test_req.ioFlag.set();
      readInternal(test_req, tick);
      vector<uint32_t> bl(1, 0);
      doGarbageCollection(bl, tick);
      readInternal(test_req, tick);
      /*
      Test 1 End
      */
      /*
      Test 2 Begin
      Multi Read And Write And Compress On Logic Block 0.
      */
      // Request test_req = Request(param.ioUnitInPage);
      // test_req.lpn = 0;
      // test_req.ioFlag.set();
      // readInternal(test_req, tick);
      // vector<uint32_t> bl(1, 0);
      // doGarbageCollection(bl, tick);
      // // Repeat Write
      // Request test_write_req = Request(param.ioUnitInPage);
      // test_write_req.lpn = 0;
      // for(uint32_t idx = 0; idx < 8; idx++){
      //   test_write_req.ioFlag.set(idx);
      // }
      // writeInternal(test_write_req, tick);
      // readInternal(test_req, tick);
      // bl[0] = 9537;
      // doGarbageCollection(bl, tick);
      // readInternal(test_req, tick);
      /*
      Test 2 End
      */
    #endif
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
        auto &mapping = mappingList.at(idx);
        auto block = blocks.find(std::get<0>(mapping));

        if (block == blocks.end()) {
          panic("Block is not in use");
        }

        block->second.invalidate(std::get<1>(mapping), idx);

        // Collect block indices
        list.push_back(std::get<0>(mapping));
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
  std::vector<uint64_t> lpns;
  Bitset bit(param.ioUnitInPage);
  uint64_t beginAt;
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;

  if (blocksToReclaim.size() == 0) {
    return;
  }

  WriteInfo w_info;
  
  auto write_submit = [&](WriteInfo& w_info, std::unordered_map<uint32_t, Block>::iterator freeBlock){
    // Block
    assert(w_info.valid);
    freeBlock->second.write(w_info);
    req.blockIndex = freeBlock->first;
    req.pageIndex = w_info.pageIndex;

    if (bRandomTweak) {
      req.ioFlag.reset();
      req.ioFlag.set(w_info.idx);
    }
    else {
      req.ioFlag.set();
    }

    writeRequests.push_back(req);
    //Clear W_info
    w_info.valid = false;
    w_info.lpns.clear();
  };

  // For all blocks to reclaim, collecting request structure only
  for (auto &iter : blocksToReclaim) {
    auto block = blocks.find(iter);

    if (block == blocks.end()) {
      panic("Invalid block");
    }

    // Copy valid pages to free block
    // TODO: 由于LastFreeBlock的机制不允许跨块压缩, 是否可以改进?
    for (uint32_t pageIndex = 0; pageIndex < param.pagesInBlock; pageIndex++) {
      // Valid?
      if (block->second.getPageInfo(pageIndex, lpns, bit)) {
        if (!bRandomTweak) {
          bit.set();
        }

        // Retrive free block
        auto freeBlock = blocks.find(getLastFreeBlock(bit));

        // Issue Read
        req.blockIndex = block->first;
        req.pageIndex = pageIndex;
        req.ioFlag = bit;

        readRequests.push_back(req);

        // Update mapping table
        uint32_t newBlockIdx = freeBlock->first;

        bool is_filled = true;
        uint32_t nowPageCnt = 0;
        uint32_t nowPageIdx = 0;
        uint32_t nowOffset = 0;

        for (uint32_t idx = 0; idx < bitsetSize; idx++) {
          if (bit.test(idx)) {
            // Invalidate
            block->second.invalidate(pageIndex, idx);

            auto mappingList = table.find(lpns.at(idx));

            if (mappingList == table.end()) {
              panic("Invalid mapping table entry");
            }

            pDRAM->read(&(*mappingList), 12 * param.ioUnitInPage, tick);

            auto &mapping = mappingList->second.at(idx);
            CompressInfo* c_info = (CompressInfo*) &(std::get<2>(mapping));
            if(c_info -> is_compressed == 0x1){
              if(w_info.valid){
                write_submit(w_info, freeBlock);
              }
              nowPageIdx = freeBlock->second.getNextWritePageIndex(idx);
              is_filled = true;
              std::get<0>(mapping) = newBlockIdx;
              std::get<1>(mapping) = nowPageIdx;
              w_info.idx = idx;
              w_info.beginAt = beginAt;
              w_info.pageIndex = nowPageIdx;
              block->second.getLPNs(pageIndex, w_info.lpns, idx);
              w_info.valid = true;
              write_submit(w_info, freeBlock);
              //DONOTHING ON Compressed Info.
            }
            else{
              if(is_filled){
                if(w_info.valid) write_submit(w_info, freeBlock);
                nowPageIdx = freeBlock->second.getNextWritePageIndex(idx);
                nowPageCnt = 0;
                nowOffset = 0;
                is_filled = false;
                w_info.valid = true;
                w_info.pageIndex = nowPageIdx;
                w_info.idx = idx;
                w_info.beginAt = beginAt;
              }
              std::get<0>(mapping) = newBlockIdx;
              std::get<1>(mapping) = nowPageIdx;
              int CompressedLength = param.pageSize / 2; // TODO: Change it to true Size
              c_info->is_compressed = 0x1;
              c_info->c_ind = nowPageCnt++;
              c_info->offset = nowOffset;
              assert(CompressedLength <= (1 << 28));
              c_info->length = CompressedLength;
              c_info->idx = w_info.idx;
              nowOffset += CompressedLength;
              //TODO: CHANGE LOGIC OF ISFILLED
              is_filled = nowOffset >= param.pageSize;
              vector<uint64_t> t_lpn;
              block->second.getLPNs(pageIndex, t_lpn, idx);
              assert(t_lpn.size() == 1);
              w_info.lpns.push_back(t_lpn[0]);
            }

            stat.validPageCopies++;//Invalid
          }
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
      pDRAM->read(&(*mappingList), 12 * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), 12, tick);
    }

    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      if (req.ioFlag.test(idx) || !bRandomTweak) {
        auto &mapping = mappingList->second.at(idx);

        if (std::get<0>(mapping) < param.totalPhysicalBlocks &&
            std::get<1>(mapping) < param.pagesInBlock) {
          palRequest.blockIndex = std::get<0>(mapping);
          palRequest.pageIndex = std::get<1>(mapping);
          CompressInfo* c_info = (CompressInfo*)(&(std::get<2>(mapping)));
          if (bRandomTweak) {
            palRequest.ioFlag.reset();
            if(c_info -> is_compressed){
              palRequest.ioFlag.set(c_info->idx);
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
          if(c_info->is_compressed){
            
          }
          debugprint(LOG_FTL_PAGE_MAPPING,("Compressed info: IsCompressed = " + std::to_string(c_info->is_compressed) + ", C_IND = "+ std::to_string(c_info->c_ind) + ", OFFSET = "+ std::to_string(c_info->offset) + ", LENGTH = " + std::to_string(c_info->length)).c_str());

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
        auto &mapping = mappingList->second.at(idx);

        if (std::get<0>(mapping) < param.totalPhysicalBlocks &&
            std::get<1>(mapping) < param.pagesInBlock) {
          block = blocks.find(std::get<0>(mapping));

          // Invalidate current page
          block->second.invalidate(std::get<1>(mapping), idx);
        }
      }
    }
  }
  else {
    // Create empty mapping
    auto ret = table.emplace(
        req.lpn,
        std::vector<std::tuple<uint32_t, uint32_t, CompressInfo>>(
            bitsetSize, {param.totalPhysicalBlocks, param.pagesInBlock, CompressInfo()}));

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
      pDRAM->read(&(*mappingList), 12 * req.ioFlag.count(), tick);
      pDRAM->write(&(*mappingList), 12 * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), 12, tick);
      pDRAM->write(&(*mappingList), 12, tick);
    }
  }

  if (!bRandomTweak && !req.ioFlag.all()) {
    // We have to read old data
    readBeforeWrite = true;
  }

  for (uint32_t idx = 0; idx < bitsetSize; idx++) {
    if (req.ioFlag.test(idx) || !bRandomTweak) {
      uint32_t pageIndex = block->second.getNextWritePageIndex(idx);
      auto &mapping = mappingList->second.at(idx);

      beginAt = tick;
      
      //first write, write 0;
      vector<uint64_t> lpns(1, req.lpn);
      block->second.write(pageIndex, lpns, idx, beginAt);

      // Read old data if needed (Only executed when bRandomTweak = false)
      // Maybe some other init procedures want to perform 'partial-write'
      // So check sendToPAL variable
      if (readBeforeWrite && sendToPAL) {
        palRequest.blockIndex = std::get<0>(mapping);
        palRequest.pageIndex = std::get<1>(mapping);

        // We don't need to read old data
        palRequest.ioFlag = req.ioFlag;
        palRequest.ioFlag.flip();

        pPAL->read(palRequest, beginAt);
      }

      // update mapping to table
      std::get<0>(mapping) = block->first;
      std::get<1>(mapping) = pageIndex;

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

void PageMapping::trimInternal(Request &req, uint64_t &tick) {
  auto mappingList = table.find(req.lpn);

  if (mappingList != table.end()) {
    if (bRandomTweak) {
      pDRAM->read(&(*mappingList), 12 * req.ioFlag.count(), tick);
    }
    else {
      pDRAM->read(&(*mappingList), 12, tick);
    }

    // Do trim
    for (uint32_t idx = 0; idx < bitsetSize; idx++) {
      auto &mapping = mappingList->second.at(idx);
      auto block = blocks.find(std::get<0>(mapping));

      if (block == blocks.end()) {
        panic("Block is not in use");
      }

      block->second.invalidate(std::get<1>(mapping), idx);
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

  //Add by ZeitHaum
  temp.name = prefix + "page_mapping.testThirdParty";
  temp.desc = "Test Included Files of ThirdParty";
  list.push_back(temp);
}

void PageMapping::getStatValues(std::vector<double> &values) {
  values.push_back(stat.gcCount);
  values.push_back(stat.reclaimedBlocks);
  values.push_back(stat.validSuperPageCopies);
  values.push_back(stat.validPageCopies);
  values.push_back(calculateWearLeveling());
  values.push_back(stat.erasedTotalBlocks);
  values.push_back(stat.testThirdParty);
}

void PageMapping::resetStatValues() {
  memset(&stat, 0, sizeof(stat));
}

}  // namespace FTL

}  // namespace SimpleSSD
