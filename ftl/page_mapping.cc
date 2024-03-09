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

PageMapping::PhysicalAddress::PhysicalAddress():
  blockIndex(0), pageIndex(0), compressunitIndex(0)
{}

PageMapping::PhysicalAddress::PhysicalAddress(uint32_t bid, uint32_t pid, uint16_t cid):
  blockIndex(bid), pageIndex(pid), compressunitIndex(cid)
{}

std::string PageMapping::PhysicalAddress::toString(){
  std::string ret = "{";
  ret += "blockIndex : " + std::to_string(blockIndex) + ", ";
  ret += "pageIndex : " + std::to_string(pageIndex) + ", ";
  ret += "compressunitIndex :" + std::to_string(compressunitIndex) + "}"; 
  return ret;
}

void PageMapping::PhysicalAddress::copy(const PhysicalAddress& p){
  blockIndex = p.blockIndex;
  pageIndex = p.pageIndex;
  compressunitIndex = p.compressunitIndex;
}

PageMapping::CompressInfo::CompressInfo(){
  changed_data = nullptr;
  disk_idx = 0;
}

PageMapping::CompressInfo::CompressInfo(uint64_t _lpn, uint32_t _new_len, uint32_t _old_len, PhysicalAddress _addr)
:lpn(_lpn), new_len(_new_len), old_len(_old_len), invalidate_addr(_addr)
{
  changed_data = nullptr;
  disk_idx = 0;
}

PageMapping::CompressRequest::CompressRequest(Parameter* p){
  param = p;
  counts.resize(param->ioUnitInPage);
  lens.resize(param->ioUnitInPage);
  compress_infos.resize(param->ioUnitInPage);
  for(uint16_t i = 0; i<param->ioUnitInPage; ++i){
    compress_infos[i].reserve(param->maxCompressUnitInPage);
  }
}

void PageMapping::CompressRequest::clear(){
  for(uint16_t i = 0; i<param->ioUnitInPage; ++i){
    compress_infos[i].clear();
    counts[i] = 0;
    lens[i] = 0;
  }
}

void PageMapping::CompressRequest::addCompressInfo(CompressInfo compress_info, uint32_t idx){
  counts[idx]++;
  lens[idx] += compress_info.new_len;
  if(counts[idx] > param->maxCompressUnitInPage || lens[idx] > param->ioUnitSize){
    panic("CheckFailed in add unit to compress_req.");
  }
  compress_infos[idx].push_back(compress_info);
}
void PageMapping::CompressRequest::merge(CompressRequest& compress_req){
  for(uint32_t idx = 0; idx < param->ioUnitInPage; ++idx){
    for(uint16_t j = 0; j<compress_req.counts[idx]; ++j){
      this->addCompressInfo(compress_req.compress_infos[idx][j], idx);
    }
  }
  compress_req.clear();
}
bool PageMapping::CompressRequest::empty(){
  for(uint32_t i = 0; i<counts.size(); ++i){
    if(counts[i]!=0){
      return false;
    }
  }
  return true;
}

PageMapping::PageMapping(ConfigReader &c, Parameter &p, PAL::PAL *l,
                         DRAM::AbstractDRAM *d)
    : AbstractFTL(p, l, d),
      pPAL(l),
      conf(c),
      lastFreeBlock(param.pageCountToMaxPerf),
      lastFreeBlockIOMap(param.ioUnitInPage),
      nowCompressReq(&param),
      bReclaimMore(false)
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
  this->cd_info = req.cd_info;

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
  this->cd_info = req.cd_info;

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
  this->cd_info = req.cd_info;

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
          block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, mapping.length);
        }
        else{
          block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, param.ioUnitSize);
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

bool PageMapping::isAvailable(const CompressRequest& compress_req){
  //check length and valid
  for(uint32_t idx = 0; idx < param.ioUnitInPage; ++idx){
    if(compress_req.lens[idx] + nowCompressReq.lens[idx] > param.ioUnitSize || compress_req.counts[idx] + nowCompressReq.counts[idx] > param.maxCompressUnitInPage){
      return false;
    }
  }
  return true;
}

uint32_t PageMapping::getLastFreeBlock(Bitset &iomap) {
  // update lastFreeBlockIndex in corresponding pos.
  if (!bRandomTweak || (lastFreeBlockIOMap & iomap).any()) {
    // Update lastFreeBlockIndex
    lastFreeBlockIndex++;

    if (lastFreeBlockIndex == param.pageCountToMaxPerf) {
      lastFreeBlockIndex = 0;
    }
    // lastFreeBlockIOMap.reset();
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

void PageMapping::getCompressedLengthFromDisk(uint64_t lpn, uint32_t idx, const MapEntry& mapping, CompressInfo& compress_info, uint64_t& tick){
  //Get Compressed Length
  uint64_t CompressedLength = 0;
  if(!conf.readBoolean(CONFIG_NVME, HIL::NVMe::NVME_ENABLE_COMPRESS)){
    //Disable Compress
    CompressedLength = param.ioUnitSize;
  }
  else if(mapping.is_actual == 0x0){
    CompressedLength = param.ioUnitSize;
  }
  else if(mapping.is_compressed == 0x0){
    //Not Compressed, Need Compress.
    uint64_t disk_offset = (lpn * param.ioUnitInPage + idx) * param.ioUnitSize - cd_info.offset;
    uint64_t disk_length = param.ioUnitSize;
    CompressedDisk* pcDisk = ((CompressedDisk*)(cd_info.pDisk));
    if(cd_info.pDisk){
      CompressedLength = pcDisk->getCompressedLength(disk_offset/param.ioUnitSize);
      if(CompressedLength == param.ioUnitSize){
        //Need Compress
        pcDisk -> readOrdinary(disk_offset, disk_length, compressedBuffer);
        compress_info.changed_data = new uint8_t[param.ioUnitSize];
        compress_info.disk_idx = disk_offset / param.ioUnitSize;
        bool is_comp = pcDisk -> compressBufferWrite(compress_info.disk_idx,CompressedLength, compressedBuffer, compress_info.changed_data);
        //apply Compress Latency
        if(pcDisk->getCompressType()==CompressType::LZ4){
          applyLatency(CPU::FTL__PAGE_MAPPING, CPU::COMPRESS_UNIT_LZ4);
        }
        if(is_comp){
          // debugprint(LOG_FTL_PAGE_MAPPING, "Compressed Trigged In GC! pageIndex =%" PRIu64 ", idx = %" PRIu64, pageIndex, idx);
        }
        else{
          CompressedLength = param.ioUnitSize;
          delete[] compress_info.changed_data;
          compress_info.changed_data = nullptr;
          compress_info.disk_idx = 0;
          ++stat.failedCompressCout;
          debugprint(LOG_FTL_PAGE_MAPPING, "Compressed Trigged Failed! lpn =%" PRIu64 ", idx = %" PRIu64 , lpn, idx);
        }
      }
    }
    else{
      //The disk is invalid.
      warn("Compress is enabled but no valid disk, GC is considered as not enabled.");
      CompressedLength = param.ioUnitSize;
    }
    assert(CompressedLength <= param.ioUnitSize && "panic: compresslength too large");
  }
  else {
    CompressedLength = mapping.length;
  }
  assert(CompressedLength > 0);
  compress_info.new_len = CompressedLength;
  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::GET_COMPRESSED_LENGTH_FROM_DISK);
}

void PageMapping::doGarbageCollection(std::vector<uint32_t> &blocksToReclaim,
                                      uint64_t &tick) {
  PAL::Request req(param.ioUnitInPage);
  std::vector<PAL::Request> readRequests;
  std::vector<PAL::Request> writeRequests;
  std::vector<PAL::Request> eraseRequests;
  std::vector<std::vector<uint64_t>> lpns(param.ioUnitInPage, vector<uint64_t>(param.maxCompressUnitInPage));
  std::vector<Bitset> bits(param.ioUnitInPage, Bitset(param.maxCompressUnitInPage));
  Bitset iomap(param.ioUnitInPage);
  uint64_t beginAt;                    
  uint64_t readFinishedAt = tick;
  uint64_t writeFinishedAt = tick;
  uint64_t eraseFinishedAt = tick;

  if (blocksToReclaim.size() == 0) {
    return;
  }

  CompressRequest compress_req = CompressRequest(&param);

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
        //Get The Compressed Length
        for(uint32_t idx = 0; idx< param.ioUnitInPage; ++idx){
          for(uint16_t cIdx = 0; cIdx<param.maxCompressUnitInPage; ++cIdx){
            if(bits[idx].test(cIdx)){
              //Need Copy
              uint64_t lpn = lpns[idx][cIdx];
              auto mappingList = table.find(lpn);
              if (mappingList == table.end()) {
                panic("Invalid mapping table entry");
              }
              pDRAM->read(&(*mappingList), (sizeof(MapEntry)) * param.ioUnitInPage, tick);
              MapEntry& mapping = mappingList->second.at(idx);
              CompressInfo compress_info = {lpn, 0, (uint32_t)mapping.length, mapping.paddr};
              getCompressedLengthFromDisk(lpn, idx, mapping, compress_info, tick);
              //Store information into CopyRequest.
              compress_req.addCompressInfo(compress_info, idx);
            }
          }
        }

        if(!isAvailable(compress_req)){
          compressSubmit(req, writeRequests, iomap, tick);
        }
        nowCompressReq.merge(compress_req);

        // Issue Read
        req.blockIndex = block->first;
        req.pageIndex = pageIndex;
        req.ioFlag = iomap;

        readRequests.push_back(req);

        stat.validSuperPageCopies++;//Invalid
      }
    }

    if(!nowCompressReq.empty()){
      compressSubmit(req, writeRequests, iomap, tick);
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
            palRequest.ioFlag.set(idx);
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
            CompressedDisk* pcDisk = ((CompressedDisk*)(cd_info.pDisk));
            if(pcDisk->getCompressType()==CompressType::LZ4){
              applyLatency(CPU::FTL__PAGE_MAPPING, CPU::DECOMPRESS_UNIT_LZ4);
            }
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
            block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, mapping.length);
          }
          else{
            block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, param.ioUnitSize);
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
            bitsetSize, {(uint32_t)param.totalPhysicalBlocks, (uint32_t)param.pagesInBlock, 0, false, 0, param.ioUnitSize}));
    if (!ret.second) {
      panic("Failed to insert new mapping");
    }

    mappingList = ret.first;
    stat.totalWriteIoUnitCount+= req.ioFlag.count();
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
  std::vector<uint64_t> lpns(1, 0);
  std::vector<uint32_t> lens(1, 0);
  validmask.set(0);
  for (uint32_t idx = 0; idx < bitsetSize; idx++) {
    if (req.ioFlag.test(idx) || !bRandomTweak) {
      uint32_t pageIndex = block->second.getNextWritePageIndex(idx);
      MapEntry &mapping = mappingList->second.at(idx);

      beginAt = tick;
      
      //first write, write 0.
      lpns[0] = req.lpn;
      lens[0] = param.ioUnitSize;
      block->second.write(pageIndex, lpns, lens, idx, beginAt);

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
      mapping.paddr.compressunitIndex = 0;
      mapping.is_compressed = 0;
      mapping.length = param.ioUnitSize;
      mapping.offset = 0;

      if (sendToPAL) {
        mapping.is_actual = 1;
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
      else{
        mapping.is_actual = 0;
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

void PageMapping::compressSubmit(PAL::Request& req, std::vector<PAL::Request>& writeRequests, Bitset& iomap, uint64_t& tick){
  //Generate iomap
  iomap.reset();
  for(uint32_t idx = 0; idx < param.ioUnitInPage; ++idx){
    if(nowCompressReq.counts[idx] > 0){
      iomap.set(idx);
    }
  }
  // Retrive free block
  std::vector<uint64_t> write_lpns(0);
  std::vector<uint32_t> write_lens(0);
  write_lpns.reserve(param.maxCompressUnitInPage);
  write_lens.reserve(param.maxCompressUnitInPage);
  auto& freeBlock = blocks.find(getLastFreeBlock(iomap))->second;
  for(uint32_t idx = 0; idx<param.ioUnitInPage; ++idx){
    if(iomap.test(idx)){
      uint32_t newPgaeIdx = freeBlock.getNextWritePageIndex(idx);
      // Update Pal::WriteRequests.
      write_lens.clear();
      write_lpns.clear();
      if (bRandomTweak) {
        req.ioFlag.reset();
        req.ioFlag.set(idx);
      }
      else {
        req.ioFlag.set();
      }
      req.blockIndex = freeBlock.getBlockIndex();
      req.pageIndex = newPgaeIdx;

      writeRequests.push_back(req);
      // Update mapping table
      uint32_t now_off = 0;
      for(uint32_t j = 0; j<nowCompressReq.counts[idx]; ++j){
        CompressInfo& compress_info = nowCompressReq.compress_infos[idx][j];
        MapEntry& mapping = table.find(compress_info.lpn)->second.at(idx);
        mapping.paddr.blockIndex = freeBlock.getBlockIndex();
        mapping.paddr.pageIndex = newPgaeIdx;
        mapping.paddr.compressunitIndex = j;
        mapping.is_compressed = (compress_info.new_len< param.ioUnitSize);
        mapping.length = compress_info.new_len;
        mapping.offset = now_off;
        now_off += compress_info.new_len;
        //Invalidate the old_pages
        auto& invalidate_block = blocks.find(compress_info.invalidate_addr.blockIndex)->second;
        invalidate_block.invalidate(compress_info.invalidate_addr.pageIndex, idx, compress_info.invalidate_addr.compressunitIndex, compress_info.old_len);
        write_lpns.push_back(compress_info.lpn);
        write_lens.push_back(compress_info.new_len);
        stat.validPageCopies++;//invalid
        //Submit disk
        if(compress_info.changed_data!= nullptr){
          assert(compress_info.old_len != compress_info.new_len);
          cd_info.pDisk->writeOrdinary(compress_info.disk_idx * param.ioUnitSize, param.ioUnitSize, compress_info.changed_data);
          ((CompressedDisk*)(cd_info.pDisk))->setCompressedLength(compress_info.disk_idx, compress_info.new_len);
          //clear
          delete[] compress_info.changed_data;
          compress_info.changed_data = nullptr;
          compress_info.disk_idx = 0;
        }
      }
      freeBlock.write(newPgaeIdx, write_lpns, write_lens, idx, tick);
    }
  }
  nowCompressReq.clear();
  tick += applyLatency(CPU::FTL__PAGE_MAPPING, CPU::COMPRESS_SUBMIT);
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
        block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, mapping.length);
      }
      else{
        block->second.invalidate(mapping.paddr.pageIndex, idx, mapping.paddr.compressunitIndex, param.ioUnitSize);
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

  temp.name = prefix + "page_mapping.compDisk.compressCount";
  temp.desc = "Compress trigged count(include compressWrite and compressBufferWrite).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compDisk.compressCyclesClk";
  temp.desc = "Total compress clock cycles(Use clock()).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compDisk.compressCyclesTsc";
  temp.desc = "Total compress clock cycles(Use __builtin_ia32_rdtsc()).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compDisk.decompressCount";
  temp.desc = "Decompress triggged count(only in readInternal).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compDisk.decompressCyclesClk";
  temp.desc = "Total decompress clock cycles(Use clock()).";
  list.push_back(temp);

  temp.name = prefix + "page_mapping.compDisk.decompressCyclesTsc";
  temp.desc = "Total decompress clock cycles(Use __builtin_ia32_rdtsc()).";
  list.push_back(temp);
}

void PageMapping::getStatValues(std::vector<double> &values) {
  values.push_back(stat.gcCount);
  values.push_back(stat.reclaimedBlocks);
  values.push_back(stat.validSuperPageCopies);
  values.push_back(stat.validPageCopies);
  values.push_back(calculateWearLeveling());
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
  CompressedDisk::CompressDiskStats* diskStats = getCompDiskStat();
  if(diskStats){
    values.push_back(diskStats->compressCount);
    values.push_back(diskStats->compressCyclesClk);
    values.push_back(diskStats->compressCyclesTsc);
    values.push_back(diskStats->decompressCout);
    values.push_back(diskStats->decompressCyclesClk);
    values.push_back(diskStats->decompressCyclesTsc);
  }
  else{
    values.push_back(0);
    values.push_back(0);
    values.push_back(0);
    values.push_back(0);
    values.push_back(0);
    values.push_back(0);
  }
}

CompressedDisk::CompressDiskStats* PageMapping::getCompDiskStat(){
  CompressedDisk::CompressDiskStats* ret = nullptr;
  CompressedDisk* comp_disk = nullptr; 
  if(cd_info.pDisk && cd_info.pDisk->getCompressType() != CompressType::NONE){
    comp_disk = (CompressedDisk*)(cd_info.pDisk);
  }
  if(comp_disk){
    ret = &comp_disk->stats;
  }
  else{
    ret = nullptr;
  }
  return ret;
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
