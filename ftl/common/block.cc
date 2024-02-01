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

#include "ftl/common/block.hh"

#include <algorithm>
#include <cstring>
#include <cassert>

namespace SimpleSSD {

namespace FTL {

//initialize static variable
uint32_t Block::iounitSize = 0;
uint16_t Block::maxCompressedPageCount = 0;


Block::Block(uint32_t blockIdx, uint32_t count, uint32_t ioUnit)
    : idx(blockIdx),
      pageCount(count),
      ioUnitInPage(ioUnit),
      pValidBits(nullptr),
      pLPNs(nullptr),
      pErasedBits(nullptr),
      ppLPNs(nullptr),
      pppLPNs(nullptr),
      lastAccessed(0),
      eraseCount(0) {
  if(Block::iounitSize == 0 || Block::maxCompressedPageCount == 0){
    panic("Block static attrs doesn't initialize");
  }
  if (ioUnitInPage == 1) {
    pErasedBits = new Bitset(pageCount);

    Bitset copy(maxCompressedPageCount);
    validBits = std::vector<Bitset>(pageCount, copy);

    ppLPNs = (LpnInfo **)calloc(pageCount, sizeof(LpnInfo*));
    for(uint32_t i = 0; i<pageCount; i++){
      ppLPNs[i] = (LpnInfo* ) calloc(maxCompressedPageCount, sizeof(LpnInfo *));
    }
  }
  else if (ioUnitInPage > 1) {
    Bitset copy(ioUnitInPage);
    erasedBits = std::vector<Bitset>(pageCount, copy);

    Bitset validcopy(maxCompressedPageCount);
    cvalidBits = std::vector<std::vector<Bitset>>(pageCount, std::vector<Bitset>(ioUnitInPage, validcopy));

    pppLPNs = (LpnInfo ***)calloc(pageCount, sizeof(LpnInfo **));

    for (uint32_t i = 0; i < pageCount; i++) {
      pppLPNs[i] = (LpnInfo **)calloc(ioUnitInPage, sizeof(LpnInfo*));
      for(uint32_t j = 0; j < ioUnitInPage; j++){
        pppLPNs[i][j] = (LpnInfo *) calloc(maxCompressedPageCount, sizeof(LpnInfo));
      }
    }
  }
  else {
    panic("Invalid I/O unit in page");
  }

  // C-style allocation
  pNextWritePageIndex = (uint32_t *)calloc(ioUnitInPage, sizeof(uint32_t));

  erase();
  eraseCount = 0;
  blockstat.reset();
}

Block::Block(const Block &old)
    : Block(old.idx, old.pageCount, old.ioUnitInPage) {
  if (ioUnitInPage == 1) {
    validBits = old.validBits;
    *pErasedBits = *old.pErasedBits;

    // memcpy(pLPNs, old.pLPNs, pageCount * sizeof(uint64_t));
    for(uint32_t i = 0; i< pageCount; i++){
      mempcpy(ppLPNs[i], old.ppLPNs[i], maxCompressedPageCount * sizeof(uint64_t));
    }
  }
  else {
    cvalidBits = old.cvalidBits;
    erasedBits = old.erasedBits;

    for (uint32_t i = 0; i < pageCount; i++) {
      // memcpy(ppLPNs[i], old.ppLPNs[i], ioUnitInPage * sizeof(uint64_t));
      for(uint32_t j = 0; j < ioUnitInPage; j++){
        memcpy(pppLPNs[i][j], old.pppLPNs[i][j], maxCompressedPageCount * sizeof(uint64_t));
      }
    }
  }

  memcpy(pNextWritePageIndex, old.pNextWritePageIndex,
         ioUnitInPage * sizeof(uint32_t));

  eraseCount = old.eraseCount;
  blockstat.copy(old.blockstat);
}

Block::Block(Block &&old) noexcept
    : idx(std::move(old.idx)),
      pageCount(std::move(old.pageCount)),
      ioUnitInPage(std::move(old.ioUnitInPage)),
      pNextWritePageIndex(std::move(old.pNextWritePageIndex)),
      pValidBits(std::move(old.pValidBits)),
      pLPNs(std::move(old.pLPNs)),
      pErasedBits(std::move(old.pErasedBits)),
      validBits(std::move(old.validBits)),
      ppLPNs(std::move(old.ppLPNs)),
      erasedBits(std::move(old.erasedBits)),
      cvalidBits(std::move(old.cvalidBits)),
      pppLPNs(std::move(old.pppLPNs)),
      lastAccessed(std::move(old.lastAccessed)),
      eraseCount(std::move(old.eraseCount)),
      blockstat(std::move(old.blockstat)) {
  // TODO Use std::exchange to set old value to null (C++14)
  old.idx = 0;
  old.pageCount = 0;
  old.ioUnitInPage = 0;
  old.pNextWritePageIndex = nullptr;
  old.pValidBits = nullptr;
  old.pErasedBits = nullptr;
  old.pLPNs = nullptr;
  old.ppLPNs = nullptr;
  old.pppLPNs = nullptr;
  old.lastAccessed = 0;
  old.eraseCount = 0;
  old.blockstat.reset();
}

Block::~Block() {
  free(pNextWritePageIndex);
  if(pLPNs){
    free(pLPNs);
  }

  delete pValidBits;
  delete pErasedBits;

  if (ppLPNs) {
    for (uint32_t i = 0; i < pageCount; i++) {
      free(ppLPNs[i]);
    }

    free(ppLPNs);
  }

  if(pppLPNs){
    for (uint32_t i = 0; i<pageCount; i++){
      for(uint32_t j = 0; j<maxCompressedPageCount; j++){
        free(pppLPNs[i][j]);
      }
      free(pppLPNs[i]);
    }
    free(pppLPNs);
  }

  pNextWritePageIndex = nullptr;
  pLPNs = nullptr;
  pValidBits = nullptr;
  pErasedBits = nullptr;
  ppLPNs = nullptr;
  pppLPNs = nullptr;
}

Block &Block::operator=(const Block &rhs) {
  if (this != &rhs) {
    this->~Block();
    *this = Block(rhs);  // Call copy constructor
  }

  return *this;
}

Block &Block::operator=(Block &&rhs) {
  if (this != &rhs) {
    this->~Block();

    idx = std::move(rhs.idx);
    pageCount = std::move(rhs.pageCount);
    ioUnitInPage = std::move(rhs.ioUnitInPage);
    pNextWritePageIndex = std::move(rhs.pNextWritePageIndex);
    pValidBits = std::move(rhs.pValidBits);
    pErasedBits = std::move(rhs.pErasedBits);
    pLPNs = std::move(rhs.pLPNs);
    validBits = std::move(rhs.validBits);
    cvalidBits = std::move(rhs.cvalidBits);
    erasedBits = std::move(rhs.erasedBits);
    ppLPNs = std::move(rhs.ppLPNs);
    pppLPNs = std::move(rhs.pppLPNs);
    lastAccessed = std::move(rhs.lastAccessed);
    eraseCount = std::move(rhs.eraseCount);
    blockstat = std::move(rhs.blockstat);

    rhs.pNextWritePageIndex = nullptr;
    rhs.pValidBits = nullptr;
    rhs.pErasedBits = nullptr;
    rhs.pLPNs = nullptr;
    rhs.ppLPNs = nullptr;
    rhs.pppLPNs = nullptr;
    rhs.lastAccessed = 0;
    rhs.eraseCount = 0;
    rhs.blockstat.reset();
  }

  return *this;
}

uint32_t Block::getBlockIndex() const {
  return idx;
}

uint64_t Block::getLastAccessedTime() {
  return lastAccessed;
}

uint32_t Block::getEraseCount() {
  return eraseCount;
}

uint32_t Block::getValidPageCount() {
  uint32_t ret = 0;

  if (ioUnitInPage == 1) {
    for(auto &iter : validBits){
      if(iter.any()){
        ++ret;
      }
    }
  }
  else {
    for (auto &iter : cvalidBits) {
      bool isvalid = false;
      for(auto & sub_it : (iter)){
        isvalid |= sub_it.any();
      }
      if(isvalid){
        ++ret;
      }
    }
  }

  return ret;
}

uint32_t Block::getValidPageCountRaw() {
  uint32_t ret = 0;

  if (ioUnitInPage == 1) {
    // Same as getValidPageCount()
    for (auto &iter : validBits) {
      if(iter.any()) ++ret;
    }
  }
  else {
    for (auto &iter : cvalidBits) {
      for(auto &sub_it : iter){
        if(sub_it.any()) ++ret;
      }
    }
  }

  return ret;
}

uint32_t Block::getDirtyPageCount() {
  uint32_t ret = 0;

  if (ioUnitInPage == 1) {
    for(uint32_t i = 0; i < pageCount; ++i){
      // Dirty: Valid(false), Erased(false)
      if(validBits[i].any() == false && pErasedBits->test(i) == false) ++ret;
    }
  }
  else {
    for (uint32_t i = 0; i < pageCount; i++) {
      // Dirty: Valid(false), Erased(false)
      for(uint32_t j = 0; j < ioUnitInPage; ++j){
        if( cvalidBits[i][j].any() == false && erasedBits[i].test(j) == false){
          ++ret;
          break;
        }
      }
    }
  }

  return ret;
}

uint32_t Block::getNextWritePageIndex() {
  uint32_t idx = 0;

  for (uint32_t i = 0; i < ioUnitInPage; i++) {
    if (idx < pNextWritePageIndex[i]) {
      idx = pNextWritePageIndex[i];
    }
  }

  return idx;
}

uint32_t Block::getNextWritePageIndex(uint32_t idx) {
  return pNextWritePageIndex[idx];
}

bool Block::getPageInfo(uint32_t pageIndex, std::vector<std::vector<LpnInfo>> &lpns,
                        std::vector<Bitset> &bits) {
  //Check Size
  assert(lpns.size() == ioUnitInPage);
  assert(bits.size() == ioUnitInPage);
  for(uint32_t i = 0; i<lpns.size(); ++i){
    assert(lpns[i].size() == maxCompressedPageCount);
    assert(bits[i].size() == maxCompressedPageCount);
  }
  //Get Info
  bool ret = false;
  if (ioUnitInPage == 1) {
    bits[0].copy(validBits[pageIndex]);
    for(uint32_t i = 0; i<maxCompressedPageCount; ++i){
      lpns[0][i] = ppLPNs[pageIndex][i];
    }
    ret = bits[0].any();
  }
  else if(ioUnitInPage > 1){
    for(uint32_t i = 0; i<ioUnitInPage; i++){
      bits[i].copy(cvalidBits[pageIndex][i]);
      for(uint32_t j = 0; j<maxCompressedPageCount; ++j){
        lpns[i][j] = pppLPNs[pageIndex][i][j];
      }
      ret |= bits[i].any();
    }
  }
  else{
    panic("ioUnit is 0.");
  }

  return ret;
}


void Block::getLPNs(uint32_t pageIndex, std::vector<LpnInfo>&lpn, Bitset& bits, uint32_t idx){
  assert(lpn.size() == maxCompressedPageCount);
  if(ioUnitInPage == 1){
    bits.copy(validBits.at(pageIndex));
    for(uint32_t i = 0; i<maxCompressedPageCount; ++i){
      lpn[i] = ppLPNs[pageIndex][i];
    }
  }
  else{
    assert(idx < ioUnitInPage);
    bits.copy(cvalidBits[pageIndex][idx]);
    for(uint32_t i = 0; i<maxCompressedPageCount; ++i){
      lpn[i] = pppLPNs[pageIndex][idx][i];
    }
  }
}

LpnInfo Block::getLPN(uint32_t pageIndex, uint16_t idx, uint16_t comp_ind){
  assert(comp_ind < maxCompressedPageCount);
  assert(idx < ioUnitInPage);
  if(ioUnitInPage == 1){
    return ppLPNs[pageIndex][comp_ind];
  }
  else{
    return pppLPNs[pageIndex][idx][comp_ind];
  }
}

bool Block::read(uint32_t pageIndex, uint32_t idx, uint64_t tick) {
  bool read = false;

  if (ioUnitInPage == 1 && idx == 0) {
    read = validBits[pageIndex].any();
  }
  else if (idx < ioUnitInPage) {
    read = cvalidBits[pageIndex][idx].any();
  }
  else {
    panic("I/O map size mismatch");
  }

  if (read) {
    lastAccessed = tick;
  }

  return read;
}

bool Block::write(uint32_t pageIndex, std::vector<LpnInfo>&lpns, std::vector<uint32_t>&lens, Bitset& validmask,uint32_t idx, 
                  uint64_t tick) {
  bool write = false;

  if(validmask.size() != maxCompressedPageCount || lpns.size() != maxCompressedPageCount){
    panic("I/O LPN size mismatch maxCompressedPageCout");
  }
  
  if (ioUnitInPage == 1 && idx == 0) {
    write = pErasedBits->test(pageIndex);
  }
  else if (idx < ioUnitInPage) {
    write = erasedBits.at(pageIndex).test(idx);
  }
  else {
    panic("I/O map size mismatch");
  }

  if (write) {
    if (pageIndex < pNextWritePageIndex[idx]) {
      panic("Write to block should sequential");
    }

    lastAccessed = tick;

    if (ioUnitInPage == 1) {
      if(idx != 0) {
        panic("Idx greater than IoUnitInPage");
      }
      pErasedBits->reset(pageIndex);
      validBits[pageIndex].copy(validmask);

      for(uint32_t i = 0; i<lpns.size(); ++i){
        ppLPNs[pageIndex][i] = lpns[i];
      }
    }
    else {
      erasedBits.at(pageIndex).reset(idx);
      cvalidBits[pageIndex][idx].copy(validmask);
      for(uint32_t i = 0; i<lpns.size(); ++i){
        pppLPNs[pageIndex][idx][i] = lpns[i];
      }
    }

    pNextWritePageIndex[idx] = pageIndex + 1;
    //update blockstat
    updateStatWrite(lens, validmask);
  }
  else {
    panic("Write to non erased page");
  }

  return write;
}

void Block::updateStatWrite(std::vector<uint32_t>&lens, Bitset& validmask){
  for(uint16_t i = 0; i<maxCompressedPageCount; ++i){
    if(validmask.test(i)){
      //update blockstat
      blockstat.totalDataLength+= Block::iounitSize;
      blockstat.totalUnitCount++;
      blockstat.validDataLength+= lens[i];
      if(lens[i] < Block::iounitSize){
        blockstat.compressUnitCount++;
      }
    }
  }
  if(validmask.any()){
    blockstat.validIoUnitCount++;
  }
  checkstat();
}

void Block::erase() {
  if (ioUnitInPage == 1) {
    pErasedBits->set();
    for(auto &iter: validBits){
      iter.reset();
    }
  }
  else {
    for (auto &iter : cvalidBits) {
      for(auto& sub_it : iter){
        sub_it.reset();
      }
    }
    for (auto &iter : erasedBits) {
      iter.set();
    }
  }

  memset(pNextWritePageIndex, 0, sizeof(uint32_t) * ioUnitInPage);

  eraseCount++;
  blockstat.reset();
}

// void Block::invalidate(uint32_t pageIndex, uint16_t idx) {
//   if (ioUnitInPage == 1) {
//     validBits.at(pageIndex).reset();
//   }
//   else {
//     cvalidBits.at(pageIndex).at(idx).reset();
//   }
// }

void Block::invalidate(uint32_t pageIndex, uint16_t idx, uint16_t c_ind, uint32_t len) {
  if (ioUnitInPage == 1) {
    if(validBits.at(pageIndex).test(c_ind)){
      //update blockstat
      updateStatInvalidate(len);
    }
    validBits.at(pageIndex).reset(c_ind);
  }
  else {
    if(cvalidBits.at(pageIndex).at(idx).test(c_ind)){
      //update blockstat
      updateStatInvalidate(len);
    }
    cvalidBits.at(pageIndex).at(idx).reset(c_ind);
  }
  if(!isvalid(pageIndex, idx)){
    blockstat.validIoUnitCount--;
  }
  checkstat();
}

void Block::updateStatInvalidate(uint32_t len){
  blockstat.totalDataLength -= Block::iounitSize;
  blockstat.totalUnitCount--;
  blockstat.validDataLength -= len;
  if(len<Block::iounitSize){
    blockstat.compressUnitCount--;
  }
  // blockstat.validiounit Need isvalid function, so update in invalidate function;
}

bool Block::isvalid(uint32_t pageIndex, uint16_t idx){
  if(ioUnitInPage==1){
    return validBits.at(pageIndex).any();
  }
  else{
    return cvalidBits.at(pageIndex).at(idx).any();
  }
}

bool Block::isvalid(uint32_t pageIndex, uint16_t idx, uint16_t c_ind){
  if(ioUnitInPage == 1){
    return validBits.at(pageIndex).test(c_ind);
  }
  else{
    return cvalidBits.at(pageIndex).at(idx).test(c_ind); 
  }
}

const BlockStat& Block::getBlockStat(){
  return blockstat;
}

void Block::setStaticAttr(uint32_t iosize, uint16_t maxclen){
  Block::iounitSize = iosize;
  Block::maxCompressedPageCount = maxclen;
}

void Block::checkstat(){
  //check block stats
  if(blockstat.totalDataLength % Block::iounitSize !=0){
    panic("Check failed in blockstat.totalDataLength.");
  }
  else if(blockstat.validDataLength > blockstat.totalDataLength || blockstat.validDataLength > ioUnitInPage * pageCount * Block::iounitSize){
    panic("Check failed in blockstat.validDataLength.");
  }
  else if(blockstat.totalUnitCount != blockstat.totalDataLength / Block::iounitSize){
    panic("Check failed in blockstat.totalUnitCount.");
  }
  else if(blockstat.compressUnitCount > blockstat.totalUnitCount){
    panic("Check failed in blockstat.compressUnitCount.");
  }
  else if(blockstat.validIoUnitCount > blockstat.totalUnitCount || blockstat.validIoUnitCount > ioUnitInPage * pageCount){
    panic("Check failed in blockstat.validIoUnitCount.");
  }
  else if((blockstat.totalDataLength - blockstat.validDataLength) * (blockstat.compressUnitCount) < (blockstat.totalUnitCount - blockstat.validIoUnitCount) * (blockstat.compressUnitCount * Block::iounitSize)){
    panic("Check failed in r_f, r_f is negtive.");
  }
}

}  // namespace FTL

}  // namespace SimpleSSD
