//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, DISABLED_SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, ReadTset) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // test ~ReadPageGuard()
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard(ReadPageGuard &&that)
  {
    auto reader_guard = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = ReadPageGuard(std::move(reader_guard));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // test ReadPageGuard::operator=(ReadPageGuard &&that)
  {
    auto reader_guard_1 = bpm->FetchPageRead(page_id_temp);
    auto reader_guard_2 = bpm->FetchPageRead(page_id_temp);
    EXPECT_EQ(3, page0->GetPinCount());
    reader_guard_1 = std::move(reader_guard_2);
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, HHTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = 0;
  page_id_t page_id_temp_a;
  auto *page0 = bpm->NewPage(&page_id_temp);
  auto *page1 = bpm->NewPage(&page_id_temp_a);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page_a = BasicPageGuard(bpm.get(), page1);

  // after drop, whether destructor decrements the pin_count_ ?
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard1.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());
  // test the move assignment
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard2 = std::move(read_guard1);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test the move constructor
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2(std::move(read_guard1));
    auto read_guard3(std::move(read_guard2));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page_id_temp, page0->GetPageId());

  // repeat drop
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  Page *init_page[6];
  page_id_t page_id_temp;
  for (auto &i : init_page) {
    i = bpm->NewPage(&page_id_temp);
  }

  BasicPageGuard basic_guard0(bpm.get(), init_page[0]);
  BasicPageGuard basic_guard1(bpm.get(), init_page[1]);
  basic_guard0 = std::move(basic_guard1);
  BasicPageGuard basic_guard2(std::move(basic_guard0));
  // page0失去guard，只有page1被gurad，bg2可用
  EXPECT_EQ(0, init_page[0]->GetPinCount());
  EXPECT_EQ(1, init_page[1]->GetPinCount());
  EXPECT_EQ(1, init_page[2]->GetPinCount());

  // BasicPageGuard basic_guard3(bpm.get(), init_page[2]);
  // BasicPageGuard basic_guard4(bpm.get(), init_page[3]);
  ReadPageGuard read_guard0(bpm.get(), init_page[2]);
  ReadPageGuard read_guard1(bpm.get(), init_page[3]);
  read_guard0 = std::move(read_guard1);
  ReadPageGuard read_guard2(std::move(read_guard0));

  // init_page[4]->WLatch();
  // init_page[5]->WLatch();
  WritePageGuard write_guard0(bpm.get(), init_page[4]);
  WritePageGuard write_guard1(bpm.get(), init_page[5]);

  // Important: here, latch the page id 4 by Page*, outside the watch of WPageGuard, but the WPageGuard still needs to
  // unlatch page 4 in operator= where the page id 4 is on the left side of the operator =.
  // Error log:
  // terminate called after throwing an instance of 'std::system_error'
  //   what():  Resource deadlock avoided
  // 1/1 Test #64: PageGuardTest.MoveTest
  init_page[4]->WLatch();
  write_guard0 = std::move(write_guard1);
  init_page[4]->WLatch();  // A deadlock will appear here if the latch is still on after operator= is called.

  WritePageGuard write_guard2(std::move(write_guard0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MyTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);
  page_id_t page_id_0;
  page_id_t page_id_1;
  page_id_t page_id_2;

  auto page_0 = bpm->NewPage(&page_id_0);
  auto page_1 = bpm->NewPage(&page_id_1);
  BasicPageGuard bpg_0_2;
  {
    BasicPageGuard bpg_0{bpm.get(), page_0};
    EXPECT_EQ(1, page_0->GetPinCount());
    auto rpg_0 = bpm->FetchPageRead(page_id_0);
    EXPECT_EQ(2, page_0->GetPinCount());
    BasicPageGuard bpg_0_1{std::move(bpg_0)};
    EXPECT_EQ(2, page_0->GetPinCount());
    bpg_0_2 = std::move(bpg_0_1);
    EXPECT_EQ(2, page_0->GetPinCount());
  }
  EXPECT_EQ(1, page_0->GetPinCount());
  auto bpg_1 = BasicPageGuard(bpm.get(), page_1);
  auto bpg_2 = bpm->NewPageGuarded(&page_id_2);
  // 失去对page1的gurad，bpg_1现在保护的page_0
  bpg_1 = std::move(bpg_0_2);
  EXPECT_EQ(1, page_0->GetPinCount());
  EXPECT_EQ(0, page_1->GetPinCount());

  auto rpg_0 = bpm->FetchPageRead(page_id_0);
  auto rpg_1 = bpm->FetchPageRead(page_id_1);
  EXPECT_EQ(2, page_0->GetPinCount());
  EXPECT_EQ(1, page_1->GetPinCount());

  // 失去对page1对保护（关闭其读锁），rpg_1现在保护的page_0
  rpg_1 = std::move(rpg_0);
  EXPECT_EQ(2, page_0->GetPinCount());
  EXPECT_EQ(0, page_1->GetPinCount());

  // 释放对page_0的读锁
  rpg_1.Drop();

  auto wpg_0 = bpm->FetchPageWrite(page_id_0);
  auto wpg_1 = bpm->FetchPageWrite(page_id_1);
  EXPECT_EQ(2, page_0->GetPinCount());
  EXPECT_EQ(1, page_1->GetPinCount());

  // 失去对page1对保护，wpg_1现在保护的page_0
  wpg_1 = std::move(wpg_0);
  EXPECT_EQ(2, page_0->GetPinCount());
  EXPECT_EQ(0, page_1->GetPinCount());
}

}  // namespace bustub
