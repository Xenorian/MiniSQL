#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";
TEST(BPlusTreeTests, asc_Insert) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i <n ; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  //ShuffleArray(keys);
  //ShuffleArray(values);
  //ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    tree.PrintTree(mgr[i]);
    ASSERT_TRUE(tree.Check());
  }

  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
}

TEST(BPlusTreeTests, dsc_Insert) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = n - 1; i >= 0; i--) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  // ShuffleArray(keys);
  // ShuffleArray(values);
  // ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    tree.PrintTree(mgr[i]);
    ASSERT_TRUE(tree.Check());
  }

  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
}
TEST(BPlusTreeTests, random_Insert) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 50;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int times = 0; times <= 50; times++) {
    keys.clear();
    values.clear();
    delete_seq.clear();
    kv_map.clear();

    for (int i = n - 1; i >= 0; i--) {
      keys.push_back(i);
      values.push_back(i);
      delete_seq.push_back(i);
    }
    // Shuffle data
    ShuffleArray(keys);
    ShuffleArray(values);
    ShuffleArray(delete_seq);
    // Map key value
    for (int i = 0; i < n; i++) {
      kv_map[keys[i]] = values[i];
    }
    // Insert data
    for (int i = 0; i < n; i++) {
      tree.Insert(keys[i], values[i]);
      if (times==36)
        tree.PrintTree(mgr[i]);
      ASSERT_TRUE(tree.Check());
    }

    // Print tree
    //tree.PrintTree(mgr[0]);
    // Search keys
    vector<int> ans;
    for (int i = 0; i < n; i++) {
      tree.GetValue(i, ans);
      ASSERT_EQ(kv_map[i], ans[i]);
    }
    ASSERT_TRUE(tree.Check());

    tree.Destroy();
  }
}

TEST(BPlusTreeTests, asc_DeleteTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 50;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }

  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);

  // Delete half keys
  for (int i = 0; i < n; i++) {
    tree.Remove(delete_seq[i]);
    ASSERT_TRUE(tree.Check());
    tree.PrintTree(mgr[1]);
  }
  tree.PrintTree(mgr[1]);
  
}

TEST(BPlusTreeTests, dsc_DeleteTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 50;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }

  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);

  // Delete half keys
  for (int i = n-1; i >= 0; i--) {
    tree.Remove(delete_seq[i]);
    ASSERT_TRUE(tree.Check());
    //tree.PrintTree(mgr[1]);
  }
  tree.PrintTree(mgr[1]);
}

TEST(BPlusTreeTests, random_DeleteTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 50;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }

  for (int times = 0; times < 50; times++) {
    // Insert data
    for (int i = 0; i < n; i++) {
      tree.Insert(keys[i], values[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Print tree
    tree.PrintTree(mgr[0]);

    ShuffleArray(delete_seq);

    // Delete keys
    for (int i = 0; i < n; i++) {
      tree.Remove(delete_seq[i]);
      ASSERT_TRUE(tree.Check());
      tree.PrintTree(mgr[1]);
    }
    tree.PrintTree(mgr[1]);
  }
}



TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 500;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  // Insert data
  for (int i = 0; i < n; i++) {
    tree.Insert(keys[i], values[i]);
    tree.PrintTree(mgr[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Print tree
  tree.PrintTree(mgr[0]);
  // Search keys
  vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    tree.Remove(delete_seq[i]);
    tree.PrintTree(mgr[i]);
  }
  tree.PrintTree(mgr[1]);
  // Check valid
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  }
  for (int i = n / 2; i < n; i++) {
    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
}