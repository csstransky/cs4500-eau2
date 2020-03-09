#include <gtest/gtest.h>
#include "dataframe.h" 

#define GT_TRUE(a)   ASSERT_EQ((a),true)
#define GT_FALSE(a)  ASSERT_EQ((a),false)
#define GT_EQUALS(a, b)   ASSERT_EQ(a, b)
#define ASSERT_EXIT_ZERO(a)  \
  ASSERT_EXIT(a(), ::testing::ExitedWithCode(0), ".*")

void test() {
  Schema s("II");

  DataFrame df(s);
  Row  r(df.get_schema());
  for (size_t i = 0; i <  1000 * 1000; i++) {
    r.set(0,(int)i);
    r.set(1,(int)i+1);
    df.add_row(r);
  }
  GT_EQUALS(df.get_int((size_t)0,1), 1);
  exit(0);
}

TEST(a4, t1){ ASSERT_EXIT_ZERO(test); }

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
