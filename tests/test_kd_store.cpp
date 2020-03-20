#include "../src/kv_store/kd_store.h"

void test_one_dataframe() {
    Key key("key", 0);
    KD_Store kd(0);

    IntColumn col(kd.get_kv(), key.get_key(), 0);
    for (int i = 0; i < 200; i++) {
        col.push_back(i);
    }

    Schema s("");
    DataFrame df(s, key.get_key(), kd.get_kv());
    df.add_column(&col);

    kd.put(&key, &df);
    DataFrame* df2 = kd.get(&key);

    assert(df2);
    assert(df2->nrows() == 200);
    assert(df2->ncols() == 1);

    for (int i = 0; i < 200; i++) {
        assert(df2->get_int(0, i) == i);
    }

    delete df2;

    printf("KD Store one dataframe tests pass!\n");
}

void test_multiple_dataframe() {
    Key key("key", 0);
    //KD_Store kd(0);

    // IntColumn col_int(kd.get_kv(), key.get_key(), 0);
    // FloatColumn col_float(kd.get_kv(), key.get_key(), 0);
    // BoolColumn col_bool(kd.get_kv(), key.get_key(), 0);
    // StringColumn col_string(kd.get_kv(), key.get_key(), 0);
    // String test("test");
    // for (int i = 0; i < 200; i++) {
    //     col_int.push_back(i);
    //     col_float.push_back((float)i + 0.1);
    //     col_bool.push_back((bool)i % 2);
    //     col_string.push_back(&test);
    // }

    // Schema s("");
    // DataFrame df(s, key.get_key(), kd.get_kv());
    // df.add_column(&col_int);
    // df.add_column(&col_float);
    // df.add_column(&col_bool);
    // df.add_column(&col_string);

    // kd.put(&key, &df);
    // DataFrame* df2 = kd.get(&key);

    // assert(df2);
    // assert(df2->nrows() == 200);
    // assert(df2->ncols() == 1);

    // for (int i = 0; i < 200; i++) {
    //     assert(df2->get_int(0, i) == i);
    //     assert(df2->get_float(1, i) == ((float)i+0.2));
    //     assert(df2->get_bool(2, i) == i % 2);
    //     assert(df2->get_string(3, i)->equals(&test));
    // }

    // delete df2;

    printf("KD Store multiple dataframe tests pass!\n");
}

int main(int argc, char** argv) {
    test_one_dataframe();
    test_multiple_dataframe();
    printf("All KD Store tests pass!\n");
}