  #include "dataframe.h"
  #include "sor.h"
  #include "kv_store.h"

  
  DataFrame* dataframe_from_file(Key* key, KV_Store* kv, char* file_name) {
    SoR sor(file_name, key->get_key(), kv);
    kv->put(key, sor.get_dataframe());
    return sor.get_dataframe();
  }