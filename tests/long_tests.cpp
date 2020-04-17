#include "../src/kv_store/kd_store.h"
#include "../src/networks/rendezvous_server.h"
#include <sys/wait.h>

/** NOTE: Time before refactor was 9m30s after 1m40s*/
void test_large_sor() {
    char* file = const_cast<char*>("data/users.ltgt");

    int cpid[3];
    const char* server_ip = "127.0.0.1";
    const char** client_ips = new const char*[3];
    client_ips[0] = "127.0.0.2";
    client_ips[1] = "127.0.0.3";
    client_ips[2] = "127.0.0.4";

    RServer* server = new RServer(server_ip); 

    for (int i = 0; i < 3; i++) {
        if ((cpid[i] = fork())) {
            // parent, do nothing now
        } else {
            // child process
            Key* k = new Key("k", 0);
            KD_Store* kd = new KD_Store(i, client_ips[i], server_ip);

            sleep(2);

            DataFrame* dataframe;

            if (i == 0) {
                dataframe = DataFrame::from_file(k, kd, file);
            } else {
                dataframe = kd->wait_and_get(k);
            }

            printf("%d: got dataframe\n", i);

            assert(dataframe->ncols() == 2);
            printf("%d: nrows %zu\n", i, dataframe->nrows());
            assert(dataframe->nrows() == 32411734); 

            kd->application_complete();   
            printf("%d: done\n", i);    

            delete k;
            delete kd;
            delete server;
            delete[] client_ips;
            exit(0);
        } 
    }

    // In parent process
    server->run_server();
    server->wait_for_shutdown();
    printf("server done\n");

    // wait for child to finish
    for (int i = 0; i < 3; i++) {
        int st;
        waitpid(cpid[i], &st, 0);
    }
    delete server;
    delete[] client_ips;

    printf("Large sor test passed!\n");
}

int main(int argc, char** argv) {
    test_large_sor();
}