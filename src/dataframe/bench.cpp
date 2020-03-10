// Made by Kaylin Devchand and Cristian Stransky
#include <time.h>
#include "dataframe.h"
#include "rowers.h"
#include "sor.h"
#include <chrono> 

using namespace std::chrono;


const int ITERATIONS = 100;

class Benchmark : public Object {
    public:
    SoR* sor_;
    DataFrame* df_;
    Rower* average_rower_;
    Rower* encrypt_rower_;

    Benchmark(char* filename) {
        sor_ = new SoR(filename, 0, 110000000); // 110000000 110MB with 700000 row
        df_ = sor_->get_dataframe();
        df_->add_column(new IntColumn(), nullptr);

        average_rower_ = new AverageRower(df_);
        encrypt_rower_ = new EncryptRower(df_);
    }

    ~Benchmark() {
        df_->schema_.num_rows = 671444;
        // delete df_ not necessary because sor_ really holds values
        delete sor_;
        delete average_rower_;
        delete encrypt_rower_;
    }

    double map_average_rower(size_t num) {
        df_->schema_.num_rows = num;
        double total_time = 0;
        for (int ii = 0; ii < ITERATIONS; ii++) {
            auto start = high_resolution_clock::now(); 

            // call map
            df_->map(*average_rower_);

            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<milliseconds>(stop - start); // Multiply by 1000 to get milliseconds
            total_time += duration.count();

        }

        return total_time / ITERATIONS;
    }

    double map_encrypt_rower(size_t num) {
        df_->schema_.num_rows = num;
        double total_time = 0;
        for (int ii = 0; ii < ITERATIONS; ii++) {
            auto start = high_resolution_clock::now(); 

            // call map
            df_->map(*encrypt_rower_);

            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<milliseconds>(stop - start); // Multiply by 1000 to get milliseconds
            total_time += duration.count();

        }

        return total_time / ITERATIONS;
    }

    double pmap_average_rower(size_t num) {
        df_->schema_.num_rows = num;
        double total_time = 0;
        for (int ii = 0; ii < ITERATIONS; ii++) {
            auto start = high_resolution_clock::now(); 

            // call map
            df_->pmap(*average_rower_);

            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<milliseconds>(stop - start); // Multiply by 1000 to get milliseconds
            total_time += duration.count();

        }

        return total_time / ITERATIONS;
    }

    double pmap_encrypt_rower(size_t num) {
        df_->schema_.num_rows = num;
        double total_time = 0;
        for (int ii = 0; ii < ITERATIONS; ii++) {
            auto start = high_resolution_clock::now(); 

            // call map
            df_->pmap(*encrypt_rower_);

            auto stop = high_resolution_clock::now(); 
            auto duration = duration_cast<milliseconds>(stop - start); // Multiply by 1000 to get milliseconds
            total_time += duration.count();
        }

        return total_time / ITERATIONS;
    }

};

int main(int argc, char **argv) {
    char* filename = (char*)"datafile.txt";
    // 1KB, 50KB, 100KB, 500KB, 1MB, 10MB, 25MB, 50MB, 75MB, 100MB
    int sizes[10] = {1000, 50000, 500000,1000000,10000000, 25000000, 50000000, 75000000, 100000000};
    int rows[4] = {50000, 100000, 500000, 671443};

    printf("\nReading file. Will take 5-10 min...\n\n");  
    Benchmark* b = new Benchmark(filename);

    printf("\nStarting benchmarking...\n\n");
    for (int ii = 0; ii < 4; ii++) {
        printf("Benchmarking with file of %d rows\n", rows[ii]);
        double time_map1 = b->map_average_rower(rows[ii]);
        printf("Time of map with AverageRower: %f\n", time_map1);
        double time_map2 = b->map_encrypt_rower(rows[ii]);
        printf("Time of map with encrypt rower: %f\n", time_map2);
        double time_pmap1 = b->pmap_average_rower(rows[ii]);
        printf("Time of pmap with average_rower: %f\n", time_pmap1);
        double time_pmap2 = b->pmap_encrypt_rower(rows[ii]);
        printf("Time of pmap with encrypt rower: %f\n\n", time_pmap2);
    } 

    return 0;

}