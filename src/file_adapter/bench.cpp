// Made by Kaylin Devchand and Cristian Stransky
#include <time.h>
#include "../dataframe/dataframe.h"
#include "../dataframe/rowers.h"
#include "sor.h"
#include <chrono> 

using namespace std::chrono;

const int ITERATIONS = 100;
const size_t FILE_SIZE = 110000000; // 110000000 110MB with 700000 row
const size_t NUM_ROWS = 4;
const int ROWS[NUM_ROWS] = {50000, 100000, 500000, 671443}; // Make sure last number is total rows

class Benchmark : public Object {
    public:
    SoR* sor_;
    DataFrame* df_;
    Rower* average_rower_;
    Rower* encrypt_rower_;

    Benchmark(char* filename) {
        auto start_time = high_resolution_clock::now(); 

        sor_ = new SoR(filename, 0, FILE_SIZE);
        df_ = sor_->get_dataframe();
        IntColumn result_column = IntColumn();
        df_->add_column(&result_column);

        auto stop_time = high_resolution_clock::now(); 
        double duration = duration_cast<milliseconds>(stop_time - start_time).count(); // Multiply by 1000 to get milliseconds
        printf("Time of file reading: %f\n", duration);

        average_rower_ = new AverageRower(df_);
        encrypt_rower_ = new EncryptRower(df_);
    }

    ~Benchmark() {
        // delete df_ not necessary because sor_ really holds values
        delete sor_;
        delete average_rower_;
        delete encrypt_rower_;
    }

    double map_average_rower(size_t num) {
        df_->schema_.num_rows_ = num;
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
        df_->schema_.num_rows_ = num;
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
        df_->schema_.num_rows_ = num;
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
        df_->schema_.num_rows_ = num;
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

    printf("\nReading file. Will take roughly 1 minute...\n");  
    Benchmark b(filename);

    printf("\nStarting benchmarking...\n\n");
    for (int ii = 0; ii < NUM_ROWS; ii++) {
        printf("Benchmarking with file of %d rows\n", ROWS[ii]);
        double time_map1 = b.map_average_rower(ROWS[ii]);
        printf("Time of map with AverageRower: %f\n", time_map1);
        double time_map2 = b.map_encrypt_rower(ROWS[ii]);
        printf("Time of map with encrypt rower: %f\n", time_map2);
        double time_pmap1 = b.pmap_average_rower(ROWS[ii]);
        printf("Time of pmap with average_rower: %f\n", time_pmap1);
        double time_pmap2 = b.pmap_encrypt_rower(ROWS[ii]);
        printf("Time of pmap with encrypt rower: %f\n\n", time_pmap2);
    } 
    return 0;
}