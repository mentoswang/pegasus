// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdio>
#include <pegasus/client.h>
#include "client_test.h"
using namespace pegasus;

int main(int argc, const char *argv[])
{
    if (!pegasus_client_factory::initialize("config.ini")) {
        fprintf(stderr, "ERROR: init pegasus failed\n");
        return -1;
    }

    if (argc != 3) {
        fprintf(stderr, "USAGE: %s <cluster-name> <app-name>\n", argv[0]);
        return -1;
    }

    // set
    pegasus_client *client = pegasus_client_factory::get_client(argv[1], argv[2]);
    std::string hash_key("pegasus_cpp_sample_hash_key");
    std::string sort_key("pegasus_cpp_sample_sort_key");
    std::string value("pegasus_cpp_sample_value");

    int ret = client->set(hash_key, sort_key, value);
    if (ret != PERR_OK) {
        fprintf(stderr, "ERROR: set failed, error=%s\n", client->get_error_string(ret));
        return -1;
    } else {
        fprintf(stdout, "INFO: set succeed\n");
    }

    // get
    std::string get_value;
    ret = client->get(hash_key, sort_key, get_value);
    if (ret != PERR_OK) {
        fprintf(stderr, "ERROR: get failed, error=%s\n", client->get_error_string(ret));
        return -1;
    } else {
        fprintf(stdout, "INFO: get succeed, value=%s\n", get_value.c_str());
    }

    // hash scan
    pegasus_client::pegasus_scanner *scanner = nullptr;
    std::string start_sort_key("pegasus_cpp_sample");
    std::string stop_sort_key(""); // empty string means scan to the end
    pegasus_client::scan_options scan_options;
    scan_options.start_inclusive = true;
    scan_options.stop_inclusive = false;
    ret = client->get_scanner(hash_key, start_sort_key, stop_sort_key, scan_options, scanner);
    if (ret != PERR_OK) {
        fprintf(stderr, "ERROR: get scanner failed, error=%s\n", client->get_error_string(ret));
        return -1;
    } else {
        fprintf(stdout, "INFO: get scanner succeed\n");
    }
    std::string scan_hash_key;
    std::string scan_sort_key;
    std::string scan_value;
    int i = 0;
    while (scanner->next(scan_hash_key, scan_sort_key, scan_value) == PERR_OK) {
        fprintf(stdout,
                "INFO: scan(%d): [%s][%s] => %s\n",
                i++,
                scan_hash_key.c_str(),
                scan_sort_key.c_str(),
                scan_value.c_str());
    }

    // del
    ret = client->del(hash_key, sort_key);
    if (ret != PERR_OK) {
        fprintf(stderr, "ERROR: del failed, error=%s\n", client->get_error_string(ret));
        return -1;
    } else {
        fprintf(stdout, "INFO: del succeed\n");
    }

    // client test
    fprintf(stdout, "======= client test begin =======\n");

    // test config
    int data_step = 2;
    int scan_step = 3;
    int data_key_len = 3;
    int scan_key_len = 3;
    int test_data_set = 2;  // 0:!-~ 1:A-z 2:0-9
    bool print_data = false; // better be turn off if data_step is small
    int scanner_count = 0;  // split count
    scan_options.batch_size = 3;
    scan_options.start_inclusive = false;
    scan_options.stop_inclusive = false;
    scan_options.reverse = true;

    fprintf(stdout,
            "\n+++++++ Test Configuration +++++++\n"
            "data_step = %d\n"
            "scan_step = %d\n"
            "data_key_len = %d\n"
            "scan_key_len = %d\n"
            "test_data_set = %d\n"
            "scan batch_size = %d\n"
            "scanner_count = %d\n"
            "%s scan\n"
            "print %sdata\n",
            data_step,
            scan_step,
            data_key_len,
            scan_key_len,
            test_data_set,
            scan_options.batch_size,
            scanner_count,
            scan_options.reverse ? "DESC" : "ASC",
            print_data ? "" : "no ");

    fprintf(stdout, "\n+++++++ Data Generation +++++++\n");
    ret = generate_data(client, data_step, data_key_len, scan_key_len, test_data_set);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: generate data failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: generate data succeed\n");
    }

    fprintf(stdout, "\n+++++++ Scanner Test +++++++\n");
    ret = scanner_test(client, scan_options, scan_step, print_data);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: scanner test failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: scanner test succeed\n");
    }

    fprintf(stdout, "\n+++++++ Unordered Full Scanners Test +++++++\n");
    ret = unordered_full_scanners_test(client, scan_options, scanner_count, print_data);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: unordered full scanners test failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: unordered full scanners test succeed\n");
    }

    fprintf(stdout, "\n+++++++ Unordered Range Scanners Test +++++++\n");
    ret = unordered_range_scanners_test(client, scan_options, scan_step, scanner_count, print_data);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: unordered full scanners test failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: unordered full scanners test succeed\n");
    }

    fprintf(stdout, "\n+++++++ Sorted Scanner Test +++++++\n");
    ret = sorted_scanner_test(client, scan_options, scan_step, print_data);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: unordered full scanners test failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: unordered full scanners test succeed\n");
    }

    fprintf(stdout, "\n+++++++ Data Cleanup +++++++\n");
    ret = cleanup_data(client, data_step);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: cleanup data failed\n");
        return -1;
    } else {
        fprintf(stdout, "INFO: cleanup data succeed\n");
    }

    fprintf(stdout, "\n======= client test end =======\n");

    return 0;
}
