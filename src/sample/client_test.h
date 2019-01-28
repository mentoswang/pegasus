#ifndef PEGASUS_CLIENT_TEST_H
#define PEGASUS_CLIENT_TEST_H

#include <pegasus/client.h>

using namespace pegasus;

int generate_data(
    pegasus_client *client, int step, int data_key_len, int scan_key_len, int test_data_set);
int cleanup_data(pegasus_client *client, int step);
int scanner_test(pegasus_client *client,
                 pegasus_client::scan_options scan_options,
                 int step,
                 bool print_data);
int unordered_full_scanners_test(pegasus_client *client,
                                 pegasus_client::scan_options scan_options,
                                 int scanner_count,
                                 bool print_data);
int unordered_range_scanners_test(pegasus_client *client,
                                  pegasus_client::scan_options scan_options,
                                  int step,
                                  int scanner_count,
                                  bool print_data);
int sorted_scanner_test(pegasus_client *client,
                        pegasus_client::scan_options scan_options,
                        int step,
                        bool print_data);

#endif // PEGASUS_CLIENT_TEST_H
