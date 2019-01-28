#include <pegasus/client.h>
#include "client_test.h"

using namespace pegasus;

//    i =- 1;
//    while (i != 0) {
//        i = -1;
//    }

std::vector<std::string> data_keys;
std::vector<std::string> scan_keys;

void generate_key(std::vector<std::string> &keys,
                  int key_len,
                  bool contain_empty,
                  int test_data_set)
{
    // test_data_set = 0: ! - ~ = 0x21 - 0x7E, 94 characters
    // test_data_set = 1: A - z = 0x41 - 0x7A, 58 characters
    // test_data_set = 2: 0 - 9 = 0x30 - 0x39, 10 characters
    int num, start = 0;
    switch (test_data_set) {
    case 0:
        num = 0x7F - 0x21;
        start = 0x21;

        break;
    case 1:
        num = 0x7B - 0x41;
        start = 0x41;
        break;
    case 2:
        num = 0x3A - 0x30;
        start = 0x30;
        break;
    default:
        num = 0x7F - 0x21;
        start = 0x21;
    }
    char *chars = new char[num];
    int i = 0;
    keys.clear();
    if (contain_empty)
        keys.push_back(std::string(""));
    for (i = 0; i < num; i++) {
        chars[i] = i + start;
    }
    for (i = 0; i < num - key_len; i++) {
        keys.push_back(std::string(chars + i, key_len));
    }
    fprintf(stdout, "Generated %d keys\n", i);
}

int generate_data(
    pegasus_client *client, int step, int data_key_len, int scan_key_len, int test_data_set)
{
    int ret = 0;
    int j = 0;
    generate_key(data_keys, data_key_len, false, test_data_set);
    generate_key(scan_keys, scan_key_len, true, test_data_set);
    if (data_keys.empty() || scan_keys.empty()) {
        fprintf(stdout, "ERROR: generate data failed\n");
        return -1;
    }
    std::string set_value("test");
    for (auto it_hash_key = data_keys.begin(); it_hash_key < data_keys.end();
         it_hash_key = it_hash_key + step) {
        for (auto it_sort_key = data_keys.begin(); it_sort_key < data_keys.end();
             it_sort_key = it_sort_key + step) {
            ret = client->set(*it_hash_key, *it_sort_key, set_value);
            if (ret != PERR_OK) {
                fprintf(stdout, "ERROR: set failed, error=%s\n", client->get_error_string(ret));
                return -1;
            } else {
                j++;
            }
        }
    }
    fprintf(stdout, "Inserted %d rows\n", j);
    return ret;
}

int cleanup_data(pegasus_client *client, int step)
{
    int ret = 0;
    int j = 0;
    for (auto it_hash_key = data_keys.begin(); it_hash_key < data_keys.end();
         it_hash_key = it_hash_key + step) {
        for (auto it_sort_key = data_keys.begin(); it_sort_key < data_keys.end();
             it_sort_key = it_sort_key + step) {
            ret = client->del(*it_hash_key, *it_sort_key);
            if (ret != PERR_OK) {
                fprintf(stdout, "ERROR: del failed, error=%s\n", client->get_error_string(ret));
                return -1;
            } else {
                j++;
            }
        }
    }
    fprintf(stdout, "Deleted %d rows\n", j);
    return ret;
}

int scanner_testcase(pegasus_client *client,
                     std::string scan_hash_key,
                     std::string scan_start_sort_key,
                     std::string scan_stop_sort_key,
                     pegasus_client::scan_options scan_options,
                     bool print_data)
{
    fprintf(stdout,
            "%s scan: %s%s-%s, %s-%s%s\n",
            //scan_options.reverse ? "DESC" : "ASC",
            scan_options.reverse ? "Range" : "Range",
            scan_options.start_inclusive ? "[" : "(",
            scan_hash_key.c_str(),
            scan_start_sort_key.c_str(),
            scan_hash_key.c_str(),
            scan_stop_sort_key.c_str(),
            scan_options.stop_inclusive ? "]" : ")");
    int ret = 0;
    pegasus_client::pegasus_scanner *scanner = nullptr;
    ret = client->get_scanner(
        scan_hash_key, scan_start_sort_key, scan_stop_sort_key, scan_options, scanner);
    if (ret != PERR_OK) {
        fprintf(stdout, "ERROR: get scanner failed, error=%s\n", client->get_error_string(ret));
        return -1;
    }
    std::string hash_key;
    std::string sort_key;
    std::string value;
    int i = 0;
    while (scanner->next(hash_key, sort_key, value) == PERR_OK) {
        i++;
        if (print_data)
            fprintf(stdout, "[%s][%s] => %s\n", hash_key.c_str(), sort_key.c_str(), value.c_str());
    }
    fprintf(stdout, "Scanned %d rows\n\n", i);
    delete scanner;
    return ret;
}

int scanner_test(pegasus_client *client,
                 pegasus_client::scan_options scan_options,
                 int step,
                 bool print_data)
{
    int ret = 0;
    int round = 1;
    for (auto it_start_sort_key = scan_keys.begin(); it_start_sort_key < scan_keys.end();
         it_start_sort_key = it_start_sort_key + step) {
        for (auto it_stop_sort_key = scan_keys.begin(); it_stop_sort_key < scan_keys.end();
             it_stop_sort_key = it_stop_sort_key + step) {
            fprintf(stdout, "******* Testcase %d *******\n", round);
            ret = scanner_testcase(client,
                                   *data_keys.begin(),
                                   *it_start_sort_key,
                                   *it_stop_sort_key,
                                   scan_options,
                                   print_data);
            if (ret != PERR_OK) {
                fprintf(stdout,
                        "ERROR: scanner testcase failed, error=%s\n",
                        client->get_error_string(ret));
                return -1;
            }
            round++;
        }
    }
    return ret;
}

int unordered_full_scanners_test(pegasus_client *client,
                                 pegasus_client::scan_options scan_options,
                                 int scanner_count,
                                 bool print_data)
{
    int ret = 0;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    ret = client->get_unordered_scanners(scanner_count, scan_options, scanners);
    if (ret != PERR_OK) {
        fprintf(stdout,
                "ERROR: get unordered scanners failed, error=%s\n",
                client->get_error_string(ret));
        return -1;
    } else {
        fprintf(stdout, "INFO: get %d unordered scanners succeed\n", ((int)scanners.size()));
    }
    std::string hash_key;
    std::string sort_key;
    std::string value;
    int i, j = 0;
    for (int idx = 0; idx < scanners.size(); idx++) {
        i = 0;
        fprintf(stdout, "Scanner(%d): \n", idx);
        while (scanners[idx]->next(hash_key, sort_key, value) == PERR_OK) {
            i++;
            j++;
            if (print_data)
                fprintf(
                    stdout, "[%s][%s] => %s\n", hash_key.c_str(), sort_key.c_str(), value.c_str());
        }
        //fprintf(stdout, "%s scanned %d rows\n\n", scan_options.reverse ? "DESC" : "ASC", i);
        fprintf(stdout, "Scanned %d rows\n\n", i);
    }
    fprintf(stdout, "Total scanned %d rows\n", j);
    for (auto scanner : scanners) {
        delete scanner;
    }
    return ret;
}

int unordered_range_scanners_testcase(pegasus_client *client,
                                      std::string scan_start_hash_key,
                                      std::string scan_stop_hash_key,
                                      std::string scan_start_sort_key,
                                      std::string scan_stop_sort_key,
                                      pegasus_client::scan_options scan_options,
                                      int scanner_count,
                                      bool print_data)
{
    fprintf(stdout,
            "%s scan: [%s-%s, %s-%s]\n",
            //scan_options.reverse ? "DESC" : "ASC",
            scan_options.reverse ? "Range" : "Range",
            scan_start_hash_key.c_str(),
            scan_start_sort_key.c_str(),
            scan_stop_hash_key.c_str(),
            scan_stop_sort_key.c_str());
    int ret = 0;
    std::vector<pegasus_client::pegasus_scanner *> scanners;
    ret = client->get_unordered_range_scanners(scanner_count,
                                               scan_start_hash_key,
                                               scan_stop_hash_key,
                                               scan_start_sort_key,
                                               scan_stop_sort_key,
                                               scan_options,
                                               scanners);
    if (ret != PERR_OK) {
        fprintf(stdout,
                "ERROR: get unordered range scanners failed, error=%s\n",
                client->get_error_string(ret));
        return 0;
    } else {
        fprintf(stdout, "INFO: get %d unordered range scanners succeed\n", ((int)scanners.size()));
    }
    std::string hash_key;
    std::string sort_key;
    std::string value;
    int i, j = 0;
    for (int idx = 0; idx < scanners.size(); idx++) {
        i = 0;
        fprintf(stdout, "Scanner(%d): \n", idx);
        while (scanners[idx]->next(hash_key, sort_key, value) == PERR_OK) {
            i++;
            j++;
            if (print_data)
                fprintf(
                    stdout, "[%s][%s] => %s\n", hash_key.c_str(), sort_key.c_str(), value.c_str());
        }
        //fprintf(stdout, "%s scanned %d rows\n\n", scan_options.reverse ? "DESC" : "ASC", i);
        fprintf(stdout, "Scanned %d rows\n\n", i);
    }
    fprintf(stdout, "Total scanned %d rows\n\n", j);
    for (auto scanner : scanners) {
        delete scanner;
    }
    return ret;
}

int unordered_range_scanners_test(pegasus_client *client,
                                  pegasus_client::scan_options scan_options,
                                  int step,
                                  int scanner_count,
                                  bool print_data)
{
    int ret = 0;
    int round = 1;
    std::string empty_key("");
    for (auto it_hash_start_key = scan_keys.begin(); it_hash_start_key < scan_keys.end();
         it_hash_start_key = it_hash_start_key + step) {
        for (auto it_hash_stop_key = scan_keys.begin(); it_hash_stop_key < scan_keys.end();
             it_hash_stop_key = it_hash_stop_key + step) {
            for (auto it_sort_start_key = scan_keys.begin(); it_sort_start_key < scan_keys.end();
                 it_sort_start_key = it_sort_start_key + step) {
                for (auto it_sort_stop_key = scan_keys.begin(); it_sort_stop_key < scan_keys.end();
                     it_sort_stop_key = it_sort_stop_key + step) {
                    fprintf(stdout, "******* Testcase %d *******\n", round);
                    ret = unordered_range_scanners_testcase(client,
                                                            *it_hash_start_key,
                                                            *it_hash_stop_key,
                                                            *it_sort_start_key,
                                                            *it_sort_stop_key,
                                                            scan_options,
                                                            scanner_count,
                                                            print_data);
                    if (ret != PERR_OK) {
                        fprintf(stdout,
                                "ERROR: unordered range scanners testcase failed, error=%s\n",
                                client->get_error_string(ret));
                        return -1;
                    }
                    round++;
                }
            }
        }
    }
    return ret;
}

int sorted_scanner_testcase(pegasus_client *client,
                            std::string scan_start_hash_key,
                            std::string scan_stop_hash_key,
                            std::string scan_start_sort_key,
                            std::string scan_stop_sort_key,
                            pegasus_client::scan_options scan_options,
                            bool print_data)
{
    fprintf(stdout,
            "%s scan: [%s-%s, %s-%s]\n",
            //scan_options.reverse ? "DESC" : "ASC",
            scan_options.reverse ? "Range" : "Range",
            scan_start_hash_key.c_str(),
            scan_start_sort_key.c_str(),
            scan_stop_hash_key.c_str(),
            scan_stop_sort_key.c_str());
    int ret = 0;
    pegasus_client::pegasus_sorted_scanner *sorted_scanner = nullptr;
    ret = client->get_sorted_scanner(scan_start_hash_key,
                                     scan_stop_hash_key,
                                     scan_start_sort_key,
                                     scan_stop_sort_key,
                                     scan_options,
                                     sorted_scanner);
    if (ret != PERR_OK) {
        fprintf(
            stdout, "ERROR: get sorted scanner failed, error=%s\n", client->get_error_string(ret));
        return 0;
    }
    std::string hash_key;
    std::string sort_key;
    std::string value;
    int i = 0;
    while (sorted_scanner->next(hash_key, sort_key, value) == PERR_OK) {
        i++;
        if (print_data)
            fprintf(stdout, "[%s][%s] => %s\n", hash_key.c_str(), sort_key.c_str(), value.c_str());
    }
    fprintf(stdout, "Scanned %d rows\n\n", i);
    delete sorted_scanner;
    return ret;
}

int sorted_scanner_test(pegasus_client *client,
                        pegasus_client::scan_options scan_options,
                        int step,
                        bool print_data) // full and range
{
    int ret = 0;
    int round = 1;
    std::string empty_key("");
    for (auto it_hash_start_key = scan_keys.begin(); it_hash_start_key < scan_keys.end();
         it_hash_start_key = it_hash_start_key + step) {
        for (auto it_hash_stop_key = scan_keys.begin(); it_hash_stop_key < scan_keys.end();
             it_hash_stop_key = it_hash_stop_key + step) {
            for (auto it_sort_start_key = scan_keys.begin(); it_sort_start_key < scan_keys.end();
                 it_sort_start_key = it_sort_start_key + step) {
                for (auto it_sort_stop_key = scan_keys.begin(); it_sort_stop_key < scan_keys.end();
                     it_sort_stop_key = it_sort_stop_key + step) {
                    fprintf(stdout, "******* Testcase %d *******\n", round);
                    ret = sorted_scanner_testcase(client,
                                                  *it_hash_start_key,
                                                  *it_hash_stop_key,
                                                  *it_sort_start_key,
                                                  *it_sort_stop_key,
                                                  scan_options,
                                                  print_data);
                    if (ret != PERR_OK) {
                        fprintf(stdout,
                                "ERROR: sorted scanner testcase failed, error=%s\n",
                                client->get_error_string(ret));
                        return -1;
                    }
                    round++;
                }
            }
        }
    }
    return ret;
}