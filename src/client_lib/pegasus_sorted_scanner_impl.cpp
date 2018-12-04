// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_client_impl.h"
#include "base/pegasus_const.h"

using namespace ::dsn;
using namespace pegasus;

namespace pegasus {
namespace client {

pegasus_client_impl::pegasus_sorted_scanner_impl::pegasus_sorted_scanner_impl(
    std::vector<pegasus_scanner *> &&scanners)
    : _replica_scanners(std::move(scanners))
{
}

int pegasus_client_impl::pegasus_sorted_scanner_impl::next(std::string &hashkey,
                                                           std::string &sortkey,
                                                           std::string &value,
                                                           internal_info *info)
{
    int ret = -1;
    std::string scan_hashkey;
    std::string scan_sortkey;
    std::string scan_value;
    dsn::blob key;
    pegasus_scanner *replica_scanner = nullptr;
    if (_replica_scanner_queue.empty()) {
        // put all scanners into queue ordered by hashkey of first entry
        for (auto scanner : _replica_scanners) {
            if (scanner->next(scan_hashkey, scan_sortkey, scan_value) == PERR_OK) {
                _replica_scan_item.hashkey = scan_hashkey;
                _replica_scan_item.sortkey = scan_sortkey;
                _replica_scan_item.value = scan_value;
                _replica_scan_item.scanner = scanner;
                //                // ordered by hashkey_len+hashkey
                //                pegasus_generate_key(key, scan_hashkey, std::string(""));
                //                _replica_scanner_queue.emplace(key.to_string(),
                //                _replica_scan_item);
                _replica_scanner_queue.emplace(scan_hashkey, _replica_scan_item);
            }
        }

        // done
        if (_replica_scanner_queue.empty())
            return ret;

        // get the lowest hashkey entry
        auto it = _replica_scanner_queue.begin();
        hashkey = it->second.hashkey;
        sortkey = it->second.sortkey;
        value = it->second.value;
        replica_scanner = it->second.scanner;

        // remove the scanner
        _replica_scanner_queue.erase(it);

        // put the scanner back to queue with new entry
        if (replica_scanner->next(scan_hashkey, scan_sortkey, scan_value) == PERR_OK) {
            _replica_scan_item.hashkey = scan_hashkey;
            _replica_scan_item.sortkey = scan_sortkey;
            _replica_scan_item.value = scan_value;
            _replica_scan_item.scanner = replica_scanner;
            //            pegasus_generate_key(key, scan_hashkey, std::string(""));
            //            _replica_scanner_queue.emplace(key.to_string(), _replica_scan_item);
            _replica_scanner_queue.emplace(scan_hashkey, _replica_scan_item);
        }
    } else {
        // get the lowest hashkey entry
        auto it = _replica_scanner_queue.begin();
        hashkey = it->second.hashkey;
        sortkey = it->second.sortkey;
        value = it->second.value;
        replica_scanner = it->second.scanner;

        // remove the scanner
        _replica_scanner_queue.erase(it);

        // put the scanner back to queue with new entry
        if (replica_scanner->next(scan_hashkey, scan_sortkey, scan_value) == PERR_OK) {
            _replica_scan_item.hashkey = scan_hashkey;
            _replica_scan_item.sortkey = scan_sortkey;
            _replica_scan_item.value = scan_value;
            _replica_scan_item.scanner = replica_scanner;
            //            pegasus_generate_key(key, scan_hashkey, std::string(""));
            //            _replica_scanner_queue.emplace(key.to_string(), _replica_scan_item);
            _replica_scanner_queue.emplace(scan_hashkey, _replica_scan_item);
        }
    }
    return PERR_OK;
}

void pegasus_client_impl::pegasus_sorted_scanner_impl::async_next(
    async_scan_next_callback_t &&callback)
{
    // TODO wss Pn
    derror("no implement yet");
}

pegasus_client::pegasus_sorted_scanner_wrapper
pegasus_client_impl::pegasus_sorted_scanner_impl::get_smart_wrapper()
{
    return std::make_shared<pegasus_sorted_scanner_impl_wrapper>(this);
}

pegasus_client_impl::pegasus_sorted_scanner_impl::~pegasus_sorted_scanner_impl()
{
    dassert(_replica_scanner_queue.empty(), "queue should be empty");
    for (auto scanner : _replica_scanners) {
        delete scanner;
    }
}

void pegasus_client_impl::pegasus_sorted_scanner_impl_wrapper::async_next(
    async_scan_next_callback_t &&callback)
{
    // TODO wss Pn+1
    derror("no implement yet");
}
}
} // namespace pegasus