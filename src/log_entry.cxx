#include "crc32.hxx"
#include "log_entry.hxx"

namespace nuraft {
log_entry::log_entry(ulong term,
                     const ptr<buffer>& buff,
                     log_val_type value_type,
                     uint64_t log_timestamp,
                     bool has_crc32,
                     uint32_t crc32,
                     bool compute_crc)
    : term_(term)
    , value_type_(value_type)
    , buff_(buff)
    , timestamp_us_(log_timestamp)
    , has_crc32_(has_crc32)
    , crc32_(crc32)
    {
        if (!buff_ && !has_crc32 && compute_crc) {
            has_crc32_ = true;
            crc32_ = crc32_8( buff->data_begin(),
                              buff->size(),
                              0 );
        }
    }
}