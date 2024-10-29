#define DUCKDB_EXTENSION_MAIN

#include "mask_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

// Scramble a string by randomly reordering its characters
// scramble_string(input_string) -> string
//   input_string: the string to scramble
//   returns: the scrambled string with characters in random order
// Example:
//   select scramble_string('password') -> 'drowsap'
inline void ScrambleStringScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &input_vector = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        input_vector, result, args.size(),
        [&](string_t input_str) {
            string input = input_str.GetString();
            if (input.empty()) {
                return StringVector::AddString(result, input);
            }

            // Create a copy to scramble
            string scrambled = input;
            
            // Use random_device for better randomization
            static thread_local std::random_device rd;
            static thread_local std::mt19937 gen(rd());
            
            // Fisher-Yates shuffle algorithm
            for (size_t i = scrambled.length() - 1; i > 0; i--) {
                std::uniform_int_distribution<size_t> dis(0, i);
                size_t j = dis(gen);
                std::swap(scrambled[i], scrambled[j]);
            }

            return StringVector::AddString(result, scrambled);
        });
}

// Mask a string with a mask character
// mask_string(input_string, start, length, mask_char) -> string
//   input_string: the string to mask
//   start: the start index of the mask (1-based)
//   length: the length of the mask
//   mask_char: the character to use for the mask
//   returns: the masked string
// Example:
//   select mask_string('hello world', 3, 5, '*') -> 'he*****orld'
//   select mask_string('hello world', 1, 5, '*') -> '***** world'
//   select mask_string('hello world', 1, 100, '*') -> '************'
inline void MaskStringScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    auto &start_vector = args.data[1];
    auto &length_vector = args.data[2];
    auto &mask_vector = args.data[3];

    TernaryExecutor::Execute<string_t, int64_t, int64_t, string_t>(
        name_vector, start_vector, length_vector, result, args.size(),
        [&](string_t name, int64_t start, int64_t length) {
            string input = name.GetString();
            int64_t input_size = input.size();

            // Convert 1-based to 0-based index and ensure start is within bounds
            start = std::max(int64_t(0), std::min(start - 1, input_size));
            int64_t end = std::max(start, std::min(start + length, input_size));

            // Get the mask character
            char mask_char = mask_vector.GetValue(0).GetValueUnsafe<string_t>().GetData()[0];

            // Create the masked string
            string masked = input.substr(0, start) +
                            string(end - start, mask_char) +
                            input.substr(end);

            return StringVector::AddString(result, masked);
        });
}

// Mask an email address, showing only the first character and domain
// mask_email(email) -> string
//   email: the email address to mask
//   returns: the masked email address where only first character and domain are visible
// Example:
//   select mask_email('johndoe@example.com') -> 'j******@example.com'
inline void MaskEmailScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &email_vector = args.data[0];

    UnaryExecutor::Execute<string_t, string_t>(
        email_vector, result, args.size(),
        [&](string_t email) {
            string input = email.GetString();
            
            // Find the @ symbol
            size_t at_pos = input.find('@');
            if (at_pos == string::npos || at_pos == 0) {
                // If no @ found or it's the first character, return the original string
                return StringVector::AddString(result, input);
            }

            // Keep first character, mask until @, then keep domain
            string masked = input.substr(0, 1) +
                          string(at_pos - 1, '*') +
                          input.substr(at_pos);

            return StringVector::AddString(result, masked);
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register existing mask_string function
    auto mask_scalar_function = ScalarFunction("mask_string", {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR, MaskStringScalarFun);
    ExtensionUtil::RegisterFunction(instance, mask_scalar_function);

    // Register new mask_email function
    auto mask_email_function = ScalarFunction("mask_email", {LogicalType::VARCHAR}, LogicalType::VARCHAR, MaskEmailScalarFun);
    ExtensionUtil::RegisterFunction(instance, mask_email_function);

    // Register scramble_string function
    auto scramble_string_function = ScalarFunction(
        "scramble_string", 
        {LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, 
        ScrambleStringScalarFun
    );
    ExtensionUtil::RegisterFunction(instance, scramble_string_function);
}

void MaskExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string MaskExtension::Name() {
	return "mask";
}

std::string MaskExtension::Version() const {
#ifdef EXT_VERSION_MASK
	return EXT_VERSION_MASK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void mask_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::MaskExtension>();
}

DUCKDB_EXTENSION_API const char *mask_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
