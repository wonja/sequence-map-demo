#define DUCKDB_EXTENSION_MAIN

#include "sequence_map_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {



inline void SequenceMapScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	static unordered_map<string, char> map = {{"TTT", 'F'},{ "TTC", 'F'},{ "TTA", 'L'},{ "TTG", 'L'},{ "TCT", 'S'},{ "TCC", 'S'},{ "TCA", 'S'},{ "TCG", 'S'},{ "TAT", 'Y'},{ "TAC", 'Y'},{ "TGT", 'C'},{ "TGC", 'C'},{ "TGG", 'W'},{ "CTT", 'L'},{ "CTC", 'L'},{ "CTA", 'L'},{ "CTG", 'L'},{ "CCT", 'P'},{ "CCC", 'P'},{ "CCA", 'P'},{ "CCG", 'P'},{ "CAT", 'H'},{ "CAC", 'H'},{ "CAA", 'Q'},{ "CAG", 'Q'},{ "CGT", 'R'},{ "CGC", 'R'},{ "CGA", 'R'},{ "CGG", 'R'},{ "ATT", 'I'},{ "ATC", 'I'},{ "ATA", 'I'},{ "ATG", 'M'},{ "ACT", 'T'},{ "ACC", 'T'},{ "ACA", 'T'},{ "ACG", 'T'},{ "AAT", 'N'},{ "AAC", 'N'},{ "AAA", 'K'},{ "AAG", 'K'},{ "AGT", 'S'},{ "AGC", 'S'},{ "AGA", 'R'},{ "AGG", 'R'},{ "GTT", 'V'},{ "GTC", 'V'},{ "GTA", 'V'},{ "GTG", 'V'},{ "GCT", 'A'},{ "GCC", 'A'},{ "GCA", 'A'},{ "GCG", 'A'},{ "GAT", 'D'},{ "GAC", 'D'},{ "GAA", 'E'},{ "GAG", 'E'},{ "GGT", 'G'},{ "GGC", 'G'},{ "GGA", 'G'},{ "GGG", 'G'},{ "TAG", '*'},{ "TAA", '*'},{ "TGA", '*'}
};

    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t in) {
	    	auto res= StringVector::EmptyString(result, in.GetSize()/3);
	    	auto res_ptr = res.GetDataWriteable();
	    	auto in_ptr = in.GetDataUnsafe();

	    	for (idx_t i = 0; i < in.GetSize()/3; i++) {
	    		auto substring = string(in_ptr + i * 3, 3); // FIXME this allocates
	    		auto it = map.find(substring);
	    		if (it == map.end()) {
	    			res_ptr[i] = '?'; // TODO what should be this?
	    			continue;
	    		}
	    		res_ptr[i] = it->second;
	    	}
			return res;
        });
}


static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto sequence_map_scalar_function = ScalarFunction("sequence_map", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SequenceMapScalarFun);
    ExtensionUtil::RegisterFunction(instance, sequence_map_scalar_function);

}

void SequenceMapExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string SequenceMapExtension::Name() {
	return "sequence_map";
}

std::string SequenceMapExtension::Version() const {
#ifdef EXT_VERSION_SEQUENCE_MAP
	return EXT_VERSION_SEQUENCE_MAP;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sequence_map_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::SequenceMapExtension>();
}

DUCKDB_EXTENSION_API const char *sequence_map_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
