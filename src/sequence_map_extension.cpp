#define DUCKDB_EXTENSION_MAIN

#include "sequence_map_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>

namespace duckdb {

/* C++ code produced by gperf version 3.0.3 */
/* Command-line:
 * /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/gperf
 * -L C++ /Users/hannes/Desktop/benchling-codes.txt  */
/* Computed positions: -k'1-3' */

#if !(                                                                         \
    (' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) && ('%' == 37) && \
    ('&' == 38) && ('\'' == 39) && ('(' == 40) && (')' == 41) &&               \
    ('*' == 42) && ('+' == 43) && (',' == 44) && ('-' == 45) && ('.' == 46) && \
    ('/' == 47) && ('0' == 48) && ('1' == 49) && ('2' == 50) && ('3' == 51) && \
    ('4' == 52) && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) && \
    ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) && ('=' == 61) && \
    ('>' == 62) && ('?' == 63) && ('A' == 65) && ('B' == 66) && ('C' == 67) && \
    ('D' == 68) && ('E' == 69) && ('F' == 70) && ('G' == 71) && ('H' == 72) && \
    ('I' == 73) && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) && \
    ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) && ('R' == 82) && \
    ('S' == 83) && ('T' == 84) && ('U' == 85) && ('V' == 86) && ('W' == 87) && \
    ('X' == 88) && ('Y' == 89) && ('Z' == 90) && ('[' == 91) &&                \
    ('\\' == 92) && (']' == 93) && ('^' == 94) && ('_' == 95) &&               \
    ('a' == 97) && ('b' == 98) && ('c' == 99) && ('d' == 100) &&               \
    ('e' == 101) && ('f' == 102) && ('g' == 103) && ('h' == 104) &&            \
    ('i' == 105) && ('j' == 106) && ('k' == 107) && ('l' == 108) &&            \
    ('m' == 109) && ('n' == 110) && ('o' == 111) && ('p' == 112) &&            \
    ('q' == 113) && ('r' == 114) && ('s' == 115) && ('t' == 116) &&            \
    ('u' == 117) && ('v' == 118) && ('w' == 119) && ('x' == 120) &&            \
    ('y' == 121) && ('z' == 122) && ('{' == 123) && ('|' == 124) &&            \
    ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error                                                                         \
    "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gnu-gperf@gnu.org>."
#endif

#define TOTAL_KEYWORDS 64
#define MIN_WORD_LENGTH 3
#define MAX_WORD_LENGTH 3
#define MIN_HASH_VALUE 7
#define MAX_HASH_VALUE 180
/* maximum key range = 174, duplicates = 0 */

class Perfect_Hash {
public:
  static inline unsigned int hash(const char *str, unsigned int len);

public:
  static const char *in_word_set(const char *str, unsigned int len);
};

inline unsigned int Perfect_Hash::hash(const char *str, unsigned int len) {
  static unsigned char asso_values[] = {
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 4,   127, 10,  90,  2,   181, 40,  45,  5,   181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 25,  0,   0,   181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181, 181,
      181, 181, 181};
  return len + asso_values[(unsigned char)str[2] + 1] +
         asso_values[(unsigned char)str[1] + 2] +
         asso_values[(unsigned char)str[0]];
}

const char *Perfect_Hash::in_word_set(const char *str, unsigned int len) {
  static const char *wordlist[] = {
      "",    "", "",    "",    "",    "",    "",    "ATT", "",    "ACT",
      "",    "", "AGT", "CTT", "",    "CCT", "",    "AAT", "CGT", "",
      "",    "", "",    "CAT", "",    "",    "",    "",    "TTT", "",
      "TCT", "", "",    "TGT", "",    "",    "",    "",    "TAT", "",
      "",    "", "",    "GTT", "",    "GCT", "",    "",    "GGT", "",
      "",    "", "ATG", "GAT", "ACG", "",    "",    "AGG", "CTG", "",
      "CCG", "", "AAG", "CGG", "",    "",    "",    "",    "CAG", "",
      "",    "", "",    "TTG", "",    "TCG", "",    "",    "TGG", "",
      "",    "", "",    "TAG", "",    "",    "",    "",    "GTG", "",
      "GCG", "", "",    "GGG", "",    "",    "",    "ATC", "GAG", "ACC",
      "",    "", "AGC", "CTC", "",    "CCC", "",    "AAC", "CGC", "",
      "",    "", "",    "CAC", "",    "",    "",    "",    "TTC", "",
      "TCC", "", "",    "TGC", "",    "",    "",    "",    "TAC", "",
      "",    "", "",    "GTC", "ATA", "GCC", "ACA", "",    "GGC", "AGA",
      "CTA", "", "CCA", "GAC", "AAA", "CGA", "",    "",    "",    "",
      "CAA", "", "",    "",    "",    "TTA", "",    "TCA", "",    "",
      "TGA", "", "",    "",    "",    "TAA", "",    "",    "",    "",
      "GTA", "", "GCA", "",    "",    "GGA", "",    "",    "",    "",
      "GAA"};

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH) {
    unsigned int key = hash(str, len);

    if (key <= MAX_HASH_VALUE) {
      const char *s = wordlist[key];

      if (*str == *s && !strcmp(str + 1, s + 1))
        return s;
    }
  }
  return 0;
}

static char sequence_map_lookup_table[MAX_HASH_VALUE];

inline void SequenceMapScalarFun(DataChunk &args, ExpressionState &state,
                                 Vector &result) {

  auto &name_vector = args.data[0];
  UnaryExecutor::Execute<string_t, string_t>(
      name_vector, result, args.size(), [&](string_t in) {
        auto res = StringVector::EmptyString(result, in.GetSize() / 3);
        auto res_ptr = res.GetDataWriteable();
        auto in_ptr = in.GetDataUnsafe();

        for (idx_t i = 0; i < in.GetSize() / 3; i++) {
          res_ptr[i] =
              sequence_map_lookup_table[Perfect_Hash::hash(in_ptr + i * 3, 3)];
        }
        return res;
      });
}

	struct TranslateSequenceData: FunctionData {
        string_t annotation_name;
        uint64_t start_index_one_based;
        int64_t end_index_one_based;

	unique_ptr<FunctionData> Copy() const override {
	    auto res = make_uniq<TranslateSequenceData>();
	    res->annotation_name = annotation_name;
	    res->start_index_one_based = start_index_one_based;
	    res->end_index_one_based = end_index_one_based;
	    return unique_ptr<FunctionData>(std::move(res));
	};
        bool Equals(const FunctionData &other) const {
            return annotation_name == other.Cast<TranslateSequenceData>().annotation_name &&
                start_index_one_based == other.Cast<TranslateSequenceData>().start_index_one_based &&
                end_index_one_based==other.Cast<TranslateSequenceData>().end_index_one_based;
        };
        TranslateSequenceData() {
            annotation_name = "";
            start_index_one_based = 1;
            end_index_one_based = 0;
        }
    };

	unique_ptr<FunctionData> TranslateSequenceBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
        auto res = make_uniq<TranslateSequenceData>();
		res->annotation_name = StringValue::Get(ExpressionExecutor::EvaluateScalar(context, *arguments[0]));
		res->start_index_one_based = UIntegerValue::Get(ExpressionExecutor::EvaluateScalar(context, *arguments[1]));
		res->end_index_one_based = IntegerValue::Get(ExpressionExecutor::EvaluateScalar(context, *arguments[2]));
        Function::EraseArgument(bound_function, arguments, 0);
		Function::EraseArgument(bound_function, arguments, 0);
		Function::EraseArgument(bound_function, arguments, 0);
        return std::move(res);
    }

inline void TranslateSequenceScalarFun(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &annotation_string_vector = args.data[0];
  auto &sequence_vector = args.data[1];
  BinaryExecutor::Execute<string_t, string_t, string_t>(
      annotation_string_vector, sequence_vector, result, args.size(), [&](string_t annotation_string, string_t sequence) {
        auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<TranslateSequenceData>();

		auto res_size = info.end_index_one_based == -1 ? sequence.GetSize() / 3 - info.start_index_one_based + 1 : info.end_index_one_based - info.start_index_one_based;
		auto res = StringVector::EmptyString(result, res_size);
        auto res_ptr = res.GetDataWriteable();
        auto sequence_ptr = sequence.GetDataUnsafe();

        for (idx_t i = 0; i < res_size; i++) {
          res_ptr[i] =
              sequence_map_lookup_table[Perfect_Hash::hash(sequence_ptr + (i + info.start_index_one_based - 1) * 3, 3)];
        }

        return res;
      });
}


static void LoadInternal(DatabaseInstance &instance) {
  // Register a scalar function

  static vector<pair<string, char>> entries = {
      {"TTT", 'F'}, {"TTC", 'F'}, {"TTA", 'L'}, {"TTG", 'L'}, {"TCT", 'S'},
      {"TCC", 'S'}, {"TCA", 'S'}, {"TCG", 'S'}, {"TAT", 'Y'}, {"TAC", 'Y'},
      {"TGT", 'C'}, {"TGC", 'C'}, {"TGG", 'W'}, {"CTT", 'L'}, {"CTC", 'L'},
      {"CTA", 'L'}, {"CTG", 'L'}, {"CCT", 'P'}, {"CCC", 'P'}, {"CCA", 'P'},
      {"CCG", 'P'}, {"CAT", 'H'}, {"CAC", 'H'}, {"CAA", 'Q'}, {"CAG", 'Q'},
      {"CGT", 'R'}, {"CGC", 'R'}, {"CGA", 'R'}, {"CGG", 'R'}, {"ATT", 'I'},
      {"ATC", 'I'}, {"ATA", 'I'}, {"ATG", 'M'}, {"ACT", 'T'}, {"ACC", 'T'},
      {"ACA", 'T'}, {"ACG", 'T'}, {"AAT", 'N'}, {"AAC", 'N'}, {"AAA", 'K'},
      {"AAG", 'K'}, {"AGT", 'S'}, {"AGC", 'S'}, {"AGA", 'R'}, {"AGG", 'R'},
      {"GTT", 'V'}, {"GTC", 'V'}, {"GTA", 'V'}, {"GTG", 'V'}, {"GCT", 'A'},
      {"GCC", 'A'}, {"GCA", 'A'}, {"GCG", 'A'}, {"GAT", 'D'}, {"GAC", 'D'},
      {"GAA", 'E'}, {"GAG", 'E'}, {"GGT", 'G'}, {"GGC", 'G'}, {"GGA", 'G'},
      {"GGG", 'G'}, {"TAG", '*'}, {"TAA", '*'}, {"TGA", '*'}};

  for (auto &entry : entries) {
    auto hash = Perfect_Hash::hash(entry.first.c_str(), entry.first.size());
    D_ASSERT(hash != 0);
    sequence_map_lookup_table[hash] = entry.second;
  }

  auto sequence_map_scalar_function =
      ScalarFunction("sequence_map", {LogicalType::VARCHAR},
                     LogicalType::VARCHAR, SequenceMapScalarFun);
  ExtensionUtil::RegisterFunction(instance, sequence_map_scalar_function);

  auto translate_sequence_scalar_function =
      ScalarFunction("translate_sequence", {LogicalType::VARCHAR, LogicalType::UINTEGER, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::VARCHAR},
                     LogicalType::VARCHAR, TranslateSequenceScalarFun, TranslateSequenceBind);
  ExtensionUtil::RegisterFunction(instance, translate_sequence_scalar_function);
}

void SequenceMapExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string SequenceMapExtension::Name() { return "sequence_map"; }

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
