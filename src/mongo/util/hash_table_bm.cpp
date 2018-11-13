
/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/util/string_map.h"
#include "mongo/util/unordered_fast_key_table.h"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <memory>
#include <random>
#include <unordered_map>



namespace mongo {
namespace {

	template<typename K>
	struct UnorderedFastKeyTableBasicTraits {
		static K hash(K a) {
			return a;
		}

		static bool equals(K a, K b) {
			return a == b;
		}

		static K toStorage(K s) {
			return s;
		}

		static K toLookup(K s) {
			return s;
		}

		class HashedKey {
		public:
			explicit HashedKey(K key = 0) : _key(key) {}

			HashedKey(K key, uint32_t hash) : _key(key) {
			}

			K key() const {
				return _key;
			}

			uint32_t hash() const {
				return _key;
			}

		private:
			K _key;
		};
	};

	using StdUnorderedInt = std::unordered_map<unsigned int, bool>;
	using StdUnorderedString = std::unordered_map<std::string, bool>;

	using MongoUnorderedFastKeyTableInt = UnorderedFastKeyTable<unsigned int, unsigned int, bool, UnorderedFastKeyTableBasicTraits<unsigned int>>;
	using MongoUnorderedFastKeyTableString = StringMap<bool>;

	template <class T>
	typename std::enable_if<std::is_same<T, StdUnorderedInt>::value, float>::type
		getLoadFactor(const T& container)
	{
		return container.load_factor();
	}

	template <class T>
	typename std::enable_if<std::is_same<T, StdUnorderedString>::value, float>::type
		getLoadFactor(const T& container)
	{
		return container.load_factor();
	}

	template <class T>
	typename std::enable_if<std::is_same<T, MongoUnorderedFastKeyTableInt>::value, float>::type
		getLoadFactor(const T& container)
	{
		return container.empty() ? 0.0f : (float)container.size() / container.capacity();
	}

	template <class T>
	typename std::enable_if<std::is_same<T, MongoUnorderedFastKeyTableString>::value, float>::type
		getLoadFactor(const T& container)
	{
		return container.empty() ? 0.0f : (float)container.size() / container.capacity();
	}


	constexpr unsigned int default_seed = 34862;
	constexpr unsigned int other_seed = 76453;

	struct BaseGenerator
	{
		StringData generateStringData(unsigned int i)
		{
			if (!_mem.get())
			{
				_mem = std::unique_ptr<char[]>(new char[1000000000]);
				_current = _mem.get();
			}
			StringData sd(itoa(i, _current, 10));
			_current += sd.size();
			return sd;
		}

		std::unique_ptr<char[]> _mem{ nullptr };
		char* _current{ nullptr };
	};

	struct Sequence : private BaseGenerator
	{
		template <typename K>
		K generate();

		template <>
		unsigned int generate<unsigned int>()
		{
			return ++_state;
		}

		template <>
		StringData generate<StringData>()
		{
			return generateStringData(generate<unsigned int>());
		}

		template <>
		std::string generate<std::string>()
		{
			return generate<StringData>().toString();
		}

		unsigned int _state{ 0 };
	};

	template <unsigned int Seed>
	struct UniformDistribution : private BaseGenerator
	{
		UniformDistribution()
			: _gen(Seed) {}

		template <typename K>
		K generate();

		template <>
		unsigned int generate<unsigned int>()
		{
			return _dist(_gen);
		}

		template <>
		StringData generate<StringData>()
		{
			return generateStringData(generate<unsigned int>());
		}

		template <>
		std::string generate<std::string>()
		{
			return generate<StringData>().toString();
		}

		std::uniform_int_distribution<unsigned int> _dist;
		std::mt19937 _gen;
	};

template <class Container, class K, class StorageGenerator, class LookupGenerator>
void LookupTest(benchmark::State& state) {
	Container container;
	StorageGenerator storage_gen;

	const int num = state.range(0) + 1;
	for (int i = num-1; i; --i)
	{
		container[storage_gen.generate<K>()];
	}

	std::vector<K> lookup_keys;
	LookupGenerator lookup_gen;
	for (int i = num; i; --i)
	{
		lookup_keys.push_back(lookup_gen.generate<K>());
	}
	std::shuffle(lookup_keys.begin(), lookup_keys.end(), std::default_random_engine(default_seed+other_seed));
	
	int i = 0;
	for (auto _ : state) {
		benchmark::ClobberMemory();
		benchmark::DoNotOptimize(container.find(lookup_keys[i++]));
		if (i == num)
		{
			i = 0;
		}
	}

	state.counters["size"] = state.range(0);
	state.counters["lf"] = getLoadFactor(container);
}

template <class Container, typename K, class StorageGenerator>
void InsertTest(benchmark::State& state) {
	std::vector<K> insert_keys;
	StorageGenerator storage_gen;

	const int num = state.range(0);
	for (int i = num; i; --i)
	{
		insert_keys.push_back(storage_gen.generate<K>());
	}
	std::shuffle(insert_keys.begin(), insert_keys.end(), std::default_random_engine(default_seed + other_seed));

	int i = 0;
	Container container;
	for (auto _ : state) {
		benchmark::ClobberMemory();
		benchmark::DoNotOptimize(container[insert_keys[i++]]);
		if (i == num)
		{
			i = 0;

			state.PauseTiming();
			{
				Container swap_container;
				std::swap(container, swap_container);
			}
			state.ResumeTiming();
		}
	}

	state.counters["size"] = state.range(0);
}

template <class Container>
void BM_SuccessfulLookup(benchmark::State& state) {
	LookupTest<Container, Container::key_type, UniformDistribution<default_seed>, UniformDistribution<default_seed>>(state);
}

template <class Container>
void BM_UnsuccessfulLookup(benchmark::State& state) {
	LookupTest<Container, Container::key_type, UniformDistribution<default_seed>, UniformDistribution<other_seed>>(state);
}

template <class Container>
void BM_UnsuccessfulLookupSeq(benchmark::State& state) {
	LookupTest<Container, Container::key_type, Sequence, UniformDistribution<default_seed>>(state);
}

template <class Container>
void BM_Insert(benchmark::State& state) {
	InsertTest<Container, Container::key_type, UniformDistribution<default_seed>>(state);
}

template <unsigned int Start = 0>
static void Range(benchmark::internal::Benchmark* b) {
	unsigned int n0 = Start, n1 = 1000000;
	double       fdn = 0.02;
	for (unsigned int n = n0; n <= n1; n += std::max(1u, (unsigned int)(n*fdn))) {
		b->Arg(n);
	}
}

BENCHMARK_TEMPLATE(BM_SuccessfulLookup, StdUnorderedInt)->Apply(Range);
BENCHMARK_TEMPLATE(BM_SuccessfulLookup, MongoUnorderedFastKeyTableInt)->Apply(Range);

BENCHMARK_TEMPLATE(BM_UnsuccessfulLookup, StdUnorderedInt)->Apply(Range);
BENCHMARK_TEMPLATE(BM_UnsuccessfulLookup, MongoUnorderedFastKeyTableInt)->Apply(Range);

BENCHMARK_TEMPLATE(BM_UnsuccessfulLookupSeq, StdUnorderedInt)->Apply(Range);
BENCHMARK_TEMPLATE(BM_UnsuccessfulLookupSeq, MongoUnorderedFastKeyTableInt)->Apply(Range);

BENCHMARK_TEMPLATE(BM_Insert, StdUnorderedInt)->Apply(Range<1>);
BENCHMARK_TEMPLATE(BM_Insert, MongoUnorderedFastKeyTableInt)->Apply(Range<1>);

BENCHMARK_TEMPLATE(BM_SuccessfulLookup, StdUnorderedString)->Apply(Range);
BENCHMARK_TEMPLATE(BM_SuccessfulLookup, MongoUnorderedFastKeyTableString)->Apply(Range);

BENCHMARK_TEMPLATE(BM_UnsuccessfulLookup, StdUnorderedString)->Apply(Range);
BENCHMARK_TEMPLATE(BM_UnsuccessfulLookup, MongoUnorderedFastKeyTableString)->Apply(Range);

BENCHMARK_TEMPLATE(BM_UnsuccessfulLookupSeq, StdUnorderedString)->Apply(Range);
BENCHMARK_TEMPLATE(BM_UnsuccessfulLookupSeq, MongoUnorderedFastKeyTableString)->Apply(Range);

BENCHMARK_TEMPLATE(BM_Insert, StdUnorderedString)->Apply(Range<1>);
BENCHMARK_TEMPLATE(BM_Insert, MongoUnorderedFastKeyTableString)->Apply(Range<1>);

}  // namespace
}  // namespace mongo
