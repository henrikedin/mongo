// string_map.h

/*    Copyright 2012 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include <third_party/murmurhash3/MurmurHash3.h>

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/unordered_fast_key_table.h"

#include "bytell_hash_map.hpp"

struct StringDataHash
{
	size_t operator()(const mongo::StringData & sd) const
	{
		uint32_t hash;
		MurmurHash3_x86_32(sd.rawData(), sd.size(), 0, &hash);
		return hash;
	}
};

template <typename V>
struct StorageConverter
{
	/*std::string emplace(mongo::StringData key)
	{
		return key.toString();
	}*/

	std::pair<std::string, V> emplace(std::pair<mongo::StringData, V> value)
	{
		return std::pair<std::string, V>(value.first.toString(), std::move(value.second));
	}

	std::pair<std::string, V> emplace(mongo::StringData key, V&& value)
	{
		return std::pair<std::string, V>(key.toString(), std::forward<V>(value));
	}
};

namespace mongo {

	struct StringMapTraits {
		static uint32_t hash(StringData a) {
			uint32_t hash;
			MurmurHash3_x86_32(a.rawData(), a.size(), 0, &hash);
			return hash;
		}

		static bool equals(StringData a, StringData b) {
			return a == b;
		}

		static std::string toStorage(StringData s) {
			return s.toString();
		}

		static StringData toLookup(const std::string& s) {
			return StringData(s);
		}

		class HashedKey {
		public:
			explicit HashedKey(StringData key = "") : _key(key), _hash(StringMapTraits::hash(_key)) {}

			HashedKey(StringData key, uint32_t hash) : _key(key), _hash(hash) {
				// If you claim to know the hash, it better be correct.
				dassert(_hash == StringMapTraits::hash(_key));
			}

			const StringData& key() const {
				return _key;
			}

			uint32_t hash() const {
				return _hash;
			}

		private:
			StringData _key;
			uint32_t _hash;
		};
	};

	//template <typename V>
	//using StringMap = UnorderedFastKeyTable<StringData,   // K_L
	//                                        std::string,  // K_S
	//                                        V,
	//                                        StringMapTraits>;

}  // namespace mongo

template<typename K_L, typename K_S, typename V, typename C, typename H = std::hash<K_L>, typename E = std::equal_to<K_L>, typename A = std::allocator<std::pair<K_S, V> > >
class mongo_bytell_hash_map
	: public ska::detailv8::sherwood_v8_table
	<
	std::pair<K_S, V>,
	K_L,
	C,
	H,
	ska::detailv8::KeyOrValueHasher<K_L, std::pair<K_S, V>, H>,
	E,
	ska::detailv8::KeyOrValueEquality<K_L, std::pair<K_S, V>, E>,
	A,
	typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
	ska::detailv8::CalculateBytellBlockSize<K_S, V>::value
	>
{
	using Table = ska::detailv8::sherwood_v8_table
		<
		std::pair<K_S, V>,
		K_L,
		C,
		H,
		ska::detailv8::KeyOrValueHasher<K_L, std::pair<K_S, V>, H>,
		E,
		ska::detailv8::KeyOrValueEquality<K_L, std::pair<K_S, V>, E>,
		A,
		typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
		ska::detailv8::CalculateBytellBlockSize<K_S, V>::value
		>;
public:

	using key_type = K_L;
	using mapped_type = V;

	using Table::Table;
	mongo_bytell_hash_map()
	{
	}

	inline V & operator[](const key_type & key)
	{
		return emplace(key, convertible_to_value()).first->second;
	}
	inline V & operator[](key_type && key)
	{
		return emplace(std::move(key), convertible_to_value()).first->second;
	}
	V & at(const key_type & key)
	{
		auto found = this->find(key);
		if (found == this->end())
			throw std::out_of_range("Argument passed to at() was not in the map.");
		return found->second;
	}
	const V & at(const key_type & key) const
	{
		auto found = this->find(key);
		if (found == this->end())
			throw std::out_of_range("Argument passed to at() was not in the map.");
		return found->second;
	}

	using Table::emplace;
	std::pair<typename Table::iterator, bool> emplace()
	{
		return emplace(key_type(), convertible_to_value());
	}
	template<typename M>
	std::pair<typename Table::iterator, bool> insert_or_assign(const key_type & key, M && m)
	{
		auto emplace_result = emplace(key, std::forward<M>(m));
		if (!emplace_result.second)
			emplace_result.first->second = std::forward<M>(m);
		return emplace_result;
	}
	template<typename M>
	std::pair<typename Table::iterator, bool> insert_or_assign(key_type && key, M && m)
	{
		auto emplace_result = emplace(std::move(key), std::forward<M>(m));
		if (!emplace_result.second)
			emplace_result.first->second = std::forward<M>(m);
		return emplace_result;
	}
	template<typename M>
	typename Table::iterator insert_or_assign(typename Table::const_iterator, const key_type & key, M && m)
	{
		return insert_or_assign(key, std::forward<M>(m)).first;
	}
	template<typename M>
	typename Table::iterator insert_or_assign(typename Table::const_iterator, key_type && key, M && m)
	{
		return insert_or_assign(std::move(key), std::forward<M>(m)).first;
	}

	friend bool operator==(const mongo_bytell_hash_map & lhs, const mongo_bytell_hash_map & rhs)
	{
		if (lhs.size() != rhs.size())
			return false;
		for (const typename Table::value_type & value : lhs)
		{
			auto found = rhs.find(value.first);
			if (found == rhs.end())
				return false;
			else if (value.second != found->second)
				return false;
		}
		return true;
	}
	friend bool operator!=(const mongo_bytell_hash_map & lhs, const mongo_bytell_hash_map & rhs)
	{
		return !(lhs == rhs);
	}

private:
	struct convertible_to_value
	{
		operator V() const
		{
			return V();
		}
	};
};

namespace mongo {
template <typename V>
using StringMap = UnorderedFastKeyTable<StringData,   // K_L
                                        std::string,  // K_S
                                        V,
                                        StringMapTraits>;

//template <typename V>
//using StringMap = mongo_bytell_hash_map<StringData, std::string, V, StorageConverter<V>, StringDataHash>;

struct A
{
	A() {}
	A(std::string s)
		: a(std::move(s))
	{}

	std::string a;
};

struct B
{
	B() {}
	B(std::string s) 
		: b(std::move(s)) 
	{}
	std::string b;
};

struct AHasher
{
	size_t operator()(const A & a) const
	{
		uint32_t hash;
		MurmurHash3_x86_32(a.a.data(), a.a.size(), 0, &hash);
		return hash;
	}

	size_t operator()(const B & b) const
	{
		uint32_t hash;
		MurmurHash3_x86_32(b.b.data(), b.b.size(), 0, &hash);
		return hash;
	}
};

template<typename V>
struct ABConverter
{
	std::pair<B, V> emplace(std::pair<B, V> value)
	{
		return std::move(value);
	}

	std::pair<B, V> emplace(std::pair<A, V> value)
	{
		return std::pair<B, V>(B(value.first.a), std::move(value.second));
	}

	std::pair<B, V> emplace(A key, V&& value)
	{
		return std::pair<B, V>(B(key.a), std::forward<V>(value));
	}
};

}  // namespace mongo
