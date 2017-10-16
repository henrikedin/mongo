/**
 *    Copyright (C) 2017 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#pragma once

#include <type_traits>

namespace mongo {
/**
 * The `mongo::relops` namespace provides a simple mechanism for imbuing a type with relational
 * operators.  Because the full set of operators for either equality or ordering can be cumbersome
 * to write, developers tend to skirt corners either through incomplete implementations or through
 * opting to avoid writing all forms of comparison.  Further, writing comparison operators is rather
 * error-prone as the salient (value-contributing) members must be listed multiple times in each
 * operator and must be listed in at least two defined operators when implementing the full set.
 * This leads to bugs, as maintaining the set of salient members in concert between the operators
 * and with the general class definition can lead to situations where these lists are not kept
 * in synchronization.
 *
 * The `std::rel_ops` namespace does not help matters here, as the ADL usage of these members
 * is impossible in certain contexts, which arise in templates.  This makes types which "borrow"
 * their operators from `std::rel_ops` impossible to use correctly with STL algorithms and
 * classes.
 *
 * A solution to this problem would be to provide a mechanism by which a single canonical listing
 * of salient members could be used to derive a complete set of relational operators for any
 * given type.  Ideally this should avoid the use of CRTP (as repeating a class name is
 * painfully redundant), and the full set of operators should use the reference listing of
 * salient members.  In C++14 it is trivial to write a function which returns such a listing,
 * and it is possible to generate the operators from this list.
 *
 * The theory behind this technique is to provide the developer a mechanism by which he or she
 * can define a new type which can be constructed from the type in question whose equality
 * and comparison operators are homomorphic to the equality and comparison relations of the
 * original type in question.  In other words, if the developer can specify a mapping from
 * every possible value for his or her type to a distinct value in another type which maintains
 * the same equality and comparison relationship, then the developer need really only write that
 * mapping.
 *
 * C++'s standard library already comes with the last equality and comparison operators that
 * ever needs to be written: the ones on `std::tuple< ... >`.  These operators order tuples
 * and equate tuples and are probably the most thoroughly tested implementations of equality
 * and comparison around. One merely need to create a mapping from his or her type into into
 * a tuple which is homomorphic under equality and comparison.
 *
 * We call the type which is homoporphic under these relations a "lens" of the type.  It is a
 * view of the original type which is specially created to have the desired homomorphism.  A
 * C++ standard library function exists to create lightweight tuple references: `std::tie`.
 * Combine this with C++14's return type deduction, and it is now trivial and painless to write
 * a lens factory function.
 *
 * Usage Example:
 * --------------
 *
 * namespace MyCode
 * {
 *     class MyDate : mongo::relops::equality::hook {
 *
 *         public:
 *             explicit MyDate( int y, int m, int d ) : month( m ), day( d ), year( y ) {}
 *
 *             inline auto make_equality_lens() const { return std::tie( year, month, day ); }
 *         private:
 *             int month;
 *             int day;
 *             int year;
 *     };
 * }
 *
 * Technical Details... Why it works
 * =================================
 *
 * The `mongo::relops::equality` namespace has operator overloads for both `==` and `!=`, which
 * will be ADL resolved on any class in that namespace and any class derived from a class in that
 * namespace.  `mongo::relops::equality::hook` is a class defined in the `mongo::relops::equality`
 * namespace.  Any class derived from it will be able to any functions in the namespace in
 * any ADL situation.  The equality operators in the `mongo::relops::equality` namespace are fully
 * generic -- they will accept any type as a parameter.  The `!=` operator in that namespace is
 * defined as the negation of the `==` operator.  The `==` operator in that namespace is defined
 * as the invocation of `operator ==` on the type returned by the `make_equality_lens` aspect
 * function.  Whatever `make_equality_lens( lhs )` returns is compared, using `operator ==` to
 * whatever `make_equality_lens( rhs )` returns.  As both `lhs` and `rhs` are required to be
 * the same type, their equality lenses will also be the same type.
 *
 * `make_equality_lens` may also be defined as a member function or as a free-function,
 * allowing the developer to decide which form makes more sense for his or her type.
 *
 * A similar situation holds for `mongo::relops::order`.  An order lens is used for implementing
 * `<`, `>`, `<=`, and `>=`.
 *
 * The hook need not be a public base class -- private base classes still participate in ADL
 * lookup.  The `mongo::relops::hook` type inherits from both `mongo::relops::equality::hook` and
 * `mongo::relops::order::hook`.  This hook can be used to provide both operations.  A
 * `make_strict_weak_order_lens` and `make_equality_lens` will be generated for any type using
 * `mongo::relops::hook` as a base -- the `make_salient_lens` need only be provided, and that
 * lens will be used for both equality and strict weak order.  Alternatively, the developer
 * could provide the implementations of the specific lenses, if that makes more sense for
 * the type in question.
 *
 * Testing Relational Operators
 * ============================
 *
 * An interesting side benefit of this technique is that a type which uses it will always have
 * "mathematically" correct relational operators, in the sense that all required algebraic
 * properties
 * for a specific operator will hold -- transitive, commutative, symmetric, etc.  The operators
 * being "created" are actually (in the recommended use style) the `std::tuple` operators, which
 * provide an ordering of the elements which is "lexicographic" in the sense that the "first"
 * element's ordering dominates over the later elements' ordering.  It is not really worth testing
 * whether each element of the tuple lens actually "contributes" to value and order, as that is
 * already tested as part of the testing of `std::tuple`'s relational operators.
 *
 * The major remaining factor that could be tested is the fact that the tuple lens type accurately
 * reflects the canonical list of salient members of the original type.  Should such a list exist,
 * independent of the listing of salient members in the lens factory, that listing risks maintenance
 * drift with respect to the listing in the lens factory.  We recommend that you treat the lens
 * factory's list as the actual canonical list of salient members.  Armed with such a list, one
 * could use it to draft a set of tests in a test driver which compare the list (tuple) of values
 * returned by the lens factories to the desired list of values.  Were this to exist in the test
 * driver, this could consititute another copy to maintain in concert with the lens functions.
 * And all such a test would really be testing is whether one actually kept both lists in synchrony.
 * To that end, it is best to look at the choice of salient members as a design problem, and since a
 * test driver is not very good at testing designs, it's best to just treat the list(s) in the
 * lens factory(ies) as the documentation of the canonical list(s) of salient values.
 */
namespace relops {
namespace relops_detail {
template <typename T>
bool eq(const T& lhs, const T& rhs) {
    return lhs == rhs;
}

template <typename T>
bool lt(const T& lhs, const T& rhs) {
    return lhs < rhs;
}
}  // namespace relops_detail

namespace equality {
template <typename T>
auto make_equality_lens(const T& t) {
    return t.make_equality_lens();
}

struct hook {
    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator==(
        const T& lhs, const T& rhs) {
        return relops_detail::eq(make_equality_lens(lhs), make_equality_lens(rhs));
    }

    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator!=(
        const T& lhs, const T& rhs) {
        return !(lhs == rhs);
    }
};
}  // namespace equality

namespace order {
template <typename T>
auto make_strict_weak_order_lens(const T& t) {
    return t.make_strict_weak_order_lens();
}

struct hook {
    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator<(
        const T& lhs, const T& rhs) {
        return relops_detail::lt(make_strict_weak_order_lens(lhs),
                                 make_strict_weak_order_lens(rhs));
    }

    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator>(
        const T& lhs, const T& rhs) {
        return rhs < lhs;
    }

    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator<=(
        const T& lhs, const T& rhs) {
        return !(lhs > rhs);
    }

    template <typename T>
    friend typename std::enable_if<std::is_base_of<hook, T>::value, bool>::type operator>=(
        const T& lhs, const T& rhs) {
        return !(lhs < rhs);
    }
};
}  // namespace order
class hook : order::hook, equality::hook {};
}  // namespace relops
}  // namespace mongo
