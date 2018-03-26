/*    Copyright 2018 MongoDB Inc.
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

#include "mongo/db/auth/sasl_command_constants.h"

namespace mongo {

const char* const saslStartCommandName = "saslStart";
const char* const saslContinueCommandName = "saslContinue";
const char* const saslCommandAutoAuthorizeFieldName = "autoAuthorize";
const char* const saslCommandConversationIdFieldName = "conversationId";
const char* const saslCommandDoneFieldName = "done";
const char* const saslCommandMechanismFieldName = "mechanism";
const char* const saslCommandMechanismListFieldName = "supportedMechanisms";
const char* const saslCommandPasswordFieldName = "pwd";
const char* const saslCommandPayloadFieldName = "payload";
const char* const saslCommandUserDBFieldName = "db";
const char* const saslCommandUserFieldName = "user";
const char* const saslCommandServiceHostnameFieldName = "serviceHostname";
const char* const saslCommandServiceNameFieldName = "serviceName";
const char* const saslCommandDigestPasswordFieldName = "digestPassword";
const char* const saslDefaultDBName = "$external";
const char* const saslDefaultServiceName = "mongodb";

}  // namespace mongo
