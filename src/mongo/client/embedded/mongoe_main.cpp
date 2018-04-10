/**
*    Copyright (C) 2018 MongoDB Inc.
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

#include "mongo/platform/basic.h"

//#include "mongo/db/dbmain.h"
#include "mongo/client/embedded/embedded.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/transport/transport_layer_manager.h"

#include "mongo/util/exit.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/text.h"

#include "mongo/db/commands.h"

namespace mongo {
	class CmdWhatsMyUri : public BasicCommand {
	public:
		CmdWhatsMyUri() : BasicCommand("whatsmyuri") {}
		AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
			return AllowedOnSecondary::kAlways;
		}
		virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
			return false;
		}
		std::string help() const override {
			return "{whatsmyuri:1}";
		}
		virtual void addRequiredPrivileges(const std::string& dbname,
			const BSONObj& cmdObj,
			std::vector<Privilege>* out) const {}  // No auth required
		virtual bool run(OperationContext* opCtx,
			const std::string& dbname,
			const BSONObj& cmdObj,
			BSONObjBuilder& result) {
			result << "you" << opCtx->getClient()->clientAddress(true /*includePort*/);
			return true;
		}
	} cmdWhatsMyUri;

	int mongoeMain(int argc, char* argv[], char** envp)
	{
		ServiceContext* serviceContext = nullptr;

		registerShutdownTask([&]() {
			if (auto tl = serviceContext->getTransportLayer()) {
				//log(LogComponent::kNetwork) << "shutdown: going to close listening sockets...";
				tl->shutdown();
			}

			embedded::shutdown(serviceContext);
		});

		setupSignalHandlers();

		serviceContext = embedded::initialize(argc, argv, envp);

		startSignalProcessingThread();

		{
			auto tl =
				transport::TransportLayerManager::createWithConfig(&serverGlobalParams, serviceContext);
			auto res = tl->setup();
			if (!res.isOK()) {
				//error() << "Failed to set up listener: " << res;
				//return EXIT_NET_ERROR;
				return 1;
			}
			serviceContext->setTransportLayer(std::move(tl));
		}

		auto start = serviceContext->getServiceExecutor()->start();
		if (!start.isOK()) {
			//error() << "Failed to start the service executor: " << start;
			//return EXIT_NET_ERROR;
			return 1;
		}

		start = serviceContext->getTransportLayer()->start();
		if (!start.isOK()) {
			//error() << "Failed to start the listener: " << start.toString();
			//return EXIT_NET_ERROR;
			return 1;
		}

		//stdx::this_thread::sleep_for(std::chrono::seconds(1000));
		waitForShutdown();

		
		return 0;
	}
}  //namespace mongo

#if defined(_WIN32)
// In Windows, wmain() is an alternate entry point for main(), and receives the same parameters
// as main() but encoded in Windows Unicode (UTF-16); "wide" 16-bit wchar_t characters.  The
// WindowsCommandLine object converts these wide character strings to a UTF-8 coded equivalent
// and makes them available through the argv() and envp() members.  This enables mongoDbMain()
// to process UTF-8 encoded arguments and environment variables without regard to platform.
int wmain(int argc, wchar_t* argvW[], wchar_t* envpW[]) {
	mongo::WindowsCommandLine wcl(argc, argvW, envpW);
	int exitCode = mongo::mongoeMain(argc, wcl.argv(), wcl.envp());
	//mongo::quickExit(exitCode);

	return exitCode;
}
#else
int main(int argc, char* argv[], char** envp) {
	int exitCode = mongo::mongoeMain(argc, argv, envp);
	//mongo::quickExit(exitCode);

	return exitCode;
}
#endif
