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
*    must comply with the GNU Affero General Public License in all respects
*    for all of the code used other than as permitted herein. If you modify
*    file(s) with this exception, you may extend this exception to your
*    version of the file(s), but you are not obligated to do so. If you do not
*    wish to do so, delete this exception statement from your version. If you
*    delete this exception statement from all source files in the program,
*    then also delete it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "embedded_main.h"
#include "service_context_embedded.h"
#include "service_entry_point_embedded.h"

#include "mongo/base/checked_cast.h"
#include "mongo/base/initializer.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/catalog/health_log.h"
#include "mongo/db/catalog/uuid_catalog.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/kill_sessions_local.h"
#include "mongo/db/logical_session_cache_factory_mongod.h"
#include "mongo/db/index_rebuilder.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/op_observer_impl.h"
#include "mongo/db/op_observer_registry.h"
#include "mongo/db/session_catalog.h"
#include "mongo/db/session_killer.h"
#include "mongo/db/ttl.h"
#include "mongo/logger/log_component.h"
#include "mongo/scripting/dbdirectclient_factory.h"
#include "mongo/scripting/engine.h"
#include "mongo/util/background.h"
#include "mongo/util/exit.h"
#include "mongo/util/fast_clock_source_factory.h"
#include "mongo/util/periodic_runner_factory.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

#include <boost/filesystem.hpp>

// remove
//#include "mongo/db/service_context_d.h"
//#include "mongo/db/service_entry_point_mongod.h"

namespace mongo
{
	namespace {
		MONGO_INITIALIZER_GENERAL(ForkServer, ("EndStartupOptionHandling"), ("default"))
			(InitializerContext* context) {
			//mongo::forkServerOrDie();
			return Status::OK();
		}
	}

	using logger::LogComponent;
	using std::endl;

	void embeddedShutdown() {
	}

	int embeddedMain(int argc, char* argv[], char** envp) {
		registerShutdownTask(embeddedShutdown);
		//	registerShutdownTask(shutdownTask);
		//
		//	setupSignalHandlers();
		//

		// Should we do this here?
		srand(static_cast<unsigned>(curTimeMicros64()));
		//

		Status status = mongo::runGlobalInitializers(argc, argv, envp);
		if (!status.isOK()) {
			severe(LogComponent::kControl) << "Failed global initializations: " << status;
			throw std::runtime_error("quick exit placeholder");
			//quickExit(EXIT_FAILURE);
		}
		//
		//startupConfigActions(std::vector<std::string>(argv, argv + argc));
		//cmdline_utils::censorArgvArray(argc, argv);
		//
		/*if (!initializeServerGlobalState())
			throw std::runtime_error("quick exit placeholder");*/
			//quickExit(EXIT_FAILURE);

		//
		//	// Per SERVER-7434, startSignalProcessingThread() must run after any forks
		//	// (initializeServerGlobalState()) and before creation of any other threads.
		//	startSignalProcessingThread();
		//
		//#if defined(_WIN32)
		//	if (ntservice::shouldStartService()) {
		//		ntservice::startService();
		//		// exits directly and so never reaches here either.
		//	}
		//#endif
		//
		//	StartupTest::runTests();
		
		//initAndListen

		Client::initThread("initandlisten");

		// heed todo?
		//initWireSpec();

		// heed todo
		auto serviceContext = checked_cast<ServiceContextMongoEmbedded*>(getGlobalServiceContext());

		serviceContext->setFastClockSource(FastClockSourceFactory::create(Milliseconds(10)));
		/*auto opObserverRegistry = stdx::make_unique<OpObserverRegistry>();
		opObserverRegistry->addObserver(stdx::make_unique<OpObserverImpl>());
		opObserverRegistry->addObserver(stdx::make_unique<UUIDCatalogObserver>());
		serviceContext->setOpObserver(std::move(opObserverRegistry));*/

		DBDirectClientFactory::get(serviceContext).registerImplementation([](OperationContext* opCtx) {
			return std::unique_ptr<DBClientBase>(new DBDirectClient(opCtx));
		});

		/*const repl::ReplSettings& replSettings =
			repl::ReplicationCoordinator::get(serviceContext)->getSettings();*/

		{
			ProcessId pid = ProcessId::getCurrent();
			LogstreamBuilder l = log(LogComponent::kControl);
			l << "MongoDB starting : pid=" << pid << " port=" << serverGlobalParams.port
				<< " dbpath=" << storageGlobalParams.dbpath;
			/*if (replSettings.isMaster())
				l << " master=" << replSettings.isMaster();
			if (replSettings.isSlave())
				l << " slave=" << (int)replSettings.isSlave();*/

			const bool is32bit = sizeof(int*) == 4;
			l << (is32bit ? " 32" : " 64") << "-bit" << endl;
		}

		DEV log(LogComponent::kControl) << "DEBUG build (which is slower)" << endl;

//#if defined(_WIN32)
//		VersionInfoInterface::instance().logTargetMinOS();
//#endif

		//logProcessDetails();

		serviceContext->createLockFile();

		serviceContext->setServiceEntryPoint(
			stdx::make_unique<ServiceEntryPointEmbedded>(serviceContext));

		{
			/*auto tl =
				transport::TransportLayerManager::createWithConfig(&serverGlobalParams, serviceContext);
			auto res = tl->setup();
			if (!res.isOK()) {
				error() << "Failed to set up listener: " << res;
				return EXIT_NET_ERROR;
			}
			serviceContext->setTransportLayer(std::move(tl));*/
		}

		serviceContext->initializeGlobalStorageEngine();

//#ifdef MONGO_CONFIG_WIREDTIGER_ENABLED
//		if (EncryptionHooks::get(serviceContext)->restartRequired()) {
//			exitCleanly(EXIT_CLEAN);
//		}
//#endif

		// Warn if we detect configurations for multiple registered storage engines in the same
		// configuration file/environment.
		if (serverGlobalParams.parsedOpts.hasField("storage")) {
			BSONElement storageElement = serverGlobalParams.parsedOpts.getField("storage");
			invariant(storageElement.isABSONObj());
			BSONObj storageParamsObj = storageElement.Obj();
			BSONObjIterator i = storageParamsObj.begin();
			while (i.more()) {
				BSONElement e = i.next();
				// Ignore if field name under "storage" matches current storage engine.
				if (storageGlobalParams.engine == e.fieldName()) {
					continue;
				}

				// Warn if field name matches non-active registered storage engine.
				if (serviceContext->isRegisteredStorageEngine(e.fieldName())) {
					warning() << "Detected configuration for non-active storage engine "
						<< e.fieldName() << " when current storage engine is "
						<< storageGlobalParams.engine;
				}
			}
		}

		//logMongodStartupWarnings(storageGlobalParams, serverGlobalParams, serviceContext);

		{
			std::stringstream ss;
			ss << endl;
			ss << "*********************************************************************" << endl;
			ss << " ERROR: dbpath (" << storageGlobalParams.dbpath << ") does not exist." << endl;
			ss << " Create this directory or give existing directory in --dbpath." << endl;
			ss << " See http://dochub.mongodb.org/core/startingandstoppingmongo" << endl;
			ss << "*********************************************************************" << endl;
			uassert(50660, ss.str().c_str(), boost::filesystem::exists(storageGlobalParams.dbpath));
		}

		{
			std::stringstream ss;
			ss << "repairpath (" << storageGlobalParams.repairpath << ") does not exist";
			uassert(50661, ss.str().c_str(), boost::filesystem::exists(storageGlobalParams.repairpath));
		}

		//initializeSNMP();

		if (!storageGlobalParams.readOnly) {
			boost::filesystem::remove_all(storageGlobalParams.dbpath + "/_tmp/");
		}

		/*if (mmapv1GlobalOptions.journalOptions & MMAPV1Options::JournalRecoverOnly)
			return EXIT_NET_ERROR;*/

		if (mongodGlobalParams.scriptingEnabled) {
			ScriptEngine::setup();
		}

		auto startupOpCtx = serviceContext->makeOperationContext(&cc());

		bool canCallFCVSetIfCleanStartup = !storageGlobalParams.readOnly &&
			!(/*replSettings.isSlave() ||*/ storageGlobalParams.engine == "devnull");
		if (canCallFCVSetIfCleanStartup /*&& !replSettings.usingReplSets()*/) {
			Lock::GlobalWrite lk(startupOpCtx.get());
			FeatureCompatibilityVersion::setIfCleanStartup(startupOpCtx.get(),
				repl::StorageInterface::get(serviceContext));
		}

		// heed todo
		//auto swNonLocalDatabases = repairDatabasesAndCheckVersion(startupOpCtx.get());
		//if (!swNonLocalDatabases.isOK()) {
		//	// SERVER-31611 introduced a return value to `repairDatabasesAndCheckVersion`. Previously,
		//	// a failing condition would fassert. SERVER-31611 covers a case where the binary (3.6) is
		//	// refusing to start up because it refuses acknowledgement of FCV 3.2 and requires the
		//	// user to start up with an older binary. Thus shutting down the server must leave the
		//	// datafiles in a state that the older binary can start up. This requires going through a
		//	// clean shutdown.
		//	//
		//	// The invariant is *not* a statement that `repairDatabasesAndCheckVersion` must return
		//	// `MustDowngrade`. Instead, it is meant as a guardrail to protect future developers from
		//	// accidentally buying into this behavior. New errors that are returned from the method
		//	// may or may not want to go through a clean shutdown, and they likely won't want the
		//	// program to return an exit code of `EXIT_NEED_DOWNGRADE`.
		//	severe(LogComponent::kControl) << "** IMPORTANT: "
		//		<< swNonLocalDatabases.getStatus().reason();
		//	invariant(swNonLocalDatabases == ErrorCodes::MustDowngrade);
		//	exitCleanly(EXIT_NEED_DOWNGRADE);
		//}

		// Assert that the in-memory featureCompatibilityVersion parameter has been explicitly set. If
		// we are part of a replica set and are started up with no data files, we do not set the
		// featureCompatibilityVersion until a primary is chosen. For this case, we expect the in-memory
		// featureCompatibilityVersion parameter to still be uninitialized until after startup.
		if (canCallFCVSetIfCleanStartup /*&&
			(!replSettings.usingReplSets() || swNonLocalDatabases.getValue())*/) {
			invariant(serverGlobalParams.featureCompatibility.isVersionInitialized());
		}

		if (storageGlobalParams.upgrade) {
			log() << "finished checking dbs";
			exitCleanly(EXIT_CLEAN);
		}

		// Start up health log writer thread.
		HealthLog::get(startupOpCtx.get()).startup();

		/*auto const globalAuthzManager = AuthorizationManager::get(serviceContext);
		uassertStatusOK(globalAuthzManager->initialize(startupOpCtx.get()));*/

		// This is for security on certain platforms (nonce generation)
		srand((unsigned)(curTimeMicros64()) ^ (unsigned(uintptr_t(&startupOpCtx))));

		//if (globalAuthzManager->shouldValidateAuthSchemaOnStartup()) {
		//	Status status = verifySystemIndexes(startupOpCtx.get());
		//	if (!status.isOK()) {
		//		log() << redact(status);
		//		if (status == ErrorCodes::AuthSchemaIncompatible) {
		//			exitCleanly(EXIT_NEED_UPGRADE);
		//		}
		//		else {
		//			quickExit(EXIT_FAILURE);
		//		}
		//	}

		//	// SERVER-14090: Verify that auth schema version is schemaVersion26Final.
		//	int foundSchemaVersion;
		//	status =
		//		globalAuthzManager->getAuthorizationVersion(startupOpCtx.get(), &foundSchemaVersion);
		//	if (!status.isOK()) {
		//		log() << "Auth schema version is incompatible: "
		//			<< "User and role management commands require auth data to have "
		//			<< "at least schema version " << AuthorizationManager::schemaVersion26Final
		//			<< " but startup could not verify schema version: " << status;
		//		exitCleanly(EXIT_NEED_UPGRADE);
		//	}

		//	if (foundSchemaVersion <= AuthorizationManager::schemaVersion26Final) {
		//		log() << "This server is using MONGODB-CR, an authentication mechanism which "
		//			<< "has been removed from MongoDB 3.8. In order to upgrade the auth schema, "
		//			<< "first downgrade MongoDB binaries to version 3.6 and then run the "
		//			<< "authSchemaUpgrade command. "
		//			<< "See http://dochub.mongodb.org/core/3.0-upgrade-to-scram-sha-1";
		//		exitCleanly(EXIT_NEED_UPGRADE);
		//	}
		//}
		//else if (globalAuthzManager->isAuthEnabled()) {
		//	error() << "Auth must be disabled when starting without auth schema validation";
		//	exitCleanly(EXIT_BADOPTIONS);
		//}
		//else {
		//	// If authSchemaValidation is disabled and server is running without auth,
		//	// warn the user and continue startup without authSchema metadata checks.
		//	log() << startupWarningsLog;
		//	log() << "** WARNING: Startup auth schema validation checks are disabled for the "
		//		"database."
		//		<< startupWarningsLog;
		//	log() << "**          This mode should only be used to manually repair corrupted auth "
		//		"data."
		//		<< startupWarningsLog;
		//}

		SessionCatalog::create(serviceContext);

		// This function may take the global lock.
		/*auto shardingInitialized =
			uassertStatusOK(ShardingState::get(startupOpCtx.get())
				->initializeShardingAwarenessIfNeeded(startupOpCtx.get()));
		if (shardingInitialized) {
			waitForShardRegistryReload(startupOpCtx.get()).transitional_ignore();
		}*/

		if (!storageGlobalParams.readOnly) {
			
			// heed todo
			//logStartup(startupOpCtx.get());

			//startMongoDFTDC();

			restartInProgressIndexesFromLastShutdown(startupOpCtx.get());

			//if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
			//	// Note: For replica sets, ShardingStateRecovery happens on transition to primary.
			//	if (!repl::getGlobalReplicationCoordinator()->isReplEnabled()) {
			//		uassertStatusOK(ShardingStateRecovery::recover(startupOpCtx.get()));
			//	}
			//}
			//else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
			//	ShardedConnectionInfo::addHook(startupOpCtx->getServiceContext());

			//	uassertStatusOK(
			//		initializeGlobalShardingStateForMongod(startupOpCtx.get(),
			//			ConnectionString::forLocal(),
			//			kDistLockProcessIdForConfigServer));

			//	Balancer::create(startupOpCtx->getServiceContext());

			//	ShardingCatalogManager::create(
			//		startupOpCtx->getServiceContext(),
			//		makeShardingTaskExecutor(executor::makeNetworkInterface("AddShard-TaskExecutor")));
			//}
			//else if (replSettings.usingReplSets()) {  // standalone replica set
			//	auto keysCollectionClient = stdx::make_unique<KeysCollectionClientDirect>();
			//	auto keyManager = std::make_shared<KeysCollectionManager>(
			//		KeysCollectionManager::kKeyManagerPurposeString,
			//		std::move(keysCollectionClient),
			//		Seconds(KeysRotationIntervalSec));
			//	keyManager->startMonitoring(startupOpCtx->getServiceContext());

			//	LogicalTimeValidator::set(startupOpCtx->getServiceContext(),
			//		stdx::make_unique<LogicalTimeValidator>(keyManager));
			//}

			/*repl::ReplicationCoordinator::get(startupOpCtx.get())->startup(startupOpCtx.get());
			const unsigned long long missingRepl =
				checkIfReplMissingFromCommandLine(startupOpCtx.get());
			if (missingRepl) {
				log() << startupWarningsLog;
				log() << "** WARNING: mongod started without --replSet yet " << missingRepl
					<< " documents are present in local.system.replset" << startupWarningsLog;
				log() << "**          Restart with --replSet unless you are doing maintenance and "
					<< " no other clients are connected." << startupWarningsLog;
				log() << "**          The TTL collection monitor will not start because of this."
					<< startupWarningsLog;
				log() << "**         ";
				log() << " For more info see http://dochub.mongodb.org/core/ttlcollections";
				log() << startupWarningsLog;
			}
			else*/ {
				//startTTLBackgroundJob();
			}

			/*if (replSettings.usingReplSets() || (!replSettings.isMaster() && replSettings.isSlave()) ||
				!internalValidateFeaturesAsMaster) {
				serverGlobalParams.validateFeaturesAsMaster.store(false);
			}*/
		}

		//startClientCursorMonitor();

		//PeriodicTask::startRunningPeriodicTasks();

		// Set up the periodic runner for background job execution
		/*auto runner = makePeriodicRunner();
		runner->startup().transitional_ignore();
		serviceContext->setPeriodicRunner(std::move(runner));*/

		/*SessionKiller::set(serviceContext,
			std::make_shared<SessionKiller>(serviceContext, killSessionsLocal));*/

		// Set up the logical session cache
		//LogicalSessionCacheServer kind = LogicalSessionCacheServer::kStandalone;
		/*if (serverGlobalParams.clusterRole == ClusterRole::ShardServer) {
			kind = LogicalSessionCacheServer::kSharded;
		}
		else if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
			kind = LogicalSessionCacheServer::kConfigServer;
		}
		else if (replSettings.usingReplSets()) {
			kind = LogicalSessionCacheServer::kReplicaSet;
		}*/

		/*auto sessionCache = makeLogicalSessionCacheD(serviceContext, kind);
		LogicalSessionCache::set(serviceContext, std::move(sessionCache));*/

		// MessageServer::run will return when exit code closes its socket and we don't need the
		// operation context anymore
		startupOpCtx.reset();

		/*auto start = serviceContext->getServiceExecutor()->start();
		if (!start.isOK()) {
			error() << "Failed to start the service executor: " << start;
			return EXIT_NET_ERROR;
		}

		start = serviceContext->getTransportLayer()->start();
		if (!start.isOK()) {
			error() << "Failed to start the listener: " << start.toString();
			return EXIT_NET_ERROR;
		}*/

		Client::releaseCurrent();

		serviceContext->notifyStartupComplete();

//#ifndef _WIN32
//		mongo::signalForkSuccess();
//#else
//		if (ntservice::shouldStartService()) {
//			ntservice::reportStatus(SERVICE_RUNNING);
//			log() << "Service running";
//		}
//#endif

		/*if (MONGO_FAIL_POINT(shutdownAtStartup)) {
			log() << "starting clean exit via failpoint";
			exitCleanly(EXIT_CLEAN);
		}

		MONGO_IDLE_THREAD_BLOCK;
		return waitForShutdown();*/

		return 0;
	}
}