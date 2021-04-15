/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::log::LogComponent::kResharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/resharding/resharding_data_replication.h"

#include "mongo/db/repl/oplog_applier.h"
#include "mongo/db/s/resharding/resharding_collection_cloner.h"
#include "mongo/db/s/resharding/resharding_future_util.h"
#include "mongo/db/s/resharding/resharding_metrics.h"
#include "mongo/db/s/resharding/resharding_oplog_applier.h"
#include "mongo/db/s/resharding/resharding_oplog_fetcher.h"
#include "mongo/db/s/resharding/resharding_recipient_service.h"
#include "mongo/db/s/resharding/resharding_server_parameters_gen.h"
#include "mongo/db/s/resharding/resharding_txn_cloner.h"
#include "mongo/db/s/resharding_util.h"
#include "mongo/executor/network_interface_factory.h"
#include "mongo/executor/thread_pool_task_executor.h"
#include "mongo/log/log.h"
#include "mongo/log/redaction.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/future_util.h"

namespace mongo {
namespace {

/**
 * Fulfills the promise if it isn't already fulfilled. Does nothing otherwise.
 *
 * This function is not thread-safe and must not be called concurrently with the promise being
 * fulfilled by another thread.
 */
void ensureFulfilledPromise(SharedPromise<void>& sp) {
    if (!sp.getFuture().isReady()) {
        sp.emplaceValue();
    }
}

/**
 * Fulfills the promise with an error if it isn't already fulfilled. Does nothing otherwise.
 *
 * This function is not thread-safe and must not be called concurrently with the promise being
 * fulfilled by another thread.
 */
void ensureFulfilledPromise(SharedPromise<void>& sp, Status error) {
    if (!sp.getFuture().isReady()) {
        sp.setError(error);
    }
}

}  // namespace

std::unique_ptr<ReshardingCollectionCloner> ReshardingDataReplication::_makeCollectionCloner(
    ReshardingMetrics* metrics,
    const CommonReshardingMetadata& metadata,
    const ShardId& myShardId,
    Timestamp fetchTimestamp) {
    return std::make_unique<ReshardingCollectionCloner>(
        std::make_unique<ReshardingCollectionCloner::Env>(metrics),
        ShardKeyPattern{metadata.getReshardingKey()},
        metadata.getSourceNss(),
        metadata.getSourceUUID(),
        myShardId,
        fetchTimestamp,
        metadata.getTempReshardingNss());
}

std::vector<std::unique_ptr<ReshardingTxnCloner>> ReshardingDataReplication::_makeTxnCloners(
    const CommonReshardingMetadata& metadata,
    const std::vector<ShardId>& donorShardIds,
    Timestamp fetchTimestamp) {
    std::vector<std::unique_ptr<ReshardingTxnCloner>> txnCloners;
    txnCloners.reserve(donorShardIds.size());

    for (const auto& donor : donorShardIds) {
        txnCloners.emplace_back(std::make_unique<ReshardingTxnCloner>(
            ReshardingSourceId(metadata.getReshardingUUID(), donor), fetchTimestamp));
    }

    return txnCloners;
}

std::vector<std::unique_ptr<ReshardingOplogFetcher>> ReshardingDataReplication::_makeOplogFetchers(
    OperationContext* opCtx,
    ReshardingMetrics* metrics,
    const CommonReshardingMetadata& metadata,
    const std::vector<ShardId>& donorShardIds,
    Timestamp fetchTimestamp,
    const ShardId& myShardId) {
    std::vector<std::unique_ptr<ReshardingOplogFetcher>> oplogFetchers;
    oplogFetchers.reserve(donorShardIds.size());

    for (const auto& donor : donorShardIds) {
        auto oplogBufferNss = getLocalOplogBufferNamespace(metadata.getSourceUUID(), donor);
        auto idToResumeFrom =
            resharding::getFetcherIdToResumeFrom(opCtx, oplogBufferNss, fetchTimestamp);
        invariant((idToResumeFrom >= ReshardingDonorOplogId{fetchTimestamp, fetchTimestamp}));

        oplogFetchers.emplace_back(std::make_unique<ReshardingOplogFetcher>(
            std::make_unique<ReshardingOplogFetcher::Env>(opCtx->getServiceContext(), metrics),
            metadata.getReshardingUUID(),
            metadata.getSourceUUID(),
            // The recipient fetches oplog entries from the donor starting from the largest _id
            // value in the oplog buffer. Otherwise, it starts at fetchTimestamp, which corresponds
            // to {clusterTime: fetchTimestamp, ts: fetchTimestamp} as a resume token value.
            std::move(idToResumeFrom),
            donor,
            myShardId,
            std::move(oplogBufferNss)));
    }

    return oplogFetchers;
}

std::shared_ptr<executor::TaskExecutor> ReshardingDataReplication::_makeOplogFetcherExecutor(
    size_t numDonors) {
    ThreadPool::Limits threadPoolLimits;
    threadPoolLimits.maxThreads = numDonors;
    ThreadPool::Options threadPoolOptions(std::move(threadPoolLimits));

    auto prefix = "ReshardingOplogFetcher"_sd;
    threadPoolOptions.threadNamePrefix = prefix + "-";
    threadPoolOptions.poolName = prefix + "ThreadPool";

    auto executor = std::make_shared<executor::ThreadPoolTaskExecutor>(
        std::make_unique<ThreadPool>(std::move(threadPoolOptions)),
        executor::makeNetworkInterface(prefix + "Network"));

    executor->startup();
    return executor;
}

std::vector<std::unique_ptr<ThreadPool>> ReshardingDataReplication::_makeOplogApplierWorkers(
    size_t numDonors) {
    std::vector<std::unique_ptr<ThreadPool>> oplogApplierWorkers;
    oplogApplierWorkers.reserve(numDonors);

    for (size_t i = 0; i < numDonors; ++i) {
        oplogApplierWorkers.emplace_back(
            repl::makeReplWriterPool(resharding::gReshardingWriterThreadCount,
                                     "ReshardingOplogApplierWorker",
                                     true /* isKillableByStepdown */));
    }

    return oplogApplierWorkers;
}

std::vector<std::unique_ptr<ReshardingOplogApplier>> ReshardingDataReplication::_makeOplogAppliers(
    OperationContext* opCtx,
    ReshardingMetrics* metrics,
    CommonReshardingMetadata metadata,
    const std::vector<ShardId>& donorShardIds,
    Timestamp fetchTimestamp,
    ChunkManager sourceChunkMgr,
    std::shared_ptr<executor::TaskExecutor> executor,
    const std::vector<NamespaceString>& stashCollections,
    const std::vector<std::unique_ptr<ReshardingOplogFetcher>>& oplogFetchers,
    const std::vector<std::unique_ptr<ThreadPool>>& oplogApplierWorkers) {
    std::vector<std::unique_ptr<ReshardingOplogApplier>> oplogAppliers;
    oplogAppliers.reserve(donorShardIds.size());

    for (size_t i = 0; i < donorShardIds.size(); ++i) {
        auto sourceId = ReshardingSourceId{metadata.getReshardingUUID(), donorShardIds[i]};
        auto idToResumeFrom = resharding::getApplierIdToResumeFrom(opCtx, sourceId, fetchTimestamp);
        invariant((idToResumeFrom >= ReshardingDonorOplogId{fetchTimestamp, fetchTimestamp}));

        const auto& oplogBufferNss =
            getLocalOplogBufferNamespace(metadata.getSourceUUID(), donorShardIds[i]);

        oplogAppliers.emplace_back(std::make_unique<ReshardingOplogApplier>(
            std::make_unique<ReshardingOplogApplier::Env>(opCtx->getServiceContext(), metrics),
            std::move(sourceId),
            oplogBufferNss,
            metadata.getSourceNss(),
            metadata.getSourceUUID(),
            stashCollections,
            i,
            fetchTimestamp,
            // The recipient applies oplog entries from the donor starting from the progress value
            // in progress_applier. Otherwise, it starts at fetchTimestamp, which corresponds to
            // {clusterTime: fetchTimestamp, ts: fetchTimestamp} as a resume token value.
            std::make_unique<ReshardingDonorOplogIterator>(
                oplogBufferNss, std::move(idToResumeFrom), oplogFetchers[i].get()),
            sourceChunkMgr,
            executor,
            oplogApplierWorkers[i].get()));
    }

    return oplogAppliers;
}

std::unique_ptr<ReshardingDataReplicationInterface> ReshardingDataReplication::make(
    OperationContext* opCtx,
    ReshardingMetrics* metrics,
    CommonReshardingMetadata metadata,
    std::vector<ShardId> donorShardIds,
    Timestamp fetchTimestamp,
    bool cloningDone,
    ShardId myShardId,
    ChunkManager sourceChunkMgr,
    std::shared_ptr<executor::TaskExecutor> executor) {
    std::unique_ptr<ReshardingCollectionCloner> collectionCloner;
    std::vector<std::unique_ptr<ReshardingTxnCloner>> txnCloners;

    if (!cloningDone) {
        collectionCloner = _makeCollectionCloner(metrics, metadata, myShardId, fetchTimestamp);
        txnCloners = _makeTxnCloners(metadata, donorShardIds, fetchTimestamp);
    }

    auto oplogFetchers =
        _makeOplogFetchers(opCtx, metrics, metadata, donorShardIds, fetchTimestamp, myShardId);

    auto oplogFetcherExecutor = _makeOplogFetcherExecutor(donorShardIds.size());
    auto oplogApplierWorkers = _makeOplogApplierWorkers(donorShardIds.size());

    auto stashCollections = resharding::ensureStashCollectionsExist(
        opCtx, sourceChunkMgr, metadata.getSourceUUID(), donorShardIds);

    auto oplogAppliers = _makeOplogAppliers(opCtx,
                                            metrics,
                                            metadata,
                                            donorShardIds,
                                            fetchTimestamp,
                                            std::move(sourceChunkMgr),
                                            std::move(executor),
                                            stashCollections,
                                            oplogFetchers,
                                            oplogApplierWorkers);

    return std::make_unique<ReshardingDataReplication>(std::move(collectionCloner),
                                                       std::move(txnCloners),
                                                       std::move(oplogAppliers),
                                                       std::move(oplogApplierWorkers),
                                                       std::move(oplogFetchers),
                                                       std::move(oplogFetcherExecutor),
                                                       TrustedInitTag{});
}

ReshardingDataReplication::ReshardingDataReplication(
    std::unique_ptr<ReshardingCollectionCloner> collectionCloner,
    std::vector<std::unique_ptr<ReshardingTxnCloner>> txnCloners,
    std::vector<std::unique_ptr<ReshardingOplogApplier>> oplogAppliers,
    std::vector<std::unique_ptr<ThreadPool>> oplogApplierWorkers,
    std::vector<std::unique_ptr<ReshardingOplogFetcher>> oplogFetchers,
    std::shared_ptr<executor::TaskExecutor> oplogFetcherExecutor,
    TrustedInitTag)
    : _collectionCloner{std::move(collectionCloner)},
      _txnCloners{std::move(txnCloners)},
      _oplogAppliers{std::move(oplogAppliers)},
      _oplogApplierWorkers{std::move(oplogApplierWorkers)},
      _oplogFetchers{std::move(oplogFetchers)},
      _oplogFetcherExecutor{std::move(oplogFetcherExecutor)} {}

void ReshardingDataReplication::startOplogApplication() {
    ensureFulfilledPromise(_startOplogApplication);
}

SharedSemiFuture<void> ReshardingDataReplication::awaitCloningDone() {
    return _cloningDone.getFuture();
}

SharedSemiFuture<void> ReshardingDataReplication::awaitConsistentButStale() {
    return _consistentButStale.getFuture();
}

SharedSemiFuture<void> ReshardingDataReplication::awaitStrictlyConsistent() {
    return _strictlyConsistent.getFuture();
}

SemiFuture<void> ReshardingDataReplication::runUntilStrictlyConsistent(
    std::shared_ptr<executor::TaskExecutor> executor,
    std::shared_ptr<executor::TaskExecutor> cleanupExecutor,
    CancellationToken cancelToken,
    CancelableOperationContextFactory opCtxFactory,
    Milliseconds minimumOperationDuration) {
    CancellationSource errorSource(cancelToken);

    auto oplogFetcherFutures = _runOplogFetchers(executor, errorSource.token(), opCtxFactory);

    auto collectionClonerFuture =
        _runCollectionCloner(executor, cleanupExecutor, errorSource.token(), opCtxFactory);

    auto txnClonerFutures = _runTxnCloners(
        executor, cleanupExecutor, errorSource.token(), opCtxFactory, minimumOperationDuration);

    auto fulfillCloningDoneFuture =
        whenAllSucceed(collectionClonerFuture.thenRunOn(executor),
                       resharding::whenAllSucceedOn(txnClonerFutures, executor))
            .thenRunOn(executor)
            .then([this] { _cloningDone.emplaceValue(); })
            .share();

    // Calling _runOplogAppliersUntilConsistentButStale() won't actually immediately start
    // performing oplog application. Only after the _startOplogApplication promise is fulfilled will
    // oplog application begin. This similarly applies to _runOplogAppliersUntilStrictlyConsistent()
    // and the _consistentButStale promise being fulfilled.
    auto oplogApplierConsistentButStaleFutures =
        _runOplogAppliersUntilConsistentButStale(executor, errorSource.token());

    auto fulfillConsistentButStaleFuture =
        resharding::whenAllSucceedOn(oplogApplierConsistentButStaleFutures, executor)
            .then([this] { _consistentButStale.emplaceValue(); })
            .share();

    auto oplogApplierStrictlyConsistentFutures =
        _runOplogAppliersUntilStrictlyConsistent(executor, errorSource.token());

    // We must additionally wait for fulfillCloningDoneFuture and fulfillConsistentButStaleFuture to
    // become ready to ensure their corresponding promises aren't being fulfilled while the
    // .onCompletion() is running.
    std::vector<SharedSemiFuture<void>> allFutures;
    allFutures.reserve(3 + oplogFetcherFutures.size() + txnClonerFutures.size() +
                       oplogApplierConsistentButStaleFutures.size() +
                       oplogApplierStrictlyConsistentFutures.size());

    for (const auto& futureList : {oplogFetcherFutures,
                                   {collectionClonerFuture},
                                   txnClonerFutures,
                                   {fulfillCloningDoneFuture},
                                   oplogApplierConsistentButStaleFutures,
                                   {fulfillConsistentButStaleFuture},
                                   oplogApplierStrictlyConsistentFutures}) {
        for (const auto& future : futureList) {
            allFutures.emplace_back(future);
        }
    }

    return resharding::cancelWhenAnyErrorThenQuiesce(allFutures, executor, errorSource)
        // Fulfilling the _strictlyConsistent promise must be the very last thing in the future
        // chain because RecipientStateMachine, along with its ReshardingDataReplication member,
        // may be destructed immediately afterwards.
        .onCompletion([this](Status status) {
            if (status.isOK()) {
                invariant(_cloningDone.getFuture().isReady());
                invariant(_consistentButStale.getFuture().isReady());
                _strictlyConsistent.emplaceValue();
            } else {
                ensureFulfilledPromise(_cloningDone, status);
                ensureFulfilledPromise(_consistentButStale, status);
                _strictlyConsistent.setError(status);
            }
        })
        .semi();
}

SharedSemiFuture<void> ReshardingDataReplication::_runCollectionCloner(
    std::shared_ptr<executor::TaskExecutor> executor,
    std::shared_ptr<executor::TaskExecutor> cleanupExecutor,
    CancellationToken cancelToken,
    CancelableOperationContextFactory opCtxFactory) {
    return _collectionCloner ? _collectionCloner
                                   ->run(std::move(executor),
                                         std::move(cleanupExecutor),
                                         std::move(cancelToken),
                                         std::move(opCtxFactory))
                                   .share()
                             : makeReadyFutureWith([] {}).share();
}

std::vector<SharedSemiFuture<void>> ReshardingDataReplication::_runTxnCloners(
    std::shared_ptr<executor::TaskExecutor> executor,
    std::shared_ptr<executor::TaskExecutor> cleanupExecutor,
    CancellationToken cancelToken,
    CancelableOperationContextFactory opCtxFactory,
    Milliseconds minimumOperationDuration) {
    std::vector<SharedSemiFuture<void>> txnClonerFutures;
    txnClonerFutures.reserve(_txnCloners.size());

    for (const auto& txnCloner : _txnCloners) {
        txnClonerFutures.emplace_back(
            executor->sleepFor(minimumOperationDuration, cancelToken)
                .then([executor,
                       cleanupExecutor,
                       cancelToken,
                       opCtxFactory,
                       txnCloner = txnCloner.get()] {
                    return txnCloner->run(executor, cleanupExecutor, cancelToken, opCtxFactory);
                })
                .share());
    }

    // ReshardingTxnCloners must complete before the recipient transitions to kApplying to avoid
    // errors caused by donor shards unpinning the fetchTimestamp.
    return txnClonerFutures;
}

std::vector<SharedSemiFuture<void>> ReshardingDataReplication::_runOplogFetchers(
    std::shared_ptr<executor::TaskExecutor> executor,
    CancellationToken cancelToken,
    CancelableOperationContextFactory opCtxFactory) {
    std::vector<SharedSemiFuture<void>> oplogFetcherFutures;
    oplogFetcherFutures.reserve(_oplogFetchers.size());

    for (const auto& fetcher : _oplogFetchers) {
        oplogFetcherFutures.emplace_back(
            fetcher->schedule(_oplogFetcherExecutor, cancelToken, opCtxFactory).share());
    }

    return oplogFetcherFutures;
}

std::vector<SharedSemiFuture<void>>
ReshardingDataReplication::_runOplogAppliersUntilConsistentButStale(
    std::shared_ptr<executor::TaskExecutor> executor, CancellationToken cancelToken) {
    std::vector<SharedSemiFuture<void>> oplogApplierFutures;
    oplogApplierFutures.reserve(_oplogAppliers.size());

    for (const auto& applier : _oplogAppliers) {
        // We must wait for the RecipientStateMachine to transition to kApplying before starting to
        // apply any oplog entries.
        oplogApplierFutures.emplace_back(
            future_util::withCancellation(_startOplogApplication.getFuture(), cancelToken)
                .thenRunOn(executor)
                .then([applier = applier.get(), cancelToken] {
                    return applier->applyUntilCloneFinishedTs(cancelToken);
                })
                .share());
    }

    return oplogApplierFutures;
}

std::vector<SharedSemiFuture<void>>
ReshardingDataReplication::_runOplogAppliersUntilStrictlyConsistent(
    std::shared_ptr<executor::TaskExecutor> executor, CancellationToken cancelToken) {
    std::vector<SharedSemiFuture<void>> oplogApplierFutures;
    oplogApplierFutures.reserve(_oplogAppliers.size());

    for (const auto& applier : _oplogAppliers) {
        // We must wait for applyUntilCloneFinishedTs() to have returned before continuing to apply
        // more oplog entries.
        oplogApplierFutures.emplace_back(
            future_util::withCancellation(_consistentButStale.getFuture(), cancelToken)
                .thenRunOn(executor)
                .then([applier = applier.get(), cancelToken] {
                    return applier->applyUntilDone(cancelToken);
                })
                .share());
    }

    return oplogApplierFutures;
}

void ReshardingDataReplication::shutdown() {
    _oplogFetcherExecutor->shutdown();

    for (const auto& worker : _oplogApplierWorkers) {
        worker->shutdown();
    }
}

}  // namespace mongo
