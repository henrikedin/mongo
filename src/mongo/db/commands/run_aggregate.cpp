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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/db/commands/run_aggregate.h"

#include <boost/optional.hpp>
#include <memory>
#include <vector>

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/curop.h"
#include "mongo/db/cursor_manager.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/accumulator.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_exchange.h"
#include "mongo/db/pipeline/document_source_geo_near.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/pipeline_d.h"
#include "mongo/db/pipeline/plan_executor_pipeline.h"
#include "mongo/db/pipeline/process_interface/mongo_process_interface.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/collection_query_info.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_executor_factory.h"
#include "mongo/db/query/plan_summary_stats.h"
#include "mongo/db/query/query_planner_common.h"
#include "mongo/db/read_concern.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/db/repl/speculative_majority_read_info.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/views/view.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/logv2/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/string_map.h"

namespace mongo {

using boost::intrusive_ptr;
using std::endl;
using std::shared_ptr;
using std::string;
using std::stringstream;
using std::unique_ptr;

namespace {
/**
 * If a pipeline is empty (assuming that a $cursor stage hasn't been created yet), it could mean
 * that we were able to absorb all pipeline stages and pull them into a single PlanExecutor. So,
 * instead of creating a whole pipeline to do nothing more than forward the results of its cursor
 * document source, we can optimize away the entire pipeline and answer the request using the query
 * engine only. This function checks if such optimization is possible.
 */
bool canOptimizeAwayPipeline(const Pipeline* pipeline,
                             const PlanExecutor* exec,
                             const AggregationRequest& request,
                             bool hasGeoNearStage,
                             bool hasChangeStreamStage) {
    return pipeline && exec && !hasGeoNearStage && !hasChangeStreamStage &&
        pipeline->getSources().empty() &&
        // For exchange we will create a number of pipelines consisting of a single
        // DocumentSourceExchange stage, so cannot not optimize it away.
        !request.getExchangeSpec();
}

/**
 * Returns true if we need to keep a ClientCursor saved for this pipeline (for future getMore
 * requests). Otherwise, returns false. The passed 'nsForCursor' is only used to determine the
 * namespace used in the returned cursor, which will be registered with the global cursor manager,
 * and thus will be different from that in 'request'.
 */
bool handleCursorCommand(OperationContext* opCtx,
                         boost::intrusive_ptr<ExpressionContext> expCtx,
                         const NamespaceString& nsForCursor,
                         std::vector<ClientCursor*> cursors,
                         const AggregationRequest& request,
                         const BSONObj& cmdObj,
                         rpc::ReplyBuilderInterface* result) {
    invariant(!cursors.empty());
    long long batchSize = request.getBatchSize();

    if (cursors.size() > 1) {

        uassert(
            ErrorCodes::BadValue, "the exchange initial batch size must be zero", batchSize == 0);

        BSONArrayBuilder cursorsBuilder;
        for (size_t idx = 0; idx < cursors.size(); ++idx) {
            invariant(cursors[idx]);

            BSONObjBuilder cursorResult;
            appendCursorResponseObject(
                cursors[idx]->cursorid(), nsForCursor.ns(), BSONArray(), &cursorResult);
            cursorResult.appendBool("ok", 1);

            cursorsBuilder.append(cursorResult.obj());

            // If a time limit was set on the pipeline, remaining time is "rolled over" to the
            // cursor (for use by future getmore ops).
            cursors[idx]->setLeftoverMaxTimeMicros(opCtx->getRemainingMaxTimeMicros());

            // Cursor needs to be in a saved state while we yield locks for getmore. State
            // will be restored in getMore().
            cursors[idx]->getExecutor()->saveState();
            cursors[idx]->getExecutor()->detachFromOperationContext();
        }

        auto bodyBuilder = result->getBodyBuilder();
        bodyBuilder.appendArray("cursors", cursorsBuilder.obj());

        return true;
    }

    CursorResponseBuilder::Options options;
    options.isInitialResponse = true;
    if (!opCtx->inMultiDocumentTransaction()) {
        options.atClusterTime = repl::ReadConcernArgs::get(opCtx).getArgsAtClusterTime();
    }
    CursorResponseBuilder responseBuilder(result, options);

    auto curOp = CurOp::get(opCtx);
    auto cursor = cursors[0];
    invariant(cursor);
    auto exec = cursor->getExecutor();
    invariant(exec);

    bool stashedResult = false;
    // We are careful to avoid ever calling 'getNext()' on the PlanExecutor when the batchSize is
    // zero to avoid doing any query execution work.
    for (int objCount = 0; objCount < batchSize; objCount++) {
        PlanExecutor::ExecState state;
        BSONObj nextDoc;

        try {
            state = exec->getNext(&nextDoc, nullptr);
        } catch (const ExceptionFor<ErrorCodes::CloseChangeStream>&) {
            // This exception is thrown when a $changeStream stage encounters an event that
            // invalidates the cursor. We should close the cursor and return without error.
            cursor = nullptr;
            exec = nullptr;
            break;
        } catch (DBException& exception) {
            auto&& explainer = exec->getPlanExplainer();
            auto&& [stats, _] =
                explainer.getWinningPlanStats(ExplainOptions::Verbosity::kExecStats);
            LOGV2_WARNING(23799,
                          "Aggregate command executor error: {error}, stats: {stats}, cmd: {cmd}",
                          "Aggregate command executor error",
                          "error"_attr = exception.toStatus(),
                          "stats"_attr = redact(stats),
                          "cmd"_attr = cmdObj);

            exception.addContext("PlanExecutor error during aggregation");
            throw;
        }

        if (state == PlanExecutor::IS_EOF) {
            if (!cursor->isTailable()) {
                // Make it an obvious error to use cursor or executor after this point.
                cursor = nullptr;
                exec = nullptr;
            }
            break;
        }

        invariant(state == PlanExecutor::ADVANCED);

        // If adding this object will cause us to exceed the message size limit, then we stash it
        // for later.

        if (!FindCommon::haveSpaceForNext(nextDoc, objCount, responseBuilder.bytesUsed())) {
            exec->enqueue(nextDoc);
            stashedResult = true;
            break;
        }

        // If this executor produces a postBatchResumeToken, add it to the cursor response.
        responseBuilder.setPostBatchResumeToken(exec->getPostBatchResumeToken());
        responseBuilder.append(nextDoc);
    }

    if (cursor) {
        invariant(cursor->getExecutor() == exec);

        // For empty batches, or in the case where the final result was added to the batch rather
        // than being stashed, we update the PBRT to ensure that it is the most recent available.
        if (!stashedResult) {
            responseBuilder.setPostBatchResumeToken(exec->getPostBatchResumeToken());
        }
        // If a time limit was set on the pipeline, remaining time is "rolled over" to the
        // cursor (for use by future getmore ops).
        cursor->setLeftoverMaxTimeMicros(opCtx->getRemainingMaxTimeMicros());

        curOp->debug().cursorid = cursor->cursorid();

        // Cursor needs to be in a saved state while we yield locks for getmore. State
        // will be restored in getMore().
        exec->saveState();
        exec->detachFromOperationContext();
    } else {
        curOp->debug().cursorExhausted = true;
    }

    const CursorId cursorId = cursor ? cursor->cursorid() : 0LL;
    responseBuilder.done(cursorId, nsForCursor.ns());

    return static_cast<bool>(cursor);
}

StatusWith<StringMap<ExpressionContext::ResolvedNamespace>> resolveInvolvedNamespaces(
    OperationContext* opCtx, const AggregationRequest& request) {
    const LiteParsedPipeline liteParsedPipeline(request);
    const auto& pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();

    // If there are no involved namespaces, return before attempting to take any locks. This is
    // important for collectionless aggregations, which may be expected to run without locking.
    if (pipelineInvolvedNamespaces.empty()) {
        return {StringMap<ExpressionContext::ResolvedNamespace>()};
    }

    // We intentionally do not drop and reacquire our system.views collection lock after resolving
    // the view definition in order to prevent the definition for any view namespaces we've already
    // resolved from changing. This is necessary to prevent a cycle from being formed among the view
    // definitions cached in 'resolvedNamespaces' because we won't re-resolve a view namespace we've
    // already encountered.
    AutoGetCollection autoColl(opCtx,
                               NamespaceString(request.getNamespaceString().db(),
                                               NamespaceString::kSystemDotViewsCollectionName),
                               MODE_IS);
    Database* const db = autoColl.getDb();
    ViewCatalog* viewCatalog = db ? ViewCatalog::get(db) : nullptr;

    std::deque<NamespaceString> involvedNamespacesQueue(pipelineInvolvedNamespaces.begin(),
                                                        pipelineInvolvedNamespaces.end());
    StringMap<ExpressionContext::ResolvedNamespace> resolvedNamespaces;

    while (!involvedNamespacesQueue.empty()) {
        auto involvedNs = std::move(involvedNamespacesQueue.front());
        involvedNamespacesQueue.pop_front();

        if (resolvedNamespaces.find(involvedNs.coll()) != resolvedNamespaces.end()) {
            continue;
        }

        if (involvedNs.db() != request.getNamespaceString().db()) {
            // If the involved namespace is not in the same database as the aggregation, it must be
            // from a $merge to a collection in a different database. Since we cannot write to
            // views, simply assume that the namespace is a collection.
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        } else if (!db ||
                   CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, involvedNs)) {
            // If the aggregation database exists and 'involvedNs' refers to a collection namespace,
            // then we resolve it as an empty pipeline in order to read directly from the underlying
            // collection. If the database doesn't exist, then we still resolve it as an empty
            // pipeline because 'involvedNs' doesn't refer to a view namespace in our consistent
            // snapshot of the view catalog.
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        } else if (viewCatalog->lookup(opCtx, involvedNs.ns())) {
            // If 'involvedNs' refers to a view namespace, then we resolve its definition.
            auto resolvedView = viewCatalog->resolveView(opCtx, involvedNs);
            if (!resolvedView.isOK()) {
                return resolvedView.getStatus().withContext(
                    str::stream() << "Failed to resolve view '" << involvedNs.ns());
            }

            resolvedNamespaces[involvedNs.coll()] = {resolvedView.getValue().getNamespace(),
                                                     resolvedView.getValue().getPipeline()};

            // We parse the pipeline corresponding to the resolved view in case we must resolve
            // other view namespaces that are also involved.
            LiteParsedPipeline resolvedViewLitePipeline(resolvedView.getValue().getNamespace(),
                                                        resolvedView.getValue().getPipeline());

            const auto& resolvedViewInvolvedNamespaces =
                resolvedViewLitePipeline.getInvolvedNamespaces();
            involvedNamespacesQueue.insert(involvedNamespacesQueue.end(),
                                           resolvedViewInvolvedNamespaces.begin(),
                                           resolvedViewInvolvedNamespaces.end());
        } else {
            // 'involvedNs' is neither a view nor a collection, so resolve it as an empty pipeline
            // to treat it as reading from a non-existent collection.
            resolvedNamespaces[involvedNs.coll()] = {involvedNs, std::vector<BSONObj>{}};
        }
    }

    return resolvedNamespaces;
}

/**
 * Returns Status::OK if each view namespace in 'pipeline' has a default collator equivalent to
 * 'collator'. Otherwise, returns ErrorCodes::OptionNotSupportedOnView.
 */
Status collatorCompatibleWithPipeline(OperationContext* opCtx,
                                      StringData dbName,
                                      const CollatorInterface* collator,
                                      const LiteParsedPipeline& liteParsedPipeline) {
    auto viewCatalog = DatabaseHolder::get(opCtx)->getSharedViewCatalog(opCtx, dbName);
    if (!viewCatalog) {
        return Status::OK();
    }
    for (auto&& potentialViewNs : liteParsedPipeline.getInvolvedNamespaces()) {
        if (CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, potentialViewNs)) {
            continue;
        }

        auto view = viewCatalog->lookup(opCtx, potentialViewNs.ns());
        if (!view) {
            continue;
        }
        if (!CollatorInterface::collatorsMatch(view->defaultCollator(), collator)) {
            return {ErrorCodes::OptionNotSupportedOnView,
                    str::stream() << "Cannot override default collation of view "
                                  << potentialViewNs.ns()};
        }
    }
    return Status::OK();
}

// A 4.7+ mongoS issues $mergeCursors pipelines with ChunkVersion::IGNORED. On the shard, this will
// skip the versioning check but also marks the operation as versioned, so the shard knows that any
// sub-operations executed by the merging pipeline should also be versioned. We manually set the
// IGNORED version here if we are running a $mergeCursors pipeline and the operation is not already
// versioned. This can happen in the case where we are running in a cluster with a 4.4 mongoS, which
// does not set any shard version on a $mergeCursors pipeline.
void setIgnoredShardVersionForMergeCursors(OperationContext* opCtx,
                                           const AggregationRequest& request) {
    auto isMergeCursors = request.isFromMongos() && request.getPipeline().size() > 0 &&
        request.getPipeline().front().firstElementFieldNameStringData() == "$mergeCursors"_sd;
    if (isMergeCursors && !OperationShardingState::isOperationVersioned(opCtx)) {
        OperationShardingState::get(opCtx).initializeClientRoutingVersions(
            request.getNamespaceString(), ChunkVersion::IGNORED(), boost::none);
    }
}

boost::intrusive_ptr<ExpressionContext> makeExpressionContext(
    OperationContext* opCtx,
    const AggregationRequest& request,
    std::unique_ptr<CollatorInterface> collator,
    boost::optional<UUID> uuid) {
    setIgnoredShardVersionForMergeCursors(opCtx, request);
    boost::intrusive_ptr<ExpressionContext> expCtx =
        new ExpressionContext(opCtx,
                              request,
                              std::move(collator),
                              MongoProcessInterface::create(opCtx),
                              uassertStatusOK(resolveInvolvedNamespaces(opCtx, request)),
                              uuid,
                              CurOp::get(opCtx)->dbProfileLevel() > 0);
    expCtx->tempDir = storageGlobalParams.dbpath + "/_tmp";
    expCtx->inMultiDocumentTransaction = opCtx->inMultiDocumentTransaction();

    return expCtx;
}

/**
 * Upconverts the read concern for a change stream aggregation, if necesssary.
 *
 * If there is no given read concern level on the given object, upgrades the level to 'majority' and
 * waits for read concern. If a read concern level is already specified on the given read concern
 * object, this method does nothing.
 */
void _adjustChangeStreamReadConcern(OperationContext* opCtx) {
    repl::ReadConcernArgs& readConcernArgs = repl::ReadConcernArgs::get(opCtx);
    // There is already a read concern level set. Do nothing.
    if (readConcernArgs.hasLevel()) {
        return;
    }
    // We upconvert an empty read concern to 'majority'.
    {
        // We must obtain the client lock to set the ReadConcernArgs on the operation
        // context as it may be concurrently read by CurrentOp.
        stdx::lock_guard<Client> lk(*opCtx->getClient());
        readConcernArgs = repl::ReadConcernArgs(repl::ReadConcernLevel::kMajorityReadConcern);

        // Change streams are allowed to use the speculative majority read mechanism, if
        // the storage engine doesn't support majority reads directly.
        if (!serverGlobalParams.enableMajorityReadConcern) {
            readConcernArgs.setMajorityReadMechanism(
                repl::ReadConcernArgs::MajorityReadMechanism::kSpeculative);
        }
    }

    // Wait for read concern again since we changed the original read concern.
    uassertStatusOK(waitForReadConcern(opCtx, readConcernArgs, true));
    setPrepareConflictBehaviorForReadConcern(
        opCtx, readConcernArgs, PrepareConflictBehavior::kIgnoreConflicts);
}

/**
 * If the aggregation 'request' contains an exchange specification, create a new pipeline for each
 * consumer and put it into the resulting vector. Otherwise, return the original 'pipeline' as a
 * single vector element.
 */
std::vector<std::unique_ptr<Pipeline, PipelineDeleter>> createExchangePipelinesIfNeeded(
    OperationContext* opCtx,
    boost::intrusive_ptr<ExpressionContext> expCtx,
    const AggregationRequest& request,
    std::unique_ptr<Pipeline, PipelineDeleter> pipeline,
    boost::optional<UUID> uuid) {
    std::vector<std::unique_ptr<Pipeline, PipelineDeleter>> pipelines;

    if (request.getExchangeSpec() && !expCtx->explain) {
        boost::intrusive_ptr<Exchange> exchange =
            new Exchange(request.getExchangeSpec().get(), std::move(pipeline));

        for (size_t idx = 0; idx < exchange->getConsumers(); ++idx) {
            // For every new pipeline we have create a new ExpressionContext as the context
            // cannot be shared between threads. There is no synchronization for pieces of
            // the execution machinery above the Exchange, so nothing above the Exchange can be
            // shared between different exchange-producer cursors.
            expCtx = makeExpressionContext(opCtx,
                                           request,
                                           expCtx->getCollator() ? expCtx->getCollator()->clone()
                                                                 : nullptr,
                                           uuid);

            // Create a new pipeline for the consumer consisting of a single
            // DocumentSourceExchange.
            boost::intrusive_ptr<DocumentSource> consumer = new DocumentSourceExchange(
                expCtx, exchange, idx, expCtx->mongoProcessInterface->getResourceYielder());
            pipelines.emplace_back(Pipeline::create({consumer}, expCtx));
        }
    } else {
        pipelines.emplace_back(std::move(pipeline));
    }

    return pipelines;
}
}  // namespace

Status runAggregate(OperationContext* opCtx,
                    const NamespaceString& nss,
                    const AggregationRequest& request,
                    const BSONObj& cmdObj,
                    const PrivilegeVector& privileges,
                    rpc::ReplyBuilderInterface* result) {
    return runAggregate(opCtx, nss, request, {request}, cmdObj, privileges, result);
}

Status runAggregate(OperationContext* opCtx,
                    const NamespaceString& origNss,
                    const AggregationRequest& request,
                    const LiteParsedPipeline& liteParsedPipeline,
                    const BSONObj& cmdObj,
                    const PrivilegeVector& privileges,
                    rpc::ReplyBuilderInterface* result) {
    // For operations on views, this will be the underlying namespace.
    NamespaceString nss = request.getNamespaceString();

    // The collation to use for this aggregation. boost::optional to distinguish between the case
    // where the collation has not yet been resolved, and where it has been resolved to nullptr.
    boost::optional<std::unique_ptr<CollatorInterface>> collatorToUse;

    // The UUID of the collection for the execution namespace of this aggregation.
    boost::optional<UUID> uuid;

    // If emplaced, AutoGetCollectionForReadCommand will throw if the sharding version for this
    // connection is out of date. If the namespace is a view, the lock will be released before
    // re-running the expanded aggregation.
    boost::optional<AutoGetCollectionForReadCommandMaybeLockFree> ctx;

    std::vector<unique_ptr<PlanExecutor, PlanExecutor::Deleter>> execs;
    boost::intrusive_ptr<ExpressionContext> expCtx;
    auto curOp = CurOp::get(opCtx);
    {
        // If we are in a transaction, check whether the parsed pipeline supports
        // being in a transaction.
        if (opCtx->inMultiDocumentTransaction()) {
            liteParsedPipeline.assertSupportsMultiDocumentTransaction(request.getExplain());
        }

        const auto& pipelineInvolvedNamespaces = liteParsedPipeline.getInvolvedNamespaces();

        // If this is a collectionless aggregation, we won't create 'ctx' but will still need an
        // AutoStatsTracker to record CurOp and Top entries.
        boost::optional<AutoStatsTracker> statsTracker;

        // If this is a change stream, perform special checks and change the execution namespace.
        if (liteParsedPipeline.hasChangeStream()) {
            uassert(4928900,
                    str::stream() << AggregationRequest::kCollectionUUIDName
                                  << " is not supported for a change stream",
                    !request.getCollectionUUID());

            // Replace the execution namespace with that of the oplog.
            nss = NamespaceString::kRsOplogNamespace;

            // Upgrade and wait for read concern if necessary.
            _adjustChangeStreamReadConcern(opCtx);

            // AutoGetCollectionForReadCommand will raise an error if 'origNss' is a view. We do not
            // need to check this if we are opening a stream on an entire db or across the cluster.
            if (!origNss.isCollectionlessAggregateNS()) {
                AutoGetCollectionForReadCommand origNssCtx(opCtx, origNss);
            }

            // If the user specified an explicit collation, adopt it; otherwise, use the simple
            // collation. We do not inherit the collection's default collation or UUID, since
            // the stream may be resuming from a point before the current UUID existed.
            collatorToUse.emplace(
                PipelineD::resolveCollator(opCtx, request.getCollation(), nullptr));

            // Obtain collection locks on the execution namespace; that is, the oplog.
            ctx.emplace(opCtx, nss, AutoGetCollectionViewMode::kViewsForbidden);
        } else if (nss.isCollectionlessAggregateNS() && pipelineInvolvedNamespaces.empty()) {
            uassert(4928901,
                    str::stream() << AggregationRequest::kCollectionUUIDName
                                  << " is not supported for a collectionless aggregation",
                    !request.getCollectionUUID());

            // If this is a collectionless agg with no foreign namespaces, don't acquire any locks.
            statsTracker.emplace(opCtx,
                                 nss,
                                 Top::LockType::NotLocked,
                                 AutoStatsTracker::LogMode::kUpdateTopAndCurOp,
                                 0);
            collatorToUse.emplace(
                PipelineD::resolveCollator(opCtx, request.getCollation(), nullptr));
        } else {
            // This is a regular aggregation. Lock the collection or view.
            ctx.emplace(opCtx, nss, AutoGetCollectionViewMode::kViewsPermitted);
            collatorToUse.emplace(
                PipelineD::resolveCollator(opCtx, request.getCollation(), ctx->getCollection()));
            if (ctx->getCollection()) {
                uuid = ctx->getCollection()->uuid();
            }
        }

        const auto& collection = ctx ? ctx->getCollection() : CollectionPtr::null;

        // If this is a view, resolve it by finding the underlying collection and stitching view
        // pipelines and this request's pipeline together. We then release our locks before
        // recursively calling runAggregate(), which will re-acquire locks on the underlying
        // collection.  (The lock must be released because recursively acquiring locks on the
        // database will prohibit yielding.)
        if (ctx && ctx->getView() && !liteParsedPipeline.startsWithCollStats()) {
            invariant(nss != NamespaceString::kRsOplogNamespace);
            invariant(!nss.isCollectionlessAggregateNS());
            uassert(ErrorCodes::OptionNotSupportedOnView,
                    str::stream() << AggregationRequest::kCollectionUUIDName
                                  << " is not supported against a view",
                    !request.getCollectionUUID());

            // Check that the default collation of 'view' is compatible with the operation's
            // collation. The check is skipped if the request did not specify a collation.
            if (!request.getCollation().isEmpty()) {
                invariant(collatorToUse);  // Should already be resolved at this point.
                if (!CollatorInterface::collatorsMatch(ctx->getView()->defaultCollator(),
                                                       collatorToUse->get())) {
                    return {ErrorCodes::OptionNotSupportedOnView,
                            "Cannot override a view's default collation"};
                }
            }


            auto resolvedView = uassertStatusOK(DatabaseHolder::get(opCtx)
                                                    ->getSharedViewCatalog(opCtx, nss.db())
                                                    ->resolveView(opCtx, nss));
            uassert(std::move(resolvedView),
                    "On sharded systems, resolved views must be executed by mongos",
                    !ShardingState::get(opCtx)->enabled());

            // With the view & collation resolved, we can relinquish locks.
            ctx.reset();

            // Parse the resolved view into a new aggregation request.
            auto newRequest = resolvedView.asExpandedViewAggregation(request);
            auto newCmd = newRequest.serializeToCommandObj().toBson();

            auto status = runAggregate(opCtx, origNss, newRequest, newCmd, privileges, result);

            {
                // Set the namespace of the curop back to the view namespace so ctx records
                // stats on this view namespace on destruction.
                stdx::lock_guard<Client> lk(*opCtx->getClient());
                curOp->setNS_inlock(nss.ns());
            }

            return status;
        }

        if (request.getCollectionUUID()) {
            // If the namespace is not a view and collectionUUID was provided, verify the collection
            // exists and has the expected UUID.
            uassert(ErrorCodes::NamespaceNotFound,
                    "No collection found with the given namespace and UUID",
                    uuid && uuid == *request.getCollectionUUID());
        }

        invariant(collatorToUse);
        expCtx = makeExpressionContext(opCtx, request, std::move(*collatorToUse), uuid);

        auto pipeline = Pipeline::parse(request.getPipeline(), expCtx);

        // Check that the view's collation matches the collation of any views involved in the
        // pipeline.
        if (!pipelineInvolvedNamespaces.empty()) {
            auto pipelineCollationStatus = collatorCompatibleWithPipeline(
                opCtx, nss.db(), expCtx->getCollator(), liteParsedPipeline);
            if (!pipelineCollationStatus.isOK()) {
                return pipelineCollationStatus;
            }
        }

        pipeline->optimizePipeline();

        // Check if the pipeline has a $geoNear stage, as it will be ripped away during the build
        // query executor phase below (to be replaced with a $geoNearCursorStage later during the
        // executor attach phase).
        auto hasGeoNearStage = !pipeline->getSources().empty() &&
            dynamic_cast<DocumentSourceGeoNear*>(pipeline->peekFront());

        // Prepare a PlanExecutor to provide input into the pipeline, if needed.
        std::pair<PipelineD::AttachExecutorCallback,
                  std::unique_ptr<PlanExecutor, PlanExecutor::Deleter>>
            attachExecutorCallback;
        if (liteParsedPipeline.hasChangeStream()) {
            // If we are using a change stream, the cursor stage should have a simple collation,
            // regardless of what the user's collation was.
            std::unique_ptr<CollatorInterface> collatorForCursor = nullptr;
            auto collatorStash = expCtx->temporarilyChangeCollator(std::move(collatorForCursor));
            attachExecutorCallback =
                PipelineD::buildInnerQueryExecutor(collection, nss, &request, pipeline.get());
        } else {
            attachExecutorCallback =
                PipelineD::buildInnerQueryExecutor(collection, nss, &request, pipeline.get());
        }

        if (canOptimizeAwayPipeline(pipeline.get(),
                                    attachExecutorCallback.second.get(),
                                    request,
                                    hasGeoNearStage,
                                    liteParsedPipeline.hasChangeStream())) {
            // This pipeline is currently empty, but once completed it will have only one source,
            // which is a DocumentSourceCursor. Instead of creating a whole pipeline to do nothing
            // more than forward the results of its cursor document source, we can use the
            // PlanExecutor by itself. The resulting cursor will look like what the client would
            // have gotten from find command.
            execs.emplace_back(std::move(attachExecutorCallback.second));
        } else {
            // Complete creation of the initial $cursor stage, if needed.
            PipelineD::attachInnerQueryExecutorToPipeline(collection,
                                                          attachExecutorCallback.first,
                                                          std::move(attachExecutorCallback.second),
                                                          pipeline.get());

            auto pipelines =
                createExchangePipelinesIfNeeded(opCtx, expCtx, request, std::move(pipeline), uuid);
            for (auto&& pipelineIt : pipelines) {
                // There are separate ExpressionContexts for each exchange pipeline, so make sure to
                // pass the pipeline's ExpressionContext to the plan executor factory.
                auto pipelineExpCtx = pipelineIt->getContext();
                execs.emplace_back(
                    plan_executor_factory::make(std::move(pipelineExpCtx),
                                                std::move(pipelineIt),
                                                liteParsedPipeline.hasChangeStream()));
            }

            // With the pipelines created, we can relinquish locks as they will manage the locks
            // internally further on. We still need to keep the lock for an optimized away pipeline
            // though, as we will be changing its lock policy to 'kLockExternally' (see details
            // below), and in order to execute the initial getNext() call in 'handleCursorCommand',
            // we need to hold the collection lock.
            ctx.reset();
        }

        {
            auto planSummary = execs[0]->getPlanExplainer().getPlanSummary();
            stdx::lock_guard<Client> lk(*opCtx->getClient());
            curOp->setPlanSummary_inlock(std::move(planSummary));
        }
    }

    // Having released the collection lock, we can now create a cursor that returns results from the
    // pipeline. This cursor owns no collection state, and thus we register it with the global
    // cursor manager. The global cursor manager does not deliver invalidations or kill
    // notifications; the underlying PlanExecutor(s) used by the pipeline will be receiving
    // invalidations and kill notifications themselves, not the cursor we create here.

    std::vector<ClientCursorPin> pins;
    std::vector<ClientCursor*> cursors;

    auto cursorFreer = makeGuard([&] {
        for (auto& p : pins) {
            p.deleteUnderlying();
        }
    });
    for (auto&& exec : execs) {
        ClientCursorParams cursorParams(
            std::move(exec),
            origNss,
            AuthorizationSession::get(opCtx->getClient())->getAuthenticatedUserNames(),
            APIParameters::get(opCtx),
            opCtx->getWriteConcern(),
            repl::ReadConcernArgs::get(opCtx),
            cmdObj,
            privileges);
        if (expCtx->tailableMode == TailableModeEnum::kTailable) {
            cursorParams.setTailable(true);
        } else if (expCtx->tailableMode == TailableModeEnum::kTailableAndAwaitData) {
            cursorParams.setTailable(true);
            cursorParams.setAwaitData(true);
        }

        auto pin = CursorManager::get(opCtx)->registerCursor(opCtx, std::move(cursorParams));

        cursors.emplace_back(pin.getCursor());
        pins.emplace_back(std::move(pin));
    }

    // Report usage statistics for each stage in the pipeline.
    liteParsedPipeline.tickGlobalStageCounters();

    // If both explain and cursor are specified, explain wins.
    if (expCtx->explain) {
        auto explainExecutor = pins[0]->getExecutor();
        auto bodyBuilder = result->getBodyBuilder();
        if (auto pipelineExec = dynamic_cast<PlanExecutorPipeline*>(explainExecutor)) {
            Explain::explainPipeline(
                pipelineExec, true /* executePipeline */, *(expCtx->explain), &bodyBuilder);
        } else {
            invariant(explainExecutor->getOpCtx() == opCtx);
            // The explainStages() function for a non-pipeline executor may need to execute the plan
            // to collect statistics. If the PlanExecutor uses kLockExternally policy, the
            // appropriate collection lock must be already held. Make sure it has not been released
            // yet.
            invariant(ctx);
            Explain::explainStages(explainExecutor,
                                   ctx->getCollection(),
                                   *(expCtx->explain),
                                   BSON("optimizedPipeline" << true),
                                   &bodyBuilder);
        }
    } else {
        // Cursor must be specified, if explain is not.
        const bool keepCursor = handleCursorCommand(
            opCtx, expCtx, origNss, std::move(cursors), request, cmdObj, result);
        if (keepCursor) {
            cursorFreer.dismiss();
        }

        PlanSummaryStats stats;
        pins[0].getCursor()->getExecutor()->getPlanExplainer().getSummaryStats(&stats);
        curOp->debug().setPlanSummaryMetrics(stats);
        curOp->debug().nreturned = stats.nReturned;
        // For an optimized away pipeline, signal the cache that a query operation has completed.
        // For normal pipelines this is done in DocumentSourceCursor.
        if (ctx && ctx->getCollection()) {
            const CollectionPtr& coll = ctx->getCollection();
            CollectionQueryInfo::get(coll).notifyOfQuery(opCtx, coll, stats);
        }
    }

    // The aggregation pipeline may change the namespace of the curop and we need to set it back to
    // the original namespace to correctly report command stats. One example when the namespace can
    // be changed is when the pipeline contains an $out stage, which executes an internal command to
    // create a temp collection, changing the curop namespace to the name of this temp collection.
    {
        stdx::lock_guard<Client> lk(*opCtx->getClient());
        curOp->setNS_inlock(origNss.ns());
    }

    // Any code that needs the cursor pinned must be inside the try block, above.
    return Status::OK();
}

}  // namespace mongo
