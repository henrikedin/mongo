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

namespace mongo {
	/*
	* Any state may transition to EndSession in case of an error, otherwise the valid state
	* transitions are:
	* Source -> SourceWait -> Process -> SinkWait -> Source (standard RPC)
	* Source -> SourceWait -> Process -> SinkWait -> Process -> SinkWait ... (exhaust)
	* Source -> SourceWait -> Process -> Source (fire-and-forget)
	*/
	enum class ServiceStateMachineState {
		Created,     // The session has been created, but no operations have been performed yet
		Source,      // Request a new Message from the network to handle
		SourceWait,  // Wait for the new Message to arrive from the network
		Process,     // Run the Message through the database
		SinkWait,    // Wait for the database result to be sent by the network
		EndSession,  // End the session - the ServiceStateMachine will be invalid after this
		Ended        // The session has ended. It is illegal to call any method besides
					 // state() if this is the current state.
	};
}  // namespace mongo
