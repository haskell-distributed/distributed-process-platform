{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.ManagedProcess
-- Copyright   :  (c) Tim Watson 2012
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a high(er) level API for building complex 'Process'
-- implementations by abstracting out the management of the process' mailbox,
-- reply/response handling, timeouts, process hiberation, error handling
-- and shutdown/stop procedures. It is modelled along similar lines to OTP's
-- gen_server API - <http://www.erlang.org/doc/man/gen_server.html>.
--
-- [API Overview]
--
-- Once started, a generic process will consume messages from its mailbox and
-- pass them on to user defined /handlers/ based on the types received (mapped
-- to those accepted by the handlers) and optionally by also evaluating user
-- supplied predicates to determine which handlers are valid.
-- Each handler returns a 'ProcessAction' which specifies how we should proceed.
-- If none of the handlers is able to process a message (because their types are
-- incompatible) then the process 'unhandledMessagePolicy' will be applied.
--
-- The 'ProcessAction' type defines the ways in which a process can respond
-- to its inputs, either by continuing to read incoming messages, setting an
-- optional timeout, sleeping for a while or by stopping. The optional timeout
-- behaves a little differently to the other process actions. If no messages
-- are received within the specified time span, the process 'timeoutHandler'
-- will be called in order to determine the next action.
--
-- Generic processes are defined by the 'ProcessDefinition' type, using record
-- syntax. The 'ProcessDefinition' fields contain handlers (or lists of them)
-- for specific tasks. In addtion to the @timeoutHandler@, a 'ProcessDefinition'
-- may also define a @terminateHandler@ which is called just before the process
-- exits. This handler will be called /whenever/ the process is stopping, i.e.,
-- when a callback returns 'stop' as the next action /or/ if an unhandled exit
-- signal or similar asynchronous exception is thrown in (or to) the process
-- itself.
--
-- The other handlers are split into two groups: /apiHandlers/ and /infoHandlers/.
-- The former contains handlers for the 'cast' and 'call' protocols, whilst the
-- latter contains handlers that deal with input messages which are not sent
-- via these API calls (i.e., messages sent using bare 'send' or signals put
-- into the process mailbox by the node controller, such as
-- 'ProcessMonitorNotification' and the like).
--
-- [The Cast/Call Protocol]
--
-- Deliberate interactions with the process will usually fall into one of two
-- categories. A 'cast' interaction involves a client sending a message
-- asynchronously and the server handling this input. No reply is sent to
-- the client. On the other hand, a 'call' interaction is a kind of /rpc/
-- where the client sends a message and waits for a reply.
--
-- The expressions given to @apiHandlers@ have to conform to the /cast|call/
-- protocol. The details of this are, however, hidden from the user. A set
-- of API functions for creating @apiHandlers@ are given instead, which
-- take expressions (i.e., a function or lambda expression) and create the
-- appropriate @Dispatcher@ for handling the cast (or call).
--
-- The cast/call protocol handlers deal with /expected/ inputs. These form
-- the explicit public API for the process, and will usually be exposed by
-- providing module level functions that defer to the cast/call API. For
-- example:
--
-- @
-- add :: ProcessId -> Double -> Double -> Double
-- add pid x y = call pid (Add x y)
-- @
--
-- [Handling Info Messages]
--
-- An explicit protocol for communicating with the process can be
-- configured using 'cast' and 'call', but it is not possible to prevent
-- other kinds of messages from being sent to the process mailbox. When
-- any message arrives for which there are no handlers able to process
-- its content, the 'UnhandledMessagePolicy' will be applied. Sometimes
-- it is desireable to process incoming messages which aren't part of the
-- protocol, rather than let the policy deal with them. This is particularly
-- true when incoming messages are important to the process, but their point
-- of origin is outside the developer's control. Handling /signals/ such as
-- 'ProcessMonitorNotification' is a typical example of this:
--
-- > handleInfo_ (\(ProcessMonitorNotification _ _ r) -> say $ show r >> continue_)
--
-- [Handling Process State]
--
-- The 'ProcessDefinition' is parameterised by the type of state it maintains.
-- A process that has no state will have the type @ProcessDefinition ()@ and can
-- be bootstrapped by evaluating 'statelessProcess'.
--
-- All call/cast handlers come in two flavours, those which take the process
-- state as an input and those which do not. Handlers that ignore the process
-- state have to return a function that takes the state and returns the required
-- action. Versions of the various action generating functions ending in an
-- underscore are provided to simplify this:
--
-- @
--   statelessProcess {
--       apiHandlers = [
--         handleCall_   (\\(n :: Int) -> return (n * 2))
--       , handleCastIf_ (\\(c :: String, _ :: Delay) -> c == \"timeout\")
--                       (\\(\"timeout\", Delay d) -> timeoutAfter_ d)
--       ]
--     , timeoutHandler = \\_ _ -> stop $ TerminateOther \"timeout\"
--   }
-- @
--
-- [Handling Errors]
--
-- Error handling appears in several contexts and process definitions can
-- hook into these with relative ease. Only process failures as a result of
-- asynchronous exceptions are supported by the API, which provides several
-- scopes for error handling.
--
-- Catching exceptions inside handler functions is no different to ordinary
-- exception handling in monadic code.
--
-- @
--   handleCall (\\x y ->
--                catch (hereBeDragons x y)
--                      (\\(e :: SmaugTheTerribleException) ->
--                           return (Left (show e))))
-- @
--
-- The caveats mentioned in "Control.Distributed.Process.Platform" about
-- exit signal handling obviously apply here as well.
--
-- [Structured Exit Signal Handling]
--
-- Because "Control.Distributed.Process.ProcessExitException" is a ubiquitous
-- /signalling mechanism/ in Cloud Haskell, it is treated unlike other
-- asynchronous exceptions. The 'ProcessDefinition' 'exitHandlers' field
-- accepts a list of handlers that, for a specific exit reason, can decide
-- how the process should respond. If none of these handlers matches the
-- type of @reason@ then the process will exit with @DiedException why@. In
-- addition, a default /exit handler/ is installed for exit signals where the
-- @reason == Shutdown@, because this is an /exit signal/ used explicitly and
-- extensively throughout the platform. The default behaviour is to gracefully
-- shut down the process, calling the @terminateHandler@ as usual, before
-- stopping with @TerminateShutdown@ given as the final outcome.
--
-- /Example: How to annoy your supervisor and end up force-killed:/
--
-- > handleExit  (\state from (sigExit :: Shutdown) -> continue s)
--
-- That code is, of course, very silly. Under some circumstances, handling
-- exit signals is perfectly legitimate. Handling of /other/ forms of
-- asynchronous exception is not supported by this API.
--
-- If any asynchronous exception goes unhandled, the process will immediately
-- exit without running the @terminateHandler@. It is very important to note
-- that in Cloud Haskell, link failures generate asynchronous exceptions in
-- the target and these will NOT be caught by the API and will therefore
-- cause the process to exit /without running the termination handler/
-- callback. If your termination handler is set up to do important work
-- (such as resource cleanup) then you should avoid linking you process
-- and use monitors instead.
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.ManagedProcess
  ( -- * Starting server processes
    InitResult(..)
  , InitHandler
  , start
  , runProcess
    -- * Client interactions
  , shutdown
  , defaultProcess
  , statelessProcess
  , statelessInit
  , call
  , safeCall
  , tryCall
  , callAsync
  , callTimeout
  , cast
    -- * Defining server processes
  , ProcessDefinition(..)
  , TerminateHandler
  , TimeoutHandler
  , ProcessAction(..)
  , ProcessReply
  , CallHandler
  , CastHandler
  , UnhandledMessagePolicy(..)
  , CallRef
  , handleCall
  , handleCallIf
  , handleCallFrom
  , handleCallFromIf
  , handleCast
  , handleCastIf
  , handleInfo
  , handleDispatch
  , handleExit
    -- * Stateless handlers
  , action
  , handleCall_
  , handleCallIf_
  , handleCast_
  , handleCastIf_
    -- * Constructing handler results
  , condition
  , state
  , input
  , reply
  , replyWith
  , noReply
  , noReply_
  , haltNoReply_
  , continue
  , continue_
  , timeoutAfter
  , timeoutAfter_
  , hibernate
  , hibernate_
  , stop
  , stop_
  , replyTo
  ) where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Platform.ManagedProcess.Client
import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.ManagedProcess.Internal.GenProcess
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Types (TerminateReason(..))
import Control.Distributed.Process.Platform.Time
import Prelude hiding (init)

-- TODO: automatic registration

-- | Starts a gen-process configured with the supplied process definition,
-- using an init handler and its initial arguments. This code will run the
-- 'Process' until completion and return @Right TerminateReason@ *or*,
-- if initialisation fails, return @Left InitResult@ which will be
-- @InitFail why@.
start :: a
      -> InitHandler a s
      -> ProcessDefinition s
      -> Process (Either (InitResult s) TerminateReason)
start = runProcess recvLoop

runProcess :: (ProcessDefinition s -> s -> Delay -> Process TerminateReason)
           -> a
           -> InitHandler a s
           -> ProcessDefinition s
           -> Process (Either (InitResult s) TerminateReason)
runProcess loop args init def = do
  ir <- init args
  case ir of
    InitOk s d -> loop def s d >>= return . Right
    f@(InitFail _) -> return $ Left f

defaultProcess :: ProcessDefinition s
defaultProcess = ProcessDefinition {
    apiHandlers      = []
  , infoHandlers     = []
  , exitHandlers     = []
  , timeoutHandler   = \s _ -> continue s
  , terminateHandler = \_ _ -> return ()
  , unhandledMessagePolicy = Terminate
  } :: ProcessDefinition s

-- | A basic, stateless process definition, where the unhandled message policy
-- is set to 'Terminate', the default timeout handlers does nothing (i.e., the
-- same as calling @continue ()@ and the terminate handler is a no-op.
statelessProcess :: ProcessDefinition ()
statelessProcess = ProcessDefinition {
    apiHandlers            = []
  , infoHandlers           = []
  , exitHandlers           = []
  , timeoutHandler         = \s _ -> continue s
  , terminateHandler       = \_ _ -> return ()
  , unhandledMessagePolicy = Terminate
  }

-- | A basic, state /unaware/ 'InitHandler' that can be used with
-- 'statelessProcess'.
statelessInit :: Delay -> InitHandler () ()
statelessInit d () = return $ InitOk () d

