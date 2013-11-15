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
-- This module provides a high(er) level API for building complex @Process@
-- implementations by abstracting out the management of the process' mailbox,
-- reply/response handling, timeouts, process hiberation, error handling
-- and shutdown/stop procedures. It is modelled along similar lines to OTP's
-- gen_server API - <http://www.erlang.org/doc/man/gen_server.html>.
--
-- In particular, a /managed process/ will interoperate cleanly with the
-- "Control.Distributed.Process.Platform.Supervisor" API.
--
-- [API Overview]
--
-- Once started, a /managed process/ will consume messages from its mailbox and
-- pass them on to user defined /handlers/ based on the types received (mapped
-- to those accepted by the handlers) and optionally by also evaluating user
-- supplied predicates to determine which handlers are valid.
-- Each handler returns a 'ProcessAction' which specifies how we should proceed.
-- If none of the handlers is able to process a message (because their types are
-- incompatible) then an 'unhandledMessagePolicy' will be applied.
--
-- The 'ProcessAction' type defines the ways in which a /managed process/ responds
-- to its inputs, either by continuing to read incoming messages, setting an
-- optional timeout, sleeping for a while or by stopping. The optional timeout
-- behaves a little differently to the other process actions. If no messages
-- are received within the specified time span, a user defined 'timeoutHandler'
-- will be called in order to determine the next action.
--
-- Generic processes are defined by the 'ProcessDefinition' type, using record
-- syntax. The 'ProcessDefinition' fields contain handlers (or lists of them)
-- for specific tasks, such as the @timeoutHandler@, and a @terminateHandler@
-- which is called just before the process exits. This handler will be called
-- /whenever/ the process is stopping, i.e., when a callback returns 'stop' as
-- the next action /or/ if an unhandled exit signal or similar asynchronous
-- exception is thrown in (or to) the process itself.
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
-- Deliberate interactions with a /managed process/ usually fall into one of
-- two categories. A 'cast' interaction involves a client sending a message
-- asynchronously and the server handling this input. No reply is sent to
-- the client. On the other hand, a 'call' is a /remote procedure call/,
-- where the client sends a message and waits for a reply from the server.
--
-- All expressions given to @apiHandlers@ have to conform to the /cast|call/
-- protocol. The protocol (messaging) implementation is hidden from the user;
-- API functions for creating user defined @apiHandlers@ are given instead,
-- which take expressions (i.e., a function or lambda expression) and create the
-- appropriate @Dispatcher@ for handling the cast (or call).
--
-- The cast/call protocol handlers deal with /expected/ inputs. These form
-- the explicit public API for the process, and will usually be exposed by
-- providing module level functions that defer to the cast/call API, giving
-- the author an opportunity to enforce the correct types. For
-- example:
--
-- @
-- {- Ask the server to add two numbers -}
-- add :: ProcessId -> Double -> Double -> Double
-- add pid x y = call pid (Add x y)
-- @
--
-- Note here that the return type from the call is /inferred/ and will not be
-- enforced by the type system. If the server sends a different type back in
-- the reply, then the caller will be blocked indefinitely! This is a slight
-- disadvantage of the loose coupling between client and server, but it does
-- allow servers to handle a variety of messages without specifying the entire
-- protocol to be supported in excruciating detail. If the latter approach
-- appeals, then the "Control.Distributed.Process.Platform.Session" API might
-- be more to your tastes. Note that /that/ API cannot handle unanticipated
-- messages, in the same way that managed processes can.
--
-- [Handling Unexpected/Info Messages]
--
-- An explicit protocol for communicating with the process can be
-- configured using 'cast' and 'call', but it is not possible to prevent
-- other kinds of messages from being sent to the process mailbox. When
-- any message arrives for which there are no handlers able to process
-- its content, the 'UnhandledMessagePolicy' will be applied. Sometimes
-- it is desireable to process incoming messages which aren't part of the
-- protocol, rather than let the policy deal with them. This is particularly
-- true when incoming messages are important to the process, but their point
-- of origin is outside the author's control. Handling /signals/ such as
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
--     , timeoutHandler = \\_ _ -> stop $ ExitOther \"timeout\"
--   }
-- @
--
-- [Avoiding Side Effects]
--
-- If you wish to only write side-effect free code in your server definition,
-- then there is an explicit API for doing so. Instead of using the handlers
-- definition functions in this module, import the /pure/ server module instead,
-- which provides a StateT based monad for building referentially transparent
-- callbacks.
--
-- See "Control.Distributed.Process.Platform.ManagedProcess.Server.Pure" for
-- details and API documentation.
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
-- [Structured Exit Handling]
--
-- Because "Control.Distributed.Process.ProcessExitException" is a ubiquitous
-- signalling mechanism in Cloud Haskell, it is treated unlike other
-- asynchronous exceptions. The 'ProcessDefinition' 'exitHandlers' field
-- accepts a list of handlers that, for a specific exit reason, can decide
-- how the process should respond. If none of these handlers matches the
-- type of @reason@ then the process will exit with @DiedException why@. In
-- addition, a private /exit handler/ is installed for exit signals where
-- @reason :: ExitReason@, which is a form of /exit signal/ used explicitly
-- by the supervision APIs. This behaviour, which cannot be overriden, is to
-- gracefully shut down the process, calling the @terminateHandler@ as usual,
-- before stopping with @reason@ given as the final outcome.
--
-- /Example: handling custom data is @ProcessExitException@/
--
-- > handleExit  (\state from (sigExit :: SomeExitData) -> continue s)
--
-- Under some circumstances, handling exit signals is perfectly legitimate.
-- Handling of /other/ forms of asynchronous exception (e.g., exceptions not
-- generated by an /exit/ signal) is not supported by this API. Cloud Haskell's
-- primitives for exception handling /will/ work normally in managed process
-- callbacks however.
--
-- If any asynchronous exception goes unhandled, the process will immediately
-- exit without running the @terminateHandler@. It is very important to note
-- that in Cloud Haskell, link failures generate asynchronous exceptions in
-- the target and these will NOT be caught by the API and will therefore
-- cause the process to exit /without running the termination handler/
-- callback. If your termination handler is set up to do important work
-- (such as resource cleanup) then you should avoid linking you process
-- and use monitors instead.
--
-- [Performance Considerations]
--
-- The server implementations are fairly well optimised already, but there /is/
-- a cost associated with scanning the mailbox to match on protocol messages,
-- and additional costs (i.e., space /and/ time) in mapping over all available
-- /info handlers/ for non-protocol (i.e., neither /call/ nor /cast/) messages.
-- There are further costs when using prioritisation, in priority space/storage
-- and in per-message processing (where priorities must be applied and matched
-- against incoming messages).
--
-- In clients, it's important to remember that the /call/ protocol will wait
-- for a reply in most cases, triggering a full O(n) scan of the caller's
-- mailbox. If the mailbox is extremely full and calls are regularly made, this
-- may have a significant impact on the caller. The @callChan@ family of client
-- API functions can alleviate this, by using (and matching on) a private typed
-- channel instead, but the server must be written to accomodate this.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.ManagedProcess
  ( -- * Starting server processes
    InitResult(..)
  , InitHandler
  , serve
  , pserve
  , runProcess
  , prioritised
    -- * Client interactions
  , module Control.Distributed.Process.Platform.ManagedProcess.Client
    -- * Defining server processes
  , ProcessDefinition(..)
  , PrioritisedProcessDefinition(..)
  , Priority(..)
  , DispatchPriority()
  , Dispatcher()
  , DeferredDispatcher()
  , ShutdownHandler
  , TimeoutHandler
  , ProcessAction(..)
  , ProcessReply
  , Condition
  , CallHandler
  , CastHandler
  , UnhandledMessagePolicy(..)
  , CallRef
  , defaultProcess
  , defaultProcessWithPriorities
  , statelessProcess
  , statelessInit
    -- * Server side callbacks
  , handleCall
  , handleCallIf
  , handleCallFrom
  , handleCallFromIf
  , handleCast
  , handleCastIf
  , handleInfo
  , handleRpcChan
  , handleRpcChanIf
  , action
  , handleDispatch
  , handleExit
    -- * Stateless callbacks
  , handleCall_
  , handleCallFrom_
  , handleCallIf_
  , handleCallFromIf_
  , handleCast_
  , handleCastIf_
  , handleRpcChan_
  , handleRpcChanIf_
    -- * Prioritised mailboxes
  , module Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
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
  , stopWith
  , stop_
  , replyTo
  , replyChan
  ) where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Platform.ManagedProcess.Client
import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
import Control.Distributed.Process.Platform.ManagedProcess.Internal.GenProcess
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Types (ExitReason(..))
import Control.Distributed.Process.Platform.Time
import Prelude hiding (init)

-- TODO: automatic registration

-- | Starts a managed process configured with the supplied process definition,
-- using an init handler and its initial arguments.
serve :: a
      -> InitHandler a s
      -> ProcessDefinition s
      -> Process ()
serve argv init def = runProcess (recvLoop def) argv init

-- | Starts a prioritised managed process configured with the supplied process
-- definition, using an init handler and its initial arguments.
pserve :: a
       -> InitHandler a s
       -> PrioritisedProcessDefinition s
       -> Process ()
pserve argv init def = runProcess (precvLoop def) argv init

-- | Wraps any /process loop/ and enforces that it adheres to the
-- managed process' start/stop semantics, i.e., evaluating the
-- @InitHandler@ with an initial state and delay will either
-- @die@ due to @InitStop@, exit silently (due to @InitIgnore@)
-- or evaluate the process' @loop@. The supplied @loop@ must evaluate
-- to @ExitNormal@, otherwise the evaluating processing will will
-- @die@ with the @ExitReason@.
--
runProcess :: (s -> Delay -> Process ExitReason)
           -> a
           -> InitHandler a s
           -> Process ()
runProcess loop args init = do
  ir <- init args
  case ir of
    InitOk s d -> loop s d >>= checkExitType
    InitStop s -> die $ ExitOther s
    InitIgnore -> return ()
  where
    checkExitType :: ExitReason -> Process ()
    checkExitType ExitNormal = return ()
    checkExitType other      = die other

defaultProcess :: ProcessDefinition s
defaultProcess = ProcessDefinition {
    apiHandlers      = []
  , infoHandlers     = []
  , exitHandlers     = []
  , timeoutHandler   = \s _ -> continue s
  , shutdownHandler  = \_ _ -> return ()
  , unhandledMessagePolicy = Terminate
  } :: ProcessDefinition s

-- | Turns a standard 'ProcessDefinition' into a 'PrioritisedProcessDefinition',
-- by virtue of the supplied list of 'DispatchPriority' expressions.
--
prioritised :: ProcessDefinition s
            -> [DispatchPriority s]
            -> PrioritisedProcessDefinition s
prioritised def ps = PrioritisedProcessDefinition def ps

defaultProcessWithPriorities :: [DispatchPriority s] -> PrioritisedProcessDefinition s
defaultProcessWithPriorities = PrioritisedProcessDefinition defaultProcess

-- | A basic, stateless process definition, where the unhandled message policy
-- is set to 'Terminate', the default timeout handlers does nothing (i.e., the
-- same as calling @continue ()@ and the terminate handler is a no-op.
statelessProcess :: ProcessDefinition ()
statelessProcess = defaultProcess :: ProcessDefinition ()

-- | A basic, state /unaware/ 'InitHandler' that can be used with
-- 'statelessProcess'.
statelessInit :: Delay -> InitHandler () ()
statelessInit d () = return $ InitOk () d
