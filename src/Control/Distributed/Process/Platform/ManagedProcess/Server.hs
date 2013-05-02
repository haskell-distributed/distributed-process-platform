{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.ManagedProcess.Server
-- Copyright   :  (c) Tim Watson 2012 - 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- The Server Portion of the /Managed Process/ API.
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.ManagedProcess.Server
  ( -- * Server actions
    condition
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
    -- * Server handler/callback creation
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
  ) where

import Control.Distributed.Process hiding (call, Message)
import qualified Control.Distributed.Process as P (Message)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Primitives
import Control.Distributed.Process.Platform.Internal.Types
  ( Recipient(..)
  , TerminateReason(..)
  )
import Control.Distributed.Process.Platform.Time
import Prelude hiding (init)

--------------------------------------------------------------------------------
-- Producing ProcessAction and ProcessReply from inside handler expressions   --
--------------------------------------------------------------------------------

-- | Creates a 'Conditon' from a function that takes a process state @a@ and
-- an input message @b@ and returns a 'Bool' indicating whether the associated
-- handler should run.
--
condition :: forall a b. (Serializable a, Serializable b)
          => (a -> b -> Bool)
          -> Condition a b
condition = Condition

-- | Create a 'Condition' from a function that takes a process state @a@ and
-- returns a 'Bool' indicating whether the associated handler should run.
--
state :: forall s m. (Serializable m) => (s -> Bool) -> Condition s m
state = State

-- | Creates a 'Condition' from a function that takes an input message @m@ and
-- returns a 'Bool' indicating whether the associated handler should run.
--
input :: forall s m. (Serializable m) => (m -> Bool) -> Condition s m
input = Input

-- | Instructs the process to send a reply and continue running.
reply :: (Serializable r) => r -> s -> Process (ProcessReply s r)
reply r s = continue s >>= replyWith r

-- | Instructs the process to send a reply /and/ evaluate the 'ProcessAction'.
replyWith :: (Serializable m)
          => m
          -> ProcessAction s
          -> Process (ProcessReply s m)
replyWith msg st = return $ ProcessReply msg st

-- | Instructs the process to skip sending a reply /and/ evaluate a 'ProcessAction'
noReply :: (Serializable r) => ProcessAction s -> Process (ProcessReply s r)
noReply = return . NoReply

-- | Continue without giving a reply to the caller - equivalent to 'continue',
-- but usable in a callback passed to the 'handleCall' family of functions.
noReply_ :: forall s r . (Serializable r) => s -> Process (ProcessReply s r)
noReply_ s = continue s >>= noReply

-- | Halt process execution during a call handler, without paying any attention
-- to the expected return type.
haltNoReply_ :: TerminateReason -> Process (ProcessReply s TerminateReason)
haltNoReply_ r = stop r >>= noReply

-- | Instructs the process to continue running and receiving messages.
continue :: s -> Process (ProcessAction s)
continue = return . ProcessContinue

-- | Version of 'continue' that can be used in handlers that ignore process state.
--
continue_ :: (s -> Process (ProcessAction s))
continue_ = return . ProcessContinue

-- | Instructs the process to wait for incoming messages until 'TimeInterval'
-- is exceeded. If no messages are handled during this period, the /timeout/
-- handler will be called. Note that this alters the process timeout permanently
-- such that the given @TimeInterval@ will remain in use until changed.
timeoutAfter :: TimeInterval -> s -> Process (ProcessAction s)
timeoutAfter d s = return $ ProcessTimeout d s

-- | Version of 'timeoutAfter' that can be used in handlers that ignore process state.
--
-- > action (\(TimeoutPlease duration) -> timeoutAfter_ duration)
--
timeoutAfter_ :: TimeInterval -> (s -> Process (ProcessAction s))
timeoutAfter_ d = return . ProcessTimeout d

-- | Instructs the process to /hibernate/ for the given 'TimeInterval'. Note
-- that no messages will be removed from the mailbox until after hibernation has
-- ceased. This is equivalent to calling @threadDelay@.
--
hibernate :: TimeInterval -> s -> Process (ProcessAction s)
hibernate d s = return $ ProcessHibernate d s

-- | Version of 'hibernate' that can be used in handlers that ignore process state.
--
-- > action (\(HibernatePlease delay) -> hibernate_ delay)
--
hibernate_ :: TimeInterval -> (s -> Process (ProcessAction s))
hibernate_ d = return . ProcessHibernate d

-- | Instructs the process to terminate, giving the supplied reason. If a valid
-- 'terminateHandler' is installed, it will be called with the 'TerminateReason'
-- returned from this call, along with the process state.
stop :: TerminateReason -> Process (ProcessAction s)
stop r = return $ ProcessStop r

-- | Version of 'stop' that can be used in handlers that ignore process state.
--
-- > action (\ClientError -> stop_ TerminateNormal)
--
stop_ :: TerminateReason -> (s -> Process (ProcessAction s))
stop_ r _ = stop r

-- | Sends a reply explicitly to a 'Caller'.
replyTo :: (Serializable m) => CallRef -> m -> Process ()
replyTo callRef msg =
  let (client, tag) = unCaller callRef
  in sendTo client (CallResponse msg tag)

--------------------------------------------------------------------------------
-- Wrapping handler expressions in Dispatcher and DeferredDispatcher          --
--------------------------------------------------------------------------------

-- | Constructs a 'call' handler from a function in the 'Process' monad.
-- The handler expression returns the reply, and the action will be
-- set to 'continue'.
--
-- > handleCall_ = handleCallIf_ $ input (const True)
--
handleCall_ :: (Serializable a, Serializable b)
           => (a -> Process b)
           -> Dispatcher s
handleCall_ = handleCallIf_ $ input (const True)

-- | Constructs a 'call' handler from an ordinary function in the 'Process'
-- monad. This variant ignores the state argument present in 'handleCall' and
-- 'handleCallIf' and is therefore useful in a stateless server. Messges are
-- only dispatched to the handler if the supplied condition evaluates to @True@
--
-- See 'handleCall'
handleCallIf_ :: forall s a b . (Serializable a, Serializable b)
    => Condition s a -- ^ predicate that must be satisfied for the handler to run
    -> (a -> Process b) -- ^ a function from an input message to a reply
    -> Dispatcher s
handleCallIf_ cond handler
  = DispatchIf {
      dispatch   = doHandle handler
    , dispatchIf = checkCall cond
    }
  where doHandle :: (Serializable a, Serializable b)
                 => (a -> Process b)
                 -> s
                 -> Message a
                 -> Process (ProcessAction s)
        doHandle h s (CallMessage p c) = (h p) >>= mkCallReply c s
        doHandle _ _ _ = die "CALL_HANDLER_TYPE_MISMATCH" -- cannot happen!

        -- handling 'reply-to' in the main process loop is awkward at best,
        -- so we handle it here instead and return the 'action' to the loop
        mkCallReply :: (Serializable b)
                    => CallRef
                    -> s
                    -> b
                    -> Process (ProcessAction s)
        mkCallReply c s m =
          let (c', t) = unCaller c
          in sendTo c' (CallResponse m t) >> continue s

-- | Constructs a 'call' handler from a function in the 'Process' monad.
-- > handleCall = handleCallIf (const True)
--
handleCall :: (Serializable a, Serializable b)
           => (s -> a -> Process (ProcessReply s b))
           -> Dispatcher s
handleCall = handleCallIf $ state (const True)

-- | Constructs a 'call' handler from an ordinary function in the 'Process'
-- monad. Given a function @f :: (s -> a -> Process (ProcessReply s b))@,
-- the expression @handleCall f@ will yield a 'Dispatcher' for inclusion
-- in a 'Behaviour' specification for the /GenProcess/. Messages are only
-- dispatched to the handler if the supplied condition evaluates to @True@
--
handleCallIf :: forall s a b . (Serializable a, Serializable b)
    => Condition s a -- ^ predicate that must be satisfied for the handler to run
    -> (s -> a -> Process (ProcessReply s b))
        -- ^ a reply yielding function over the process state and input message
    -> Dispatcher s
handleCallIf cond handler
  = DispatchIf {
      dispatch   = doHandle handler
    , dispatchIf = checkCall cond
    }
  where doHandle :: (Serializable a, Serializable b)
                 => (s -> a -> Process (ProcessReply s b))
                 -> s
                 -> Message a
                 -> Process (ProcessAction s)
        doHandle h s (CallMessage p c) = (h s p) >>= mkReply c
        doHandle _ _ _ = die "CALL_HANDLER_TYPE_MISMATCH" -- cannot happen!

-- | As 'handleCall' but passes the 'CallRef' to the handler function.
-- This can be useful if you wish to /reply later/ to the caller by, e.g.,
-- spawning a process to do some work and have it @replyTo caller response@
-- out of band. In this case the callback can pass the 'CallRef' to the
-- worker (or stash it away itself) and return 'noReply'.
--
handleCallFrom :: forall s a b . (Serializable a, Serializable b)
           => (s -> CallRef -> a -> Process (ProcessReply s b))
           -> Dispatcher s
handleCallFrom = handleCallFromIf $ state (const True)

-- | As 'handleCallFrom' but only runs the handler if the supplied 'Condition'
-- evaluates to @True@.
--
handleCallFromIf :: forall s a b . (Serializable a, Serializable b)
    => Condition s a -- ^ predicate that must be satisfied for the handler to run
    -> (s -> CallRef -> a -> Process (ProcessReply s b))
        -- ^ a reply yielding function over the process state, sender and input message
    -> Dispatcher s
handleCallFromIf cond handler
  = DispatchIf {
      dispatch   = doHandle handler
    , dispatchIf = checkCall cond
    }
  where doHandle :: (Serializable a, Serializable b)
                 => (s -> CallRef -> a -> Process (ProcessReply s b))
                 -> s
                 -> Message a
                 -> Process (ProcessAction s)
        doHandle h s (CallMessage p c) = (h s c p) >>= mkReply c
        doHandle _ _ _ = die "CALL_HANDLER_TYPE_MISMATCH" -- cannot happen!

-- | Constructs a 'cast' handler from an ordinary function in the 'Process'
-- monad.
-- > handleCast = handleCastIf (const True)
--
handleCast :: (Serializable a)
           => (s -> a -> Process (ProcessAction s))
           -> Dispatcher s
handleCast = handleCastIf $ input (const True)

-- | Constructs a 'cast' handler from an ordinary function in the 'Process'
-- monad. Given a function @f :: (s -> a -> Process (ProcessAction s))@,
-- the expression @handleCall f@ will yield a 'Dispatcher' for inclusion
-- in a 'Behaviour' specification for the /GenProcess/.
--
handleCastIf :: forall s a . (Serializable a)
    => Condition s a -- ^ predicate that must be satisfied for the handler to run
    -> (s -> a -> Process (ProcessAction s))
       -- ^ an action yielding function over the process state and input message
    -> Dispatcher s
handleCastIf cond h
  = DispatchIf {
      dispatch   = (\s (CastMessage p) -> h s p)
    , dispatchIf = checkCast cond
    }

-- | Version of 'handleCast' that ignores the server state.
--
handleCast_ :: (Serializable a)
            => (a -> (s -> Process (ProcessAction s))) -> Dispatcher s
handleCast_ = handleCastIf_ $ input (const True)

-- | Version of 'handleCastIf' that ignores the server state.
--
handleCastIf_ :: forall s a . (Serializable a)
    => Condition s a -- ^ predicate that must be satisfied for the handler to run
    -> (a -> (s -> Process (ProcessAction s)))
        -- ^ a function from the input message to a /stateless action/, cf 'continue_'
    -> Dispatcher s
handleCastIf_ cond h
  = DispatchIf {
      dispatch   = (\s (CastMessage p) -> h p $ s)
    , dispatchIf = checkCast cond
    }

-- | Constructs an /action/ handler. Like 'handleDispatch' this can handle both
-- 'cast' and 'call' messages, but you won't know which you're dealing with.
-- This can be useful where certain inputs require a definite action, such as
-- stopping the server, without concern for the state (e.g., when stopping we
-- need only decide to stop, as the terminate handler can deal with state
-- cleanup etc). For example:
--
-- @action (\MyCriticalErrorSignal -> stop_ TerminateNormal)@
--
action :: forall s a . (Serializable a)
    => (a -> (s -> Process (ProcessAction s)))
          -- ^ a function from the input message to a /stateless action/, cf 'continue_'
    -> Dispatcher s
action h = handleDispatch perform
  where perform :: (s -> a -> Maybe CallRef -> Process (ProcessAction s))
        perform s a _ = let f = h a in f s

-- | Constructs a handler for both /call/ and /cast/ messages.
-- @handleDispatch = handleDispatchIf (const True)@
--
handleDispatch :: (Serializable a)
               => (s -> a -> Maybe CallRef -> Process (ProcessAction s))
               -> Dispatcher s
handleDispatch = handleDispatchIf $ input (const True)

-- | Constructs a handler for both /call/ and /cast/ messages. Messages are only
-- dispatched to the handler if the supplied condition evaluates to @True@.
-- Handlers defined in this way have no access to the call context (if one
-- exists) and cannot therefore reply to calls.
--
handleDispatchIf :: forall s a . (Serializable a)
                 => Condition s a
                 -> (s -> a -> Maybe CallRef -> Process (ProcessAction s))
                 -> Dispatcher s
handleDispatchIf cond handler = DispatchIf {
      dispatch = doHandle handler
    , dispatchIf = check cond
    }
  where doHandle :: (Serializable a)
                 => (s -> a -> Maybe CallRef -> Process (ProcessAction s))
                 -> s
                 -> Message a
                 -> Process (ProcessAction s)
        doHandle h s msg =
            case msg of
                (CallMessage p c) -> (h s p (Just c))
                (CastMessage p)   -> (h s p Nothing)

-- | Creates a generic input handler (i.e., for recieved messages that are /not/
-- sent using the 'cast' or 'call' APIs) from an ordinary function in the
-- 'Process' monad.
handleInfo :: forall s a. (Serializable a)
           => (s -> a -> Process (ProcessAction s))
           -> DeferredDispatcher s
handleInfo h = DeferredDispatcher { dispatchInfo = doHandleInfo h }
  where
    doHandleInfo :: forall s2 a2. (Serializable a2)
                             => (s2 -> a2 -> Process (ProcessAction s2))
                             -> s2
                             -> P.Message
                             -> Process (Maybe (ProcessAction s2))
    doHandleInfo h' s msg = handleMessage msg (h' s)

-- | Creates an /exit handler/ scoped to the execution of any and all the
-- registered call, cast and info handlers for the process.
handleExit :: forall s a. (Serializable a)
           => (s -> ProcessId -> a -> Process (ProcessAction s))
           -> ExitSignalDispatcher s
handleExit h = ExitSignalDispatcher { dispatchExit = doHandleExit h }
  where
    doHandleExit :: (s -> ProcessId -> a -> Process (ProcessAction s))
                 -> s
                 -> ProcessId
                 -> P.Message
                 -> Process (Maybe (ProcessAction s))
    doHandleExit h' s p msg = handleMessage msg (h' s p)

-- handling 'reply-to' in the main process loop is awkward at best,
-- so we handle it here instead and return the 'action' to the loop
mkReply :: (Serializable b)
        => CallRef
        -> ProcessReply s b
        -> Process (ProcessAction s)
mkReply _ (NoReply a)         = return a
mkReply c (ProcessReply r' a) =
  let (c', t) = unCaller c
  in sendTo c' (CallResponse r' t) >> return a

-- these functions are the inverse of 'condition', 'state' and 'input'

check :: forall s m . (Serializable m)
            => Condition s m
            -> s
            -> Message m
            -> Bool
check (Condition c) st msg = c st $ decode msg
check (State     c) st _   = c st
check (Input     c) _  msg = c $ decode msg

checkCall :: forall s m . (Serializable m)
             => Condition s m
             -> s
             -> Message m
             -> Bool
checkCall cond st msg@(CallMessage _ _) = check cond st msg
checkCall _    _     _                    = False

checkCast :: forall s m . (Serializable m)
             => Condition s m
             -> s
             -> Message m
             -> Bool
checkCast cond st msg@(CastMessage _) = check cond st msg
checkCast _    _     _                = False

decode :: Message a -> a
decode (CallMessage a _) = a
decode (CastMessage a)   = a
