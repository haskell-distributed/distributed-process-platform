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
-- The Client Portion of the /Managed Process/ API.
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.ManagedProcess.Client
  ( -- * API for client interactions with the process
    shutdown
  , call
  , safeCall
  , tryCall
  , callTimeout
  , callAsync
  , cast
  ) where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Async hiding (check)
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Primitives
import Control.Distributed.Process.Platform.Internal.Types
  ( Recipient(..)
  , ExitReason(..)
  , Shutdown(..)
  )
import Control.Distributed.Process.Platform.Time
import Data.Maybe (fromJust)

import Prelude hiding (init)

-- | Send a signal instructing the process to terminate. The /receive loop/ which
-- manages the process mailbox will prioritise @Shutdown@ signals higher than
-- any other incoming messages, but the server might be busy (i.e., still in the
-- process of excuting a handler) at the time of sending however, so the caller
-- should not make any assumptions about the timeliness with which the shutdown
-- signal will be handled. If responsiveness is important, a better approach
-- might be to send an /exit signal/ with 'Shutdown' as the reason. An exit
-- signal will interrupt any operation currently underway and force the running
-- process to clean up and terminate.
shutdown :: ProcessId -> Process ()
shutdown pid = cast pid Shutdown

-- | Make a synchronous call - will block until a reply is received.
-- The calling process will exit with 'ExitReason' if the calls fails.
call :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process b
call sid msg = initCall sid msg >>= waitResponse Nothing >>= decodeResult
  where decodeResult (Just (Right r))  = return r
        decodeResult (Just (Left err)) = die err
        decodeResult Nothing {- the impossible happened -} = terminate

-- | Safe version of 'call' that returns information about the error
-- if the operation fails. If an error occurs then the explanation will be
-- will be stashed away as @(ExitOther String)@.
safeCall :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process (Either ExitReason b)
safeCall s m = initCall s m >>= waitResponse Nothing >>= return . fromJust

-- | Version of 'safeCall' that returns 'Nothing' if the operation fails. If
-- you need information about *why* a call has failed then you should use
-- 'safeCall' or combine @catchExit@ and @call@ instead.
tryCall :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process (Maybe b)
tryCall s m = initCall s m >>= waitResponse Nothing >>= decodeResult
  where decodeResult (Just (Right r)) = return $ Just r
        decodeResult _                = return Nothing

-- | Make a synchronous call, but timeout and return @Nothing@ if a reply
-- is not received within the specified time interval.
--
-- If the result of the call is a failure (or the call was cancelled) then
-- the calling process will exit, with the 'ExitReason' given as the reason.
--
callTimeout :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> TimeInterval -> Process (Maybe b)
callTimeout s m d = initCall s m >>= waitResponse (Just d) >>= decodeResult
  where decodeResult :: (Serializable b)
               => Maybe (Either ExitReason b)
               -> Process (Maybe b)
        decodeResult Nothing               = return Nothing
        decodeResult (Just (Right result)) = return $ Just result
        decodeResult (Just (Left reason))  = die reason

-- | Invokes 'call' /out of band/, and returns an "async handle."
--
-- See "Control.Distributed.Process.Platform.Async".
--
callAsync :: forall s a b . (Addressable s, Serializable a, Serializable b)
          => s -> a -> Process (Async b)
callAsync server msg = async $ call server msg

-- | Sends a /cast/ message to the server identified by 'ServerId'. The server
-- will not send a response. Like Cloud Haskell's 'send' primitive, cast is
-- fully asynchronous and /never fails/ - therefore 'cast'ing to a non-existent
-- (e.g., dead) server process will not generate an error.
--
cast :: forall a m . (Addressable a, Serializable m)
                 => a -> m -> Process ()
cast server msg = sendTo server (CastMessage msg)

-- note [rpc calls]
-- One problem with using plain expect/receive primitives to perform a
-- synchronous (round trip) call is that a reply matching the expected type
-- could come from anywhere! The Call.hs module uses a unique integer tag to
-- distinguish between inputs but this is easy to forge, and forces all callers
-- to maintain a tag pool, which is quite onerous.
--
-- Here, we use a private (internal) tag based on a 'MonitorRef', which is
-- guaranteed to be unique per calling process (in the absence of mallicious
-- peers). This is handled throughout the roundtrip, such that the reply will
-- either contain the CallId (i.e., the ame 'MonitorRef' with which we're
-- tracking the server process) or we'll see the server die.
--
-- Of course, the downside to all this is that the monitoring and receiving
-- clutters up your mailbox, and if your mailbox is extremely full, could
-- incur delays in delivery. The callAsync function provides a neat
-- work-around for that, relying on the insulation provided by Async.

initCall :: forall s a . (Addressable s, Serializable a)
         => s -> a -> Process CallRef
initCall sid msg = do
  (Just pid) <- resolve sid
  mRef <- monitor pid
  self <- getSelfPid
  let cRef = makeRef (Pid self) mRef in do
    sendTo sid $ CallMessage msg cRef
    return cRef

waitResponse :: forall b. (Serializable b)
             => Maybe TimeInterval
             -> CallRef
             -> Process (Maybe (Either ExitReason b))
waitResponse mTimeout cRef =
  let (_, mRef) = unCaller cRef
      matchers  = [ matchIf (\((CallResponse _ ref) :: CallResponse b) -> ref == mRef)
                            (\((CallResponse m _) :: CallResponse b) -> return (Right m))
                  , matchIf (\(ProcessMonitorNotification ref _ _) -> ref == mRef)
                      (\(ProcessMonitorNotification _ _ r) -> return (Left (err r)))
                  ]
      err r     = ExitOther $ show r in
    case mTimeout of
      (Just ti) -> receiveTimeout (asTimeout ti) matchers
      Nothing   -> receiveWait matchers >>= return . Just

