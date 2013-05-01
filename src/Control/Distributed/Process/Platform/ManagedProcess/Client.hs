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
  , callAsync
  , callTimeout
  , cast
  ) where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Async hiding (check)
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Primitives
import Control.Distributed.Process.Platform.Internal.Types
  ( Recipient(..)
  , TerminateReason(..)
  , Shutdown(..)
  )
import Control.Distributed.Process.Platform.Internal.Common
import Control.Distributed.Process.Platform.Time

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
-- The calling process will exit with 'TerminateReason' if the calls fails.
call :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process b
call sid msg = callAsync sid msg >>= wait >>= unpack -- note [call using async]
  where unpack :: AsyncResult b -> Process b
        unpack (AsyncDone   r)     = return r
        unpack (AsyncFailed r)     = die $ explain "CallFailed" r
        unpack (AsyncLinkFailed r) = die $ explain "LinkFailed" r
        unpack AsyncCancelled      = die $ TerminateOther $ "Cancelled"
        unpack AsyncPending        = terminate -- as this *cannot* happen

-- | Safe version of 'call' that returns information about the error
-- if the operation fails. If an error occurs then the explanation will be
-- will be stashed away as @(TerminateOther String)@.
safeCall :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process (Either TerminateReason b)
safeCall s m = callAsync s m >>= wait >>= unpack    -- note [call using async]
  where unpack (AsyncDone   r)     = return $ Right r
        unpack (AsyncFailed r)     = return $ Left $ explain "CallFailed" r
        unpack (AsyncLinkFailed r) = return $ Left $ explain "LinkFailed" r
        unpack AsyncCancelled      = return $ Left $ TerminateOther $ "Cancelled"
        unpack AsyncPending        = return $ Left $ TerminateOther $ "Pending"

-- | Version of 'safeCall' that returns 'Nothing' if the operation fails. If
-- you need information about *why* a call has failed then you should use
-- 'safeCall' or combine @catchExit@ and @call@ instead.
tryCall :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process (Maybe b)
tryCall s m = callAsync s m >>= wait >>= unpack    -- note [call using async]
  where unpack (AsyncDone r) = return $ Just r
        unpack _             = return Nothing

-- | Make a synchronous call, but timeout and return @Nothing@ if the reply
-- is not received within the specified time interval.
--
-- If the result of the call is a failure (or the call was cancelled) then
-- the calling process will exit, with the 'AsyncResult' given as the reason.
--
callTimeout :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> TimeInterval -> Process (Maybe b)
callTimeout s m d = callAsync s m >>= waitTimeout d >>= unpack
  where unpack :: (Serializable b) => Maybe (AsyncResult b) -> Process (Maybe b)
        unpack Nothing              = return Nothing
        unpack (Just (AsyncDone r)) = return $ Just r
        unpack (Just other)         = die other

-- | Performs a synchronous 'call' to the the given server address, however the
-- call is made /out of band/ and an async handle is returned immediately. This
-- can be passed to functions in the /Async/ API in order to obtain the result.
--
-- See "Control.Distributed.Process.Platform.Async"
--
callAsync :: forall s a b . (Addressable s, Serializable a, Serializable b)
                 => s -> a -> Process (Async b)
callAsync = callAsyncUsing async

-- | As 'callAsync' but takes a function that can be used to generate an async
-- task and return an async handle to it. This can be used to switch between
-- async implementations, by e.g., using an async channel instead of the default
-- STM based handle.
--
-- See "Control.Distributed.Process.Platform.Async"
--
callAsyncUsing :: forall s a b . (Addressable s, Serializable a, Serializable b)
                  => (Process b -> Process (Async b))
                  -> s -> a -> Process (Async b)
callAsyncUsing asyncStart sid msg = do
  asyncStart $ do  -- note [call using async]
    (Just pid) <- resolve sid
    mRef <- monitor pid
    wpid <- getSelfPid
    sendTo sid (CallMessage msg (Pid wpid))
    r <- receiveWait [
            match (\((CallResponse m) :: CallResponse b) -> return (Right m))
          , matchIf (\(ProcessMonitorNotification ref _ _) -> ref == mRef)
              (\(ProcessMonitorNotification _ _ reason) -> return (Left reason))
        ]
    -- TODO: better failure API
    unmonitor mRef
    case r of
      Right m  -> return m
      Left err -> die $ TerminateOther ("ServerExit (" ++ (show err) ++ ")")

-- note [call using async]
-- One problem with using plain expect/receive primitives to perform a
-- synchronous (round trip) call is that a reply matching the expected type
-- could come from anywhere! The Call.hs module uses a unique integer tag to
-- distinguish between inputs but this is easy to forge, as is tagging the
-- response with the sender's pid.
--
-- The approach we take here is to rely on AsyncSTM (by default) to insulate us
-- from erroneous incoming messages without the need for tagging. The /handle/
-- returned uses an @STM (AsyncResult a)@ field to handle the response /and/
-- the implementation spawns a new process to perform the actual call and
-- await the reply before atomically updating the result. Whilst in theory,
-- given a hypothetical 'listAllProcesses' primitive, it might be possible for
-- malacious code to obtain the ProcessId of the worker and send a false reply,
-- the likelihood of this is small enough that it seems reasonable to assume
-- we've solved the problem without the need for tags or globally unique
-- identifiers.

-- | Sends a /cast/ message to the server identified by 'ServerId'. The server
-- will not send a response. Like Cloud Haskell's 'send' primitive, cast is
-- fully asynchronous and /never fails/ - therefore 'cast'ing to a non-existent
-- (e.g., dead) server process will not generate an error.
cast :: forall a m . (Addressable a, Serializable m)
                 => a -> m -> Process ()
cast sid msg = sendTo sid (CastMessage msg)
