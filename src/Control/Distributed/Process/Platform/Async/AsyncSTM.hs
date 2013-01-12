{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE TemplateHaskell           #-}
{-# LANGUAGE StandaloneDeriving        #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Platform.Async.AsyncSTM
-- Copyright   :  (c) Tim Watson 2012
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a set of operations for spawning Process operations
-- and waiting for their results.  It is a thin layer over the basic
-- concurrency operations provided by "Control.Distributed.Process".
--
-- The difference between 'Control.Distributed.Platform.Async.AsyncSTM' and
-- 'Control.Distributed.Platform.Async.AsyncChan' is that handles of the
-- former (i.e., returned by /this/ module) can be used by processes other
-- than the caller of 'async', but are not 'Serializable'.
--
-- Like 'Control.Distributed.Platform.Async.AsyncChan', workers can be
-- started on a local or remote node.
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Async.AsyncSTM
  ( -- types/data
    AsyncRef
  , AsyncTask
  , AsyncSTM(_asyncWorker)
  -- functions for starting/spawning
  , async
  , asyncLinked
  -- and stopping/killing
  , cancel
  , cancelWait
  , cancelWith
  , cancelKill
  -- functions to query an async-result
  , poll
  , check
  , wait
  , waitAny
  , waitAnyTimeout
  , waitTimeout
  , waitCheckTimeout
  -- STM versions
  , pollSTM
  , waitTimeoutSTM
  ) where


import Control.Applicative
import Control.Concurrent.STM hiding (check)
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Async hiding (asyncDo)
import Control.Distributed.Process.Platform.Internal.Types
  ( CancelWait(..)
  , Channel
  )
import Control.Distributed.Process.Platform.Time
  ( asTimeout
  , TimeInterval
  )
import Control.Monad
import Data.Maybe
  ( fromMaybe
  )
import Prelude hiding (catch)
import System.Timeout (timeout)

--------------------------------------------------------------------------------
-- Cloud Haskell STM Async Process API                                        --
--------------------------------------------------------------------------------

-- | An handle for an asynchronous action spawned by 'async'.
-- Asynchronous operations are run in a separate process, and
-- operations are provided for waiting for asynchronous actions to
-- complete and obtaining their results (see e.g. 'wait').
--
-- Handles of this type cannot cross remote boundaries, nor are they
-- @Serializable@.
data AsyncSTM a = AsyncSTM {
    _asyncWorker  :: AsyncRef
  , _asyncMonitor :: AsyncRef
  , _asyncWait    :: STM (AsyncResult a)
  }

instance Eq (AsyncSTM a) where
  AsyncSTM a b _ == AsyncSTM c d _  =  a == c && b == d

-- | Spawns an asynchronous action in a new process.
--
-- There is currently a contract for async workers which is that they should
-- exit normally (i.e., they should not call the @exit selfPid reason@ nor
-- @terminate@ primitives), otherwise the 'AsyncResult' will end up being
-- @AsyncFailed DiedException@ instead of containing the result.
--
async :: (Serializable a) => AsyncTask a -> Process (AsyncSTM a)
async = asyncDo False

-- | This is a useful variant of 'async' that ensures an @AsyncChan@ is
-- never left running unintentionally. We ensure that if the caller's process
-- exits, that the worker is killed. Because an @AsyncChan@ can only be used
-- by the initial caller's process, if that process dies then the result
-- (if any) is discarded.
--
asyncLinked :: (Serializable a) => AsyncTask a -> Process (AsyncSTM a)
asyncLinked = asyncDo True

asyncDo :: (Serializable a) => Bool -> AsyncTask a -> Process (AsyncSTM a)
asyncDo shouldLink (AsyncRemoteTask d n c) =
  let proc = call d n c in asyncDo shouldLink AsyncTask { asyncTask = proc }
asyncDo shouldLink (AsyncTask proc) = do
    root <- getSelfPid
    result <- liftIO $ newEmptyTMVarIO
    
    -- listener/response proxy
    mPid <- spawnLocal $ do
        wPid <- spawnLocal $ do
            () <- expect
            r <- proc
            void $ liftIO $ atomically $ putTMVar result (AsyncDone r)

        send root wPid   -- let the parent process know the worker pid

        wref <- monitor wPid
        rref <- case shouldLink of
                    True  -> monitor root >>= return . Just
                    False -> return Nothing
        finally (pollUntilExit wPid result)
                (unmonitor wref >>
                    return (maybe (return ()) unmonitor rref))

    workerPid <- expect
    send workerPid ()

    return AsyncSTM {
          _asyncWorker  = workerPid
        , _asyncMonitor = mPid
        , _asyncWait    = (readTMVar result)
        }

  where
    pollUntilExit :: (Serializable a)
                  => ProcessId
                  -> TMVar (AsyncResult a)
                  -> Process ()
    pollUntilExit wpid result' = do
      r <- receiveWait [
          match (\c@(CancelWait) -> kill wpid "cancel" >> return (Left c))
        , match (\(ProcessMonitorNotification _ pid' r) ->
                  return (Right (pid', r)))
        ]
      case r of
          Left CancelWait
            -> liftIO $ atomically $ putTMVar result' AsyncCancelled
          Right (fpid, d)
            | fpid == wpid
              -> case d of
                     DiedNormal -> return ()
                     _          -> liftIO $ atomically $ putTMVar result' (AsyncFailed d)
            | otherwise -> kill wpid "linkFailed"

-- | Check whether an 'AsyncSTM' has completed yet. The status of the
-- action is encoded in the returned 'AsyncResult'. If the action has not
-- completed, the result will be 'AsyncPending', or one of the other
-- constructors otherwise. This function does not block waiting for the result.
-- Use 'wait' or 'waitTimeout' if you need blocking/waiting semantics.
-- See 'Async'.
poll :: (Serializable a) => AsyncSTM a -> Process (AsyncResult a)
poll hAsync = do
  r <- liftIO $ atomically $ pollSTM hAsync
  return $ fromMaybe (AsyncPending) r

-- | Like 'poll' but returns 'Nothing' if @(poll hAsync) == AsyncPending@.
-- See 'poll'.
check :: (Serializable a) => AsyncSTM a -> Process (Maybe (AsyncResult a))
check hAsync = poll hAsync >>= \r -> case r of
  AsyncPending -> return Nothing
  ar           -> return (Just ar)

-- | Wait for an asynchronous operation to complete or timeout. This variant
-- returns the 'AsyncResult' itself, which will be 'AsyncPending' if the
-- result has not been made available, otherwise one of the other constructors.
waitCheckTimeout :: (Serializable a) =>
                    TimeInterval -> AsyncSTM a -> Process (AsyncResult a)
waitCheckTimeout t hAsync =
  waitTimeout t hAsync >>= return . fromMaybe (AsyncPending)

-- | Wait for an asynchronous action to complete, and return its
-- value. The result (which can include failure and/or cancellation) is
-- encoded by the 'AsyncResult' type.
--
-- > wait = liftIO . atomically . waitSTM
--
{-# INLINE wait #-}
wait :: AsyncSTM a -> Process (AsyncResult a)
wait = liftIO . atomically . waitSTM

-- | Wait for an asynchronous operation to complete or timeout. Returns
-- @Nothing@ if the 'AsyncResult' does not change from @AsyncPending@ within
-- the specified delay, otherwise @Just asyncResult@ is returned. If you want
-- to wait/block on the 'AsyncResult' without the indirection of @Maybe@ then
-- consider using 'wait' or 'waitCheckTimeout' instead.
waitTimeout :: (Serializable a) =>
               TimeInterval -> AsyncSTM a -> Process (Maybe (AsyncResult a))
waitTimeout t hAsync = do
  -- this is not the most efficient thing to do, but it's the most erlang-ish
  -- we might be better off with something more like this though:
  -- 
  (sp, rp) <- newChan :: (Serializable a) => Process (Channel (AsyncResult a))
  pid <- spawnLocal $ wait hAsync >>= sendChan sp
  receiveChanTimeout (asTimeout t) rp `finally` kill pid "timeout"

-- | As 'waitTimeout' but uses STM directly, which might be more efficient.
waitTimeoutSTM :: (Serializable a)
                 => TimeInterval
                 -> AsyncSTM a
                 -> Process (Maybe (AsyncResult a))
waitTimeoutSTM t hAsync = 
  let t' = (asTimeout t)
  in liftIO $ timeout t' $ atomically $ waitSTM hAsync

-- | Wait for any of the supplied @AsyncChans@s to complete. If multiple
-- 'Async's complete, then the value returned corresponds to the first
-- completed 'Async' in the list.
--
-- NB: Unlike @AsyncChan@, 'AsyncSTM' does not discard its 'AsyncResult' once
-- read, therefore the semantics of this function are different to the
-- former.
-- 
waitAny :: (Serializable a)
        => [AsyncSTM a]
        -> Process (AsyncResult a)
waitAny asyncs = do
  r <- liftIO $ waitAnySTM asyncs
  return $ snd r

-- | Like 'waitAny' but times out after the specified delay.
waitAnyTimeout :: (Serializable a)
               => TimeInterval
               -> [AsyncSTM a]
               -> Process (Maybe (AsyncResult a))
waitAnyTimeout delay asyncs =
  let t' = asTimeout delay
  in liftIO $ timeout t' $ do 
    r <- waitAnySTM asyncs
    return $ snd r

-- | Cancel an asynchronous operation. Cancellation is asynchronous in nature.
-- To wait for cancellation to complete, use 'cancelWait' instead. The notes
-- about the asynchronous nature of 'cancelWait' apply here also.
--
-- See 'Control.Distributed.Process'
cancel :: AsyncSTM a -> Process ()
cancel (AsyncSTM _ g _) = send g CancelWait

-- | Cancel an asynchronous operation and wait for the cancellation to complete.
-- Because of the asynchronous nature of message passing, the instruction to
-- cancel will race with the asynchronous worker, so it is /entirely possible/
-- that the 'AsyncResult' returned will not necessarily be 'AsyncCancelled'. For
-- example, the worker may complete its task after this function is called, but
-- before the cancellation instruction is acted upon.
--
-- If you wish to stop an asychronous operation /immediately/ (with caveats)
-- then consider using 'cancelWith' or 'cancelKill' instead.
--
cancelWait :: (Serializable a) => AsyncSTM a -> Process (AsyncResult a)
cancelWait hAsync = cancel hAsync >> wait hAsync

-- | Cancel an asynchronous operation immediately.
-- This operation is performed by sending an /exit signal/ to the asynchronous
-- worker, which leads to the following semantics:
--
-- 1. if the worker already completed, this function has no effect
-- 2. the worker might complete after this call, but before the signal arrives
-- 3. the worker might ignore the exit signal using @catchExit@
--
-- In case of (3), this function has no effect. You should use 'cancel'
-- if you need to guarantee that the asynchronous task is unable to ignore
-- the cancellation instruction.
--
-- You should also consider that when sending exit signals to a process, the
-- definition of 'immediately' is somewhat vague and a scheduler might take
-- time to handle the request, which can lead to situations similar to (1) as
-- listed above, if the scheduler to which the calling process' thread is bound
-- decides to GC whilst another scheduler on which the worker is running is able
-- to continue.
--
-- See 'Control.Distributed.Process.exit'
cancelWith :: (Serializable b) => b -> AsyncSTM a -> Process ()
cancelWith reason = (flip exit) reason . _asyncWorker

-- | Like 'cancelWith' but sends a @kill@ instruction instead of an exit.
--
-- See 'Control.Distributed.Process.kill'
cancelKill :: String -> AsyncSTM a -> Process ()
cancelKill reason = (flip kill) reason . _asyncWorker

--------------------------------------------------------------------------------
-- STM Specific API                                                           --
--------------------------------------------------------------------------------

-- | STM version of 'waitAny'.
waitAnySTM :: [AsyncSTM a] -> IO (AsyncSTM a, AsyncResult a)
waitAnySTM asyncs =
  atomically $
    foldr orElse retry $
      map (\a -> do r <- waitSTM a; return (a, r)) asyncs

-- | A version of 'wait' that can be used inside an STM transaction.
--
waitSTM :: AsyncSTM a -> STM (AsyncResult a)
waitSTM (AsyncSTM _ _ w) = w

-- | A version of 'poll' that can be used inside an STM transaction.
--
{-# INLINE pollSTM #-}
pollSTM :: AsyncSTM a -> STM (Maybe (AsyncResult a))
pollSTM (AsyncSTM _ _ w) = (Just <$> w) `orElse` return Nothing
