{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Task.Pool.WorkerPool
-- Copyright   :  (c) Tim Watson 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- [Process Pool (Backend)]
--
-- This module implements a pool of worker processes, implemented with the
-- 'BackingPool' API from "Control.Distributed.Process.Platform.Task.Pool'.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Task.Pool.WorkerPool where

import Control.DeepSeq (NFData)
import Control.Distributed.Process
  ( Process
  , MonitorRef
  , ProcessMonitorNotification(..)
  , ProcessId
  , SendPort
  , liftIO
  , spawnLocal
  , handleMessageIf
  , send
  , sendChan
  , unsafeSendChan
  , getSelfPid
  , receiveChan
  , exit
  , kill
  , match
  , Match
  , Message
  , link
  , monitor
  , unmonitor
  , finally
  , unwrapMessage
  )
import Control.Distributed.Process.Platform.Internal.IdentityPool
  ( IDPool
  , newIdPool
  , takeId
  )
import Control.Distributed.Process.Platform.Internal.Primitives
  ( spawnMonitorLocal
  , awaitExit
  )
import Control.Distributed.Process.Platform.Internal.Types
  ( Resolvable(..)
  , Shutdown(..)
  , Linkable(..)
  , NFSerializable(..)
  , ExitReason(..)
  )
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Platform.Task.Pool
import Data.Binary
import Data.Hashable
import Data.Typeable (Typeable)
import GHC.Generics

type PoolSize        = Integer
type InitialPoolSize = Integer

type RefType = (ProcessId, MonitorRef)

-- TODO: deduplicate these two definitions

worker :: Process () -> Resource RefType
worker w =
  Resource {
      create   = spawnMonitorLocal w
    , destroy  = \(p, _) -> exit p Shutdown >> awaitExit p
    , checkRef = (\m (_, r) -> do
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead))
    , accept   = \t r -> sendChan (ticketChan t) r
    }

spawnWorker :: Process RefType -> Resource RefType
spawnWorker w =
  Resource {
      create   = w
    , destroy  = \(p, _) -> exit p Shutdown >> awaitExit p
    , checkRef = (\m (_, r) -> do
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead))
    , accept   = \t r -> sendChan (ticketChan t) r
    }

data WPState = WPState { sizeLimit :: PoolSize
--                       , acquired  :: Integer
--                       , released  :: Integer
                         -- TODO: keep track of things and report via getStats
                       }

runWorkerPool :: Resource RefType
              -> PoolSize
              -> InitPolicy
              -> RotationPolicy WPState RefType
              -> ReclamationStrategy
              -> Process ()
runWorkerPool rt sz ip rp rs = runPool rt poolDef ip rp rs (initState sz)

initState :: PoolSize -> WPState
initState sz = WPState sz

poolDef :: PoolBackend WPState RefType
poolDef = PoolBackend { acquire  = apiAcquire
                      , release  = apiRelease
                      , dispose  = apiDispose
                      , setup    = apiSetup
                      , teardown = apiTeardown
                      , infoCall = apiInfoCall
                      , getStats = apiGetStats
                      }

apiAcquire  :: Pool WPState RefType (Take (ProcessId, MonitorRef))
apiAcquire = do
  pol <- getInitPolicy
  case pol of
    OnDemand -> do sz <- getState >>= return . sizeLimit
                   (a, b) <- resourceQueueLen
                   if (toInteger $ a + b) < sz
                      then tryAcquire
                      else return Block
    OnInit   -> tryAcquire
  where
    tryAcquire = return . maybe Block Take =<< acquirePooledResource

apiRelease :: (ProcessId, MonitorRef) -> Pool WPState RefType ()
apiRelease res = do
  releasePooledResource res

apiDispose :: (ProcessId, MonitorRef) -> Pool WPState RefType ()
apiDispose r = do
  rType <- getResourceType
  res <- lift $ destroy rType r
  removePooledResource r

apiSetup :: Pool WPState RefType ()
apiSetup = do
  pol <- getInitPolicy
  case pol of
    OnDemand -> return ()
    OnInit   -> startResources 0
  where
    startResources :: PoolSize -> Pool WPState RefType ()
    startResources cnt = do
      st <- getState :: Pool WPState RefType WPState
      if cnt <= (sizeLimit st)
         then do rType <- getResourceType
                 res <- lift $ create rType
                 addPooledResource res
                 startResources (cnt + 1)
         else return ()

apiTeardown :: ExitReason -> Pool WPState RefType ()
apiTeardown = const $ foldResources (const apiDispose) ()

apiInfoCall :: Message -> Pool WPState RefType ()
apiInfoCall msg = do
  -- If this is a monitor signal and pertains to one of our resources,
  -- we need to permanently remove it so its ref doesn't leak to
  -- some unfortunate consumer (who'll expect the pid to be valid).
  mSig <- lift $ checkMonitor msg
  case mSig of
    Nothing                                 -> return ()
    Just (ProcessMonitorNotification r p _) -> removePooledResource (p, r)

  where
    checkMonitor :: Message -> Process (Maybe ProcessMonitorNotification)
    checkMonitor = unwrapMessage

apiGetStats :: Pool WPState RefType [PoolStatsInfo]
apiGetStats = do
  st <- getState
  return [PoolStatsCounter "sizeLimit" $ sizeLimit st]

