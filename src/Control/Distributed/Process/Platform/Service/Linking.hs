{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Service.Linking
-- Copyright   :  (c) Tim Watson 2013 - 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- This module provides a set of alternative /link/ implementations, backed by
-- a /distributed-process Management Agent/. Once the 'linkManager' agent is
-- started, the APIs provided here will become operable for the local node.
--
-- These APIs provide a kind of /legacy/ linking support, in that they work in
-- the same way as Erlang's link primitives, providing links that only terminate
-- when the linked process dies abnormally and where links are bi-directional.
--
-- These primitives are non-standard, are not covered by Cloud Haskell's formal
-- semantics and are provided only as an example of how to implement alternate
-- semantics using the rich Mx (Management) API in distributed-process.
--
-- Note the no ordering guarantees are made with respect to link failures raised
-- by this module, versus messages being delivered between the linked processes.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Service.Linking
  ( linkManager
  , linkManagerAgentId
  , link
  ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process hiding (link)
import Control.Distributed.Process.Management
  ( MxEvent(MxProcessDied)
  , MxAgentId(..)
  , mxAgent
  , mxSink
  , mxReady
  , liftMX
  , mxGetLocal
  , mxSetLocal
  , mxUpdateLocal
  )
import Control.Distributed.Process.Platform (ExitReason(ExitOther))
import Data.Binary
import qualified Data.Foldable as Foldable
import Data.Sequence
  ( Seq
  , (<|)
  )
import qualified Data.Sequence as Seq
import Data.Typeable (Typeable)
import GHC.Generics

type From   = ProcessId
type To     = ProcessId

type Sender = SendPort BiDirectionalLinkOk

data BiDirectionalLink = BiDirectionalLink From To Sender
  deriving (Typeable, Generic)
instance Binary BiDirectionalLink where
instance NFData BiDirectionalLink where

data BiDirectionalLinkOk = BiDirectionalLinkOk
  deriving (Typeable, Generic)
instance Binary BiDirectionalLinkOk where
instance NFData BiDirectionalLinkOk where

link :: ProcessId -> Process ()
link pid = do
  self <- getSelfPid
  (sp, rp) <- newChan
  nsend linkManagerAgentName (BiDirectionalLink self pid sp)
  BiDirectionalLinkOk <- receiveChan rp
  return ()

-- | The @MxAgentId@ for the link management agent.
linkManagerAgentId :: MxAgentId
linkManagerAgentId = MxAgentId linkManagerAgentName

linkManagerAgentName :: String
linkManagerAgentName = "service.linking"

-- | Starts the link manager agent. No calls to the APIs in this
-- module will have any effect until the agent is running.
--
linkManager :: Process ProcessId
linkManager = do
  mxAgent linkManagerAgentId initState [
        (mxSink $ \(BiDirectionalLink from' to' sender) -> do
            mxUpdateLocal $ ((from', to') <|)
            liftMX $ sendChan sender BiDirectionalLinkOk
            mxReady)
      , (mxSink $ \(MxProcessDied pid reason) -> do
            case reason of
              DiedNormal -> mxReady
              -- technically, we cannot receive DiedUnknownId here,
              -- though the compiler cannot tell that's the case...
              _ -> do
                st <- mxGetLocal
                st' <- liftMX $ do Foldable.foldlM (notifyLinks pid reason) initState st
                mxSetLocal st'
                mxReady)
    ]
  where
    initState :: Seq (ProcessId, ProcessId)
    initState = Seq.empty

    notifyLinks :: ProcessId
                -> DiedReason
                -> Seq (ProcessId, ProcessId)
                -> (ProcessId, ProcessId)
                -> Process (Seq (ProcessId, ProcessId))
    notifyLinks died reason acc (a, b)
      | a == died = doNotify b died reason acc
      | b == died = doNotify a died reason acc
      | otherwise = return ((a,b) <| acc)

    doNotify :: ProcessId
             -> ProcessId
             -> DiedReason
             -> (Seq (ProcessId, ProcessId))
             -> Process (Seq (ProcessId, ProcessId))
    doNotify pid died reason st = do exit pid (died, ExitOther $ show reason)
                                     return st

