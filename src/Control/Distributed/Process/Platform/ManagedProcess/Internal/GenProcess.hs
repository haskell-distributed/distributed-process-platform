{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}

-- | This is the @Process@ implementation of a /managed process/ server
module Control.Distributed.Process.Platform.ManagedProcess.Internal.GenProcess
  (recvLoop) where

import Control.Concurrent (threadDelay)
import Control.Distributed.Process hiding (call, Message)
import qualified Control.Distributed.Process as P (Message)
import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Internal.Types
  ( ExitReason(..)
  , Shutdown(..)
  )
import Control.Distributed.Process.Platform.Time
import Prelude hiding (init)

--------------------------------------------------------------------------------
-- Internal Process Implementation                                            --
--------------------------------------------------------------------------------

recvLoop :: ProcessDefinition s -> s -> Delay -> Process ExitReason
recvLoop pDef pState recvDelay =
  let p             = unhandledMessagePolicy pDef
      handleTimeout = timeoutHandler pDef
      handleStop    = shutdownHandler pDef
      shutdown'     = matchDispatch p pState shutdownHandler'
      matchers      = map (matchDispatch p pState) (apiHandlers pDef)
      ex'           = (trapExit:(exitHandlers pDef))
      ms' = (shutdown':matchers) ++ matchAux p pState (infoHandlers pDef)
  in do
    ac <- catchesExit (processReceive ms' handleTimeout pState recvDelay)
                      (map (\d' -> (dispatchExit d') pState) ex')
    case ac of
        (ProcessContinue s')     -> recvLoop pDef s' recvDelay
        (ProcessTimeout t' s')   -> recvLoop pDef s' (Delay t')
        (ProcessHibernate d' s') -> block d' >> recvLoop pDef s' recvDelay
        (ProcessStop r) -> handleStop pState r >> return (r :: ExitReason)

-- an explicit 'cast' giving 'Shutdown' will stop the server gracefully
shutdownHandler' :: Dispatcher s
shutdownHandler' = handleCast (\_ Shutdown -> stop $ ExitNormal)

-- @(ProcessExitException from Shutdown)@ will stop the server gracefully
trapExit :: ExitSignalDispatcher s
trapExit = handleExit (\_ _ (reason :: ExitReason) -> stop reason)

block :: TimeInterval -> Process ()
block i = liftIO $ threadDelay (asTimeout i)

applyPolicy :: UnhandledMessagePolicy
            -> s
            -> P.Message
            -> Process (ProcessAction s)
applyPolicy p s m =
  case p of
    Terminate      -> stop $ ExitOther "UnhandledInput"
    DeadLetter pid -> forward m pid >> continue s
    Drop           -> continue s

matchAux :: UnhandledMessagePolicy
         -> s
         -> [DeferredDispatcher s]
         -> [Match (ProcessAction s)]
matchAux p ps ds = [matchAny (auxHandler (applyPolicy p ps) ps ds)]

auxHandler :: (P.Message -> Process (ProcessAction s))
           -> s
           -> [DeferredDispatcher s]
           -> P.Message
           -> Process (ProcessAction s)
auxHandler policy _  [] msg = policy msg
auxHandler policy st (d:ds :: [DeferredDispatcher s]) msg
  | length ds > 0  = let dh = dispatchInfo d in do
    -- NB: we *do not* want to terminate/dead-letter messages until
    -- we've exhausted all the possible info handlers
      m <- dh st msg
      case m of
        Nothing  -> auxHandler policy st ds msg
        Just act -> return act
      -- but here we *do* let the policy kick in
  | otherwise = let dh = dispatchInfo d in do
      m <- dh st msg
      case m of
        Nothing  -> policy msg
        Just act -> return act

processReceive :: [Match (ProcessAction s)]
               -> TimeoutHandler s
               -> s
               -> Delay
               -> Process (ProcessAction s)
processReceive ms handleTimeout st d = do
    next <- recv ms d
    case next of
        Nothing -> handleTimeout st d
        Just pa -> return pa
  where
    recv :: [Match (ProcessAction s)]
         -> Delay
         -> Process (Maybe (ProcessAction s))
    recv matches d' =
        case d' of
            Infinity -> receiveWait matches >>= return . Just
            Delay t' -> receiveTimeout (asTimeout t') matches

