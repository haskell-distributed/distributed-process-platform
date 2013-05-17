{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE ViewPatterns               #-}

-- | TBC
module Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
  ( prioritiseCall
  , prioritiseCast
  ) where

import Control.Distributed.Process hiding (call, Message)
import qualified Control.Distributed.Process as P (Message)
import Control.Distributed.Process.Platform.ManagedProcess.Server
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Platform.Time
import Control.Distributed.Process.Serializable
import Prelude hiding (init)

data MessageView a =
    MatchCall a
  | MatchCast a
  | MatchInfo a
  | Unmatched

view :: (Serializable a) => Message a -> MessageView a
view (CastMessage (p :: b))   = MatchCast p
view (CallMessage (p :: b) _) = MatchCall p

prioritiseCall :: forall s a . (Serializable a)
               => (s -> a -> Int)
               -> Priority s
prioritiseCall h = PrioritiseCall (\s -> unCall $ h s)
  where
    unCall :: (a -> Int) -> P.Message -> Process (Maybe Int)
    unCall h' m = unwrapMessage m >>= return . matchPrioritise h'

    matchPrioritise :: (a -> Int) -> Maybe (Message a) -> Maybe Int
    matchPrioritise p (Just (CallMessage m _))  = Just (p m)
    matchPrioritise p _                         = Nothing

prioritiseCast :: forall s a . (Serializable a)
               => (s -> a -> Int)
               -> Priority s
prioritiseCast h = PrioritiseCast (\s -> unCall $ h s)
  where
    unCall :: (a -> Int) -> P.Message -> Process (Maybe Int)
    unCall h' m = unwrapMessage m >>= return . matchPrioritise h'

    matchPrioritise :: (a -> Int) -> Maybe (Message a) -> Maybe Int
    matchPrioritise p (Just (CastMessage m))  = Just (p m)
    matchPrioritise p _                       = Nothing

