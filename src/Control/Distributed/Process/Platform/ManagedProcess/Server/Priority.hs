{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}

-- | TBC
module Control.Distributed.Process.Platform.ManagedProcess.Server.Priority
  ( prioritiseCall
  , prioritiseCast
  , prioritiseInfo
  ) where

import Control.Distributed.Process hiding (call, Message)
import qualified Control.Distributed.Process as P (Message)
import Control.Distributed.Process.Platform.ManagedProcess.Internal.Types
import Control.Distributed.Process.Serializable
import Prelude hiding (init)

{-
data MessageView a =
    MatchCall a
  | MatchCast a
  | MatchInfo a
  | Unmatched

view :: (Serializable a) => Message a -> MessageView a
view (CastMessage (p :: b))   = MatchCast p
view (CallMessage (p :: b) _) = MatchCall p
-}

prioritiseCall :: forall s a . (Serializable a)
               => (s -> a -> Int)
               -> Priority s
prioritiseCall h = PrioritiseCall (\s -> unCall $ h s)
  where
    unCall :: (a -> Int) -> P.Message -> Process (Maybe (Int, P.Message))
    unCall h' m = unwrapMessage m >>= return . matchPrioritise m h'

    matchPrioritise :: P.Message
                    -> (a -> Int)
                    -> Maybe (Message a)
                    -> Maybe (Int, P.Message)
    matchPrioritise msg p msgIn
      | (Just a@(CallMessage m _)) <- msgIn
      , True  <- isEncoded msg = Just (p m, wrapMessage a)
      | (Just   (CallMessage m _)) <- msgIn
      , False <- isEncoded msg = Just (p m, msg)
      | otherwise              = Nothing

prioritiseCast :: forall s a . (Serializable a)
               => (s -> a -> Int)
               -> Priority s
prioritiseCast h = PrioritiseCast (\s -> unCast $ h s)
  where
    unCast :: (a -> Int) -> P.Message -> Process (Maybe (Int, P.Message))
    unCast h' m = unwrapMessage m >>= return . matchPrioritise m h'

    matchPrioritise :: P.Message
                    -> (a -> Int)
                    -> Maybe (Message a)
                    -> Maybe (Int, P.Message)
    matchPrioritise msg p msgIn
      | (Just a@(CastMessage m)) <- msgIn
      , True  <- isEncoded msg = Just (p m, wrapMessage a)
      | (Just   (CastMessage m)) <- msgIn
      , False <- isEncoded msg = Just (p m, msg)
      | otherwise              = Nothing

prioritiseInfo :: forall s a . (Serializable a)
               => (s -> a -> Int)
               -> Priority s
prioritiseInfo h = PrioritiseInfo (\s -> unMsg $ h s)
  where
    unMsg :: (a -> Int) -> P.Message -> Process (Maybe (Int, P.Message))
    unMsg h' m = unwrapMessage m >>= return . matchPrioritise m h'

    matchPrioritise :: P.Message
                    -> (a -> Int)
                    -> Maybe a
                    -> Maybe (Int, P.Message)
    matchPrioritise msg p msgIn
      | (Just m') <- msgIn
      , True <- isEncoded msg  = Just (p m', wrapMessage m')
      | (Just m') <- msgIn
      , False <- isEncoded msg = Just (p m', msg)
      | otherwise              = Nothing

