-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Internal.Queue.Mailbox
-- Copyright   :  (c) Tim Watson 2012 - 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
--
-- A re-usable mailbox, implemented as a dequeue with support for various
-- order preserving and/or destructive message retrieval operations.
-----------------------------------------------------------------------------

module Control.Distributed.Process.Platform.Internal.Queue.Mailbox
  ( Mailbox
  , empty
  , isEmpty
  , singleton
  , enqueue
  , enqueueLast
  , enqueueFirst
  , dequeue
  , push
  , pop
  , peekFirst
  , peekLast
  , select
  , dropUntil
  ) where

import Control.Distributed.Process
  ( handleMessage
  , Message
  )
import Control.Distributed.Process.Serializable (Serializable)
import Data.Sequence
  ( Seq
  , ViewR(..)
  , ViewL(..)
  , (<|)
  , (|>)
  , (><)
  , viewr
  , viewl
  )
import qualified Data.Sequence as Seq

type Mailbox = Seq Message

select :: (Monad m, Serializable a, Serializable b)
       => Mailbox
       -> (a -> m b)
       -> m (Mailbox, Maybe b)
select m = selectAux (viewr m, empty)
  where
    selectAux :: (Monad m, Serializable a, Serializable b)
              => (ViewR Message, Mailbox)
              -> (a -> m b)
              -> m (Mailbox, Maybe b)
    selectAux (EmptyR,  acc) _ = return (acc, Nothing)
    selectAux (s' :> a, acc) p = do
      r <- handleMessage a p
      case r of
        Nothing -> selectAux (viewr s', enqueueFirst acc a) p
        Just r' -> return (s' >< acc, Just r')

dropUntil :: (Monad m, Serializable a, Serializable b)
          => Mailbox
          -> (a -> m b)
          -> m (Either Mailbox (Mailbox, b))
dropUntil m = dropAux (viewr m)
  where
    dropAux :: (Monad m, Serializable a, Serializable b)
            => ViewR Message
            -> (a -> m b)
            -> m (Either Mailbox (Mailbox, b))
    dropAux EmptyR    _ = return $ Left m
    dropAux (s' :> a) p = do
      r <- handleMessage a p
      case r of
        Nothing -> dropAux (viewr s') p
        Just r' -> return $ Right (s', r')

{-# INLINE empty #-}
empty :: Mailbox
empty = Seq.empty

{-# INLINE isEmpty #-}
isEmpty :: Mailbox -> Bool
isEmpty = Seq.null

{-# INLINE singleton #-}
singleton :: Message -> Mailbox
singleton = Seq.singleton

{-# INLINE enqueue #-}
enqueue :: Mailbox -> Message -> Mailbox
enqueue = flip (<|)

{-# INLINE enqueueLast #-}
enqueueLast :: Mailbox -> Message -> Mailbox
enqueueLast = enqueue

{-# INLINE enqueueFirst #-}
enqueueFirst :: Mailbox -> Message -> Mailbox
enqueueFirst = (|>)

{-# INLINE push #-}
push :: Mailbox -> Message -> Mailbox
push = enqueueFirst

{-# INLINE dequeue #-}
dequeue :: Mailbox -> Maybe (Message, Mailbox)
dequeue s = maybe Nothing (\(s' :> a) -> Just (a, s')) $ getR s

{-# INLINE pop #-}
pop :: Mailbox -> Maybe (Message, Mailbox)
pop s = maybe Nothing (\(a :< s') -> Just (a, s')) $ getL s

{-# INLINE peekFirst #-}
peekFirst :: Mailbox -> Maybe Message
peekFirst = maybe Nothing (\(_ :> a) -> Just a) . getR

{-# INLINE peekLast #-}
peekLast :: Mailbox -> Maybe Message
peekLast = maybe Nothing (\(a :< _) -> Just a) . getL

getR :: Mailbox -> Maybe (ViewR Message)
getR s =
  case (viewr s) of
    EmptyR -> Nothing
    a      -> Just a

getL :: Mailbox -> Maybe (ViewL Message)
getL s =
  case (viewl s) of
    EmptyL -> Nothing
    a      -> Just a

