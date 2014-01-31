
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE UndecidableInstances #-}
module Control.Distributed.Process.Platform.Internal.IdentityPool where

import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TVar
import Control.Distributed.Process (Process)
import Control.Distributed.Process.Platform.Internal.Queue.SeqQ (SeqQ)
import qualified Control.Distributed.Process.Platform.Internal.Queue.SeqQ as Queue
import Control.Monad.IO.Class (MonadIO, liftIO)

data IDPool a =
  IDPool { reserved :: !a
         , returns  :: !(SeqQ a)
         }

class IdentityPool m where
  new     :: (Integral i, Monad m) => i -> m (IDPool i)
  take    :: (Integral i, Monad m) => IDPool i -> m (i, IDPool i)
  release :: (Integral i, Monad m) => i -> IDPool i -> m (IDPool i)

instance (Monad m) => IdentityPool m where
  new     i   = return $ newIdPool i
  take    p   = return $ takeId p
  release i p = return $ releaseId i p

{-
newtype SharedPool i = SharedPool { idPool :: TVar (IDPool i) }

class SharedIdentityPool m where
  newSharedPool   :: (Integral i, MonadIO m) => i -> m (SharedPool i)
  takeSharedId    :: (Integral i, MonadIO m) => SharedPool i -> m i
  releaseSharedId :: (Integral i, MonadIO m) => i -> SharedPool i -> m ()

instance SharedIdentityPool IO where
  newSharedPool i = liftIO $ atomically $ do
    p <- newTVar (newPool i)
    return $ SharedPool p
  takeSharedId p = liftIO $ atomically $ do
    let pool = (idPool p)
    ids <- readTVar pool
    let (i, p') = takeId ids
    writeTVar pool p'
    return i
  releaseSharedId i p = do
    let pool = (idPool p)
    p' <- readTVar pool
    writeTVar pool (releaseId i p')
-}

newIdPool :: (Integral i) => i -> IDPool i
newIdPool i = IDPool i Queue.empty

takeId :: (Integral i) => IDPool i -> (i, IDPool i)
takeId p@IDPool{..} = do
  case Queue.dequeue returns of
    Nothing     -> let i' = reserved + 1 in (i', p { reserved = i' })
    Just (n, q) -> (n, p { returns = q })

releaseId :: (Integral i) => i -> IDPool i -> IDPool i
releaseId i p@IDPool{..} = p { returns = Queue.enqueue returns i }

